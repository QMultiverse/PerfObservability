"""MX parser: ``hub.in.mx.raw`` to ``hub.pay.canonical``.

Parses the AppHdr and Document, runs structural checks (and XSD checks when
CBPR+ schemas are configured — see :class:`hub_format.mx.XsdValidator`), maps
to the canonical model and stamps T2.

This is the most expensive stage on the MX lane, which is exactly why it sits
behind its own topic and consumer group: it scales independently of the FIN
lane.
"""

from __future__ import annotations

import logging
from typing import Final

from hub_format import map_mx
from hub_format import mx as mx_format
from hub_model import proto as pb
from hub_model import topics as tp
from hub_model.envelope import STAGE_PARSED, new_envelope, now_ns
from hub_model.flows import PACS_009_COV
from hub_model.ids import InvalidIdentifier, is_valid_bic, require_uetr
from hub_model.proto import PaymentEnvelope, RawInbound
from hub_telemetry import metrics
from hub_telemetry.messaging import Record

from hub.common.config import ServiceSettings
from hub.common.processor import Outbox, PermanentError, Processor

log = logging.getLogger(__name__)

SERVICE_NAME: Final = "hub-mx-parser"


class MxParser(Processor):
    """MX in, canonical out."""

    name = SERVICE_NAME
    stage = STAGE_PARSED
    group_id = tp.CG_MX_PARSER
    input_topics = (tp.IN_MX_RAW,)

    def __init__(self, settings: ServiceSettings, events: object) -> None:
        super().__init__(settings, events)  # type: ignore[arg-type]
        # No schema directory configured means structural validation only.
        self.xsd = mx_format.XsdValidator()
        if self.xsd.available:
            log.info("XSD validation on, schemas from %s", self.xsd.schema_dir)

    async def handle(self, record: Record, out: Outbox) -> None:
        raw = RawInbound()
        try:
            raw.ParseFromString(record.value)
        except Exception as exc:
            raise PermanentError(f"undecodable RawInbound: {exc}", code="MX_BAD_ENVELOPE") from exc

        envelope = self._parse(raw)
        if self.done(envelope):
            log.debug("skipping %s: already parsed", raw.ref.uetr)
            return

        stages = self.mark(envelope)
        out.advance_to(envelope, tp.PAY_CANONICAL, stages=stages, reason="MX parsed to canonical")
        metrics.stage_latency.labels(SERVICE_NAME, "parse", "MX").observe(
            max(0.0, (envelope.timings.t2_canonical_ns - raw.received_ns) / 1e9)
        )

    def _parse(self, raw: RawInbound) -> PaymentEnvelope:
        try:
            message = mx_format.parse_document(raw.document, raw.app_hdr or None)
        except mx_format.MxParseError as exc:
            raise PermanentError(f"malformed MX: {exc}", code="MX_PARSE") from exc

        try:
            mx_format.validate_structure(message)
            self.xsd.validate(message)
        except mx_format.MxValidationError as exc:
            detail = "; ".join(exc.errors) if exc.errors else str(exc)
            raise PermanentError(f"{exc}: {detail}", code="MX_VALIDATION") from exc
        except mx_format.MxParseError as exc:
            raise PermanentError(f"malformed MX: {exc}", code="MX_PARSE") from exc

        try:
            payment = map_mx.to_payment(message)
        except (mx_format.MxValidationError, InvalidIdentifier, ValueError) as exc:
            raise PermanentError(f"MX mapping failed: {exc}", code="MX_MAPPING") from exc

        _validate(payment, raw)

        msg_type = map_mx.effective_msg_type(message)
        payment.ref.uetr = raw.ref.uetr
        payment.ref.flow = raw.ref.flow
        payment.network_ts_ns = raw.network_ts_ns
        payment.raw.topic = tp.IN_MX_RAW

        envelope = new_envelope(
            payment.ref,
            pb.MX,
            payment=payment,
            state=pb.PARSED,
            trace_level=raw.trace_level,
            run_id=raw.run_id,
        )
        envelope.ref.msg_type = msg_type
        envelope.payment.ref.msg_type = msg_type
        envelope.timings.t0_network_sent_ns = raw.network_ts_ns
        envelope.timings.t1_edge_accepted_ns = raw.received_ns
        envelope.timings.t2_canonical_ns = now_ns()

        if msg_type == PACS_009_COV:
            underlying = map_mx.underlying_payment(message)
            if underlying is not None:
                envelope.payment.cover.leg = pb.CoverLink.COVER
                envelope.payment.cover.partner_msg_type = underlying.ref.msg_type
        return envelope


def _validate(payment: pb.Payment, raw: RawInbound) -> None:
    """Business checks beyond the XSD: the fields the Hub routes and settles on."""
    errors: list[str] = []

    if not payment.interbank_settlement_amount.amount:
        errors.append("missing IntrBkSttlmAmt")
    if not payment.interbank_settlement_amount.currency:
        errors.append("IntrBkSttlmAmt has no currency")
    if not payment.end_to_end_id:
        errors.append("missing PmtId/EndToEndId")

    for label, party in (
        ("Dbtr", payment.debtor),
        ("Cdtr", payment.creditor),
    ):
        if not (party.name or party.bic or party.account.iban or party.account.other_id):
            errors.append(f"{label} is empty")

    for label, bic in (
        ("DbtrAgt", payment.debtor_agent.bic),
        ("CdtrAgt", payment.creditor_agent.bic),
    ):
        if bic and not is_valid_bic(bic):
            errors.append(f"{label} BICFI is malformed: {bic!r}")

    if payment.ref.uetr and payment.ref.uetr != raw.ref.uetr:
        # The edge keyed Kafka on the UETR it read; if the body disagrees the
        # message cannot be trusted to be about the payment we think it is.
        errors.append(
            f"UETR in the Document ({payment.ref.uetr}) does not match "
            f"the delivered UETR ({raw.ref.uetr})"
        )

    try:
        require_uetr(raw.ref.uetr)
    except InvalidIdentifier as exc:
        errors.append(str(exc))

    if errors:
        raise PermanentError("; ".join(errors), code="MX_VALIDATION")


def build(settings: ServiceSettings, events: object) -> MxParser:
    return MxParser(settings, events)
