"""FIN parser: ``hub.in.fin.raw`` to ``hub.pay.canonical``.

Parses the MT, applies MT field rules, maps to the canonical model and stamps
T2. From the canonical topic onward, MT and MX are the same code.

Anything malformed is a :class:`PermanentError` — an MT103 missing field 32A
will still be missing it on the second attempt — so it goes straight to the
DLQ rather than round the retry ladder.
"""

from __future__ import annotations

import logging
from typing import Final

from hub_format import map_mt
from hub_format import mt as mt_format
from hub_model import proto as pb
from hub_model import topics as tp
from hub_model.envelope import STAGE_PARSED, new_envelope, now_ns
from hub_model.ids import InvalidIdentifier, is_valid_bic, require_uetr
from hub_model.proto import PaymentEnvelope, RawInbound
from hub_telemetry import metrics
from hub_telemetry.messaging import Record

from hub.common.processor import Outbox, PermanentError, Processor

log = logging.getLogger(__name__)

SERVICE_NAME: Final = "hub-fin-parser"


class FinParser(Processor):
    """MT in, canonical out."""

    name = SERVICE_NAME
    stage = STAGE_PARSED
    group_id = tp.CG_FIN_PARSER
    input_topics = (tp.IN_FIN_RAW,)

    async def handle(self, record: Record, out: Outbox) -> None:
        raw = RawInbound()
        try:
            raw.ParseFromString(record.value)
        except Exception as exc:
            raise PermanentError(f"undecodable RawInbound: {exc}", code="FIN_BAD_ENVELOPE") from exc

        envelope = self._parse(raw)
        if self.done(envelope):
            log.debug("skipping %s: already parsed", raw.ref.uetr)
            return

        stages = self.mark(envelope)
        out.advance_to(envelope, tp.PAY_CANONICAL, stages=stages, reason="MT parsed to canonical")
        metrics.stage_latency.labels(SERVICE_NAME, "parse", "MT").observe(
            max(0.0, (envelope.timings.t2_canonical_ns - raw.received_ns) / 1e9)
        )

    def _parse(self, raw: RawInbound) -> PaymentEnvelope:
        try:
            message = mt_format.parse(raw.fin_message)
        except mt_format.MtParseError as exc:
            raise PermanentError(f"malformed FIN message: {exc}", code="FIN_PARSE") from exc

        try:
            msg_type = map_mt.classify(message)
            payment = map_mt.to_payment(message)
        except (map_mt.MtMappingError, InvalidIdentifier, ValueError) as exc:
            raise PermanentError(f"MT mapping failed: {exc}", code="FIN_MAPPING") from exc

        _validate(message, payment, raw)

        payment.ref.uetr = raw.ref.uetr
        payment.ref.flow = raw.ref.flow
        payment.network_ts_ns = raw.network_ts_ns
        payment.raw.topic = tp.IN_FIN_RAW
        payment.raw.partition = -1
        payment.raw.offset = -1
        if raw.sender_bic:
            payment.sender_bic = raw.sender_bic

        envelope = new_envelope(
            payment.ref,
            pb.MT,
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

        if message.sequences()[1]:
            # MT202 COV: keep the underlying customer leg so the cover matcher
            # and any downstream translation have it.
            underlying = map_mt.underlying_payment(message)
            if underlying is not None:
                envelope.payment.cover.leg = pb.CoverLink.COVER
                envelope.payment.cover.partner_msg_type = underlying.ref.msg_type
        return envelope


def _validate(message: mt_format.MtMessage, payment: pb.Payment, raw: RawInbound) -> None:
    """MT field rules the Hub depends on.

    Deliberately not a SWIFT validation suite — the ESS is not a certification
    tool either. These are the fields without which the Hub cannot route,
    screen or settle.
    """
    errors: list[str] = []

    if not message.get("20"):
        errors.append("missing field 20 (sender's reference)")
    if not payment.interbank_settlement_amount.amount:
        errors.append("missing or unparseable field 32A")
    if not payment.interbank_settlement_date:
        errors.append("field 32A has no usable value date")
    if not payment.interbank_settlement_amount.currency:
        errors.append("field 32A has no currency")

    if message.msg_type == "103":
        if not (payment.debtor.name or payment.debtor.account.iban or payment.debtor.bic):
            errors.append("missing field 50a (ordering customer)")
        if not (payment.creditor.name or payment.creditor.account.iban or payment.creditor.bic):
            errors.append("missing field 59a (beneficiary customer)")
    else:
        if not (payment.creditor.bic or payment.creditor.name):
            errors.append("missing field 58a (beneficiary institution)")

    for label, bic in (
        ("debtor agent", payment.debtor_agent.bic),
        ("creditor agent", payment.creditor_agent.bic),
    ):
        if bic and not is_valid_bic(bic):
            errors.append(f"{label} BIC is malformed: {bic!r}")

    try:
        require_uetr(raw.ref.uetr)
    except InvalidIdentifier as exc:
        errors.append(str(exc))

    if errors:
        raise PermanentError("; ".join(errors), code="FIN_VALIDATION")


def build(settings: object, events: object) -> FinParser:
    """Factory used by :func:`hub.common.service.run_processor`."""
    return FinParser(settings, events)  # type: ignore[arg-type]
