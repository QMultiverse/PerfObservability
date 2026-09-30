"""Settlement: ``hub.pay.routed`` to ``hub.out.fin`` / ``hub.out.mx``.

Two jobs:

* post to the ledger, and
* build the outbound message in the format routing chose, so the dispatcher
  only has to hand bytes to the network.

Building the message here and not in the dispatcher keeps the dispatcher a thin
gRPC client whose only failure mode is the network, which is what makes the
retry rules on ``SendMt`` / ``SendMx`` simple.

Ledger posting is a placeholder: the real posting rules and the ledger
interface are an open question in docs/platform-design.md section 10. What is
here records a deterministic reference and the value date, so the rest of the
pipeline and the tests are exercised end to end.
"""

from __future__ import annotations

import logging
from typing import Final

from hub_format import map_mt, map_mx
from hub_format.mt import MtParseError
from hub_format.mx import MxValidationError
from hub_model import proto as pb
from hub_model import topics as tp
from hub_model.envelope import STAGE_SETTLED, advance, now_ns
from hub_model.flows import MT103, MT202, MT202COV, PACS_008, PACS_009, PACS_009_COV
from hub_model.proto import OutboundMessage, PaymentEnvelope
from hub_telemetry.events import Events
from hub_telemetry.messaging import Record

from hub.common.config import ServiceSettings
from hub.common.processor import Outbox, PermanentError, Processor

log = logging.getLogger(__name__)

SERVICE_NAME: Final = "hub-settlement"


class Settlement(Processor):
    """Routed in; an outbound message on the right lane out."""

    name = SERVICE_NAME
    stage = STAGE_SETTLED
    group_id = tp.CG_SETTLEMENT
    input_topics = (tp.PAY_ROUTED,)

    async def handle(self, record: Record, out: Outbox) -> None:
        env = PaymentEnvelope()
        try:
            env.ParseFromString(record.value)
        except Exception as exc:
            raise PermanentError(
                f"undecodable PaymentEnvelope: {exc}", code="BAD_ENVELOPE"
            ) from exc

        if self.done(env):
            log.debug("skipping %s: already settled", env.ref.uetr)
            return

        settled = advance(env, pb.SETTLED)
        settled.settlement.CopyFrom(post_to_ledger(settled))
        outbound = build_outbound(settled)

        topic = tp.OUT_MX if settled.route.outbound_format == pb.MX else tp.OUT_FIN
        out.send(
            topic,
            settled.ref.uetr,
            outbound.SerializeToString(),
            {
                "format": pb.format_name(settled.route.outbound_format),
                "msg-type": settled.route.outbound_msg_type,
                "flow": settled.ref.flow,
            },
        )
        stages = self.mark(settled)
        out.emit_status(settled, reason=f"posted {settled.settlement.ledger_ref}")
        out.emit_state(settled, stages=stages)


def post_to_ledger(env: PaymentEnvelope) -> pb.Settlement:
    """Post the payment to the ledger.

    TODO(business-rules): a real posting needs the account hierarchy, the nostro
    or vostro to debit, and the accounting calendar. Section 10 of the design
    doc lists this as open. The reference below is deterministic so a replayed
    payment posts the same way twice.
    """
    settlement = pb.Settlement(
        ledger_ref=f"LDG-{env.ref.uetr[:8].upper()}",
        value_date=env.payment.interbank_settlement_date,
        posted_ns=now_ns(),
    )
    settlement.posted_amount.CopyFrom(env.payment.interbank_settlement_amount)
    return settlement


def build_outbound(env: PaymentEnvelope) -> OutboundMessage:
    """Render the canonical payment into the outbound format."""
    msg_type = env.route.outbound_msg_type or env.ref.msg_type
    outbound = OutboundMessage(
        format=env.route.outbound_format,
        receiver_bic=env.route.receiver_bic,
        requestor_dn=env.route.requestor_dn,
        responder_dn=env.route.responder_dn,
        request_delivery_notification=env.route.network == pb.SNF,
    )
    outbound.envelope.CopyFrom(env)

    sender_bic = env.payment.instructed_agent.bic or env.payment.debtor_agent.bic
    if not sender_bic:
        sender_bic = env.payment.sender_bic

    if env.route.outbound_format == pb.MX:
        _build_mx(outbound, env, msg_type, sender_bic)
    else:
        _build_mt(outbound, env, msg_type, sender_bic)
    return outbound


def _build_mx(
    outbound: OutboundMessage, env: PaymentEnvelope, msg_type: str, sender_bic: str
) -> None:
    if msg_type not in (PACS_008, PACS_009, PACS_009_COV):
        raise PermanentError(f"cannot build outbound {msg_type}", code="OUT_BAD_TYPE")
    payment = pb.Payment()
    payment.CopyFrom(env.payment)
    payment.direction = pb.OUTBOUND
    payment.ref.msg_type = msg_type
    try:
        outbound.document = map_mx.build_document(payment, msg_type)
        outbound.app_hdr = map_mx.build_header_for(
            payment,
            msg_type,
            from_bic=sender_bic or payment.sender_bic,
            to_bic=env.route.receiver_bic,
        )
    except (MxValidationError, ValueError) as exc:
        raise PermanentError(f"failed to build {msg_type}: {exc}", code="OUT_BUILD") from exc


def _build_mt(
    outbound: OutboundMessage, env: PaymentEnvelope, msg_type: str, sender_bic: str
) -> None:
    if msg_type not in (MT103, MT202, MT202COV):
        raise PermanentError(f"cannot build outbound {msg_type}", code="OUT_BAD_TYPE")
    payment = pb.Payment()
    payment.CopyFrom(env.payment)
    payment.direction = pb.OUTBOUND
    payment.ref.msg_type = msg_type

    underlying: pb.Payment | None = None
    if msg_type == MT202COV:
        # The cover leg carries the underlying customer transfer in sequence B.
        # Without the partner leg there is nothing to put there.
        underlying = pb.Payment()
        underlying.CopyFrom(env.payment)
        underlying.ref.msg_type = MT103

    try:
        outbound.fin_message = map_mt.build_message(
            payment,
            msg_type,
            sender_bic=sender_bic or payment.sender_bic,
            receiver_bic=env.route.receiver_bic,
            underlying=underlying,
        ).encode("utf-8")
    except (map_mt.MtMappingError, MtParseError, ValueError) as exc:
        raise PermanentError(f"failed to build {msg_type}: {exc}", code="OUT_BUILD") from exc


def build(settings: ServiceSettings, events: Events) -> Settlement:
    return Settlement(settings, events)
