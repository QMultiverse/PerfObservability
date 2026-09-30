"""Routing: ``hub.pay.screened`` to ``hub.pay.routed``.

Chooses the outbound network (FIN or SnF) and the outbound format. The
outbound format is decided *here*, not by the inbound lane — an MT103 can
leave as a pacs.008 through in-flow translation, which is the
``MT_TO_MX_103`` flow.

The real bank's routing rules — correspondent selection, currency corridors,
cut-off windows — are an open question in both design documents. What is
implemented is the part the flows define, plus a currency cut-off check with
the table in :data:`CUT_OFFS`, which is deliberately small and obvious so it is
easy to replace when the real rules arrive.
"""

from __future__ import annotations

import datetime as dt
import logging
from typing import Final

from hub_model import proto as pb
from hub_model import topics as tp
from hub_model.envelope import STAGE_ROUTED, advance
from hub_model.flows import (
    FLOW_MT_TO_MX_103,
    MT103,
    MT202,
    MT202COV,
    PACS_008,
    PACS_009,
    PACS_009_COV,
    get_flow,
)
from hub_model.proto import PaymentEnvelope
from hub_telemetry.events import Events
from hub_telemetry.messaging import Record

from hub.common.config import ServiceSettings
from hub.common.processor import Outbox, PermanentError, Processor

log = logging.getLogger(__name__)

SERVICE_NAME: Final = "hub-routing"

# TODO(business-rules): placeholder cut-offs, in UTC. The real windows are per
# currency, per correspondent and per calendar, and are an open question in
# docs/platform-design.md section 10.
CUT_OFFS: Final[dict[str, int]] = {"EUR": 17, "GBP": 17, "USD": 22, "CHF": 16, "JPY": 8}

# What an MT becomes when a flow translates it, and the reverse.
MT_TO_MX: Final = {MT103: PACS_008, MT202: PACS_009, MT202COV: PACS_009_COV}
MX_TO_MT: Final = {PACS_008: MT103, PACS_009: MT202, PACS_009_COV: MT202COV}


class Routing(Processor):
    """Screened in, routed out."""

    name = SERVICE_NAME
    stage = STAGE_ROUTED
    group_id = tp.CG_ROUTING
    input_topics = (tp.PAY_SCREENED,)

    async def handle(self, record: Record, out: Outbox) -> None:
        env = PaymentEnvelope()
        try:
            env.ParseFromString(record.value)
        except Exception as exc:
            raise PermanentError(
                f"undecodable PaymentEnvelope: {exc}", code="BAD_ENVELOPE"
            ) from exc

        if self.done(env):
            log.debug("skipping %s: already routed", env.ref.uetr)
            return

        routed = advance(env, pb.ROUTED)
        routed.route.CopyFrom(decide_route(routed))
        stages = self.mark(routed)
        out.advance_to(routed, tp.PAY_ROUTED, stages=stages, reason=routed.route.reason)


def decide_route(env: PaymentEnvelope, *, now: dt.datetime | None = None) -> pb.Route:
    """Pick the network, the outbound format and the receiver."""
    flow = get_flow(env.ref.flow)
    payment = env.payment
    route = pb.Route()

    if flow is not None:
        route.network = flow.outbound_network
        route.outbound_format = flow.outbound_format
        route.translated = flow.translated
    else:
        # No declared flow: stay on the lane the payment arrived on.
        route.network = payment.inbound_network or (pb.SNF if env.format == pb.MX else pb.FIN)
        route.outbound_format = env.format
        route.translated = False

    route.outbound_msg_type = _outbound_msg_type(env.ref.msg_type, route.outbound_format)
    route.receiver_bic = (
        payment.creditor_agent.bic
        or payment.instructed_agent.bic
        or payment.receiver_bic
        or payment.creditor.bic
    )
    if not route.receiver_bic:
        raise PermanentError(
            "cannot route: no creditor agent, instructed agent or receiver BIC",
            code="ROUTE_NO_RECEIVER",
        )

    if route.network == pb.SNF:
        # SnF addresses are distinguished names derived from the BIC.
        route.requestor_dn = _dn(payment.sender_bic or payment.debtor_agent.bic)
        route.responder_dn = _dn(route.receiver_bic)

    window, same_day = _cut_off(payment, now)
    route.cut_off_window = window
    route.reason = (
        f"{'translated to ' if route.translated else ''}"
        f"{route.outbound_msg_type} over {pb.network_name(route.network)}"
        f"{'' if same_day else ', next business day'}"
    )
    if env.ref.flow == FLOW_MT_TO_MX_103:
        route.reason += " (in-flow MT to MX translation)"
    return route


def _outbound_msg_type(inbound: str, outbound_format: int) -> str:
    if outbound_format == pb.MX:
        return MT_TO_MX.get(inbound, inbound)
    return MX_TO_MT.get(inbound, inbound)


def _dn(bic: str) -> str:
    """The SWIFTNet distinguished name for a BIC8."""
    return f"ou=xxx,o={bic[:8].lower()},o=swift" if bic else ""


def _cut_off(payment: pb.Payment, now: dt.datetime | None) -> tuple[str, bool]:
    """Whether the payment still makes today's window for its currency."""
    moment = now or dt.datetime.now(dt.UTC)
    currency = payment.interbank_settlement_amount.currency
    hour = CUT_OFFS.get(currency)
    if hour is None:
        return f"{currency}:none", True
    same_day = moment.hour < hour
    return f"{currency}:{hour:02d}00Z", same_day


def build(settings: ServiceSettings, events: Events) -> Routing:
    return Routing(settings, events)
