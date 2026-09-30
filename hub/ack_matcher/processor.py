"""ACK matcher: ``hub.net.ack`` to ``hub.pay.status``.

Matches a network ACK or NAK back to the payment that was sent, by UETR, and
writes the terminal status: COMPLETED on an ACK, REJECTED on a NAK. T6 is
stamped here, which closes the T0-T6 measurement the performance framework
reads off ``hub.pay.status``.

**Completion is defined as the network ACK.** That is one of three candidates
the design doc leaves open (outbound handoff, network ACK, or the final
pacs.002); the ACK is what the flow in section 5 stamps T6 on, so it is what
this implements. :data:`COMPLETION_RULE` names the choice in one place — if the
business decides on the pacs.002 instead, this module and the status API are
what change.

A cover pair completes together: both legs share the UETR, so their ACKs land
on the same partition, and the pair is COMPLETED only once both have been
acknowledged.
"""

from __future__ import annotations

import logging
from typing import Final

from hub_model import proto as pb
from hub_model import topics as tp
from hub_model.envelope import STAGE_ACKED, advance, end_to_end_seconds, now_ns
from hub_model.flows import get_flow
from hub_model.proto import NetworkAckRecord, PaymentEnvelope
from hub_telemetry import metrics
from hub_telemetry.events import Events
from hub_telemetry.messaging import Record

from hub.common.config import ServiceSettings
from hub.common.processor import Outbox, PermanentError, Processor

log = logging.getLogger(__name__)

SERVICE_NAME: Final = "hub-ack-matcher"

#: What the Hub treats as a completed payment. See the module docstring.
COMPLETION_RULE: Final = "NETWORK_ACK"


class AckMatcher(Processor):
    """Network ACK / NAK in; COMPLETED or REJECTED out."""

    name = SERVICE_NAME
    stage = STAGE_ACKED
    group_id = tp.CG_ACK_MATCHER
    input_topics = (tp.NET_ACK,)
    # The ACK arrives on its own topic with nothing but a UETR. The payment it
    # belongs to comes from the compacted state topic.
    reads_state = True

    async def handle(self, record: Record, out: Outbox) -> None:
        ack = NetworkAckRecord()
        try:
            ack.ParseFromString(record.value)
        except Exception as exc:
            raise PermanentError(f"undecodable NetworkAckRecord: {exc}", code="ACK_BAD") from exc

        if ack.delivery_notification:
            # Informational, not a state change: the payment is already
            # COMPLETED by its ACK. It is logged with the payment's baggage, so
            # it is searchable by UETR, but it does not go on hub.pay.status.
            log.info(
                "SnF delivery notification for %s: %s (%s)",
                ack.ref.uetr,
                "delivered" if ack.delivered else "not delivered",
                ack.send_ref,
            )
            return

        env = self._payment_for(ack)
        if env is None:
            if self.store.is_complete(ack.ref.uetr):
                # A repeat of an ACK we have already acted on. The network
                # re-sends; completing twice would be a defect.
                log.debug("ignoring repeated ACK for completed %s", ack.ref.uetr)
                return
            # No dispatched payment for this UETR on this replica. Either it
            # belongs to another partition owner or the ACK outran the
            # dispatcher's state write; a bare status event keeps the journey
            # visible without inventing a payment.
            log.info("unmatched ACK for %s (send_ref %s)", ack.ref.uetr, ack.send_ref)
            self._orphan(ack, out)
            return

        if self.done(env):
            log.debug("skipping %s: already acked", ack.ref.uetr)
            return

        completed_ns = now_ns()
        if ack.ack:
            final = advance(env, pb.COMPLETED)
            final.dispatch.acked = True
            reason = f"{pb.network_name(ack.network)} ACK {ack.send_ref}"
        else:
            final = advance(env, pb.REJECTED)
            final.dispatch.nak_code = ack.error_code
            reason = f"{pb.network_name(ack.network)} NAK {ack.error_code}"
        final.timings.t5_network_ack_ns = ack.received_ns or completed_ns
        final.timings.t6_completed_ns = completed_ns

        stages = self.mark(final)
        if not self._cover_complete(final, stages):
            waiting = advance(final, pb.AWAITING_COVER)
            out.emit_status(waiting, reason=f"{reason}; waiting for the cover partner")
            out.emit_state(waiting, stages=stages)
            return

        out.emit_status(
            final,
            reason=reason,
            error_code=ack.error_code if not ack.ack else "",
        )
        out.emit_state(final, stages=stages)

        elapsed = end_to_end_seconds(final.timings)
        if elapsed is not None:
            metrics.payment_end_to_end.labels(pb.format_name(final.format), final.ref.flow).observe(
                elapsed
            )
        # Terminal: the payment leaves the working set but is remembered, so a
        # repeated callback is recognised rather than treated as a new payment.
        self.store.complete(final.ref.uetr)

    def _payment_for(self, ack: NetworkAckRecord) -> PaymentEnvelope | None:
        """Find the payment this ACK belongs to.

        For a cover pair the UETR alone is ambiguous — both legs share it — so
        the ACK's message type picks the leg.
        """
        flow = get_flow(ack.ref.flow)
        if flow is not None and flow.cover_pair and ack.ref.msg_type:
            leg = self.store.cover_leg(ack.ref.uetr, ack.ref.msg_type)
            if leg is not None:
                return leg
        return self.store.envelope(ack.ref.uetr)

    def _cover_complete(self, env: PaymentEnvelope, stages: list[str]) -> bool:
        """A cover pair completes only when both legs have been acknowledged.

        Each leg's ACK writes its own ``acked:<msg_type>`` marker, and those
        accumulate on the shared UETR, so the last ACK to arrive is the one
        that sees the pair complete.
        """
        flow = get_flow(env.ref.flow)
        if flow is None or not flow.cover_pair:
            return True
        done = set(stages)
        return all(f"{STAGE_ACKED}:{msg_type}" in done for msg_type in flow.inbound_types)

    def _orphan(self, ack: NetworkAckRecord, out: Outbox) -> None:
        env = PaymentEnvelope()
        env.ref.CopyFrom(ack.ref)
        env.state = pb.COMPLETED if ack.ack else pb.REJECTED
        env.timings.t5_network_ack_ns = ack.received_ns or now_ns()
        env.timings.t6_completed_ns = now_ns()
        out.emit_status(
            env,
            reason=f"unmatched {pb.network_name(ack.network)} "
            f"{'ACK' if ack.ack else 'NAK'} {ack.send_ref}",
            error_code=ack.error_code,
        )
        self.store.complete(ack.ref.uetr)


def build(settings: ServiceSettings, events: Events) -> AckMatcher:
    return AckMatcher(settings, events)
