"""Dispatcher: ``hub.out.fin`` / ``hub.out.mx`` to ``SendMt`` / ``SendMx``.

A thin gRPC client. It takes the next message to send, hands it to the network
with a 500 ms deadline, stamps T4 (handoff) and records DISPATCHED. The network
ACK arrives later and asynchronously, through ``NotifyAck`` at the edge.

Two dispatchers run, one per lane and consumer group (``cg-dispatch-fin`` and
``cg-dispatch-mx``), so a slow or unavailable FIN cannot back up the MX lane.

``SendMt`` / ``SendMx`` are outside the Kafka transaction: a crash between the
send and the commit re-sends on redelivery, which is why the receiver
de-duplicates on (UETR, message type, direction) and why we check
``stages_done`` before sending.
"""

from __future__ import annotations

import logging
from typing import Final

import grpc
from hub_model import proto as pb
from hub_model import topics as tp
from hub_model.envelope import STAGE_DISPATCHED, advance, now_ns
from hub_model.proto import OutboundMessage, PaymentEnvelope, SendAccepted
from hub_telemetry import metrics
from hub_telemetry.events import Events
from hub_telemetry.grpc_telemetry import DEADLINE_SEND_S, channel
from hub_telemetry.messaging import Record

from hub.common.config import ServiceSettings
from hub.common.processor import Outbox, PermanentError, Processor, RetryableError

log = logging.getLogger(__name__)

SERVICE_NAME_FIN: Final = "hub-dispatcher-fin"
SERVICE_NAME_MX: Final = "hub-dispatcher-mx"


class Dispatcher(Processor):
    """Base for the two lanes. Subclasses fix the topic, group and target."""

    stage = STAGE_DISPATCHED
    lane: str = ""

    def __init__(self, settings: ServiceSettings, events: Events) -> None:
        super().__init__(settings, events)
        self._channel: grpc.aio.Channel | None = None
        self._fin: pb.FinGatewayStub | None = None
        self._snf: pb.SnfGatewayStub | None = None

    def target(self) -> str:
        return (
            self.settings.external.fin_target
            if self.lane == "FIN"
            else self.settings.external.snf_target
        )

    def _open(self) -> grpc.aio.Channel:
        if self._channel is None:
            self._channel = channel(
                self.target(),
                self.events,
                strip_context=self.settings.external.real_networks,
            )
        return self._channel

    async def close(self) -> None:
        if self._channel is not None:
            await self._channel.close()
            self._channel = None
            self._fin = None
            self._snf = None

    async def handle(self, record: Record, out: Outbox) -> None:
        outbound = OutboundMessage()
        try:
            outbound.ParseFromString(record.value)
        except Exception as exc:
            raise PermanentError(
                f"undecodable OutboundMessage: {exc}", code="BAD_OUTBOUND"
            ) from exc

        env = outbound.envelope
        if self.done(env):
            log.debug("skipping %s: already dispatched", env.ref.uetr)
            return

        accepted = await self._send(outbound)
        handoff_ns = now_ns()

        dispatched = advance(env, pb.DISPATCHED)
        dispatched.dispatch.send_ref = accepted.send_ref
        dispatched.dispatch.attempts = 1
        dispatched.dispatch.handoff_ns = handoff_ns
        dispatched.timings.t4_handoff_ns = handoff_ns

        stages = self.mark(dispatched)
        out.emit_status(dispatched, reason=f"handed to {self.lane} as {accepted.send_ref}")
        out.emit_state(dispatched, stages=stages)

        if dispatched.timings.t2_canonical_ns:
            metrics.stage_latency.labels(
                self.name, "dispatch", pb.format_name(outbound.format)
            ).observe(max(0.0, (handoff_ns - dispatched.timings.t2_canonical_ns) / 1e9))

    async def _send(self, outbound: OutboundMessage) -> SendAccepted:
        raise NotImplementedError

    def _classify(self, exc: grpc.aio.AioRpcError, what: str) -> Exception:
        code = exc.code()
        if code in (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED):
            return RetryableError(
                f"{what} unreachable: {code.name}", code=f"{self.lane}_UNAVAILABLE"
            )
        if code == grpc.StatusCode.INVALID_ARGUMENT:
            return PermanentError(
                f"{what} rejected the message: {exc.details()}", code=f"{self.lane}_INVALID"
            )
        return RetryableError(
            f"{what} error {code.name}: {exc.details()}", code=f"{self.lane}_ERROR"
        )

    @staticmethod
    def _check(accepted: SendAccepted, lane: str) -> SendAccepted:
        if accepted.status == SendAccepted.REJECTED:
            # The network refused the message itself. Retrying sends the same
            # bytes to the same validator, so this is terminal.
            raise PermanentError(
                f"{lane} rejected the message: {accepted.reason}", code=f"{lane}_REJECTED"
            )
        return accepted


class FinDispatcher(Dispatcher):
    """The MT lane."""

    name = SERVICE_NAME_FIN
    group_id = tp.CG_DISPATCH_FIN
    input_topics = (tp.OUT_FIN,)
    lane = "FIN"

    async def _send(self, outbound: OutboundMessage) -> SendAccepted:
        if self._fin is None:
            self._fin = pb.FinGatewayStub(self._open())
        request = pb.SendMtRequest(
            fin_message=outbound.fin_message,
            receiver_bic=outbound.receiver_bic,
        )
        request.ref.CopyFrom(outbound.envelope.ref)
        request.ref.msg_type = outbound.envelope.route.outbound_msg_type or request.ref.msg_type
        try:
            accepted = await self._fin.SendMt(request, timeout=DEADLINE_SEND_S)
        except grpc.aio.AioRpcError as exc:
            raise self._classify(exc, "FIN") from exc
        return self._check(accepted, "FIN")


class MxDispatcher(Dispatcher):
    """The MX lane."""

    name = SERVICE_NAME_MX
    group_id = tp.CG_DISPATCH_MX
    input_topics = (tp.OUT_MX,)
    lane = "SNF"

    async def _send(self, outbound: OutboundMessage) -> SendAccepted:
        if self._snf is None:
            self._snf = pb.SnfGatewayStub(self._open())
        request = pb.SendMxRequest(
            app_hdr=outbound.app_hdr,
            document=outbound.document,
            requestor_dn=outbound.requestor_dn,
            responder_dn=outbound.responder_dn,
            request_delivery_notification=outbound.request_delivery_notification,
        )
        request.ref.CopyFrom(outbound.envelope.ref)
        request.ref.msg_type = outbound.envelope.route.outbound_msg_type or request.ref.msg_type
        try:
            accepted = await self._snf.SendMx(request, timeout=DEADLINE_SEND_S)
        except grpc.aio.AioRpcError as exc:
            raise self._classify(exc, "SnF") from exc
        return self._check(accepted, "SNF")


def build_fin(settings: ServiceSettings, events: Events) -> FinDispatcher:
    return FinDispatcher(settings, events)


def build_mx(settings: ServiceSettings, events: Events) -> MxDispatcher:
    return MxDispatcher(settings, events)


__all__ = [
    "Dispatcher",
    "FinDispatcher",
    "MxDispatcher",
    "PaymentEnvelope",
    "build_fin",
    "build_mx",
]
