"""The Hub gRPC edge: ``HubInbound``, ``HubNetworkEvents``, ``HubCompliance``.

This is the only way into the Hub. Nothing outside — not the ESS, not a test
tool — writes to the Hub's Kafka.

Three rules govern everything here:

* **Light checks only.** Size, a well-formed UETR, a known message type, a
  duplicate check. No parsing: a malformed body is the parser's problem, and
  parsing on this path would put format cost in front of the durability write.
* **Durable before acknowledged.** ``ACCEPTED`` is returned only after the
  Kafka write with ``acks=all`` is confirmed.
* **Baggage is set here, once.** The edge is where a payment gets its
  identifiers and its tracking level, which then travel unchanged to every
  other service.
"""

from __future__ import annotations

import logging
from typing import Final

import grpc
from hub_format import mt as mt_format
from hub_format import mx as mx_format
from hub_model import proto as pb
from hub_model import topics as tp
from hub_model.envelope import now_ns
from hub_model.flows import flow_for, format_of, normalise_msg_type
from hub_model.ids import is_valid_uetr
from hub_model.proto import (
    DeliveryNotification,
    DeliveryReceipt,
    FccDecision,
    FccDecisionRecord,
    FinDelivery,
    MxDelivery,
    NetworkAck,
    NetworkAckRecord,
    RawInbound,
    Received,
    StatusEvent,
)
from hub_telemetry import metrics
from hub_telemetry.baggage import PaymentBaggage, payment_context
from hub_telemetry.events import Events
from hub_telemetry.tracked import TrackedProducer
from hub_telemetry.tracking import TrackingPolicy

from hub.common.config import ServiceSettings
from hub.common.processor import HDR_FLOW, HDR_FORMAT, HDR_MSG_TYPE

log = logging.getLogger(__name__)

SERVICE_NAME: Final = "hub-edge"

# A pacs.008 is a few kB; 1 MB is generous and still bounds the damage a
# malformed or hostile caller can do before the durability write.
MAX_MESSAGE_BYTES: Final = 1024 * 1024


class DuplicateWindow:
    """Remembers recently accepted (UETR, message type, direction) keys.

    Section 4 requires a repeat to return ``DUPLICATE`` rather than create a
    second payment. This is a bounded in-process window, which catches the
    network retries it exists for; the authoritative de-duplication is the
    stage-level idempotency check in each processor, backed by
    ``hub.pay.state``.
    """

    def __init__(self, capacity: int = 100_000) -> None:
        self.capacity = capacity
        self._seen: dict[tuple[str, str, str], int] = {}

    def check_and_add(self, uetr: str, msg_type: str, direction: str) -> bool:
        """True if this is the first time we have seen the key."""
        key = (uetr, msg_type, direction)
        if key in self._seen:
            return False
        if len(self._seen) >= self.capacity:
            # Drop the oldest tenth; dicts keep insertion order.
            for stale in list(self._seen)[: self.capacity // 10]:
                self._seen.pop(stale, None)
        self._seen[key] = now_ns()
        return True

    def __len__(self) -> int:
        return len(self._seen)


class EdgeBase:
    """Shared plumbing for the three edge services."""

    def __init__(
        self,
        settings: ServiceSettings,
        events: Events,
        producer: TrackedProducer,
        *,
        duplicates: DuplicateWindow | None = None,
    ) -> None:
        self.settings = settings
        self.events = events
        self.producer = producer
        self.policy: TrackingPolicy = settings.tracking_policy()
        self.duplicates = duplicates if duplicates is not None else DuplicateWindow()

    def baggage_for(
        self,
        uetr: str,
        *,
        fmt: str,
        msg_type: str,
        flow: str,
        biz_msg_id: str = "",
        run_id: str = "",
    ) -> PaymentBaggage:
        """Build the baggage for a payment, including its tracking level."""
        return PaymentBaggage(
            uetr=uetr,
            format=fmt,
            msg_type=msg_type,
            flow=flow,
            biz_msg_id=biz_msg_id,
            trace_level=self.policy.choose_level(uetr).value,
            run_id=run_id,
        )

    @staticmethod
    def run_id_from(context: grpc.aio.ServicerContext) -> str:
        """``run-id`` metadata, set by the ESS and the performance framework."""
        for key, value in context.invocation_metadata() or ():
            if key == "run-id":
                return value.decode() if isinstance(value, bytes) else str(value)
        return ""

    def reject(self, reason: str, code: str) -> DeliveryReceipt:
        self.events.payment_error(code, message=reason, error_type="EdgeRejection")
        metrics.payments_errors.labels(SERVICE_NAME, "edge", code).inc()
        return DeliveryReceipt(status=DeliveryReceipt.REJECTED, accepted_ns=now_ns(), reason=reason)


class HubInboundService(EdgeBase, pb.HubInboundServicer):
    """FIN and SnF deliver inbound payments here."""

    async def DeliverFin(  # noqa: N802 - gRPC method name
        self, request: FinDelivery, context: grpc.aio.ServicerContext
    ) -> DeliveryReceipt:
        raw = request.fin_message
        if not raw:
            return self.reject("empty FIN message", "EDGE_EMPTY")
        if len(raw) > MAX_MESSAGE_BYTES:
            return self.reject(f"FIN message over {MAX_MESSAGE_BYTES} bytes", "EDGE_TOO_LARGE")

        # Field 121 is read straight out of block 3 — a regex, not a parse.
        uetr = (request.ref.uetr or mt_format.peek_uetr(raw)).lower()
        if not is_valid_uetr(uetr):
            return self.reject(f"missing or malformed UETR: {uetr!r}", "EDGE_BAD_UETR")

        msg_type = request.ref.msg_type or mt_format.peek_msg_type(raw)
        try:
            normalised = normalise_msg_type(msg_type)
            format_of(normalised)
        except ValueError:
            return self.reject(f"unsupported message type {msg_type!r}", "EDGE_BAD_TYPE")

        flow = request.ref.flow or flow_for(normalised)
        bag = self.baggage_for(
            uetr,
            fmt="MT",
            msg_type=normalised,
            flow=flow,
            run_id=self.run_id_from(context),
        )

        with payment_context(bag):
            if not self.duplicates.check_and_add(uetr, normalised, "INBOUND"):
                self.events.payment_state("RECEIVED", reason="duplicate delivery")
                return DeliveryReceipt(status=DeliveryReceipt.DUPLICATE, accepted_ns=now_ns())

            record = RawInbound(
                format=pb.MT,
                fin_message=raw,
                sender_bic=request.sender_bic,
                network_ts_ns=request.network_ts_ns or now_ns(),
                received_ns=now_ns(),
                trace_level=bag.trace_level,
                run_id=bag.run_id,
            )
            record.ref.uetr = uetr
            record.ref.msg_type = normalised
            record.ref.flow = flow
            return self._accept(record, tp.IN_FIN_RAW, "MT")

    async def DeliverMx(  # noqa: N802 - gRPC method name
        self, request: MxDelivery, context: grpc.aio.ServicerContext
    ) -> DeliveryReceipt:
        document = request.document
        if not document:
            return self.reject("empty MX document", "EDGE_EMPTY")
        total = len(document) + len(request.app_hdr)
        if total > MAX_MESSAGE_BYTES:
            return self.reject(f"MX message over {MAX_MESSAGE_BYTES} bytes", "EDGE_TOO_LARGE")

        uetr = (request.ref.uetr or mx_format.peek_uetr(document)).lower()
        if not is_valid_uetr(uetr):
            return self.reject(f"missing or malformed UETR: {uetr!r}", "EDGE_BAD_UETR")

        msg_type = request.ref.msg_type or mx_format.peek_msg_type(document)
        try:
            normalised = normalise_msg_type(msg_type)
            format_of(normalised)
        except ValueError:
            return self.reject(f"unsupported message type {msg_type!r}", "EDGE_BAD_TYPE")

        flow = request.ref.flow or flow_for(normalised)
        bag = self.baggage_for(
            uetr,
            fmt="MX",
            msg_type=normalised,
            flow=flow,
            biz_msg_id=mx_format.peek_biz_msg_id(request.app_hdr or document),
            run_id=self.run_id_from(context),
        )

        with payment_context(bag):
            if not self.duplicates.check_and_add(uetr, normalised, "INBOUND"):
                self.events.payment_state("RECEIVED", reason="duplicate delivery")
                return DeliveryReceipt(status=DeliveryReceipt.DUPLICATE, accepted_ns=now_ns())

            record = RawInbound(
                format=pb.MX,
                app_hdr=request.app_hdr,
                document=document,
                snf_ref=request.snf_ref,
                network_ts_ns=request.network_ts_ns or now_ns(),
                received_ns=now_ns(),
                trace_level=bag.trace_level,
                run_id=bag.run_id,
            )
            record.ref.uetr = uetr
            record.ref.msg_type = normalised
            record.ref.flow = flow
            return self._accept(record, tp.IN_MX_RAW, "MX")

    def _accept(self, record: RawInbound, topic: str, fmt: str) -> DeliveryReceipt:
        """Durable write, then the reply. Never the other way round."""
        headers = {
            HDR_FORMAT: fmt,
            HDR_MSG_TYPE: record.ref.msg_type,
            HDR_FLOW: record.ref.flow,
        }
        self.producer.produce_now(topic, record.ref.uetr, record.SerializeToString(), headers)
        accepted_ns = now_ns()
        metrics.kafka_produced.labels(SERVICE_NAME, topic).inc()
        metrics.payments_state.labels(SERVICE_NAME, fmt, "RECEIVED").inc()
        self.events.payment_state("RECEIVED", reason=f"durable on {topic}")

        # hub.pay.status carries every state change, RECEIVED included, so the
        # status API and the DB sink see a payment from its first hop. This is
        # produced without flushing: the durability promise is the raw write
        # above, and the reply must not wait on telemetry.
        self._emit_received(record, accepted_ns, headers)
        return DeliveryReceipt(status=DeliveryReceipt.ACCEPTED, accepted_ns=accepted_ns)

    def _emit_received(self, record: RawInbound, accepted_ns: int, headers: dict[str, str]) -> None:
        event = StatusEvent(
            format=record.format,
            state=pb.RECEIVED,
            service=SERVICE_NAME,
            reason="accepted at the edge",
            emitted_ns=accepted_ns,
            run_id=record.run_id,
        )
        event.ref.CopyFrom(record.ref)
        event.timings.t0_network_sent_ns = record.network_ts_ns
        event.timings.t1_edge_accepted_ns = accepted_ns
        self.producer.produce_now(
            tp.PAY_STATUS,
            record.ref.uetr,
            event.SerializeToString(),
            headers,
            flush=False,
        )
        metrics.kafka_produced.labels(SERVICE_NAME, tp.PAY_STATUS).inc()


class HubNetworkEventsService(EdgeBase, pb.HubNetworkEventsServicer):
    """FIN and SnF report what happened to an outbound message."""

    async def NotifyAck(  # noqa: N802 - gRPC method name
        self, request: NetworkAck, context: grpc.aio.ServicerContext
    ) -> Received:
        uetr = request.ref.uetr.lower()
        if not is_valid_uetr(uetr):
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, f"bad UETR {uetr!r}")

        bag = self.baggage_for(
            uetr,
            fmt="",
            msg_type=request.ref.msg_type,
            flow=request.ref.flow,
            run_id=self.run_id_from(context),
        )
        with payment_context(bag):
            record = NetworkAckRecord(
                network=request.network,
                ack=request.ack,
                error_code=request.error_code,
                send_ref=request.send_ref,
                network_ts_ns=request.network_ts_ns or now_ns(),
                received_ns=now_ns(),
            )
            record.ref.CopyFrom(request.ref)
            record.ref.uetr = uetr
            self.producer.produce_now(
                tp.NET_ACK,
                uetr,
                record.SerializeToString(),
                {HDR_MSG_TYPE: request.ref.msg_type, HDR_FLOW: request.ref.flow},
            )
            metrics.kafka_produced.labels(SERVICE_NAME, tp.NET_ACK).inc()
            return Received(received=True)

    async def NotifyDeliveryNotification(  # noqa: N802 - gRPC method name
        self, request: DeliveryNotification, context: grpc.aio.ServicerContext
    ) -> Received:
        uetr = request.ref.uetr.lower()
        if not is_valid_uetr(uetr):
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, f"bad UETR {uetr!r}")

        bag = self.baggage_for(
            uetr,
            fmt="MX",
            msg_type=request.ref.msg_type,
            flow=request.ref.flow,
            run_id=self.run_id_from(context),
        )
        with payment_context(bag):
            record = NetworkAckRecord(
                network=pb.SNF,
                ack=request.delivered,
                error_code="" if request.delivered else request.reason,
                send_ref=request.snf_ref,
                delivery_notification=True,
                delivered=request.delivered,
                network_ts_ns=request.network_ts_ns or now_ns(),
                received_ns=now_ns(),
            )
            record.ref.CopyFrom(request.ref)
            record.ref.uetr = uetr
            self.producer.produce_now(
                tp.NET_ACK,
                uetr,
                record.SerializeToString(),
                {HDR_MSG_TYPE: request.ref.msg_type, HDR_FLOW: request.ref.flow},
            )
            metrics.kafka_produced.labels(SERVICE_NAME, tp.NET_ACK).inc()
            return Received(received=True)


class HubComplianceService(EdgeBase, pb.HubComplianceServicer):
    """FCC returns an analyst decision for a payment parked in HELD."""

    async def NotifyFccDecision(  # noqa: N802 - gRPC method name
        self, request: FccDecision, context: grpc.aio.ServicerContext
    ) -> Received:
        uetr = request.ref.uetr.lower()
        if not is_valid_uetr(uetr):
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, f"bad UETR {uetr!r}")

        bag = self.baggage_for(
            uetr,
            fmt="",
            msg_type=request.ref.msg_type,
            flow=request.ref.flow,
            run_id=self.run_id_from(context),
        )
        with payment_context(bag):
            record = FccDecisionRecord(
                case_id=request.case_id,
                release=request.decision == FccDecision.RELEASE,
                reason=request.reason,
                decided_ns=request.decided_ns or now_ns(),
                received_ns=now_ns(),
            )
            record.ref.CopyFrom(request.ref)
            record.ref.uetr = uetr
            self.producer.produce_now(
                tp.FCC_DECISION,
                uetr,
                record.SerializeToString(),
                {HDR_MSG_TYPE: request.ref.msg_type, HDR_FLOW: request.ref.flow},
            )
            metrics.kafka_produced.labels(SERVICE_NAME, tp.FCC_DECISION).inc()
            return Received(received=True)


def register(
    server: grpc.aio.Server,
    settings: ServiceSettings,
    events: Events,
    producer: TrackedProducer,
) -> tuple[HubInboundService, HubNetworkEventsService, HubComplianceService]:
    """Attach all three edge services to ``server``, sharing one producer."""
    duplicates = DuplicateWindow()
    inbound = HubInboundService(settings, events, producer, duplicates=duplicates)
    network = HubNetworkEventsService(settings, events, producer, duplicates=duplicates)
    compliance = HubComplianceService(settings, events, producer, duplicates=duplicates)
    pb.add_HubInboundServicer_to_server(inbound, server)
    pb.add_HubNetworkEventsServicer_to_server(network, server)
    pb.add_HubComplianceServicer_to_server(compliance, server)
    return inbound, network, compliance
