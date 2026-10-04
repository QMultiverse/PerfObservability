"""The inbound sender: plays FIN and SnF delivering messages into the Hub.

This is the client side of ``HubInbound.DeliverFin`` / ``DeliverMx``. It is how
``ess send`` and the named cases put a payment into the Hub — and the only way
anything gets in, because the gRPC edge is the sole boundary.

For high-rate load this is *not* the right tool: that is paygen's job in the
performance framework. This sends ones and tens.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Final

import grpc
from hub_format import samples
from hub_format.samples import SampleOptions
from hub_model import proto as pb
from hub_model.envelope import now_ns
from hub_model.flows import flow_for, format_of, is_cover, normalise_msg_type
from hub_model.ids import new_uetr
from hub_model.proto import DeliveryReceipt, FinDelivery, MsgRef, MxDelivery
from hub_telemetry.events import Events
from hub_telemetry.grpc_telemetry import DEADLINE_DELIVER_S, channel

from .recorder import Recorder

log = logging.getLogger(__name__)

DEFAULT_SENDER_BIC: Final = "TESTGB2LXXX"


@dataclass(slots=True)
class Delivery:
    """One message ready to be delivered to the Hub."""

    msg_type: str
    uetr: str
    payload: bytes
    app_hdr: bytes = b""
    flow: str = ""
    sender_bic: str = DEFAULT_SENDER_BIC

    @property
    def format(self) -> int:
        return format_of(self.msg_type)


@dataclass(slots=True)
class SendOutcome:
    uetrs: list[str] = field(default_factory=list)
    accepted: int = 0
    duplicate: int = 0
    rejected: int = 0
    #: Deliveries that never got a receipt — the call itself failed. Distinct
    #: from ``rejected``, which is the edge answering REJECTED.
    failed: int = 0
    errors: list[str] = field(default_factory=list)

    @property
    def delivered(self) -> int:
        """Payments the Hub now owns. A duplicate is one that already arrived."""
        return self.accepted + self.duplicate

    @property
    def ok(self) -> bool:
        return not self.errors and self.rejected == 0 and self.failed == 0


class InboundSender:
    """Delivers messages into the Hub edge."""

    def __init__(
        self,
        hub_target: str,
        events: Events,
        recorder: Recorder,
        *,
        run_id: str = "",
    ) -> None:
        self.hub_target = hub_target
        self.events = events
        self.recorder = recorder
        self.run_id = run_id
        self._channel: grpc.aio.Channel | None = None

    def _open(self) -> grpc.aio.Channel:
        if self._channel is None:
            self._channel = channel(self.hub_target, self.events)
        return self._channel

    async def close(self) -> None:
        if self._channel is not None:
            await self._channel.close()
            self._channel = None

    # ----------------------------------------------------------- building
    @staticmethod
    def build(
        msg_type: str,
        *,
        uetr: str = "",
        payload: bytes = b"",
        app_hdr: bytes = b"",
        flow: str = "",
        sanctions_hit: bool = False,
        seed: int | None = None,
    ) -> Delivery:
        """Build one delivery, generating a synthetic message if none is given."""
        canonical = normalise_msg_type(msg_type)
        chosen_uetr = uetr or new_uetr()
        chosen_flow = flow or flow_for(canonical, cover=is_cover(canonical))

        if payload:
            return Delivery(
                msg_type=canonical,
                uetr=chosen_uetr,
                payload=payload,
                app_hdr=app_hdr,
                flow=chosen_flow,
            )

        options = SampleOptions(uetr=chosen_uetr, sanctions_hit=sanctions_hit, seed=seed)
        if format_of(canonical) == pb.MX:
            header, document, payment = samples.make_mx(canonical, options)
            return Delivery(
                msg_type=canonical,
                uetr=chosen_uetr,
                payload=document,
                app_hdr=header,
                flow=chosen_flow,
                sender_bic=payment.sender_bic,
            )
        text, payment = samples.make_mt(canonical, options)
        return Delivery(
            msg_type=canonical,
            uetr=chosen_uetr,
            payload=text.encode("utf-8"),
            flow=chosen_flow,
            sender_bic=payment.sender_bic,
        )

    @staticmethod
    def build_cover_pair(
        msg_types: tuple[str, str],
        *,
        uetr: str = "",
        flow: str = "",
        sanctions_hit: bool = False,
        seed: int | None = None,
    ) -> list[Delivery]:
        """Both legs of a cover pair, sharing one UETR."""
        shared = uetr or new_uetr()
        legs = samples.make_cover_pair(
            msg_types, SampleOptions(uetr=shared, sanctions_hit=sanctions_hit, seed=seed)
        )
        chosen_flow = flow or flow_for(msg_types[0], cover=True)
        return [
            Delivery(
                msg_type=msg_type,
                uetr=shared,
                payload=payload,
                app_hdr=app_hdr,
                flow=chosen_flow,
                sender_bic=payment.sender_bic,
            )
            for msg_type, app_hdr, payload, payment in legs
        ]

    # ----------------------------------------------------------- sending
    async def deliver(self, delivery: Delivery) -> DeliveryReceipt:
        """Deliver one message and return the edge's receipt."""
        ref = MsgRef(uetr=delivery.uetr, msg_type=delivery.msg_type, flow=delivery.flow)
        metadata = [("uetr", delivery.uetr), ("flow", delivery.flow)]
        if self.run_id:
            metadata.append(("run-id", self.run_id))

        started = now_ns()
        fmt = pb.format_name(delivery.format)
        self.recorder.m1_sending(delivery.uetr, started, fmt=fmt, flow=delivery.flow)
        stub = pb.HubInboundStub(self._open())
        try:
            if delivery.format == pb.MX:
                request = MxDelivery(
                    ref=ref,
                    app_hdr=delivery.app_hdr,
                    document=delivery.payload,
                    snf_ref=f"SNFIN{delivery.uetr[:6].upper()}",
                    network_ts_ns=started,
                )
                receipt = await stub.DeliverMx(
                    request, timeout=DEADLINE_DELIVER_S, metadata=metadata
                )
                method = "HubInbound.DeliverMx"
            else:
                request_fin = FinDelivery(
                    ref=ref,
                    fin_message=delivery.payload,
                    sender_bic=delivery.sender_bic,
                    network_ts_ns=started,
                )
                receipt = await stub.DeliverFin(
                    request_fin, timeout=DEADLINE_DELIVER_S, metadata=metadata
                )
                method = "HubInbound.DeliverFin"
        except grpc.aio.AioRpcError as exc:
            self.recorder.m1_done(
                delivery.uetr, started, fmt=fmt, flow=delivery.flow, accepted=False
            )
            self.recorder.record(
                method="HubInbound.Deliver",
                direction="MADE",
                outcome="ERROR",
                started_ns=started,
                uetr=delivery.uetr,
                flow=delivery.flow,
                msg_type=delivery.msg_type,
                run_id=self.run_id,
                error=exc.code().name,
            )
            raise

        self.recorder.m1_done(
            delivery.uetr,
            started,
            fmt=fmt,
            flow=delivery.flow,
            accepted=receipt.status == DeliveryReceipt.ACCEPTED,
        )
        self.recorder.record(
            method=method,
            direction="MADE",
            outcome=DeliveryReceipt.Status.Name(receipt.status),
            started_ns=started,
            uetr=delivery.uetr,
            flow=delivery.flow,
            fmt=fmt,
            msg_type=delivery.msg_type,
            run_id=self.run_id,
        )
        return DeliveryReceipt(
            status=receipt.status, accepted_ns=receipt.accepted_ns, reason=receipt.reason
        )

    async def send(self, deliveries: list[Delivery]) -> SendOutcome:
        """Deliver several messages in order, collecting the outcome.

        Cover legs are sent in order on purpose: the instruction leg first, so
        the Hub sees the pair form the way it would in production.
        """
        outcome = SendOutcome()
        for delivery in deliveries:
            try:
                receipt = await self.deliver(delivery)
            except grpc.aio.AioRpcError as exc:
                outcome.failed += 1
                outcome.errors.append(f"{delivery.msg_type}: {exc.code().name}")
                continue
            outcome.uetrs.append(delivery.uetr)
            if receipt.status == DeliveryReceipt.ACCEPTED:
                outcome.accepted += 1
            elif receipt.status == DeliveryReceipt.DUPLICATE:
                outcome.duplicate += 1
            else:
                outcome.rejected += 1
                outcome.errors.append(f"{delivery.msg_type}: {receipt.reason}")
        return outcome
