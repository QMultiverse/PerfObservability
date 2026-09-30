"""The FIN, SnF and FCC emulators.

Each one serves the gRPC service the real system serves and calls back into
the Hub edge exactly as the real system would, so switching the Hub from the
ESS to production is three addresses in configuration, not a code change.

Callbacks (``NotifyAck``, ``NotifyDeliveryNotification``, ``NotifyFccDecision``)
are scheduled as background tasks after a configured delay, because that is how
the real networks behave: the synchronous reply says "accepted", and what
happened arrives later.

Overrides let a named case force an outcome for one UETR — a NAK, a sanctions
hit, a block — without changing the profile every other payment sees.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import random
import time
from dataclasses import dataclass
from typing import Final

import grpc
from hub_model import proto as pb
from hub_model.envelope import now_ns
from hub_model.ids import new_uetr
from hub_model.proto import (
    FccDecision,
    NetworkAck,
    ScreenRequest,
    ScreenResult,
    SendAccepted,
    SendMtRequest,
    SendMxRequest,
)
from hub_telemetry.events import Events
from hub_telemetry.grpc_telemetry import DEADLINE_NOTIFY_S, channel

from .profiles import Profile, ProfileStore
from .recorder import Recorder

log = logging.getLogger(__name__)

SERVICE_NAME: Final = "ess"

# Party names the FCC emulator treats as a deterministic hit. Invented; see
# hub_format.samples.SANCTIONS_NAMES.
WATCHLIST: Final = frozenset({"REDLIST HOLDINGS SA", "BLOCKED VENTURES LLC"})
BLOCKLIST: Final = frozenset({"BLOCKED VENTURES LLC"})


@dataclass(slots=True)
class Override:
    """A forced outcome for one UETR, set by a named case."""

    nak: bool = False
    nak_code: str = ""
    hit: bool = False
    block: bool = False
    release: bool = True
    duplicate: bool = False
    reject_send: bool = False
    outage: bool = False
    decision_delay_s: float | None = None


class Overrides:
    """Per-UETR forced outcomes. Consumed once, then forgotten."""

    def __init__(self) -> None:
        self._by_uetr: dict[str, Override] = {}

    def set(self, uetr: str, override: Override) -> None:
        self._by_uetr[uetr] = override

    def get(self, uetr: str) -> Override | None:
        return self._by_uetr.get(uetr)

    def clear(self) -> None:
        self._by_uetr.clear()


class EmulatorBase:
    """Shared plumbing: profiles, latency, callbacks and recording."""

    target_name = ""

    def __init__(
        self,
        profiles: ProfileStore,
        recorder: Recorder,
        events: Events,
        *,
        hub_target: str,
        overrides: Overrides | None = None,
        rng: random.Random | None = None,
    ) -> None:
        self.profiles = profiles
        self.recorder = recorder
        self.events = events
        self.hub_target = hub_target
        self.overrides = overrides if overrides is not None else Overrides()
        self.rng = rng or random.Random()
        self._channel: grpc.aio.Channel | None = None
        self._tasks: set[asyncio.Task[None]] = set()

    @property
    def profile(self) -> Profile:
        return self.profiles.get(self.target_name)

    def hub_channel(self) -> grpc.aio.Channel:
        if self._channel is None:
            self._channel = channel(self.hub_target, self.events)
        return self._channel

    async def close(self) -> None:
        for task in list(self._tasks):
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()
        if self._channel is not None:
            await self._channel.close()
            self._channel = None

    async def pause_accept(self) -> None:
        await asyncio.sleep(self.profile.accept_latency.sample(self.rng))

    def schedule(self, coro: object) -> None:
        """Run a callback in the background, keeping a reference to the task."""
        task: asyncio.Task[None] = asyncio.create_task(coro)  # type: ignore[arg-type]
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def guard_outage(self, context: grpc.aio.ServicerContext, method: str) -> None:
        if self.profile.outage():
            self.recorder.record(
                method=method,
                direction="SERVED",
                outcome="UNAVAILABLE",
                started_ns=now_ns(),
                error="outage",
            )
            await context.abort(
                grpc.StatusCode.UNAVAILABLE, f"{self.target_name} is unavailable (injected outage)"
            )

    async def drain(self) -> None:
        """Wait for every scheduled callback. Used by cases and tests."""
        while self._tasks:
            await asyncio.gather(*list(self._tasks), return_exceptions=True)


class FinEmulator(EmulatorBase, pb.FinGatewayServicer):
    """Accepts outbound MT; returns an ACK or NAK after a delay."""

    target_name = "FIN"

    async def SendMt(  # noqa: N802 - gRPC method name
        self, request: SendMtRequest, context: grpc.aio.ServicerContext
    ) -> SendAccepted:
        started = now_ns()
        await self.guard_outage(context, "FinGateway.SendMt")
        await self.pause_accept()

        override = self.overrides.get(request.ref.uetr)
        if override and override.reject_send:
            self.recorder.record(
                method="FinGateway.SendMt",
                direction="SERVED",
                outcome="REJECTED",
                started_ns=started,
                uetr=request.ref.uetr,
                flow=request.ref.flow,
                fmt="MT",
                msg_type=request.ref.msg_type,
            )
            return SendAccepted(
                status=SendAccepted.REJECTED,
                reason="injected rejection",
                accepted_ns=now_ns(),
            )

        send_ref = f"ISN{self.rng.randint(100000, 999999)}"
        self.recorder.record(
            method="FinGateway.SendMt",
            direction="SERVED",
            outcome="ACCEPTED",
            started_ns=started,
            uetr=request.ref.uetr,
            flow=request.ref.flow,
            fmt="MT",
            msg_type=request.ref.msg_type,
        )
        self.schedule(self._send_ack(request.ref, send_ref, override))
        return SendAccepted(status=SendAccepted.ACCEPTED, send_ref=send_ref, accepted_ns=now_ns())

    async def _send_ack(self, ref: pb.MsgRef, send_ref: str, override: Override | None) -> None:
        await asyncio.sleep(self.profile.ack_latency.sample(self.rng))
        nak = override.nak if override else self.rng.random() < self.profile.nak_rate
        error_code = (override.nak_code if override else "") or ("T13" if nak else "")
        ack = NetworkAck(
            network=pb.FIN,
            ack=not nak,
            error_code=error_code,
            send_ref=send_ref,
            network_ts_ns=now_ns(),
        )
        ack.ref.CopyFrom(ref)
        repeat = (override and override.duplicate) or (
            self.rng.random() < self.profile.duplicate_rate
        )
        await self._notify(ack, repeat=bool(repeat))

    async def _notify(self, ack: NetworkAck, *, repeat: bool) -> None:
        stub = pb.HubNetworkEventsStub(self.hub_channel())
        for attempt in range(2 if repeat else 1):
            started = now_ns()
            try:
                await stub.NotifyAck(ack, timeout=DEADLINE_NOTIFY_S)
                outcome = "ACK" if ack.ack else "NAK"
                error = ""
            except grpc.aio.AioRpcError as exc:
                outcome = "ERROR"
                error = exc.code().name
                log.warning("NotifyAck failed for %s: %s", ack.ref.uetr, error)
            self.recorder.record(
                method="HubNetworkEvents.NotifyAck",
                direction="MADE",
                outcome=outcome if attempt == 0 else f"{outcome}_DUPLICATE",
                started_ns=started,
                uetr=ack.ref.uetr,
                flow=ack.ref.flow,
                fmt="MT",
                msg_type=ack.ref.msg_type,
                error=error,
            )


class SnfEmulator(EmulatorBase, pb.SnfGatewayServicer):
    """Accepts outbound MX; returns an ACK and an optional delivery notification."""

    target_name = "SNF"

    async def SendMx(  # noqa: N802 - gRPC method name
        self, request: SendMxRequest, context: grpc.aio.ServicerContext
    ) -> SendAccepted:
        started = now_ns()
        await self.guard_outage(context, "SnfGateway.SendMx")
        await self.pause_accept()

        override = self.overrides.get(request.ref.uetr)
        if override and override.reject_send:
            self.recorder.record(
                method="SnfGateway.SendMx",
                direction="SERVED",
                outcome="REJECTED",
                started_ns=started,
                uetr=request.ref.uetr,
                flow=request.ref.flow,
                fmt="MX",
                msg_type=request.ref.msg_type,
            )
            return SendAccepted(
                status=SendAccepted.REJECTED, reason="injected rejection", accepted_ns=now_ns()
            )

        send_ref = f"SNF{self.rng.randint(100000, 999999)}"
        self.recorder.record(
            method="SnfGateway.SendMx",
            direction="SERVED",
            outcome="ACCEPTED",
            started_ns=started,
            uetr=request.ref.uetr,
            flow=request.ref.flow,
            fmt="MX",
            msg_type=request.ref.msg_type,
        )
        self.schedule(
            self._send_ack(request.ref, send_ref, override, request.request_delivery_notification)
        )
        return SendAccepted(status=SendAccepted.ACCEPTED, send_ref=send_ref, accepted_ns=now_ns())

    async def _send_ack(
        self,
        ref: pb.MsgRef,
        send_ref: str,
        override: Override | None,
        wants_notification: bool,
    ) -> None:
        await asyncio.sleep(self.profile.ack_latency.sample(self.rng))
        nak = override.nak if override else self.rng.random() < self.profile.nak_rate
        error_code = (override.nak_code if override else "") or ("H50" if nak else "")
        ack = NetworkAck(
            network=pb.SNF,
            ack=not nak,
            error_code=error_code,
            send_ref=send_ref,
            network_ts_ns=now_ns(),
        )
        ack.ref.CopyFrom(ref)

        stub = pb.HubNetworkEventsStub(self.hub_channel())
        started = now_ns()
        try:
            await stub.NotifyAck(ack, timeout=DEADLINE_NOTIFY_S)
            outcome, error = ("ACK" if ack.ack else "NAK"), ""
        except grpc.aio.AioRpcError as exc:
            outcome, error = "ERROR", exc.code().name
            log.warning("NotifyAck failed for %s: %s", ref.uetr, error)
        self.recorder.record(
            method="HubNetworkEvents.NotifyAck",
            direction="MADE",
            outcome=outcome,
            started_ns=started,
            uetr=ref.uetr,
            flow=ref.flow,
            fmt="MX",
            msg_type=ref.msg_type,
            error=error,
        )

        if ack.ack and wants_notification and self.profile.delivery_notifications:
            await self._send_delivery_notification(ref, send_ref)

    async def _send_delivery_notification(self, ref: pb.MsgRef, send_ref: str) -> None:
        await asyncio.sleep(self.profile.ack_latency.sample(self.rng))
        notification = pb.DeliveryNotification(
            snf_ref=send_ref, delivered=True, network_ts_ns=now_ns()
        )
        notification.ref.CopyFrom(ref)
        stub = pb.HubNetworkEventsStub(self.hub_channel())
        started = now_ns()
        try:
            await stub.NotifyDeliveryNotification(notification, timeout=DEADLINE_NOTIFY_S)
            outcome, error = "DELIVERED", ""
        except grpc.aio.AioRpcError as exc:
            outcome, error = "ERROR", exc.code().name
        self.recorder.record(
            method="HubNetworkEvents.NotifyDeliveryNotification",
            direction="MADE",
            outcome=outcome,
            started_ns=started,
            uetr=ref.uetr,
            flow=ref.flow,
            fmt="MX",
            msg_type=ref.msg_type,
            error=error,
        )


class FccEmulator(EmulatorBase, pb.FccScreeningServicer):
    """Answers screening by watchlist name or by rate, and decides hits later."""

    target_name = "FCC"

    async def Screen(  # noqa: N802 - gRPC method name
        self, request: ScreenRequest, context: grpc.aio.ServicerContext
    ) -> ScreenResult:
        started = now_ns()
        await self.guard_outage(context, "FccScreening.Screen")
        await self.pause_accept()

        outcome, matched = self._decide(request)
        result = ScreenResult(outcome=outcome, screened_ns=now_ns())
        for name in matched:
            result.matched_parties.append(name)

        if outcome == ScreenResult.HIT_PENDING:
            result.case_id = f"CASE-{new_uetr()[:8].upper()}"
            result.reason = "party matched the test watchlist"
            self.schedule(self._decide_later(request.ref, result.case_id))
        elif outcome == ScreenResult.BLOCK:
            result.reason = "party matched the test blocklist"

        self.recorder.record(
            method="FccScreening.Screen",
            direction="SERVED",
            outcome=ScreenResult.Outcome.Name(outcome),
            started_ns=started,
            uetr=request.ref.uetr,
            flow=request.ref.flow,
            msg_type=request.ref.msg_type,
        )
        return result

    def _decide(self, request: ScreenRequest) -> tuple[ScreenResult.Outcome, list[str]]:
        override = self.overrides.get(request.ref.uetr)
        if override is not None:
            if override.block:
                return ScreenResult.BLOCK, ["forced by case"]
            if override.hit:
                return ScreenResult.HIT_PENDING, ["forced by case"]
            return ScreenResult.NO_HIT, []

        # Deterministic first: a name on the list always hits, whatever the
        # rate is, so a test can ask for a hit without tuning probabilities.
        matched = [p.name for p in request.parties if p.name.upper() in WATCHLIST]
        blocked = [p.name for p in request.parties if p.name.upper() in BLOCKLIST]
        if blocked:
            return ScreenResult.BLOCK, blocked
        if matched:
            return ScreenResult.HIT_PENDING, matched

        draw = self.rng.random()
        if draw < self.profile.block_rate:
            return ScreenResult.BLOCK, ["sampled by block_rate"]
        if draw < self.profile.block_rate + self.profile.hit_rate:
            return ScreenResult.HIT_PENDING, ["sampled by hit_rate"]
        return ScreenResult.NO_HIT, []

    async def _decide_later(self, ref: pb.MsgRef, case_id: str) -> None:
        """The analyst decision, minutes later in production."""
        override = self.overrides.get(ref.uetr)
        delay = (
            override.decision_delay_s
            if override and override.decision_delay_s is not None
            else self.profile.ack_latency.sample(self.rng)
        )
        await asyncio.sleep(delay)

        release = (
            override.release
            if override is not None
            else self.rng.random() < self.profile.release_rate
        )
        decision = FccDecision(
            case_id=case_id,
            decision=FccDecision.RELEASE if release else FccDecision.BLOCK,
            reason="analyst review complete",
            decided_ns=now_ns(),
        )
        decision.ref.CopyFrom(ref)

        stub = pb.HubComplianceStub(self.hub_channel())
        started = now_ns()
        try:
            await stub.NotifyFccDecision(decision, timeout=DEADLINE_NOTIFY_S)
            outcome, error = ("RELEASE" if release else "BLOCK"), ""
        except grpc.aio.AioRpcError as exc:
            outcome, error = "ERROR", exc.code().name
            log.warning("NotifyFccDecision failed for %s: %s", ref.uetr, error)
        self.recorder.record(
            method="HubCompliance.NotifyFccDecision",
            direction="MADE",
            outcome=outcome,
            started_ns=started,
            uetr=ref.uetr,
            flow=ref.flow,
            msg_type=ref.msg_type,
            error=error,
        )


async def wait_for(predicate: object, timeout_s: float, interval_s: float = 0.02) -> bool:
    """Poll ``predicate`` until it is true or the timeout passes."""
    deadline = time.monotonic() + timeout_s
    check = predicate  # type: ignore[assignment]
    while time.monotonic() < deadline:
        if check():  # type: ignore[operator]
            return True
        await asyncio.sleep(interval_s)
    return bool(check())  # type: ignore[operator]


@contextlib.asynccontextmanager
async def emulators_closed(*instances: EmulatorBase):  # type: ignore[no-untyped-def]
    try:
        yield instances
    finally:
        for instance in instances:
            await instance.close()
