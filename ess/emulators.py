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
from collections.abc import Awaitable, Callable
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
from hub_telemetry.grpc_telemetry import DEADLINE_NOTIFY_S, RETRYABLE, channel
from hub_telemetry.metrics import REGISTRY
from prometheus_client import Counter as PromCounter

from .profiles import Profile, ProfileStore
from .recorder import Recorder

log = logging.getLogger(__name__)

SERVICE_NAME: Final = "ess"

# Network notifications (ACK, delivery notification, FCC decision) are
# redelivered until the Hub takes them, as a real FIN / SnF interface keeps an
# undelivered ACK queued. Giving up after one call left payments DISPATCHED
# forever (found by perf scenarios 1, 3 and 4). The Hub de-duplicates repeats.
#
# Redelivery is bounded, because unbounded retries turn a slow Hub into an
# overloaded one (the retry storm found by perf scenario 2): one call per round
# (the channel does not retry on top), backoff from 2 s to 60 s with jitter, and
# at most REDELIVERY_RATE_PER_S redeliveries a second per emulator, however
# many notifications are waiting.
REDELIVERY_HORIZON_S: Final = 600.0
REDELIVERY_FIRST_S: Final = 2.0
REDELIVERY_MAX_S: Final = 60.0
REDELIVERY_RATE_PER_S: Final = 5.0
#: What a redelivery retries. Wider than the Hub's own RETRYABLE: under
#: overload the Hub's side can answer CANCELLED, and abandoning on it left 276
#: payments DISPATCHED (perf scenario 2). A repeat is harmless, the Hub
#: de-duplicates notifications.
REDELIVERABLE: Final = RETRYABLE | {grpc.StatusCode.CANCELLED}

ess_redeliveries = PromCounter(
    "ess_notify_redeliveries_total",
    "Notifications to the Hub sent again after a failed attempt",
    ["method", "error"],
    registry=REGISTRY,
)
ess_notify_abandoned = PromCounter(
    "ess_notify_abandoned_total",
    "Notifications the ESS gave up on: not retryable, or past the redelivery horizon",
    ["method", "error"],
    registry=REGISTRY,
)

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


class RateBudget:
    """A token bucket: at most ``rate`` takes a second, bursting to ``burst``."""

    def __init__(
        self,
        rate: float,
        burst: float = 1.0,
        *,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.rate = rate
        self.burst = burst
        self._clock = clock
        self._tokens = burst
        self._last = clock()
        self._lock = asyncio.Lock()

    def _refill(self) -> None:
        now = self._clock()
        self._tokens = min(self.burst, self._tokens + (now - self._last) * self.rate)
        self._last = now

    async def take(self, sleep: Callable[[float], Awaitable[None]] = asyncio.sleep) -> None:
        """Wait for a token. Callers queue in order behind the lock."""
        async with self._lock:
            self._refill()
            if self._tokens < 1.0:
                await sleep((1.0 - self._tokens) / self.rate)
                self._refill()
            self._tokens -= 1.0


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
        self.redelivery_budget = RateBudget(REDELIVERY_RATE_PER_S)

    @property
    def profile(self) -> Profile:
        return self.profiles.get(self.target_name)

    def hub_channel(self) -> grpc.aio.Channel:
        if self._channel is None:
            # One attempt per call: notify_reliably is the retry policy.
            self._channel = channel(self.hub_target, self.events, max_attempts=1)
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

    async def notify_reliably(
        self,
        call: Callable[[], Awaitable[object]],
        *,
        method: str,
        ref: pb.MsgRef,
        outcome: str,
        fmt: str = "",
    ) -> str:
        """Deliver one notification to the Hub, redelivering until it lands.

        Every attempt is recorded. Returns ``outcome`` once the Hub takes it,
        or ``ERROR`` if the Hub refused it for good or the horizon ran out.
        """
        give_up_at = time.monotonic() + REDELIVERY_HORIZON_S
        delay = REDELIVERY_FIRST_S
        first = True
        while True:
            if not first:
                # Redeliveries share a budget; first attempts never wait on it.
                await self.redelivery_budget.take(self.sleep)
            first = False
            started = now_ns()
            try:
                await call()
            except grpc.aio.AioRpcError as exc:
                error = exc.code().name
                self.recorder.record(
                    method=method,
                    direction="MADE",
                    outcome="ERROR",
                    started_ns=started,
                    uetr=ref.uetr,
                    flow=ref.flow,
                    fmt=fmt,
                    msg_type=ref.msg_type,
                    error=error,
                )
                wait_s = delay * self.rng.uniform(0.5, 1.0)
                if exc.code() not in REDELIVERABLE or time.monotonic() + wait_s > give_up_at:
                    ess_notify_abandoned.labels(method, error).inc()
                    log.error("%s for %s abandoned: %s", method, ref.uetr, error)
                    return "ERROR"
                ess_redeliveries.labels(method, error).inc()
                log.warning("%s for %s failed (%s); redelivering", method, ref.uetr, error)
                await self.sleep(wait_s)
                delay = min(delay * 2, REDELIVERY_MAX_S)
                continue
            self.recorder.record(
                method=method,
                direction="MADE",
                outcome=outcome,
                started_ns=started,
                uetr=ref.uetr,
                flow=ref.flow,
                fmt=fmt,
                msg_type=ref.msg_type,
            )
            return outcome

    async def sleep(self, seconds: float) -> None:
        """Backoff between redeliveries; tests replace it to skip the wait."""
        await asyncio.sleep(seconds)

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
        self.recorder.m2_received(request.ref.uetr, fmt="MT")

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
        outcome = "ACK" if ack.ack else "NAK"
        for attempt in range(2 if repeat else 1):
            await self.notify_reliably(
                lambda: stub.NotifyAck(ack, timeout=DEADLINE_NOTIFY_S),
                method="HubNetworkEvents.NotifyAck",
                ref=ack.ref,
                outcome=outcome if attempt == 0 else f"{outcome}_DUPLICATE",
                fmt="MT",
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
        self.recorder.m2_received(request.ref.uetr, fmt="MX")

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
        await self.notify_reliably(
            lambda: stub.NotifyAck(ack, timeout=DEADLINE_NOTIFY_S),
            method="HubNetworkEvents.NotifyAck",
            ref=ref,
            outcome="ACK" if ack.ack else "NAK",
            fmt="MX",
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
        await self.notify_reliably(
            lambda: stub.NotifyDeliveryNotification(notification, timeout=DEADLINE_NOTIFY_S),
            method="HubNetworkEvents.NotifyDeliveryNotification",
            ref=ref,
            outcome="DELIVERED",
            fmt="MX",
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
        await self.notify_reliably(
            lambda: stub.NotifyFccDecision(decision, timeout=DEADLINE_NOTIFY_S),
            method="HubCompliance.NotifyFccDecision",
            ref=ref,
            outcome="RELEASE" if release else "BLOCK",
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
