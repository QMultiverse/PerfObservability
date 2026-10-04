"""Bug B from the performance scenarios: lost network ACKs.

The ESS gave up on an ACK after one failed call, and nothing in the Hub
noticed, so the payment stayed DISPATCHED forever. Now the ESS redelivers, as a
real network keeps an undelivered ACK queued, and the Hub reports any payment
still waiting past its timeout.
"""

from __future__ import annotations

import grpc
import pytest
from hub_model.envelope import now_ns
from hub_model.flows import FLOW_MX_SNF_PACS008, PACS_008
from hub_model.proto import MsgRef
from hub_telemetry import metrics
from hub_telemetry.events import Events

import hub.ack_matcher.processor as ack_matcher
from ess.emulators import EmulatorBase
from ess.profiles import FIXED, Latency, Profile, ProfileStore
from ess.recorder import Recorder
from tests.harness import Harness

REF = MsgRef(uetr="22222222-2222-4222-8222-222222222222", msg_type="pacs.008", flow="F")


def emulator() -> EmulatorBase:
    emu = EmulatorBase(ProfileStore(), Recorder(enabled=False), Events("ess"), hub_target="x")

    async def no_wait(seconds: float) -> None:
        return None

    emu.sleep = no_wait  # type: ignore[method-assign]
    return emu


def failing(times: int, code: grpc.StatusCode):  # type: ignore[no-untyped-def]
    calls = {"n": 0}

    async def call() -> object:
        calls["n"] += 1
        if calls["n"] <= times:
            raise grpc.aio.AioRpcError(code, None, None, "injected", None)
        return object()

    return call, calls


def sample(name: str, labels: dict[str, str]) -> float:
    return metrics.REGISTRY.get_sample_value(name, labels) or 0.0


async def test_an_ack_the_hub_misses_is_redelivered_until_it_lands() -> None:
    call, calls = failing(4, grpc.StatusCode.DEADLINE_EXCEEDED)
    labels = {"method": "HubNetworkEvents.NotifyAck", "error": "DEADLINE_EXCEEDED"}
    before = sample("ess_notify_redeliveries_total", labels)

    outcome = await emulator().notify_reliably(
        call, method="HubNetworkEvents.NotifyAck", ref=REF, outcome="ACK"
    )

    assert outcome == "ACK"
    assert calls["n"] == 5
    assert sample("ess_notify_redeliveries_total", labels) == before + 4


async def test_a_cancelled_notification_is_redelivered() -> None:
    """Under overload the Hub's side answered CANCELLED; that must not abandon an ACK."""
    call, calls = failing(2, grpc.StatusCode.CANCELLED)
    outcome = await emulator().notify_reliably(
        call, method="HubNetworkEvents.NotifyAck", ref=REF, outcome="ACK"
    )
    assert outcome == "ACK"
    assert calls["n"] == 3


async def test_a_refused_notification_is_not_retried() -> None:
    call, calls = failing(99, grpc.StatusCode.INVALID_ARGUMENT)
    labels = {"method": "HubNetworkEvents.NotifyAck", "error": "INVALID_ARGUMENT"}
    before = sample("ess_notify_abandoned_total", labels)

    outcome = await emulator().notify_reliably(
        call, method="HubNetworkEvents.NotifyAck", ref=REF, outcome="ACK"
    )

    assert outcome == "ERROR"
    assert calls["n"] == 1
    assert sample("ess_notify_abandoned_total", labels) == before + 1


async def test_redelivery_stops_at_the_horizon(monkeypatch: pytest.MonkeyPatch) -> None:
    import ess.emulators as emulators

    monkeypatch.setattr(emulators, "REDELIVERY_HORIZON_S", 2.0)
    call, calls = failing(99, grpc.StatusCode.UNAVAILABLE)
    outcome = await emulator().notify_reliably(
        call, method="HubNetworkEvents.NotifyAck", ref=REF, outcome="ACK"
    )
    assert outcome == "ERROR"
    assert 1 < calls["n"] < 99


async def test_a_payment_waiting_for_its_ack_is_reported_overdue(
    hub: Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    # SnF takes the payment but its ACK does not come back for a minute.
    hub.ess.profiles.set(Profile(target="SNF", ack_latency=Latency(FIXED, 60_000.0)))
    delivery = hub.ess.sender.build(PACS_008, flow=FLOW_MX_SNF_PACS008)
    await hub.ess.sender.deliver(delivery)
    assert await hub.run_until_state(delivery.uetr, "DISPATCHED") == "DISPATCHED"
    await hub.settle(timeout_s=1.0)

    waiting = {"service": ack_matcher.SERVICE_NAME, "overdue": "false"}
    overdue = {"service": ack_matcher.SERVICE_NAME, "overdue": "true"}
    assert sample("hub_awaiting_ack", waiting) == 1
    assert sample("hub_awaiting_ack", overdue) == 0

    # Past the timeout, the same payment counts as overdue.
    later = now_ns() + int((ack_matcher.ACK_OVERDUE_S + 1) * 1e9)
    monkeypatch.setattr(ack_matcher, "now_ns", lambda: later)
    assert sample("hub_awaiting_ack", overdue) == 1


# ------------------------------------------- bounded redelivery (retry storm)
class FakeClock:
    def __init__(self) -> None:
        self.now = 100.0

    def __call__(self) -> float:
        return self.now


async def test_the_redelivery_budget_caps_the_rate() -> None:
    from ess.emulators import RateBudget

    clock = FakeClock()
    budget = RateBudget(5.0, clock=clock)
    waited: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        waited.append(seconds)
        clock.now += seconds

    for _ in range(11):
        await budget.take(fake_sleep)
    # 1 token up front, then 10 more at 5 a second: 2 s of waiting in all.
    assert sum(waited) == pytest.approx(2.0)


async def test_only_redeliveries_draw_on_the_budget() -> None:
    emu = emulator()
    takes = {"n": 0}

    async def counting_take(sleep: object = None) -> None:
        takes["n"] += 1

    emu.redelivery_budget.take = counting_take  # type: ignore[method-assign]
    ok_call, _ = failing(0, grpc.StatusCode.UNAVAILABLE)
    await emu.notify_reliably(ok_call, method="m", ref=REF, outcome="ACK")
    assert takes["n"] == 0, "a first attempt never waits on the budget"

    retry_call, calls = failing(3, grpc.StatusCode.UNAVAILABLE)
    await emu.notify_reliably(retry_call, method="m", ref=REF, outcome="ACK")
    assert takes["n"] == 3
    assert calls["n"] == 4, "one call per round: nothing retries underneath"


async def test_the_ess_channel_to_the_hub_does_not_retry_on_its_own() -> None:
    from hub_telemetry.grpc_telemetry import ClientTelemetryInterceptor

    emu = emulator()
    chan = emu.hub_channel()
    interceptors = [
        i
        for i in getattr(chan, "_unary_unary_interceptors", [])
        if isinstance(i, ClientTelemetryInterceptor)
    ]
    assert interceptors, "expected the telemetry interceptor on the channel"
    assert all(i._max_attempts == 1 for i in interceptors)
    await emu.close()


async def test_a_late_dispatched_record_does_not_reopen_a_completed_payment(
    hub: Harness,
) -> None:
    """The ACK race: COMPLETED first, the dispatcher's DISPATCHED record after.

    It used to put the finished payment back in the ACK matcher's store, where
    it counted as a payment waiting for its ACK forever (a false overdue).
    """
    from hub_model import proto as pb
    from hub_model import topics as tp
    from hub_model.envelope import advance, state_record

    delivery = hub.ess.sender.build(PACS_008, flow=FLOW_MX_SNF_PACS008)
    await hub.ess.sender.deliver(delivery)
    assert await hub.run_until_state(delivery.uetr, "COMPLETED") == "COMPLETED"

    matcher = hub.processor("ack-matcher")
    env = matcher.store.envelope(delivery.uetr)
    assert env is None, "a completed payment leaves the working store"

    late = pb.PaymentEnvelope()
    late.ref.uetr = delivery.uetr
    late.ref.msg_type = PACS_008
    late.ref.flow = FLOW_MX_SNF_PACS008
    record = state_record(advance(late, pb.DISPATCHED), stages_done=("dispatched",))
    hub.bus.producer().produce(tp.PAY_STATE, delivery.uetr, record.SerializeToString())
    await hub.settle(timeout_s=1.0)

    assert matcher.store.get(delivery.uetr) is None
    assert matcher.store.count_in_state(pb.DISPATCHED) == 0
