"""The External Systems Simulator: cases, profiles, control API and recorder."""

from __future__ import annotations

import pytest
from hub_model import proto as pb

from ess.cases import CASES, CaseRunner, StatusProbe
from ess.profiles import FIXED, LOGNORMAL, Latency, Profile, ProfileStore
from ess.recorder import Recorder
from tests.harness import Harness


class HarnessProbe(StatusProbe):
    """Reads the in-process status view instead of the HTTP API."""

    def __init__(self, harness: Harness) -> None:
        super().__init__("")
        self.harness = harness

    def state_of(self, uetr: str) -> str:
        return self.harness.state_of(uetr)

    async def wait_for_state(self, uetr: str, expected: str, timeout_s: float) -> tuple[bool, str]:
        actual = await self.harness.run_until_state(uetr, expected, timeout_s=timeout_s)
        return actual == expected, actual


def _runner(harness: Harness) -> CaseRunner:
    return CaseRunner(
        harness.ess.sender, harness.ess.overrides, harness.ess.recorder, HarnessProbe(harness)
    )


# ---------------------------------------------------------------- cases
@pytest.mark.parametrize("case_id", sorted(CASES))
async def test_every_case_passes(hub: Harness, case_id: str) -> None:
    """The catalogue CI runs on every Hub change."""
    outcome = await _runner(hub).run(case_id, timeout_ms=20_000)
    assert outcome.passed, f"{case_id}: {outcome.detail}"
    assert outcome.uetrs


async def test_a_case_reports_its_steps(hub: Harness) -> None:
    outcome = await _runner(hub).run("pacs008_happy", timeout_ms=20_000)
    names = [step.name for step in outcome.steps]
    assert any(name.startswith("deliver") for name in names)
    assert any(name.startswith("await") for name in names)
    assert all(step.passed for step in outcome.steps)


async def test_an_unknown_case_fails_cleanly(hub: Harness) -> None:
    outcome = await _runner(hub).run("no_such_case")
    assert not outcome.passed
    assert "unknown case" in outcome.detail


async def test_a_case_can_be_repeated(hub: Harness) -> None:
    outcome = await _runner(hub).run("pacs008_happy", repeat=3, timeout_ms=20_000)
    assert outcome.passed
    assert len(outcome.uetrs) == 3
    assert len(set(outcome.uetrs)) == 3, "each repeat gets its own UETR"


# ------------------------------------------------------------ control API
async def test_run_case_through_the_control_api(hub: Harness) -> None:
    hub.ess.runner.probe = HarnessProbe(hub)
    result = await hub.ess.control.RunCase(
        pb.CaseRequest(case_id="mt103_happy", run_id="S99-TEST", timeout_ms=20_000), None
    )
    assert result.passed, result.detail
    assert result.case_id == "mt103_happy"
    assert result.duration_ms >= 0


async def test_list_cases_through_the_control_api(hub: Harness) -> None:
    listed = await hub.ess.control.ListCases(pb.ListCasesRequest(), None)
    assert {case.case_id for case in listed.cases} == set(CASES)
    for case in listed.cases:
        assert case.expected_state in {"COMPLETED", "REJECTED", "BLOCKED", "HELD"}


async def test_send_inbound_through_the_control_api(hub: Harness) -> None:
    result = await hub.ess.control.SendInbound(
        pb.SendInboundRequest(msg_type="pacs.008", count=2, run_id="S99-TEST"), None
    )
    assert result.accepted == 2
    assert result.rejected == 0
    assert len(result.uetrs) == 2

    for uetr in result.uetrs:
        assert await hub.run_until_state(uetr, "COMPLETED") == "COMPLETED"


async def test_counters_are_reported_per_run(hub: Harness) -> None:
    await hub.ess.control.SendInbound(
        pb.SendInboundRequest(msg_type="pacs.008", run_id="S99-TEST"), None
    )
    await hub.settle(timeout_s=5.0)

    counters = await hub.ess.control.GetCounters(pb.CounterRequest(), None)
    assert counters.calls_made >= 1
    assert counters.by_method["HubInbound.DeliverMx"] >= 1
    assert counters.by_flow["MX_SNF_PACS008"] >= 1

    scoped = await hub.ess.control.GetCounters(pb.CounterRequest(run_id="S99-TEST"), None)
    assert scoped.by_outcome.get("ACCEPTED", 0) >= 1

    empty = await hub.ess.control.GetCounters(pb.CounterRequest(run_id="nobody"), None)
    assert dict(empty.by_outcome) == {}


async def test_reset_clears_counters_and_profiles(hub: Harness) -> None:
    await hub.ess.control.SendInbound(pb.SendInboundRequest(msg_type="pacs.008"), None)
    await hub.ess.control.SetProfile(
        pb.BehaviourProfile(target=pb.Target.COMPLIANCE, hit_rate=0.5), None
    )
    assert hub.ess.profiles.get("COMPLIANCE").hit_rate == 0.5

    applied = await hub.ess.control.Reset(pb.ResetRequest(), None)
    assert applied.applied
    assert hub.ess.profiles.get("COMPLIANCE").hit_rate == 0.0
    counters = await hub.ess.control.GetCounters(pb.CounterRequest(), None)
    assert counters.calls_made == 0


async def test_set_profile_changes_behaviour_at_runtime(hub: Harness) -> None:
    applied = await hub.ess.control.SetProfile(
        pb.BehaviourProfile(
            target=pb.Target.FIN,
            accept_latency=pb.LatencyDist(kind="fixed", p50_ms=7.0),
            nak_rate=0.25,
        ),
        None,
    )
    assert applied.applied
    profile = hub.ess.profiles.get("FIN")
    assert profile.accept_latency.p50_ms == 7.0
    assert profile.nak_rate == 0.25
    assert hub.ess.profiles.get("SNF").nak_rate == 0.0, "only FIN was targeted"


async def test_set_profile_for_all_targets(hub: Harness) -> None:
    await hub.ess.control.SetProfile(pb.BehaviourProfile(target=pb.Target.ALL, nak_rate=0.1), None)
    for target in ("FIN", "SNF", "COMPLIANCE"):
        assert hub.ess.profiles.get(target).nak_rate == 0.1


async def test_an_outage_is_time_limited(hub: Harness) -> None:
    await hub.ess.control.SetProfile(
        pb.BehaviourProfile(target=pb.Target.FIN, outage=True, outage_for_ms=1), None
    )
    profile = hub.ess.profiles.get("FIN")
    assert profile.outage_until_ns > 0
    import time

    assert not profile.outage(now_ns=time.time_ns() + 2_000_000)


# -------------------------------------------------------------- profiles
def test_functional_defaults_are_deterministic_and_fault_free() -> None:
    store = ProfileStore("functional")
    for target in ("FIN", "SNF", "COMPLIANCE"):
        profile = store.get(target)
        assert profile.accept_latency.kind == FIXED
        assert profile.nak_rate == 0.0
        assert profile.hit_rate == 0.0
        assert not profile.outage()


def test_performance_defaults_draw_from_a_distribution() -> None:
    store = ProfileStore("performance")
    assert store.get("FIN").accept_latency.kind == LOGNORMAL
    assert store.get("COMPLIANCE").hit_rate > 0
    assert store.get("COMPLIANCE").ack_latency.p50_ms > 1000, "analyst decisions take minutes"


def test_fixed_latency_is_exactly_its_p50() -> None:
    import random

    latency = Latency(FIXED, 25.0, 900.0)
    assert latency.sample(random.Random(0)) == pytest.approx(0.025)


def test_a_lognormal_latency_has_roughly_the_requested_median() -> None:
    import random
    import statistics

    latency = Latency(LOGNORMAL, 20.0, 200.0)
    rng = random.Random(7)
    draws = [latency.sample(rng) * 1000 for _ in range(4000)]
    assert 17 < statistics.median(draws) < 24
    assert max(draws) > 100, "the tail must actually be long"
    assert all(d >= 0 for d in draws)


def test_an_indefinite_outage_lasts_until_cleared() -> None:
    profile = Profile(target="FIN", outage_until_ns=-1)
    assert profile.outage()
    assert profile.outage(now_ns=2**62)


def test_profiles_reset_to_their_mode_defaults() -> None:
    store = ProfileStore("functional")
    store.set(Profile(target="COMPLIANCE", hit_rate=0.9, outage_until_ns=-1))
    store.reset()
    assert store.get("COMPLIANCE").hit_rate == 0.0
    assert not store.get("COMPLIANCE").outage()


# -------------------------------------------------------------- recorder
def test_the_recorder_counts_by_method_flow_and_outcome() -> None:
    recorder = Recorder()
    recorder.record(
        method="FinGateway.SendMt",
        direction="SERVED",
        outcome="ACCEPTED",
        started_ns=1,
        flow="MT_FIN_103",
        run_id="S1",
    )
    recorder.record(
        method="HubNetworkEvents.NotifyAck",
        direction="MADE",
        outcome="ACK",
        started_ns=1,
        flow="MT_FIN_103",
        run_id="S1",
        error="",
    )
    counters = recorder.counters()
    assert counters["by_method"]["FinGateway.SendMt"] == 1
    assert counters["by_flow"]["MT_FIN_103"] == 2
    assert counters["by_outcome"]["ACK"] == 1
    assert recorder.totals() == (1, 1, 0)


def test_the_recorder_ring_is_bounded() -> None:
    recorder = Recorder(max_rows=10)
    for index in range(50):
        recorder.record(method="m", direction="MADE", outcome="ok", started_ns=1, uetr=str(index))
    assert len(recorder) == 10
    assert recorder.dropped == 40
    assert recorder.totals()[1] == 50, "counters still see every call"


def test_the_recorder_flushes_to_json_lines(tmp_path) -> None:  # type: ignore[no-untyped-def]
    import json

    recorder = Recorder()
    recorder.record(method="m", direction="MADE", outcome="ok", started_ns=1, uetr="u1")
    path, fmt = recorder.flush(tmp_path / "rows.jsonl")

    assert fmt == "jsonl"
    rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
    assert rows[0]["uetr"] == "u1"
    assert rows[0]["method"] == "m"
    assert len(recorder) == 0, "flush clears the ring by default"


def test_flushing_an_empty_recorder_writes_nothing(tmp_path) -> None:  # type: ignore[no-untyped-def]
    path, fmt = Recorder().flush(tmp_path / "rows.jsonl")
    assert fmt == "empty"
    assert not path.exists()


async def test_the_recorder_sees_both_sides_of_a_payment(hub: Harness) -> None:
    delivery = hub.ess.sender.build("pacs.008")
    await hub.ess.sender.deliver(delivery)
    await hub.run_until_state(delivery.uetr, "COMPLETED")

    # The Hub can mark the payment COMPLETED a moment before the emulator's own
    # NotifyAck call returns and is recorded, so wait for the row, not the state.
    def recorded() -> bool:
        return "HubNetworkEvents.NotifyAck" in hub.ess.recorder.counters()["by_method"]

    assert await hub.settle(until=recorded, timeout_s=10.0)

    methods = hub.ess.recorder.counters()["by_method"]
    assert methods["HubInbound.DeliverMx"] == 1  # made by the sender
    assert methods["SnfGateway.SendMx"] == 1  # served by the SnF emulator
    assert methods["HubNetworkEvents.NotifyAck"] == 1  # made by the SnF emulator


async def test_a_retried_delivery_is_reported_as_a_duplicate(hub: Harness) -> None:
    """The Deliver* deadline is 200 ms. A slow first attempt is retried, and the
    edge answers DUPLICATE rather than creating a second payment — so the result
    has to say so, or an accepted payment looks like nothing happened.
    """
    delivery = hub.ess.sender.build("pacs.008")
    first = await hub.ess.sender.deliver(delivery)
    assert first.status == pb.DeliveryReceipt.ACCEPTED

    outcome = await hub.ess.sender.send([delivery])
    assert outcome.duplicate == 1
    assert outcome.accepted == 0
    assert outcome.rejected == 0
    assert outcome.ok, "a duplicate is not a failure"


async def test_send_inbound_reports_duplicates(hub: Harness) -> None:
    uetr = hub.ess.sender.build("pacs.008").uetr
    request = pb.SendInboundRequest(msg_type="pacs.008", uetr=uetr)

    first = await hub.ess.control.SendInbound(request, None)
    assert first.accepted == 1
    assert first.duplicate == 0

    second = await hub.ess.control.SendInbound(request, None)
    assert second.accepted == 0
    assert second.duplicate == 1, "a repeat must be visible in the result"
    assert second.rejected == 0
