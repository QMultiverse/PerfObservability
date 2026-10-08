"""The metrics behind the Grafana performance dashboard (03-performance.json)."""

from __future__ import annotations

import gc
import time

import pytest
from hub_model import proto as pb
from hub_model.proto import PaymentEnvelope
from hub_telemetry import metrics
from hub_telemetry.grpc_telemetry import split_method
from hub_telemetry.memory_bus import MemoryBus
from prometheus_client import CollectorRegistry

from ess.recorder import Recorder
from hub.common.state_store import StateStore


def sample(name: str, labels: dict[str, str] | None = None) -> float:
    return metrics.REGISTRY.get_sample_value(name, labels or {}) or 0.0


# ------------------------------------------------------------- WindowedMax
class FakeClock:
    def __init__(self) -> None:
        self.now = 1_000.0

    def __call__(self) -> float:
        return self.now


def windowed(clock: FakeClock) -> metrics.WindowedMax:
    return metrics.WindowedMax(
        "test_max", "test", ["service"], window_s=30.0, registry=None, clock=clock
    )


def test_windowed_max_keeps_the_largest_value() -> None:
    clock = FakeClock()
    peak = windowed(clock)
    for value in (0.2, 0.9, 0.4):
        peak.observe(("a",), value)
    assert peak.value(("a",)) == 0.9


def test_a_spike_survives_one_window_then_falls_away() -> None:
    clock = FakeClock()
    peak = windowed(clock)
    peak.observe(("a",), 0.9)

    clock.now += 31  # next window: the spike is now "previous", still reported
    peak.observe(("a",), 0.1)
    assert peak.value(("a",)) == 0.9

    clock.now += 30  # and one more: gone
    assert peak.value(("a",)) == 0.1


def test_a_long_silence_resets_the_maximum() -> None:
    clock = FakeClock()
    peak = windowed(clock)
    peak.observe(("a",), 0.9)
    clock.now += 120
    assert peak.value(("a",)) == 0.0


def test_windowed_max_is_exposed_as_a_gauge() -> None:
    registry = CollectorRegistry()
    peak = metrics.WindowedMax("test_peak_seconds", "test", ["service"], registry=registry)
    peak.observe(("hub-routing",), 0.25)
    assert registry.get_sample_value("test_peak_seconds", {"service": "hub-routing"}) == 0.25


# -------------------------------------------------------------- state store
def envelope(uetr: str) -> PaymentEnvelope:
    env = PaymentEnvelope()
    env.ref.uetr = uetr
    env.ref.msg_type = "pacs.008"
    env.state = pb.SCREENED
    return env


def test_state_store_reports_keys_and_bytes() -> None:
    store = StateStore()
    for n in range(5):
        store.record(envelope(f"00000000-0000-4000-8000-00000000000{n}"), stages=("screened",))
    store.complete("00000000-0000-4000-8000-000000000000")

    working, completed, approx = store.size_stats()
    assert (working, completed) == (4, 1)
    assert approx > 4 * 36


def test_sampled_size_extrapolates_to_the_whole_store() -> None:
    store = StateStore()
    for n in range(50):
        store.record(envelope(f"00000000-0000-4000-8000-{n:012d}"))
    exact = store.size_stats(sample=50)[2]
    estimated = store.size_stats(sample=10)[2]
    assert estimated == pytest.approx(exact, rel=0.05)


def test_registered_stores_appear_per_service() -> None:
    store = StateStore()
    store.record(envelope("00000000-0000-4000-8000-000000000001"))
    metrics.state_stores.register("test-stage", store)
    try:
        labels = {"service": "test-stage", "kind": "working"}
        assert sample("hub_state_store_keys", labels) == 1
        assert sample("hub_state_store_bytes", {"service": "test-stage"}) > 0
    finally:
        metrics.state_stores.unregister("test-stage")
    assert metrics.REGISTRY.get_sample_value("hub_state_store_keys", labels) is None


# ------------------------------------------------------------------ runtime
def test_runtime_collectors_install_once_and_time_gc_pauses() -> None:
    metrics.install_runtime_collectors()
    metrics.install_runtime_collectors()  # idempotent: a second call must not raise

    before = sample("hub_python_gc_pause_seconds_count", {"generation": "2"})
    gc.collect()
    assert sample("hub_python_gc_pause_seconds_count", {"generation": "2"}) == before + 1
    assert sample("python_gc_collections_total", {"generation": "2"}) > 0
    assert sample("hub_python_allocated_blocks") > 0


def test_rebalances_are_counted_per_group() -> None:
    labels = {"group": "cg-test-rebalance", "event": "assign"}
    before = sample("hub_kafka_rebalances_total", labels)
    bus = MemoryBus()
    bus.consumer("cg-test-rebalance", ["t"])
    bus.consumer("cg-test-rebalance", ["t"])
    assert sample("hub_kafka_rebalances_total", labels) == before + 2


# --------------------------------------------------------------------- gRPC
@pytest.mark.parametrize(
    ("method", "expected"),
    [
        ("/hub.v1.HubInbound/DeliverMx", ("hub.v1.HubInbound", "DeliverMx")),
        ("/ext.v1.ComplianceScreening/Screen", ("ext.v1.ComplianceScreening", "Screen")),
        ("Bare", ("unknown", "Bare")),
    ],
)
def test_grpc_method_paths_split_into_service_and_method(
    method: str, expected: tuple[str, str]
) -> None:
    assert split_method(method) == expected


# ------------------------------------------------- simulator M1 -> M2 latency
UETR = "11111111-1111-4111-8111-111111111111"


def test_m2_stops_the_clock_m1_started() -> None:
    recorder = Recorder(enabled=False)
    labels = {"format": "MX", "flow": "MX_SNF_PACS008"}
    sent_before = sample("ess_m1_sent_total", labels)
    count_before = sample("ess_e2e_latency_seconds_count", labels)

    sent = time.time_ns() - 50_000_000  # 50 ms ago
    recorder.m1_sending(UETR, sent, fmt="MX", flow="MX_SNF_PACS008")
    recorder.m1_done(UETR, sent, fmt="MX", flow="MX_SNF_PACS008", accepted=True)
    latency = recorder.m2_received(UETR, fmt="MX")

    assert latency is not None and latency >= 0.05
    assert sample("ess_m1_sent_total", labels) == sent_before + 1
    assert sample("ess_e2e_latency_seconds_count", labels) == count_before + 1
    assert recorder.pending_count() == 0


def test_an_m2_that_beats_the_receipt_still_counts_the_m1() -> None:
    recorder = Recorder(enabled=False)
    labels = {"format": "MT", "flow": "MT_FIN_103"}
    before = sample("ess_m1_sent_total", labels)
    sent = time.time_ns()
    recorder.m1_sending(UETR, sent, fmt="MT", flow="MT_FIN_103")
    assert recorder.m2_received(UETR, fmt="MT") is not None
    recorder.m1_done(UETR, sent, fmt="MT", flow="MT_FIN_103", accepted=True)
    assert sample("ess_m1_sent_total", labels) == before + 1


def test_a_cover_pair_matches_first_in_first_out() -> None:
    recorder = Recorder(enabled=False)
    first = time.time_ns() - 200_000_000
    second = first + 100_000_000
    for sent in (first, second):
        recorder.m1_sending(UETR, sent, fmt="MT", flow="MT_FIN_103_202COV")

    older = recorder.m2_received(UETR, fmt="MT")
    newer = recorder.m2_received(UETR, fmt="MT")
    assert older is not None and newer is not None
    assert older > newer
    assert recorder.pending_count() == 0


def test_a_repeated_m2_is_unmatched_and_adds_no_latency() -> None:
    recorder = Recorder(enabled=False)
    before = sample("ess_m2_received_total", {"format": "MX", "matched": "false"})
    assert recorder.m2_received(UETR, fmt="MX") is None
    assert sample("ess_m2_received_total", {"format": "MX", "matched": "false"}) == before + 1


def test_a_refused_m1_withdraws_its_clock() -> None:
    recorder = Recorder(enabled=False)
    sent = time.time_ns()
    recorder.m1_sending(UETR, sent, fmt="MX", flow="MX_SNF_PACS008")
    recorder.m1_done(UETR, sent, fmt="MX", flow="MX_SNF_PACS008", accepted=False)
    assert recorder.pending_count() == 0
    assert recorder.m2_received(UETR, fmt="MX") is None
