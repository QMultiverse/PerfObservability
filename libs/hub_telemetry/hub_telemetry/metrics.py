"""Prometheus metrics, shared by every Hub service and by the ESS.

These are the measurement of record in performance runs, where logging is at
``errors`` or ``none`` and there is no journey to read in Kibana.

MT and MX always use different RPC methods and different topics, so a ``format``
label is enough to split every series without cardinality games.

The Grafana dashboard ``03-performance.json`` is built on these. Several of its
panels are named after their Kafka Streams / JVM counterparts (process latency,
commit latency, poll time, IO wait ratio); the docstring of each metric below
says what the Python equivalent actually measures.
"""

from __future__ import annotations

import gc
import sys
import threading
import time
from collections.abc import Callable, Iterable, Iterator
from typing import Final, Protocol

from prometheus_client import (
    CollectorRegistry,
    Counter,
    Gauge,
    GCCollector,
    Histogram,
    PlatformCollector,
    ProcessCollector,
    start_http_server,
)
from prometheus_client.core import GaugeMetricFamily
from prometheus_client.registry import Collector

REGISTRY: Final = CollectorRegistry(auto_describe=True)

# Latency buckets chosen around the deadlines in design doc section 4
# (200 ms Deliver*, 500 ms Send*, 1 s Screen), with headroom above.
_RPC_BUCKETS: Final = (
    0.001,
    0.005,
    0.010,
    0.025,
    0.050,
    0.100,
    0.200,
    0.500,
    1.0,
    2.0,
    5.0,
)
_STAGE_BUCKETS: Final = (
    0.001,
    0.005,
    0.010,
    0.050,
    0.100,
    0.250,
    0.500,
    1.0,
    2.5,
    5.0,
    10.0,
    30.0,
)

grpc_server_calls = Counter(
    "hub_grpc_server_calls_total",
    "Served gRPC calls",
    ["service", "method", "status"],
    registry=REGISTRY,
)
grpc_server_latency = Histogram(
    "hub_grpc_server_latency_seconds",
    "Served gRPC call latency",
    ["service", "method"],
    buckets=_RPC_BUCKETS,
    registry=REGISTRY,
)
grpc_client_calls = Counter(
    "hub_grpc_client_calls_total",
    "Outbound gRPC calls",
    ["service", "method", "status"],
    registry=REGISTRY,
)
grpc_client_latency = Histogram(
    "hub_grpc_client_latency_seconds",
    "Outbound gRPC call latency",
    ["service", "method"],
    buckets=_RPC_BUCKETS,
    registry=REGISTRY,
)
grpc_client_retries = Counter(
    "hub_grpc_client_retries_total",
    "Retried outbound gRPC calls",
    ["service", "method", "status"],
    registry=REGISTRY,
)

kafka_produced = Counter(
    "hub_kafka_produced_total",
    "Records produced",
    ["service", "topic"],
    registry=REGISTRY,
)
kafka_consumed = Counter(
    "hub_kafka_consumed_total",
    "Records consumed",
    ["service", "topic", "group"],
    registry=REGISTRY,
)
kafka_consumer_lag = Gauge(
    "hub_kafka_consumer_lag",
    "Lag at read, per topic partition",
    ["service", "topic", "group", "partition"],
    registry=REGISTRY,
)
kafka_transaction_latency = Histogram(
    "hub_kafka_transaction_seconds",
    "Time to process and commit one batch",
    ["service"],
    buckets=_STAGE_BUCKETS,
    registry=REGISTRY,
)
kafka_transactions_aborted = Counter(
    "hub_kafka_transactions_aborted_total",
    "Aborted processing transactions",
    ["service", "reason"],
    registry=REGISTRY,
)

payments_state = Counter(
    "hub_payments_state_total",
    "Payment state transitions",
    ["service", "format", "state"],
    registry=REGISTRY,
)
payments_errors = Counter(
    "hub_payments_errors_total",
    "Payment processing errors",
    ["service", "stage", "error_code"],
    registry=REGISTRY,
)
payments_retried = Counter(
    "hub_payments_retried_total",
    "Records sent to a retry topic or the DLQ",
    ["service", "target"],
    registry=REGISTRY,
)
stage_latency = Histogram(
    "hub_stage_latency_seconds",
    "Time spent in one processing stage",
    ["service", "stage", "format"],
    buckets=_STAGE_BUCKETS,
    registry=REGISTRY,
)
# End to end runs to minutes when a payment rides the retry ladder (30 s, then
# 5 min), so its histogram reaches past the stage buckets' 30 s ceiling.
_E2E_BUCKETS: Final = (*_STAGE_BUCKETS, 60.0, 120.0, 300.0, 600.0)

payment_end_to_end = Histogram(
    "hub_payment_end_to_end_seconds",
    "T0 to T6: network delivery to COMPLETED",
    ["format", "flow"],
    buckets=_E2E_BUCKETS,
    registry=REGISTRY,
)
held_payments = Gauge(
    "hub_held_payments",
    "Payments parked awaiting an FCC decision",
    ["service"],
    registry=REGISTRY,
)
awaiting_ack = Gauge(
    "hub_awaiting_ack",
    "Payments handed to the network and still waiting for its ACK; overdue=true "
    "once past the ACK matcher's timeout",
    ["service", "overdue"],
    registry=REGISTRY,
)
pending_cover_legs = Gauge(
    "hub_pending_cover_legs",
    "Cover legs waiting for their partner",
    ["service"],
    registry=REGISTRY,
)

# ------------------------------------------------- the processing loop
# The Kafka Streams-style view of each stage. One stage is one thread (see
# hub/common/all_in_one.py), so "seconds per second" of a time counter is a
# ratio of that thread's wall clock.

_RECORD_BUCKETS: Final = (
    0.0001,
    0.0005,
    0.001,
    0.0025,
    0.005,
    0.010,
    0.025,
    0.050,
    0.100,
    0.250,
    0.500,
    1.0,
    2.5,
)

records_processed = Counter(
    "hub_records_processed_total",
    "Records a stage processed (state-topic records excluded), by outcome",
    ["service", "outcome"],  # ok | retry | dlq
    registry=REGISTRY,
)
process_latency = Histogram(
    "hub_process_latency_seconds",
    "Time to process one record, excluding the transaction commit",
    ["service"],
    buckets=_RECORD_BUCKETS,
    registry=REGISTRY,
)
kafka_poll = Histogram(
    "hub_kafka_poll_seconds",
    "Time spent inside one consumer poll",
    ["service"],
    buckets=_RECORD_BUCKETS,
    registry=REGISTRY,
)
kafka_io_wait = Counter(
    "hub_kafka_io_wait_seconds_total",
    "Time the stage thread spent waiting for records: polling plus idle back-off. "
    "rate() of this is the IO wait ratio",
    ["service"],
    registry=REGISTRY,
)
kafka_commit = Histogram(
    "hub_kafka_commit_seconds",
    "Time for the producer to commit one transaction, offsets included",
    ["service"],
    buckets=_RECORD_BUCKETS,
    registry=REGISTRY,
)
post_commit = Histogram(
    "hub_post_commit_seconds",
    "Work a stage does after its transaction commits, per batch (the DB sink's write)",
    ["service"],
    buckets=_STAGE_BUCKETS,
    registry=REGISTRY,
)
kafka_rebalances = Counter(
    "hub_kafka_rebalances_total",
    "Partition assignment changes seen by a consumer",
    ["group", "event"],  # assign | revoke | lost
    registry=REGISTRY,
)
kafka_assigned_partitions = Gauge(
    "hub_kafka_assigned_partitions",
    "Partitions currently assigned to this process's consumer",
    ["group"],
    registry=REGISTRY,
)

# ----------------------------------------------------------- database
# The JDBC panels' equivalent: psycopg, used by the DB sink.
db_calls = Counter(
    "hub_db_calls_total",
    "Database statements and transaction calls",
    ["service", "operation", "outcome"],
    registry=REGISTRY,
)
db_call_latency = Histogram(
    "hub_db_call_seconds",
    "Database call duration",
    ["service", "operation"],
    # Up to 30 s: a batch write under a slow database ran past 5 s (perf
    # scenario 2) and the old top bucket hid by how much.
    buckets=_STAGE_BUCKETS,
    registry=REGISTRY,
)
db_connections = Gauge(
    "hub_db_connections",
    "Database connections held, by state",
    ["service", "state"],  # active | idle
    registry=REGISTRY,
)

# -------------------------------------------------------- Python runtime
# The memory panels' Python side. Python has no heap / old gen split; the
# nearest equivalents are the resident set (process_resident_memory_bytes,
# from ProcessCollector) and generation-2 garbage collection, which is the
# full collection.
python_gc_pause = Histogram(
    "hub_python_gc_pause_seconds",
    "Stop-the-world garbage collection pause, by generation (2 is a full collection)",
    ["generation"],
    buckets=(0.0001, 0.0005, 0.001, 0.005, 0.010, 0.025, 0.050, 0.100, 0.250, 0.500, 1.0),
    registry=REGISTRY,
)
python_allocated_blocks = Gauge(
    "hub_python_allocated_blocks",
    "Memory blocks currently allocated by the interpreter",
    registry=REGISTRY,
)
python_allocated_blocks.set_function(lambda: float(sys.getallocatedblocks()))


class WindowedMax(Collector):
    """The largest value seen recently, as a gauge.

    A histogram cannot give a maximum, and Kafka Streams' ``*-max`` metrics are
    windowed, so this keeps the max of the current window and the previous one
    and reports the larger. A spike stays visible for one to two windows: long
    enough for a 5 s scrape to catch it, short enough to fall back once it has
    passed.
    """

    def __init__(
        self,
        name: str,
        documentation: str,
        labelnames: Iterable[str],
        *,
        window_s: float = 30.0,
        registry: CollectorRegistry | None = REGISTRY,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._name = name
        self._doc = documentation
        self._labelnames = tuple(labelnames)
        self._window_s = window_s
        self._clock = clock
        self._lock = threading.Lock()
        # labels -> (window start, max this window, max previous window)
        self._series: dict[tuple[str, ...], tuple[float, float, float]] = {}
        if registry is not None:
            registry.register(self)

    def observe(self, labels: tuple[str, ...], value: float) -> None:
        now = self._clock()
        with self._lock:
            start, current, previous = self._roll(labels, now)
            self._series[labels] = (start, max(current, value), previous)

    def value(self, labels: tuple[str, ...]) -> float:
        with self._lock:
            _, current, previous = self._roll(labels, self._clock())
            return max(current, previous)

    def _roll(self, labels: tuple[str, ...], now: float) -> tuple[float, float, float]:
        start, current, previous = self._series.get(labels, (now, 0.0, 0.0))
        elapsed = now - start
        if elapsed >= 2 * self._window_s:
            start, current, previous = now, 0.0, 0.0
        elif elapsed >= self._window_s:
            start, current, previous = start + self._window_s, 0.0, current
        self._series[labels] = (start, current, previous)
        return start, current, previous

    def describe(self) -> Iterable[GaugeMetricFamily]:
        return [GaugeMetricFamily(self._name, self._doc, labels=self._labelnames)]

    def collect(self) -> Iterator[GaugeMetricFamily]:
        family = GaugeMetricFamily(self._name, self._doc, labels=self._labelnames)
        with self._lock:
            keys = list(self._series)
        for labels in keys:
            family.add_metric(list(labels), self.value(labels))
        yield family


process_latency_max = WindowedMax(
    "hub_process_latency_max_seconds",
    "Longest single-record processing time in the last 30-60 s",
    ["service"],
)
post_commit_max = WindowedMax(
    "hub_post_commit_max_seconds",
    "Longest post-commit step in the last 30-60 s",
    ["service"],
)
kafka_commit_max = WindowedMax(
    "hub_kafka_commit_max_seconds",
    "Longest transaction commit in the last 30-60 s",
    ["service"],
)


# ------------------------------------------------------------ state stores
class StoreStats(Protocol):
    """What a state store reports. ``hub.common.state_store.StateStore`` has it."""

    def size_stats(self) -> tuple[int, int, int]:
        """(working keys, remembered completed keys, approximate bytes)."""
        ...


class StateStoreCollector(Collector):
    """Reports every registered state store at scrape time.

    The Kafka Streams panels ("disk usage per instance", "number of keys per
    instance") read RocksDB. The Hub's store is in memory, so these are its
    keys and an estimate of the bytes it holds; ``instance`` already tells one
    process from another, and ``service`` tells the stages in one process apart.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._stores: dict[str, StoreStats] = {}

    def register(self, service: str, store: StoreStats) -> None:
        with self._lock:
            self._stores[service] = store

    def unregister(self, service: str) -> None:
        with self._lock:
            self._stores.pop(service, None)

    def describe(self) -> Iterable[GaugeMetricFamily]:
        return []

    def collect(self) -> Iterator[GaugeMetricFamily]:
        keys = GaugeMetricFamily(
            "hub_state_store_keys", "Keys in a stage's state store", labels=["service", "kind"]
        )
        size = GaugeMetricFamily(
            "hub_state_store_bytes",
            "Approximate payload bytes held by a stage's state store",
            labels=["service"],
        )
        with self._lock:
            stores = list(self._stores.items())
        for service, store in stores:
            working, completed, approx_bytes = store.size_stats()
            keys.add_metric([service, "working"], working)
            keys.add_metric([service, "completed"], completed)
            size.add_metric([service], approx_bytes)
        yield keys
        yield size


state_stores: Final = StateStoreCollector()
REGISTRY.register(state_stores)


# ------------------------------------------------------- runtime collectors
_runtime_lock = threading.Lock()
_runtime_installed = False
_gc_started: dict[int, float] = {}


def _on_gc(phase: str, info: dict[str, int]) -> None:
    generation = info.get("generation", -1)
    if phase == "start":
        _gc_started[generation] = time.perf_counter()
        return
    started = _gc_started.pop(generation, None)
    if started is not None:
        python_gc_pause.labels(str(generation)).observe(time.perf_counter() - started)


def install_runtime_collectors() -> None:
    """Process, platform and GC metrics on our registry, plus GC pause timing.

    ``prometheus_client`` registers these on its *default* registry, which the
    Hub does not serve, so without this a dashboard has no memory panels.
    Idempotent.
    """
    global _runtime_installed
    with _runtime_lock:
        if _runtime_installed:
            return
        # Constructed fresh: prometheus_client's own instances are already
        # registered on the default registry.
        ProcessCollector(registry=REGISTRY)
        PlatformCollector(registry=REGISTRY)
        GCCollector(registry=REGISTRY)
        gc.callbacks.append(_on_gc)
        _runtime_installed = True


_server_lock = threading.Lock()
_server_started = False


def serve_metrics(port: int) -> None:
    """Expose ``/metrics``. Idempotent, so a restarted service does not bind twice."""
    global _server_started
    with _server_lock:
        if _server_started:
            return
        install_runtime_collectors()
        start_http_server(port, registry=REGISTRY)
        _server_started = True
