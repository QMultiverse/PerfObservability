"""Prometheus metrics, shared by every Hub service and by the ESS.

These are the measurement of record in performance runs, where logging is at
``errors`` or ``none`` and there is no journey to read in Kibana.

MT and MX always use different RPC methods and different topics, so a ``format``
label is enough to split every series without cardinality games.
"""

from __future__ import annotations

import threading
from typing import Final

from prometheus_client import (
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    start_http_server,
)

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
payment_end_to_end = Histogram(
    "hub_payment_end_to_end_seconds",
    "T0 to T6: network delivery to COMPLETED",
    ["format", "flow"],
    buckets=_STAGE_BUCKETS,
    registry=REGISTRY,
)
held_payments = Gauge(
    "hub_held_payments",
    "Payments parked awaiting an FCC decision",
    ["service"],
    registry=REGISTRY,
)
pending_cover_legs = Gauge(
    "hub_pending_cover_legs",
    "Cover legs waiting for their partner",
    ["service"],
    registry=REGISTRY,
)

_server_lock = threading.Lock()
_server_started = False


def serve_metrics(port: int) -> None:
    """Expose ``/metrics``. Idempotent, so a restarted service does not bind twice."""
    global _server_started
    with _server_lock:
        if _server_started:
            return
        start_http_server(port, registry=REGISTRY)
        _server_started = True
