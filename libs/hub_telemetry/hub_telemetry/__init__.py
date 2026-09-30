"""hub-telemetry — the shared observability library.

Used by every Payment Hub service *and* by the External Systems Simulator, so
logging, baggage and metrics are identical on both sides of the boundary.

What it provides (design doc section 9):

* baggage set once at the edge and carried on every gRPC call and Kafka record;
* gRPC server and client interceptors;
* Kafka produce / consume wrappers that carry context and log the hop;
* a logging filter that copies baggage into every log line;
* a baggage span processor, so traces and logs share search keys;
* asynchronous (queue) logging, so logging never adds latency;
* tracking levels, checked before an event is built.
"""

from __future__ import annotations

from . import baggage, metrics
from .baggage import PaymentBaggage, payment_context, restored_context
from .ecs import BaggageFilter, EcsFormatter, setup_from_env, setup_logging
from .events import Events
from .grpc_telemetry import (
    DEADLINE_DELIVER_S,
    DEADLINE_NOTIFY_S,
    DEADLINE_SCREEN_S,
    DEADLINE_SEND_S,
    ClientTelemetryInterceptor,
    ServerTelemetryInterceptor,
    channel,
    metadata_for_call,
    metadata_to_headers,
)
from .memory_bus import MemoryBus
from .messaging import Bus, BusConsumer, BusProducer, Record, TopicSpec, TransactionAborted
from .otel import BaggageSpanProcessor, setup_propagators_only, setup_tracing
from .tracked import TrackedProducer, carry, consumed
from .tracking import Level, Mode, TrackingPolicy, should_log

__all__ = [
    "DEADLINE_DELIVER_S",
    "DEADLINE_NOTIFY_S",
    "DEADLINE_SCREEN_S",
    "DEADLINE_SEND_S",
    "BaggageFilter",
    "BaggageSpanProcessor",
    "Bus",
    "BusConsumer",
    "BusProducer",
    "ClientTelemetryInterceptor",
    "EcsFormatter",
    "Events",
    "Level",
    "MemoryBus",
    "Mode",
    "PaymentBaggage",
    "Record",
    "ServerTelemetryInterceptor",
    "TopicSpec",
    "TrackedProducer",
    "TrackingPolicy",
    "TransactionAborted",
    "baggage",
    "carry",
    "channel",
    "consumed",
    "metadata_for_call",
    "metadata_to_headers",
    "metrics",
    "payment_context",
    "restored_context",
    "setup_from_env",
    "setup_logging",
    "setup_propagators_only",
    "setup_tracing",
    "should_log",
]
