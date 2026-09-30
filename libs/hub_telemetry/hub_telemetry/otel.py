"""OpenTelemetry tracing setup and the baggage span processor.

Traces go to the OTel Collector over OTLP. The span processor copies the
payment baggage onto every span, so traces and logs share search keys and
Kibana can pivot between them on ``trace.id``.
"""

from __future__ import annotations

import os

from opentelemetry import trace
from opentelemetry.baggage.propagation import W3CBaggagePropagator
from opentelemetry.context import Context
from opentelemetry.propagate import set_global_textmap
from opentelemetry.propagators.composite import CompositePropagator
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import SpanProcessor, TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.trace.sampling import ALWAYS_OFF, ParentBased, TraceIdRatioBased
from opentelemetry.trace import Span
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

from . import baggage


class BaggageSpanProcessor(SpanProcessor):
    """Copy the payment baggage onto each span as it starts."""

    def on_start(self, span: Span, parent_context: Context | None = None) -> None:
        for key, value in baggage.get_all(parent_context).items():
            if key in baggage.PAYMENT_KEYS:
                span.set_attribute(key, value)

    def on_end(self, span: object) -> None:  # pragma: no cover - nothing to do
        return

    def shutdown(self) -> None:  # pragma: no cover
        return

    def force_flush(self, timeout_millis: int = 30_000) -> bool:  # pragma: no cover
        return True


def setup_tracing(
    service_name: str,
    *,
    endpoint: str | None = None,
    sample_ratio: float = 1.0,
    environment: str = "",
) -> trace.Tracer:
    """Install the tracer provider, the propagators and the span processor.

    With no endpoint (local runs and tests) spans are created but never
    exported, so the baggage plumbing still works with nothing to collect them.
    """
    resource = Resource.create(
        {
            "service.name": service_name,
            "service.namespace": "payment-hub",
            **({"deployment.environment": environment} if environment else {}),
        }
    )
    sampler = ParentBased(TraceIdRatioBased(sample_ratio) if sample_ratio > 0 else ALWAYS_OFF)
    provider = TracerProvider(resource=resource, sampler=sampler)
    provider.add_span_processor(BaggageSpanProcessor())

    target = endpoint if endpoint is not None else os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT")
    if target:
        from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

        provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=target)))

    trace.set_tracer_provider(provider)
    set_global_textmap(
        CompositePropagator([TraceContextTextMapPropagator(), W3CBaggagePropagator()])
    )
    return trace.get_tracer(service_name)


def setup_propagators_only() -> None:
    """Install just the W3C propagators.

    Enough for tests and for ``none``-level performance runs, where baggage
    must still travel but no spans are wanted.
    """
    set_global_textmap(
        CompositePropagator([TraceContextTextMapPropagator(), W3CBaggagePropagator()])
    )
