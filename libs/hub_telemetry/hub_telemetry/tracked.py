"""Kafka wrappers that carry the context and log the hop.

``produce()`` adds the ``baggage`` and ``traceparent`` headers and logs the
offset once the write is confirmed. The consume side restores the context from
the headers *before* processing, and logs ``kafka.consume``.

Every Hub processor uses these rather than a bare producer, so a payment's
journey is complete in Kibana without any service remembering to log a hop.
"""

from __future__ import annotations

import time
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager

from opentelemetry import trace
from opentelemetry.context import Context
from opentelemetry.trace import SpanKind

from . import baggage
from .events import Events
from .messaging import BusConsumer, BusProducer, Record


class TrackedProducer:
    """Wraps a :class:`~hub_telemetry.messaging.BusProducer`.

    Buffers what it produced during the open transaction and logs
    ``kafka.produce`` for each record only after the commit succeeds — an
    aborted transaction never appears in the payment journey.

    Each buffered record carries the context it was produced in. The commit
    happens after the batch loop, by which point the per-record context has
    been detached, so without this the produce events would lose the UETR and
    the trace id that make them part of a payment journey.
    """

    def __init__(self, producer: BusProducer, events: Events) -> None:
        self._producer = producer
        self._events = events
        # topic, size, started, the context the record was produced in
        self._pending: list[tuple[str, int, float, Context]] = []

    @property
    def raw(self) -> BusProducer:
        return self._producer

    def begin(self) -> None:
        self._pending.clear()
        self._producer.begin()

    def produce(
        self,
        topic: str,
        key: str,
        value: bytes,
        headers: Mapping[str, str] | None = None,
    ) -> None:
        """Produce with the current context injected into the headers."""
        carrier = baggage.inject(dict(headers or {}))
        started = time.perf_counter()
        self._producer.produce(topic, key, value, carrier)
        self._pending.append((topic, len(value), started, baggage.capture()))

    def commit(self, consumer: BusConsumer | None = None) -> None:
        self._producer.commit(consumer)
        self._log_pending()

    def abort(self) -> None:
        self._pending.clear()
        self._producer.abort()

    def produce_now(
        self,
        topic: str,
        key: str,
        value: bytes,
        headers: Mapping[str, str] | None = None,
        *,
        flush: bool = True,
    ) -> None:
        """Non-transactional produce, used by the gRPC edge.

        ``Deliver*`` replies ACCEPTED only after the write is acknowledged, so
        the default is to flush before returning (design doc section 4).
        """
        carrier = baggage.inject(dict(headers or {}))
        started = time.perf_counter()
        self._producer.produce(topic, key, value, carrier)
        if flush:
            self._producer.flush()
        self._events.kafka_produce(
            topic,
            offset=self._offset_of(topic),
            partition=self._partition_of(topic),
            size_bytes=len(value),
            duration_ns=int((time.perf_counter() - started) * 1e9),
        )

    def _log_pending(self) -> None:
        for topic, size, started, ctx in self._pending:
            with baggage.attached(ctx):
                self._events.kafka_produce(
                    topic,
                    partition=self._partition_of(topic),
                    offset=self._offset_of(topic),
                    size_bytes=size,
                    duration_ns=int((time.perf_counter() - started) * 1e9),
                )
        self._pending.clear()

    def _offset_of(self, topic: str) -> int:
        record = getattr(self._producer, "last_produced", None)
        if record is None:
            return -1
        found: Record | None = record(topic)
        return found.offset if found else -1

    def _partition_of(self, topic: str) -> int:
        record = getattr(self._producer, "last_produced", None)
        if record is None:
            return -1
        found: Record | None = record(topic)
        return found.partition if found else -1

    def flush(self, timeout_s: float = 10.0) -> int:
        return self._producer.flush(timeout_s)

    def close(self) -> None:
        self._producer.close()


@contextmanager
def consumed(record: Record, events: Events, group_id: str) -> Iterator[baggage.PaymentBaggage]:
    """Restore the record's context and log ``kafka.consume``.

    Everything inside the block — business logs, spans, downstream produces —
    inherits the payment's baggage.
    """
    tracer = trace.get_tracer(__name__)
    with (
        baggage.restored_context(record.headers) as bag,
        tracer.start_as_current_span(
            f"{record.topic} receive",
            kind=SpanKind.CONSUMER,
            attributes={
                "messaging.system": "kafka",
                "messaging.source": record.topic,
                "messaging.kafka.partition": record.partition,
                "messaging.kafka.offset": record.offset,
            },
        ),
    ):
        events.kafka_consume(
            record.topic,
            partition=record.partition,
            offset=record.offset,
            consumer_group=group_id,
            lag=record.lag,
            size_bytes=record.size_bytes,
        )
        yield bag


def header_view(record: Record) -> dict[str, str]:
    """Headers worth carrying to the next hop, minus the context headers.

    The context headers are re-injected by :meth:`TrackedProducer.produce` from
    the live context, so copying the inbound ones would pin a stale span.
    """
    return baggage.strip(record.headers)


def carry(record: Record, extra: Mapping[str, str] | None = None) -> dict[str, str]:
    headers = header_view(record)
    if extra:
        headers.update(extra)
    return headers


def group_partitions(records: Sequence[Record]) -> dict[tuple[str, int], list[Record]]:
    """Batch by (topic, partition), preserving order within each partition."""
    out: dict[tuple[str, int], list[Record]] = {}
    for record in records:
        out.setdefault((record.topic, record.partition), []).append(record)
    return out
