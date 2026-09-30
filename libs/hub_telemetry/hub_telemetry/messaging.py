"""The Kafka contract every Hub processor is written against.

Design doc section 7 requires that each processor reads, processes and writes in
**one Kafka transaction**: outputs and consumer offsets commit atomically, so a
crash never leaves a half-done stage. That is the shape of :class:`BusProducer`
and :class:`BusConsumer` here.

Two implementations satisfy it:

* :mod:`hub_telemetry.kafka_bus` — ``confluent_kafka``, for every real run;
* :mod:`hub_telemetry.memory_bus` — an in-process log, so the vertical slice
  and the fault paths can be exercised in ``pytest`` without a broker.

Service code never imports either one directly; it takes a :class:`Bus`.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Protocol, runtime_checkable


@dataclass(frozen=True, slots=True)
class Record:
    """One Kafka record, after the headers have been decoded to strings."""

    topic: str
    key: str
    value: bytes
    headers: Mapping[str, str] = field(default_factory=dict)
    partition: int = -1
    offset: int = -1
    timestamp_ms: int = 0
    lag: int | None = None

    @property
    def size_bytes(self) -> int:
        return len(self.value)


@dataclass(frozen=True, slots=True)
class TopicSpec:
    """A topic as the Hub wants it created.

    The payment-flow topics all share one partition count so a UETR maps to the
    same partition number everywhere (design doc section 7).
    """

    name: str
    partitions: int = 24
    replication: int = 3
    cleanup_policy: str = "delete"  # or "compact"
    retention_ms: int | None = None

    def config(self) -> dict[str, str]:
        cfg = {"cleanup.policy": self.cleanup_policy}
        if self.cleanup_policy == "compact":
            cfg["min.cleanable.dirty.ratio"] = "0.1"
            cfg["segment.ms"] = str(60 * 60 * 1000)
        elif self.retention_ms is not None:
            cfg["retention.ms"] = str(self.retention_ms)
        return cfg


class TransactionAborted(RuntimeError):
    """The transaction was rolled back; the batch will be redelivered."""


@runtime_checkable
class BusProducer(Protocol):
    """A transactional producer.

    ``commit`` writes the buffered records *and* the consumer's offsets in one
    transaction, which is what makes a stage exactly-once inside Kafka.
    """

    def begin(self) -> None: ...

    def produce(
        self,
        topic: str,
        key: str,
        value: bytes,
        headers: Mapping[str, str] | None = None,
    ) -> None: ...

    def commit(self, consumer: BusConsumer | None = None) -> None: ...

    def abort(self) -> None: ...

    def flush(self, timeout_s: float = 10.0) -> int: ...

    def close(self) -> None: ...


@runtime_checkable
class BusConsumer(Protocol):
    """A read_committed consumer with auto-commit off."""

    group_id: str

    def consume(self, max_records: int = 500, timeout_s: float = 0.05) -> list[Record]: ...

    def assignment(self) -> Sequence[tuple[str, int]]: ...

    def close(self) -> None: ...


@runtime_checkable
class Bus(Protocol):
    """Factory for producers and consumers, plus topic creation."""

    def producer(self, transactional_id: str | None = None) -> BusProducer: ...

    def consumer(self, group_id: str, topics: Sequence[str]) -> BusConsumer: ...

    def ensure_topics(self, specs: Iterable[TopicSpec]) -> None: ...

    def close(self) -> None: ...
