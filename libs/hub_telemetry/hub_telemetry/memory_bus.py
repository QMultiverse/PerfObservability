"""An in-process Kafka stand-in with the same transactional semantics.

This exists so the vertical slice, the cover-pair matcher, the HELD path and
the retry / DLQ paths can be tested in ``pytest`` with no broker running. It
implements the parts of the contract the Hub actually depends on:

* partitioning by key, so one UETR always lands on one partition, in order;
* ``read_committed``: records buffered in an open transaction are invisible;
* offsets committed inside the transaction, so an abort redelivers the batch;
* log compaction for ``hub.pay.state``;
* consumer groups that spread partitions across their members.

It is deliberately single-process and lock-guarded, not a Kafka emulator.
"""

from __future__ import annotations

import threading
import time
from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field

from . import metrics
from .messaging import Bus, BusConsumer, Record, TopicSpec, TransactionAborted


def partition_for(key: str, partitions: int) -> int:
    """Kafka's default partitioner is murmur2; any stable hash will do here.

    What matters is that it is a pure function of the key, so cover legs and
    the ACK for a payment share a partition.
    """
    if partitions <= 1:
        return 0
    digest = 0
    for byte in key.encode("utf-8"):
        digest = (digest * 31 + byte) & 0xFFFF_FFFF
    return digest % partitions


@dataclass
class _Topic:
    spec: TopicSpec
    log: dict[int, list[Record]] = field(default_factory=lambda: defaultdict(list))

    def append(self, record: Record) -> Record:
        part = partition_for(record.key, self.spec.partitions)
        offset = len(self.log[part])
        stored = Record(
            topic=record.topic,
            key=record.key,
            value=record.value,
            headers=dict(record.headers),
            partition=part,
            offset=offset,
            timestamp_ms=int(time.time() * 1000),
        )
        self.log[part].append(stored)
        return stored

    def compact(self) -> int:
        """Keep only the newest record per key. Returns the records dropped.

        Never called automatically. Kafka compacts closed segments in the
        background and never the active one, so a live consumer sees every
        record a producer wrote; compacting on append would hide records from
        a consumer that had not read them yet, which is not how the compacted
        ``hub.pay.state`` topic behaves.
        """
        if self.spec.cleanup_policy != "compact":
            return 0
        dropped = 0
        for part, log in self.log.items():
            seen: set[str] = set()
            kept: list[Record] = []
            for rec in reversed(log):
                if rec.key in seen:
                    dropped += 1
                    continue
                seen.add(rec.key)
                kept.append(rec)
            self.log[part] = list(reversed(kept))
        return dropped

    def end_offset(self, part: int) -> int:
        return len(self.log[part])


class MemoryBus(Bus):
    """A shared in-memory log. One instance stands in for the whole cluster."""

    def __init__(self, *, default_partitions: int = 4, auto_create: bool = True) -> None:
        self._lock = threading.RLock()
        self._topics: dict[str, _Topic] = {}
        self._offsets: dict[tuple[str, str, int], int] = {}  # (group, topic, part) -> next
        self._default_partitions = default_partitions
        self._auto_create = auto_create
        self._groups: dict[str, list[_MemoryConsumer]] = defaultdict(list)

    # ------------------------------------------------------------- topics
    def ensure_topics(self, specs: Iterable[TopicSpec]) -> None:
        with self._lock:
            for spec in specs:
                existing = self._topics.get(spec.name)
                if existing is None:
                    self._topics[spec.name] = _Topic(spec)
                elif existing.spec.partitions != spec.partitions:
                    raise ValueError(
                        f"topic {spec.name} already exists with "
                        f"{existing.spec.partitions} partitions"
                    )

    def _topic(self, name: str) -> _Topic:
        topic = self._topics.get(name)
        if topic is None:
            if not self._auto_create:
                raise KeyError(f"unknown topic {name!r}")
            topic = _Topic(TopicSpec(name, partitions=self._default_partitions, replication=1))
            self._topics[name] = topic
        return topic

    def topics(self) -> list[str]:
        with self._lock:
            return sorted(self._topics)

    def compact(self, topic: str) -> int:
        """Run compaction on ``topic``, the way a background cleaner would."""
        with self._lock:
            entry = self._topics.get(topic)
            return entry.compact() if entry is not None else 0

    def records(self, topic: str) -> list[Record]:
        """Every committed record on ``topic``, partition by partition.

        Test helper; nothing in the Hub reads a topic this way.
        """
        with self._lock:
            entry = self._topics.get(topic)
            if entry is None:
                return []
            out: list[Record] = []
            for part in sorted(entry.log):
                out.extend(entry.log[part])
            return out

    # -------------------------------------------------------- factories
    def producer(self, transactional_id: str | None = None) -> _MemoryProducer:
        return _MemoryProducer(self, transactional_id or "anonymous")

    def consumer(self, group_id: str, topics: Sequence[str]) -> _MemoryConsumer:
        consumer = _MemoryConsumer(self, group_id, list(topics))
        with self._lock:
            self._groups[group_id].append(consumer)
            self._rebalance(group_id)
        return consumer

    def _rebalance(self, group_id: str) -> None:
        """Spread each subscribed topic's partitions across the live members."""
        members = [c for c in self._groups[group_id] if not c.closed]
        self._groups[group_id] = members
        if not members:
            return
        by_topic: dict[str, list[_MemoryConsumer]] = defaultdict(list)
        for member in members:
            for topic in member.topics:
                by_topic[topic].append(member)
        for member in members:
            member._assigned = []
        metrics.kafka_rebalances.labels(group_id, "assign").inc()
        for topic, subscribers in by_topic.items():
            partitions = self._topic(topic).spec.partitions
            for part in range(partitions):
                owner = subscribers[part % len(subscribers)]
                owner._assigned.append((topic, part))

    def close(self) -> None:
        with self._lock:
            for members in self._groups.values():
                for member in members:
                    member.closed = True
            self._groups.clear()


class _MemoryProducer:
    def __init__(self, bus: MemoryBus, transactional_id: str) -> None:
        self._bus = bus
        self.transactional_id = transactional_id
        self._buffer: list[Record] = []
        self._open = False
        self.committed: list[Record] = []

    def begin(self) -> None:
        if self._open:
            raise RuntimeError("transaction already open")
        self._buffer = []
        self._open = True

    def produce(
        self,
        topic: str,
        key: str,
        value: bytes,
        headers: Mapping[str, str] | None = None,
    ) -> None:
        record = Record(topic=topic, key=key, value=value, headers=dict(headers or {}))
        if self._open:
            self._buffer.append(record)
        else:
            # Non-transactional produce: the gRPC edge path, where acks=all is
            # enough and there is no consumer offset to tie the write to.
            with self._bus._lock:
                self.committed.append(self._bus._topic(topic).append(record))

    def commit(self, consumer: BusConsumer | None = None) -> None:
        if not self._open:
            raise RuntimeError("no open transaction")
        with self._bus._lock:
            written = [self._bus._topic(r.topic).append(r) for r in self._buffer]
            if consumer is not None:
                if not isinstance(consumer, _MemoryConsumer):
                    raise TypeError("MemoryBus can only commit offsets for its own consumer")
                consumer._commit_offsets()
        self.committed.extend(written)
        self._buffer = []
        self._open = False

    def abort(self) -> None:
        if not self._open:
            return
        self._buffer = []
        self._open = False

    def last_produced(self, topic: str) -> Record | None:
        for record in reversed(self.committed):
            if record.topic == topic:
                return record
        return None

    def flush(self, timeout_s: float = 10.0) -> int:
        return len(self._buffer)

    def close(self) -> None:
        self.abort()


class _MemoryConsumer:
    def __init__(self, bus: MemoryBus, group_id: str, topics: list[str]) -> None:
        self._bus = bus
        self.group_id = group_id
        self.topics = topics
        self.closed = False
        self._assigned: list[tuple[str, int]] = []
        self._pending: dict[tuple[str, int], int] = {}  # offsets read but not committed
        with bus._lock:
            for topic in topics:
                bus._topic(topic)

    def assignment(self) -> Sequence[tuple[str, int]]:
        return list(self._assigned)

    def consume(self, max_records: int = 500, timeout_s: float = 0.05) -> list[Record]:
        if self.closed:
            return []
        out: list[Record] = []
        with self._bus._lock:
            for topic, part in self._assigned:
                if len(out) >= max_records:
                    break
                entry = self._bus._topic(topic)
                key = (self.group_id, topic, part)
                start = self._pending.get((topic, part), self._bus._offsets.get(key, 0))
                log = entry.log[part]
                for record in log:
                    if record.offset < start:
                        continue
                    if len(out) >= max_records:
                        break
                    end = entry.end_offset(part)
                    out.append(
                        Record(
                            topic=record.topic,
                            key=record.key,
                            value=record.value,
                            headers=dict(record.headers),
                            partition=record.partition,
                            offset=record.offset,
                            timestamp_ms=record.timestamp_ms,
                            lag=max(0, end - record.offset - 1),
                        )
                    )
                    self._pending[(topic, part)] = record.offset + 1
        return out

    def _commit_offsets(self) -> None:
        """Called from inside the producer's commit, holding the bus lock."""
        for (topic, part), offset in self._pending.items():
            self._bus._offsets[(self.group_id, topic, part)] = offset
        self._pending.clear()

    def rewind(self) -> None:
        """Forget uncommitted reads, the way a rebalance after an abort would."""
        self._pending.clear()

    def committed_offset(self, topic: str, partition: int) -> int:
        with self._bus._lock:
            return self._bus._offsets.get((self.group_id, topic, partition), 0)

    def close(self) -> None:
        self.closed = True
        with self._bus._lock:
            self._bus._rebalance(self.group_id)


__all__ = ["MemoryBus", "TransactionAborted", "partition_for"]
