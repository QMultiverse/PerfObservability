"""The real ``KafkaBus``, against a live broker.

The rest of the suite runs on the in-process bus, which means the
``confluent_kafka`` implementation is the one path no other test covers. These
tests close that gap and are **deselected by default** — they need a broker:

    docker compose up -d kafka
    $env:KAFKA_BOOTSTRAP_SERVERS = "localhost:29092"
    python -m pytest tests/contract/test_kafka_bus.py -m kafka

They assert the four properties the design depends on and the in-memory bus can
only imitate: partitioning by UETR, ``read_committed`` isolation, offsets
committed inside the transaction, and an abort redelivering the batch.
"""

from __future__ import annotations

import os
import time
import uuid

import pytest
from hub_telemetry.messaging import TopicSpec

pytestmark = pytest.mark.kafka

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "")


@pytest.fixture(scope="module")
def bus():  # type: ignore[no-untyped-def]
    if not BOOTSTRAP:
        pytest.skip("set KAFKA_BOOTSTRAP_SERVERS to run the broker-backed tests")
    from hub_telemetry.kafka_bus import KafkaBus

    handle = KafkaBus(BOOTSTRAP, client_id="kafka-bus-test", default_replication=1)
    yield handle
    handle.close()


@pytest.fixture
def topic(bus):  # type: ignore[no-untyped-def]
    """A throwaway topic with four partitions, created per test."""
    name = f"test.kafka-bus.{uuid.uuid4().hex[:8]}"
    bus.ensure_topics([TopicSpec(name, partitions=4, replication=1, retention_ms=600_000)])
    return name


def _drain(consumer, expected: int, timeout_s: float = 20.0) -> list:  # type: ignore[no-untyped-def]
    """Consume until ``expected`` records have arrived, or time runs out."""
    out: list = []
    deadline = time.monotonic() + timeout_s
    while len(out) < expected and time.monotonic() < deadline:
        out.extend(consumer.consume(max_records=100, timeout_s=0.5))
    return out


def test_a_committed_transaction_is_readable(bus, topic) -> None:  # type: ignore[no-untyped-def]
    producer = bus.producer(f"test-tx-{uuid.uuid4().hex[:8]}")
    consumer = bus.consumer(f"cg-test-{uuid.uuid4().hex[:8]}", [topic])
    try:
        _drain(consumer, 0, timeout_s=5.0)  # let the group get its assignment

        producer.begin()
        producer.produce(topic, "uetr-1", b"one", {"format": "MX"})
        producer.produce(topic, "uetr-2", b"two", {"format": "MT"})
        producer.commit(consumer)

        records = _drain(consumer, 2)
        assert {r.value for r in records} == {b"one", b"two"}
        assert {r.headers["format"] for r in records} == {"MX", "MT"}
    finally:
        producer.close()
        consumer.close()


def test_an_aborted_transaction_is_never_readable(bus, topic) -> None:  # type: ignore[no-untyped-def]
    """read_committed: an aborted batch must not reach a consumer."""
    producer = bus.producer(f"test-tx-{uuid.uuid4().hex[:8]}")
    consumer = bus.consumer(f"cg-test-{uuid.uuid4().hex[:8]}", [topic])
    try:
        _drain(consumer, 0, timeout_s=5.0)

        producer.begin()
        producer.produce(topic, "uetr-1", b"aborted")
        producer.abort()

        producer.begin()
        producer.produce(topic, "uetr-1", b"committed")
        producer.commit(consumer)

        records = _drain(consumer, 1)
        assert [r.value for r in records] == [b"committed"]
    finally:
        producer.close()
        consumer.close()


def test_one_key_always_lands_on_one_partition(bus, topic) -> None:  # type: ignore[no-untyped-def]
    """Both legs of a cover pair and the payment's ACK share a partition."""
    producer = bus.producer(f"test-tx-{uuid.uuid4().hex[:8]}")
    consumer = bus.consumer(f"cg-test-{uuid.uuid4().hex[:8]}", [topic])
    uetr = str(uuid.uuid4())
    try:
        _drain(consumer, 0, timeout_s=5.0)
        producer.begin()
        for index in range(6):
            producer.produce(topic, uetr, str(index).encode())
        producer.commit(consumer)

        records = _drain(consumer, 6)
        assert len(records) == 6
        assert len({r.partition for r in records}) == 1
        # And in order, which is what the cover matcher relies on.
        assert [r.value for r in records] == [str(i).encode() for i in range(6)]
    finally:
        producer.close()
        consumer.close()


def test_offsets_commit_inside_the_transaction(bus, topic) -> None:  # type: ignore[no-untyped-def]
    """A new consumer in the same group must not see a committed batch again."""
    group = f"cg-test-{uuid.uuid4().hex[:8]}"
    producer = bus.producer(f"test-tx-{uuid.uuid4().hex[:8]}")
    first = bus.consumer(group, [topic])
    try:
        _drain(first, 0, timeout_s=5.0)
        producer.begin()
        producer.produce(topic, "uetr-1", b"payload")
        producer.commit()
        producer.flush()

        assert len(_drain(first, 1)) == 1
        # Commit the read offsets in a transaction of their own, the way a
        # processor does at the end of a batch.
        producer.begin()
        producer.commit(first)
    finally:
        first.close()

    second = bus.consumer(group, [topic])
    try:
        assert _drain(second, 1, timeout_s=8.0) == [], "the batch was redelivered"
    finally:
        second.close()
        producer.close()


def test_the_topic_catalogue_can_be_created_twice(bus) -> None:  # type: ignore[no-untyped-def]
    """``ensure_topics`` is what the stack runs at start-up; it must be idempotent."""
    name = f"test.kafka-bus.idem.{uuid.uuid4().hex[:8]}"
    specs = [TopicSpec(name, partitions=2, replication=1, cleanup_policy="compact")]
    bus.ensure_topics(specs)
    bus.ensure_topics(specs)
