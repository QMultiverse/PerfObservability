"""The ``confluent_kafka`` implementation of :mod:`hub_telemetry.messaging`.

Settings are fixed here rather than left to each service, because the design
depends on them:

* producers are idempotent and transactional, ``acks=all``;
* consumers are ``read_committed`` with ``enable.auto.commit=false``;
* offsets are sent *inside* the transaction, never committed separately.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable, Mapping, Sequence
from typing import Any

from .messaging import Bus, BusConsumer, Record, TopicSpec, TransactionAborted

log = logging.getLogger(__name__)


def _decode_headers(raw: Any) -> dict[str, str]:
    if not raw:
        return {}
    out: dict[str, str] = {}
    for key, value in raw:
        if value is None:
            out[key] = ""
        elif isinstance(value, bytes):
            out[key] = value.decode("utf-8", "replace")
        else:
            out[key] = str(value)
    return out


class KafkaBus(Bus):
    """Cluster handle. One per service process."""

    def __init__(
        self,
        bootstrap_servers: str,
        *,
        security: Mapping[str, Any] | None = None,
        client_id: str = "payment-hub",
        default_partitions: int = 24,
        default_replication: int = 3,
    ) -> None:
        self.bootstrap_servers = bootstrap_servers
        self.security = dict(security or {})
        self.client_id = client_id
        self.default_partitions = default_partitions
        self.default_replication = default_replication
        self._admin: Any = None

    def _base_config(self) -> dict[str, Any]:
        return {
            "bootstrap.servers": self.bootstrap_servers,
            "client.id": self.client_id,
            **self.security,
        }

    # ------------------------------------------------------------- topics
    def ensure_topics(self, specs: Iterable[TopicSpec]) -> None:
        from confluent_kafka.admin import AdminClient, NewTopic

        if self._admin is None:
            self._admin = AdminClient(self._base_config())
        existing = set(self._admin.list_topics(timeout=10).topics)
        wanted = [
            NewTopic(
                spec.name,
                num_partitions=spec.partitions,
                replication_factor=min(spec.replication, self.default_replication),
                config=spec.config(),
            )
            for spec in specs
            if spec.name not in existing
        ]
        if not wanted:
            return
        for name, future in self._admin.create_topics(wanted).items():
            try:
                future.result()
                log.info("created topic %s", name)
            except Exception as exc:
                if "already exists" not in str(exc).lower():
                    raise

    # ---------------------------------------------------------- factories
    def producer(self, transactional_id: str | None = None) -> KafkaProducer:
        return KafkaProducer(self, transactional_id)

    def consumer(self, group_id: str, topics: Sequence[str]) -> KafkaConsumer:
        return KafkaConsumer(self, group_id, list(topics))

    def close(self) -> None:
        self._admin = None


class KafkaProducer:
    def __init__(self, bus: KafkaBus, transactional_id: str | None) -> None:
        from confluent_kafka import Producer

        config: dict[str, Any] = {
            **bus._base_config(),
            "enable.idempotence": True,
            "acks": "all",
            "max.in.flight.requests.per.connection": 5,
            "compression.type": "lz4",
            "linger.ms": 5,
            "retries": 10,
        }
        if transactional_id:
            config["transactional.id"] = transactional_id
            config["transaction.timeout.ms"] = 60_000
        self.transactional_id = transactional_id
        self._producer = Producer(config)
        self._open = False
        if transactional_id:
            self._producer.init_transactions(30)

    def begin(self) -> None:
        if not self.transactional_id:
            raise RuntimeError("producer is not transactional")
        self._producer.begin_transaction()
        self._open = True

    def produce(
        self,
        topic: str,
        key: str,
        value: bytes,
        headers: Mapping[str, str] | None = None,
    ) -> None:
        self._producer.produce(
            topic=topic,
            key=key.encode("utf-8"),
            value=value,
            headers=[(k, v.encode("utf-8")) for k, v in (headers or {}).items()],
        )

    def commit(self, consumer: BusConsumer | None = None) -> None:
        from confluent_kafka import KafkaException

        if not self._open:
            raise RuntimeError("no open transaction")
        try:
            if consumer is not None:
                if not isinstance(consumer, KafkaConsumer):
                    raise TypeError("KafkaBus can only commit offsets for its own consumer")
                raw = consumer._consumer
                positions = raw.position(raw.assignment())
                if positions:
                    self._producer.send_offsets_to_transaction(
                        positions, raw.consumer_group_metadata(), 30
                    )
            self._producer.commit_transaction(30)
        except KafkaException as exc:
            self._producer.abort_transaction(30)
            self._open = False
            raise TransactionAborted(str(exc)) from exc
        self._open = False

    def abort(self) -> None:
        if not self._open:
            return
        self._producer.abort_transaction(30)
        self._open = False

    def flush(self, timeout_s: float = 10.0) -> int:
        remaining: int = self._producer.flush(timeout_s)
        return remaining

    def close(self) -> None:
        self.abort()
        self._producer.flush(5.0)


class KafkaConsumer:
    def __init__(self, bus: KafkaBus, group_id: str, topics: list[str]) -> None:
        from confluent_kafka import Consumer

        self.group_id = group_id
        self.topics = topics
        self._consumer = Consumer(
            {
                **bus._base_config(),
                "group.id": group_id,
                "enable.auto.commit": False,
                "isolation.level": "read_committed",
                "auto.offset.reset": "earliest",
                "session.timeout.ms": 45_000,
                "max.poll.interval.ms": 300_000,
                "fetch.min.bytes": 1,
            }
        )
        self._consumer.subscribe(topics)

    def consume(self, max_records: int = 500, timeout_s: float = 0.05) -> list[Record]:
        from confluent_kafka import KafkaError

        messages = self._consumer.consume(num_messages=max_records, timeout=timeout_s)
        out: list[Record] = []
        for msg in messages:
            err = msg.error()
            if err is not None:
                if err.code() == KafkaError._PARTITION_EOF:
                    continue
                log.warning("kafka consume error: %s", err)
                continue
            key = msg.key()
            out.append(
                Record(
                    topic=msg.topic(),
                    key=key.decode("utf-8", "replace") if key else "",
                    value=msg.value() or b"",
                    headers=_decode_headers(msg.headers()),
                    partition=msg.partition(),
                    offset=msg.offset(),
                    timestamp_ms=msg.timestamp()[1],
                    lag=self._lag(msg),
                )
            )
        return out

    def _lag(self, msg: Any) -> int | None:
        """Lag at read: how far behind the end of the partition we are."""
        try:
            _, high = self._consumer.get_watermark_offsets(
                _TopicPartition(msg.topic(), msg.partition()), timeout=0.2, cached=True
            )
        except Exception:
            return None
        if high is None or high < 0:
            return None
        return max(0, int(high) - int(msg.offset()) - 1)

    def assignment(self) -> Sequence[tuple[str, int]]:
        return [(tp.topic, tp.partition) for tp in self._consumer.assignment()]

    def close(self) -> None:
        self._consumer.close()


def _TopicPartition(topic: str, partition: int) -> Any:  # noqa: N802 - mirrors the Kafka name
    from confluent_kafka import TopicPartition

    return TopicPartition(topic, partition)


__all__ = ["KafkaBus", "KafkaConsumer", "KafkaProducer"]
