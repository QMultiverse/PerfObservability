"""hub-telemetry: baggage, tracking levels, ECS formatting and the bus."""

from __future__ import annotations

import io
import json
import logging

import pytest
from hub_telemetry import baggage
from hub_telemetry.baggage import PaymentBaggage, baggage_header_size, payment_context
from hub_telemetry.ecs import EcsFormatter, setup_logging
from hub_telemetry.events import Events
from hub_telemetry.grpc_telemetry import BACKOFF_MAX_S, backoff_delay
from hub_telemetry.memory_bus import MemoryBus, partition_for
from hub_telemetry.messaging import TopicSpec
from hub_telemetry.tracked import TrackedProducer, carry, consumed
from hub_telemetry.tracking import (
    GRPC_SERVER_RECV,
    KAFKA_CONSUME,
    KAFKA_PRODUCE,
    PAYMENT_ERROR,
    PAYMENT_STATE,
    Level,
    Mode,
    TrackingPolicy,
    parse_spot_check,
    should_log,
)


# --------------------------------------------------------------- baggage
def test_baggage_round_trips_through_headers() -> None:
    bag = PaymentBaggage(
        uetr="eb6305c9-1f1d-4b3a-8a0f-2d3c4e5f6a7b",
        format="MX",
        msg_type="pacs.008.001.08",
        flow="MX_SNF_PACS008",
        biz_msg_id="BIZ1",
        trace_level="full",
        run_id="S02-2026-09-28-01",
    )
    with payment_context(bag):
        headers = baggage.inject()

    assert "baggage" in headers
    with baggage.restored_context(headers) as restored:
        assert restored == bag


def test_empty_baggage_fields_are_not_carried() -> None:
    bag = PaymentBaggage(uetr="x", format="MT")
    assert bag.as_dict() == {"payment.uetr": "x", "payment.format": "MT"}


def test_baggage_is_capped_at_512_bytes() -> None:
    """Section 9's rule. Least important keys are dropped, not the UETR."""
    bag = PaymentBaggage(
        uetr="eb6305c9-1f1d-4b3a-8a0f-2d3c4e5f6a7b",
        format="MX",
        msg_type="pacs.008.001.08",
        flow="MX_SNF_PACS008",
        biz_msg_id="B" * 400,
        trace_level="full",
        run_id="R" * 400,
    )
    with payment_context(bag):
        headers = baggage.inject()
    header = headers["baggage"]
    assert len(header) <= baggage.MAX_BAGGAGE_BYTES
    assert "payment.uetr=" in header, "the primary search key must always survive"
    assert "payment.format=MX" in header


def test_biz_msg_id_is_truncated_not_dropped() -> None:
    bag = PaymentBaggage(uetr="x", biz_msg_id="B" * 100)
    assert len(bag.as_dict()["payment.biz_msg_id"]) == 35


def test_header_size_is_measurable() -> None:
    assert baggage_header_size(PaymentBaggage(uetr="")) == 0
    assert baggage_header_size(PaymentBaggage(uetr="abc")) == len("payment.uetr=abc")


def test_strip_removes_only_the_context_headers() -> None:
    headers = {
        "baggage": "payment.uetr=x",
        "traceparent": "00-a-b-01",
        "tracestate": "x=y",
        "uetr": "x",
        "authorization": "Bearer token",
    }
    stripped = baggage.strip(headers)
    assert set(stripped) == {"uetr", "authorization"}


def test_context_is_detached_after_the_block() -> None:
    with payment_context(PaymentBaggage(uetr="abc")):
        assert baggage.current_uetr() == "abc"
    assert baggage.current_uetr() == ""


# -------------------------------------------------------- tracking levels
@pytest.mark.parametrize(
    ("level", "action", "expected"),
    [
        (Level.FULL, KAFKA_CONSUME, True),
        (Level.FULL, PAYMENT_STATE, True),
        (Level.STANDARD, GRPC_SERVER_RECV, True),
        (Level.STANDARD, KAFKA_CONSUME, False),
        (Level.STANDARD, PAYMENT_STATE, True),
        (Level.MINIMAL, PAYMENT_STATE, True),
        (Level.MINIMAL, GRPC_SERVER_RECV, False),
        (Level.MINIMAL, KAFKA_PRODUCE, False),
        (Level.ERRORS, PAYMENT_ERROR, True),
        (Level.ERRORS, KAFKA_CONSUME, False),
        (Level.ERRORS, GRPC_SERVER_RECV, False),
        (Level.NONE, PAYMENT_ERROR, False),
        (Level.NONE, PAYMENT_STATE, False),
    ],
)
def test_levels_admit_the_right_actions(level: Level, action: str, expected: bool) -> None:
    assert should_log(level, action) is expected


@pytest.mark.parametrize(
    ("state", "expected"),
    [
        ("COMPLETED", False),
        ("RECEIVED", False),
        ("REJECTED", True),
        ("HELD", True),
        ("FAILED", True),
    ],
)
def test_errors_level_keeps_the_state_that_caused_a_failure(state: str, expected: bool) -> None:
    assert should_log(Level.ERRORS, PAYMENT_STATE, state=state) is expected


@pytest.mark.parametrize(
    ("mode", "level"),
    [
        (Mode.FUNCTIONAL, Level.FULL),
        (Mode.CI, Level.FULL),
        (Mode.PERFORMANCE, Level.ERRORS),
    ],
)
def test_the_default_level_per_mode(mode: Mode, level: Level) -> None:
    assert TrackingPolicy(mode).choose_level("any-uetr") is level


def test_the_level_choice_is_deterministic_per_uetr() -> None:
    policy = TrackingPolicy(Mode.PERFORMANCE, spot_check=(Level.MINIMAL, 50.0))
    uetr = "eb6305c9-1f1d-4b3a-8a0f-2d3c4e5f6a7b"
    assert policy.choose_level(uetr) is policy.choose_level(uetr)


def test_the_spot_check_samples_roughly_the_right_share() -> None:
    from hub_model.ids import new_uetr

    policy = TrackingPolicy(Mode.PERFORMANCE, spot_check=(Level.MINIMAL, 10.0))
    sampled = sum(1 for _ in range(4000) if policy.choose_level(new_uetr()) is Level.MINIMAL)
    assert 250 < sampled < 550, f"sampled {sampled}/4000, expected around 400"


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("minimal:0.1%", (Level.MINIMAL, 0.1)),
        ("minimal:0.1", (Level.MINIMAL, 0.1)),
        ("full:5%", (Level.FULL, 5.0)),
        ("", None),
        ("garbage", None),
    ],
)
def test_spot_check_parsing(text: str, expected: object) -> None:
    assert parse_spot_check(text) == expected


def test_policy_from_env() -> None:
    policy = TrackingPolicy.from_env(
        {"HUB_TRACKING_MODE": "performance", "HUB_TRACKING_SPOT_CHECK": "minimal:0.1%"}
    )
    assert policy.mode is Mode.PERFORMANCE
    assert policy.base_level is Level.ERRORS
    assert policy.spot_check == (Level.MINIMAL, 0.1)


def test_an_explicit_level_overrides_the_mode_default() -> None:
    policy = TrackingPolicy.from_env(
        {"HUB_TRACKING_MODE": "performance", "HUB_TRACKING_LEVEL": "full"}
    )
    assert policy.choose_level("x") is Level.FULL


# ------------------------------------------------------------ ECS format
def test_a_record_renders_as_one_ecs_json_line() -> None:
    formatter = EcsFormatter("hub-screening", environment="test", version="0.1.0")
    record = logging.LogRecord(
        "hub-screening", logging.INFO, __file__, 1, "consume %s", ("topic",), None
    )
    record.__dict__.update(
        {
            "event.action": KAFKA_CONSUME,
            "messaging.destination": "hub.pay.canonical",
            "payment.uetr": "eb6305c9",
        }
    )
    doc = json.loads(formatter.format(record))
    assert doc["service.name"] == "hub-screening"
    assert doc["service.environment"] == "test"
    assert doc["log.level"] == "info"
    assert doc["message"] == "consume topic"
    assert doc["event.action"] == KAFKA_CONSUME
    assert doc["payment.uetr"] == "eb6305c9"
    assert doc["@timestamp"].endswith("Z")


def test_an_exception_becomes_ecs_error_fields() -> None:
    formatter = EcsFormatter("hub-edge")
    try:
        raise ValueError("boom")
    except ValueError:
        import sys

        record = logging.LogRecord("x", logging.ERROR, __file__, 1, "failed", (), sys.exc_info())
    doc = json.loads(formatter.format(record))
    assert doc["error.type"] == "ValueError"
    assert doc["error.message"] == "boom"
    assert "Traceback" in doc["error.stack_trace"]


def test_baggage_reaches_ordinary_log_lines() -> None:
    stream = io.StringIO()
    setup_logging("hub-test", level=logging.INFO, stream=stream, asynchronous=False)
    try:
        with payment_context(PaymentBaggage(uetr="abc123", format="MX")):
            logging.getLogger("business").info("posting to the ledger")
    finally:
        setup_logging("tests", level=logging.WARNING, asynchronous=False)

    doc = json.loads(stream.getvalue().strip())
    assert doc["payment.uetr"] == "abc123"
    assert doc["payment.format"] == "MX"
    assert doc["message"] == "posting to the ledger"


def test_an_event_names_its_own_service() -> None:
    stream = io.StringIO()
    setup_logging("process-default", level=logging.INFO, stream=stream, asynchronous=False)
    try:
        with payment_context(PaymentBaggage(uetr="abc", trace_level="full")):
            Events("hub-routing").payment_state("ROUTED", reason="to SnF")
    finally:
        setup_logging("tests", level=logging.WARNING, asynchronous=False)

    doc = json.loads(stream.getvalue().strip())
    assert doc["service.name"] == "hub-routing"
    assert doc["payment.state"] == "ROUTED"
    assert doc["event.action"] == PAYMENT_STATE


def test_events_are_suppressed_by_the_level_before_being_built() -> None:
    stream = io.StringIO()
    setup_logging("x", level=logging.INFO, stream=stream, asynchronous=False)
    try:
        with payment_context(PaymentBaggage(uetr="abc", trace_level="errors")):
            events = Events("hub-screening")
            events.kafka_consume("hub.pay.canonical")
            events.payment_state("SCREENED")
            events.payment_state("REJECTED")
            events.payment_error("BOOM")
    finally:
        setup_logging("tests", level=logging.WARNING, asynchronous=False)

    actions = [json.loads(line)["event.action"] for line in stream.getvalue().splitlines()]
    assert actions == [PAYMENT_STATE, PAYMENT_ERROR]


# ---------------------------------------------------------------- retry
def test_backoff_grows_and_is_jittered() -> None:
    import random

    rng = random.Random(0)
    for attempt in (1, 2, 3):
        ceiling = min(BACKOFF_MAX_S, 0.020 * 2 ** (attempt - 1))
        for _ in range(50):
            delay = backoff_delay(attempt, rng=rng)
            assert 0.0 <= delay <= ceiling


def test_backoff_is_capped() -> None:
    import random

    assert backoff_delay(20, rng=random.Random(1)) <= BACKOFF_MAX_S


# ----------------------------------------------------------- memory bus
def test_the_same_key_always_lands_on_the_same_partition() -> None:
    for key in ("a", "eb6305c9-1f1d-4b3a-8a0f-2d3c4e5f6a7b", ""):
        assert partition_for(key, 24) == partition_for(key, 24)
        assert 0 <= partition_for(key, 24) < 24
    assert partition_for("anything", 1) == 0


def test_an_open_transaction_is_invisible_to_read_committed_consumers() -> None:
    bus = MemoryBus(default_partitions=2)
    bus.ensure_topics([TopicSpec("t", partitions=2, replication=1)])
    consumer = bus.consumer("g", ["t"])
    producer = bus.producer("tx")

    producer.begin()
    producer.produce("t", "k", b"v")
    assert consumer.consume() == [], "an uncommitted write must not be readable"

    producer.commit(consumer)
    assert [r.value for r in consumer.consume()] == [b"v"]


def test_an_aborted_transaction_writes_nothing() -> None:
    bus = MemoryBus(default_partitions=2)
    producer = bus.producer("tx")
    consumer = bus.consumer("g", ["t"])
    producer.begin()
    producer.produce("t", "k", b"v")
    producer.abort()
    assert consumer.consume() == []
    assert bus.records("t") == []


def test_offsets_commit_with_the_transaction() -> None:
    """A crash before the commit redelivers the batch; after it, it does not."""
    bus = MemoryBus(default_partitions=1)
    producer = bus.producer("tx")
    producer.produce("in", "k", b"v")

    consumer = bus.consumer("g", ["in"])
    first = consumer.consume()
    assert len(first) == 1

    # Aborting and rewinding is what a rebalance after a crash looks like.
    tx = bus.producer("tx2")
    tx.begin()
    tx.produce("out", "k", b"v2")
    tx.abort()
    consumer.rewind()
    assert len(consumer.consume()) == 1, "an aborted batch is redelivered"

    tx.begin()
    tx.produce("out", "k", b"v2")
    tx.commit(consumer)
    assert consumer.consume() == [], "a committed batch is not redelivered"


def test_compaction_keeps_the_newest_record_per_key() -> None:
    bus = MemoryBus(default_partitions=1)
    bus.ensure_topics([TopicSpec("state", partitions=1, replication=1, cleanup_policy="compact")])
    producer = bus.producer()
    for value in (b"v1", b"v2", b"v3"):
        producer.produce("state", "k", value)
    producer.produce("state", "other", b"x")

    assert len(bus.records("state")) == 4, "compaction is not eager"
    bus.compact("state")
    kept = {r.key: r.value for r in bus.records("state")}
    assert kept == {"k": b"v3", "other": b"x"}


def test_a_consumer_group_spreads_partitions_across_members() -> None:
    bus = MemoryBus(default_partitions=4)
    bus.ensure_topics([TopicSpec("t", partitions=4, replication=1)])
    a = bus.consumer("g", ["t"])
    b = bus.consumer("g", ["t"])
    assert len(a.assignment()) + len(b.assignment()) == 4
    assert set(a.assignment()).isdisjoint(b.assignment())


def test_lag_is_reported_at_read() -> None:
    bus = MemoryBus(default_partitions=1)
    producer = bus.producer()
    for index in range(5):
        producer.produce("t", "k", str(index).encode())
    records = bus.consumer("g", ["t"]).consume()
    assert [r.lag for r in records] == [4, 3, 2, 1, 0]


# ------------------------------------------------------ tracked producer
def test_produce_injects_the_context_into_the_headers() -> None:
    bus = MemoryBus(default_partitions=1)
    events = Events("hub-test")
    producer = TrackedProducer(bus.producer("tx"), events)

    with payment_context(PaymentBaggage(uetr="abc", format="MX", trace_level="full")):
        producer.begin()
        producer.produce("t", "abc", b"v", {"format": "MX"})
        producer.commit()

    record = bus.records("t")[0]
    assert record.headers["format"] == "MX"
    assert "payment.uetr=abc" in record.headers["baggage"]


def test_consume_restores_the_context_before_processing() -> None:
    bus = MemoryBus(default_partitions=1)
    events = Events("hub-test")
    producer = TrackedProducer(bus.producer("tx"), events)
    with payment_context(PaymentBaggage(uetr="abc", format="MT", trace_level="full")):
        producer.begin()
        producer.produce("t", "abc", b"v")
        producer.commit()

    record = bus.records("t")[0]
    assert baggage.current_uetr() == ""
    with consumed(record, events, "cg-test") as bag:
        assert bag.uetr == "abc"
        assert baggage.current_uetr() == "abc"
    assert baggage.current_uetr() == ""


def test_carry_drops_the_context_headers_but_keeps_ours() -> None:
    from hub_telemetry.messaging import Record

    record = Record(
        topic="t",
        key="k",
        value=b"v",
        headers={"baggage": "payment.uetr=x", "traceparent": "00-a-b-01", "format": "MX"},
    )
    carried = carry(record, {"hub-retry-attempt": "1"})
    assert carried == {"format": "MX", "hub-retry-attempt": "1"}
