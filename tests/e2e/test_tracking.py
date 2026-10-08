"""Payment tracking: baggage, ECS events and tracking levels.

Design doc section 9. These are the tests that say "you can follow this
payment in Kibana", and section 9.1's promise that a performance run costs
nothing in logging.
"""

from __future__ import annotations

import io
import json
import logging

from hub_model import topics as tp
from hub_model.flows import FLOW_MX_SNF_PACS008, PACS_008
from hub_telemetry import baggage
from hub_telemetry.ecs import setup_logging
from hub_telemetry.tracking import (
    GRPC_SERVER_RECV,
    KAFKA_CONSUME,
    KAFKA_PRODUCE,
    PAYMENT_ERROR,
    PAYMENT_STATE,
    Level,
)

from tests.harness import Harness, start_harness


class CapturedLogs:
    """Capture ECS lines synchronously, so assertions see them immediately."""

    def __init__(self) -> None:
        self.stream = io.StringIO()

    def __enter__(self) -> CapturedLogs:
        setup_logging("test-capture", level=logging.INFO, stream=self.stream, asynchronous=False)
        return self

    def __exit__(self, *exc: object) -> None:
        setup_logging("tests", level=logging.WARNING, asynchronous=False)

    def events(self) -> list[dict[str, object]]:
        out: list[dict[str, object]] = []
        for line in self.stream.getvalue().splitlines():
            if line.strip().startswith("{"):
                out.append(json.loads(line))
        return out

    def actions(self, uetr: str = "") -> list[str]:
        return [
            str(e.get("event.action"))
            for e in self.events()
            if e.get("event.action") and (not uetr or e.get("payment.uetr") == uetr)
        ]


# --------------------------------------------------------------- baggage
async def test_baggage_travels_on_every_kafka_record(hub: Harness) -> None:
    delivery = hub.ess.sender.build(PACS_008, flow=FLOW_MX_SNF_PACS008)
    await hub.ess.sender.deliver(delivery)
    await hub.run_until_state(delivery.uetr, "COMPLETED")

    for topic in (
        tp.IN_MX_RAW,
        tp.PAY_CANONICAL,
        tp.PAY_SCREENED,
        tp.PAY_ROUTED,
        tp.OUT_MX,
        tp.PAY_STATUS,
        tp.PAY_STATE,
    ):
        record = next(r for r in hub.bus.records(topic) if r.key == delivery.uetr)
        header = record.headers.get("baggage", "")
        assert header, f"no baggage header on {topic}"
        assert f"payment.uetr={delivery.uetr}" in header
        assert "payment.format=MX" in header
        assert "payment.trace=" in header


async def test_baggage_stays_under_512_bytes(hub: Harness) -> None:
    delivery = hub.ess.sender.build(PACS_008, flow=FLOW_MX_SNF_PACS008)
    await hub.ess.sender.deliver(delivery)
    await hub.run_until_state(delivery.uetr, "COMPLETED")

    for topic in (tp.IN_MX_RAW, tp.PAY_CANONICAL, tp.PAY_STATUS):
        for record in hub.bus.records(topic):
            assert len(record.headers.get("baggage", "")) <= baggage.MAX_BAGGAGE_BYTES


async def test_baggage_never_carries_names_accounts_or_amounts(hub: Harness) -> None:
    """Section 9's hard rule. The screening request carries them instead."""
    delivery = hub.ess.sender.build(PACS_008, flow=FLOW_MX_SNF_PACS008)
    await hub.ess.sender.deliver(delivery)
    await hub.run_until_state(delivery.uetr, "COMPLETED")

    env_record = next(r for r in hub.bus.records(tp.PAY_CANONICAL) if r.key == delivery.uetr)
    from hub_model.proto import PaymentEnvelope

    env = PaymentEnvelope()
    env.ParseFromString(env_record.value)
    secrets = [
        env.payment.debtor.name,
        env.payment.creditor.name,
        env.payment.debtor.account.iban,
        env.payment.creditor.account.iban,
        env.payment.interbank_settlement_amount.amount,
    ]

    for topic in (tp.IN_MX_RAW, tp.PAY_CANONICAL, tp.PAY_SCREENED, tp.PAY_STATUS):
        for record in hub.bus.records(topic):
            header = record.headers.get("baggage", "")
            for secret in secrets:
                if secret:
                    assert secret not in header, f"{secret!r} leaked into baggage on {topic}"

    # The allowed keys, and nothing else.
    keys = {
        pair.split("=", 1)[0]
        for record in hub.bus.records(tp.PAY_CANONICAL)
        for pair in record.headers.get("baggage", "").split(",")
        if "=" in pair
    }
    assert keys <= set(baggage.PAYMENT_KEYS), f"unexpected baggage keys: {keys}"


async def test_traceparent_travels_with_the_baggage(hub: Harness) -> None:
    delivery = hub.ess.sender.build(PACS_008, flow=FLOW_MX_SNF_PACS008)
    await hub.ess.sender.deliver(delivery)
    await hub.run_until_state(delivery.uetr, "COMPLETED")

    record = next(r for r in hub.bus.records(tp.PAY_CANONICAL) if r.key == delivery.uetr)
    assert "traceparent" in record.headers
    assert record.headers["traceparent"].startswith("00-")


def test_baggage_is_stripped_for_the_real_networks() -> None:
    """The real FIN / SnF / compliance service must never see our internal context."""
    headers = {
        "baggage": "payment.uetr=x,payment.format=MX",
        "traceparent": "00-abc-def-01",
        "uetr": "x",
        "content-type": "application/grpc",
    }
    stripped = baggage.strip(headers)
    assert "baggage" not in stripped
    assert "traceparent" not in stripped
    assert stripped["content-type"] == "application/grpc"


# ------------------------------------------------------------ ECS events
async def test_full_tracking_logs_every_touchpoint_class(hub: Harness) -> None:
    with CapturedLogs() as logs:
        delivery = hub.ess.sender.build(PACS_008, flow=FLOW_MX_SNF_PACS008)
        await hub.ess.sender.deliver(delivery)
        await hub.run_until_state(delivery.uetr, "COMPLETED")

    actions = set(logs.actions(delivery.uetr))
    for expected in (GRPC_SERVER_RECV, KAFKA_PRODUCE, KAFKA_CONSUME, PAYMENT_STATE):
        assert expected in actions, f"no {expected} event; saw {sorted(actions)}"


async def test_events_are_ecs_json_with_the_payment_fields(hub: Harness) -> None:
    with CapturedLogs() as logs:
        delivery = hub.ess.sender.build(PACS_008, flow=FLOW_MX_SNF_PACS008)
        await hub.ess.sender.deliver(delivery)
        await hub.run_until_state(delivery.uetr, "COMPLETED")

    consume = [
        e
        for e in logs.events()
        if e.get("event.action") == KAFKA_CONSUME and e.get("payment.uetr") == delivery.uetr
    ]
    assert consume, "no kafka.consume events"
    sample = consume[0]
    for field in (
        "@timestamp",
        "service.name",
        "event.action",
        "event.outcome",
        "messaging.source",
        "messaging.kafka.partition",
        "messaging.kafka.offset",
        "messaging.kafka.consumer.group",
        "payment.uetr",
        "payment.format",
        "payment.msg_type",
        "payment.flow",
    ):
        assert field in sample, f"{field} missing from a kafka.consume event"


async def test_the_whole_journey_is_searchable_by_uetr(hub: Harness) -> None:
    """The Kibana 'payment journey' saved search, in test form."""
    with CapturedLogs() as logs:
        delivery = hub.ess.sender.build(PACS_008, flow=FLOW_MX_SNF_PACS008)
        await hub.ess.sender.deliver(delivery)
        await hub.run_until_state(delivery.uetr, "COMPLETED")

    journey = [e for e in logs.events() if e.get("payment.uetr") == delivery.uetr]
    services = {str(e.get("service.name")) for e in journey}
    assert len(journey) >= 20, f"only {len(journey)} events for one payment"
    # Every stage the payment passed through must appear under its own name.
    for service in ("hub-mx-parser", "hub-screening", "hub-routing", "hub-settlement"):
        assert service in services, f"{service} left no trace; saw {sorted(services)}"

    states = [
        str(e.get("payment.state")) for e in journey if e.get("event.action") == PAYMENT_STATE
    ]
    assert states[0] == "RECEIVED"
    assert states[-1] == "COMPLETED"


# ----------------------------------------------------------- levels
async def test_performance_mode_logs_almost_nothing(perf_hub: Harness) -> None:
    """Section 9.1: at ``errors`` a clean payment costs zero log events."""
    with CapturedLogs() as logs:
        delivery = perf_hub.ess.sender.build(PACS_008, flow=FLOW_MX_SNF_PACS008)
        await perf_hub.ess.sender.deliver(delivery)
        await perf_hub.run_until_state(delivery.uetr, "COMPLETED", timeout_s=20.0)

    assert perf_hub.state_of(delivery.uetr) == "COMPLETED"
    actions = logs.actions(delivery.uetr)
    assert actions == [], f"a clean payment logged {actions} at the errors level"


async def test_performance_mode_still_logs_failures(perf_hub: Harness) -> None:
    from ess.emulators import Override

    with CapturedLogs() as logs:
        delivery = perf_hub.ess.sender.build(PACS_008, flow=FLOW_MX_SNF_PACS008)
        perf_hub.ess.overrides.set(delivery.uetr, Override(nak=True, nak_code="H50"))
        await perf_hub.ess.sender.deliver(delivery)
        await perf_hub.run_until_state(delivery.uetr, "REJECTED", timeout_s=20.0)

    actions = set(logs.actions(delivery.uetr))
    assert PAYMENT_STATE in actions, "a REJECTED state must survive the errors level"
    assert KAFKA_CONSUME not in actions, "routine hops must not be logged at errors"


async def test_the_edge_stamps_the_level_into_baggage(hub: Harness) -> None:
    delivery = hub.ess.sender.build(PACS_008, flow=FLOW_MX_SNF_PACS008)
    await hub.ess.sender.deliver(delivery)

    record = hub.bus.records(tp.IN_MX_RAW)[0]
    assert f"payment.trace={Level.FULL.value}" in record.headers["baggage"]

    from hub_model.proto import RawInbound

    raw = RawInbound()
    raw.ParseFromString(record.value)
    assert raw.trace_level == Level.FULL.value


async def test_performance_mode_stamps_errors_level() -> None:
    harness = await start_harness(tracking_mode="performance", ess_mode="performance")
    try:
        delivery = harness.ess.sender.build(PACS_008, flow=FLOW_MX_SNF_PACS008)
        await harness.ess.sender.deliver(delivery)
        record = harness.bus.records(tp.IN_MX_RAW)[0]
        assert f"payment.trace={Level.ERRORS.value}" in record.headers["baggage"]
    finally:
        await harness.close()


async def test_errors_are_always_visible(hub: Harness) -> None:
    """``payment.error`` is the one event no level except ``none`` suppresses."""
    from ess.profiles import Profile

    with CapturedLogs() as logs:
        hub.ess.profiles.set(Profile(target="COMPLIANCE", outage_until_ns=-1))
        delivery = hub.ess.sender.build(PACS_008, flow=FLOW_MX_SNF_PACS008)
        await hub.ess.sender.deliver(delivery)
        await hub.settle(until=lambda: PAYMENT_ERROR in logs.actions(delivery.uetr), timeout_s=5.0)

    assert PAYMENT_ERROR in logs.actions(delivery.uetr)


async def test_every_produce_event_belongs_to_its_payment(hub: Harness) -> None:
    """A Kafka write is logged after its transaction commits — by which point
    the per-record context has been detached. The producer has to carry the
    context forward, or nine of the eighteen Kafka touchpoints drop out of the
    payment journey with no error anywhere.
    """
    with CapturedLogs() as logs:
        delivery = hub.ess.sender.build(PACS_008, flow=FLOW_MX_SNF_PACS008)
        await hub.ess.sender.deliver(delivery)
        await hub.run_until_state(delivery.uetr, "COMPLETED")

    produces = [e for e in logs.events() if e.get("event.action") == KAFKA_PRODUCE]
    assert produces, "no kafka.produce events at all"

    orphans = [
        f"{e.get('service.name')} -> {e.get('messaging.destination')}"
        for e in produces
        if not e.get("payment.uetr")
    ]
    assert orphans == [], f"produce events with no UETR: {orphans}"
    assert all(e.get("payment.uetr") == delivery.uetr for e in produces)


async def test_produce_events_keep_their_trace_id(hub: Harness) -> None:
    """Logs and traces join on trace.id, so a produce must keep the span it
    was made under, not just the baggage."""
    with CapturedLogs() as logs:
        delivery = hub.ess.sender.build(PACS_008, flow=FLOW_MX_SNF_PACS008)
        await hub.ess.sender.deliver(delivery)
        await hub.run_until_state(delivery.uetr, "COMPLETED")

    produces = [
        e
        for e in logs.events()
        if e.get("event.action") == KAFKA_PRODUCE and e.get("payment.uetr") == delivery.uetr
    ]
    assert produces
    assert all(e.get("trace.id") for e in produces), "a produce event lost its trace id"
