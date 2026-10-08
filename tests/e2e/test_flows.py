"""The remaining flows: cover pairs, screening hits, NAKs, rejects and the DLQ.

Step 4 of CLAUDE.md's build order.
"""

from __future__ import annotations

from hub_model import proto as pb
from hub_model import topics as tp
from hub_model.flows import (
    FLOW_MT_FIN_103,
    FLOW_MT_FIN_103_202COV,
    FLOW_MT_TO_MX_103,
    FLOW_MX_SNF_PACS008,
    FLOW_MX_SNF_PACS009,
    FLOW_MX_SNF_PACS009COV_PAIR,
    MT103,
    MT202COV,
    PACS_008,
    PACS_009,
    PACS_009_COV,
)
from hub_model.proto import FailedRecord, OutboundMessage, PaymentEnvelope
from hub_telemetry.memory_bus import partition_for

from ess.emulators import Override
from tests.harness import Harness


# ------------------------------------------------------------ screening
async def test_screening_hit_is_held_then_released(hub: Harness) -> None:
    """HIT_PENDING parks the payment; NotifyComplianceDecision resumes it."""
    delivery = hub.ess.sender.build(PACS_008, flow=FLOW_MX_SNF_PACS008)
    hub.ess.overrides.set(delivery.uetr, Override(hit=True, release=True, decision_delay_s=0.05))
    await hub.ess.sender.deliver(delivery)

    # HELD first...
    await hub.settle(until=lambda: hub.state_of(delivery.uetr) == "HELD", timeout_s=5.0)
    assert hub.state_of(delivery.uetr) == "HELD"

    # ...then the analyst releases it and it runs to completion.
    state = await hub.run_until_state(delivery.uetr, "COMPLETED")
    assert state == "COMPLETED"
    assert "HELD" in hub.history_of(delivery.uetr)

    env = _envelope_on(hub, tp.PAY_SCREENED, delivery.uetr)
    assert env.screening.outcome == pb.Screening.RELEASED
    assert env.screening.case_id
    assert env.screening.decided_ns > 0


async def test_screening_hit_then_analyst_block_is_terminal(hub: Harness) -> None:
    delivery = hub.ess.sender.build(PACS_008, flow=FLOW_MX_SNF_PACS008)
    hub.ess.overrides.set(delivery.uetr, Override(hit=True, release=False, decision_delay_s=0.05))
    await hub.ess.sender.deliver(delivery)

    state = await hub.run_until_state(delivery.uetr, "BLOCKED")
    assert state == "BLOCKED"
    # A blocked payment never reaches the outbound lane.
    assert delivery.uetr not in [r.key for r in hub.bus.records(tp.OUT_MX)]


async def test_immediate_block_stops_at_screening(hub: Harness) -> None:
    delivery = hub.ess.sender.build(PACS_008, flow=FLOW_MX_SNF_PACS008)
    hub.ess.overrides.set(delivery.uetr, Override(block=True))
    await hub.ess.sender.deliver(delivery)

    state = await hub.run_until_state(delivery.uetr, "BLOCKED")
    assert state == "BLOCKED"
    assert hub.history_of(delivery.uetr) == ["RECEIVED", "PARSED", "BLOCKED"]
    assert delivery.uetr not in [r.key for r in hub.bus.records(tp.PAY_SCREENED)]


async def test_watchlist_name_hits_deterministically(hub: Harness) -> None:
    """A name on the test watchlist hits whatever the profile's rate is."""
    delivery = hub.ess.sender.build(PACS_008, flow=FLOW_MX_SNF_PACS008, sanctions_hit=True)
    await hub.ess.sender.deliver(delivery)
    await hub.settle(until=lambda: hub.state_of(delivery.uetr) == "HELD", timeout_s=5.0)
    assert hub.state_of(delivery.uetr) == "HELD"


# ----------------------------------------------------------- cover pairs
async def test_mt_cover_pair_completes_together(hub: Harness) -> None:
    legs = hub.ess.sender.build_cover_pair((MT103, MT202COV), flow=FLOW_MT_FIN_103_202COV)
    uetr = legs[0].uetr
    assert legs[1].uetr == uetr, "both legs share one UETR"

    # Only the first leg so far: the pair must wait.
    await hub.ess.sender.deliver(legs[0])
    await hub.settle(until=lambda: hub.state_of(uetr) == "AWAITING_COVER", timeout_s=5.0)
    assert hub.state_of(uetr) == "AWAITING_COVER"
    assert hub.bus.records(tp.PAY_SCREENED) == []

    await hub.ess.sender.deliver(legs[1])
    assert await hub.run_until_state(uetr, "COMPLETED") == "COMPLETED"

    # Both legs went out, and both were acknowledged.
    outbound = [r for r in hub.bus.records(tp.OUT_FIN) if r.key == uetr]
    assert len(outbound) == 2


async def test_mx_cover_pair_completes_together(hub: Harness) -> None:
    legs = hub.ess.sender.build_cover_pair(
        (PACS_008, PACS_009_COV), flow=FLOW_MX_SNF_PACS009COV_PAIR
    )
    uetr = legs[0].uetr
    await hub.ess.sender.send(legs)
    assert await hub.run_until_state(uetr, "COMPLETED") == "COMPLETED"
    assert len([r for r in hub.bus.records(tp.OUT_MX) if r.key == uetr]) == 2


async def test_cover_legs_share_a_partition(hub: Harness) -> None:
    """Kafka keys on the UETR, so both legs land on the same partition."""
    legs = hub.ess.sender.build_cover_pair(
        (PACS_008, PACS_009_COV), flow=FLOW_MX_SNF_PACS009COV_PAIR
    )
    await hub.ess.sender.send(legs)
    await hub.run_until_state(legs[0].uetr, "COMPLETED")

    partitions = {
        record.partition for record in hub.bus.records(tp.IN_MX_RAW) if record.key == legs[0].uetr
    }
    assert len(partitions) == 1
    assert partitions == {partition_for(legs[0].uetr, 4)}


# ---------------------------------------------------------- NAK / reject
async def test_nak_rejects_the_payment(hub: Harness) -> None:
    delivery = hub.ess.sender.build(PACS_008, flow=FLOW_MX_SNF_PACS008)
    hub.ess.overrides.set(delivery.uetr, Override(nak=True, nak_code="H50"))
    await hub.ess.sender.deliver(delivery)

    assert await hub.run_until_state(delivery.uetr, "REJECTED") == "REJECTED"
    final = hub.status_events(delivery.uetr)[-1]
    assert final.error_code == "H50"
    assert "NAK" in final.reason


async def test_mt_nak_rejects_the_payment(hub: Harness) -> None:
    delivery = hub.ess.sender.build(MT103, flow=FLOW_MT_FIN_103)
    hub.ess.overrides.set(delivery.uetr, Override(nak=True, nak_code="T13"))
    await hub.ess.sender.deliver(delivery)
    assert await hub.run_until_state(delivery.uetr, "REJECTED") == "REJECTED"


async def test_network_rejecting_the_send_dead_letters_the_record(hub: Harness) -> None:
    """A REJECTED SendMx is terminal: retrying sends the same bytes."""
    delivery = hub.ess.sender.build(PACS_008, flow=FLOW_MX_SNF_PACS008)
    hub.ess.overrides.set(delivery.uetr, Override(reject_send=True))
    await hub.ess.sender.deliver(delivery)

    await hub.settle(until=lambda: bool(hub.dlq(tp.OUT_MX)), timeout_s=5.0)
    dead = hub.dlq(tp.OUT_MX)
    assert dead, "the rejected send should be dead-lettered"
    failed = FailedRecord()
    failed.ParseFromString(dead[0].value)
    assert failed.origin_topic == tp.OUT_MX
    assert "rejected" in failed.error_message.lower()
    assert hub.state_of(delivery.uetr) == "REJECTED"


# ------------------------------------------------------------ duplicates
async def test_duplicate_delivery_is_reported_not_reprocessed(hub: Harness) -> None:
    delivery = hub.ess.sender.build(PACS_008, flow=FLOW_MX_SNF_PACS008)
    first = await hub.ess.sender.deliver(delivery)
    second = await hub.ess.sender.deliver(delivery)

    assert first.status == pb.DeliveryReceipt.ACCEPTED
    assert second.status == pb.DeliveryReceipt.DUPLICATE
    # Only one record on the raw topic: the duplicate never became a payment.
    assert len([r for r in hub.bus.records(tp.IN_MX_RAW) if r.key == delivery.uetr]) == 1


async def test_duplicate_ack_completes_only_once(hub: Harness) -> None:
    delivery = hub.ess.sender.build(PACS_008, flow=FLOW_MX_SNF_PACS008)
    hub.ess.overrides.set(delivery.uetr, Override(duplicate=True))
    await hub.ess.sender.deliver(delivery)

    assert await hub.run_until_state(delivery.uetr, "COMPLETED") == "COMPLETED"
    await hub.settle(timeout_s=2.0)

    completions = [
        event for event in hub.status_events(delivery.uetr) if event.state == int(pb.COMPLETED)
    ]
    assert len(completions) == 1, "the repeated ACK must not complete the payment twice"


# ------------------------------------------------------------ translation
async def test_mt103_translates_to_pacs008_in_flow(hub: Harness) -> None:
    """MT_TO_MX_103: in over FIN as MT103, out over SnF as pacs.008."""
    delivery = hub.ess.sender.build(MT103, flow=FLOW_MT_TO_MX_103)
    await hub.ess.sender.deliver(delivery)
    assert await hub.run_until_state(delivery.uetr, "COMPLETED") == "COMPLETED"

    # In on the FIN lane...
    assert [r.key for r in hub.bus.records(tp.IN_FIN_RAW)] == [delivery.uetr]
    # ...out on the MX lane.
    assert [r.key for r in hub.bus.records(tp.OUT_MX)] == [delivery.uetr]
    assert hub.bus.records(tp.OUT_FIN) == []

    outbound = OutboundMessage()
    outbound.ParseFromString(hub.bus.records(tp.OUT_MX)[0].value)
    assert outbound.format == pb.MX
    assert b"pacs.008" in outbound.document
    assert outbound.envelope.route.translated is True


async def test_pacs009_without_a_cover_leg_completes_alone(hub: Harness) -> None:
    delivery = hub.ess.sender.build(PACS_009, flow=FLOW_MX_SNF_PACS009)
    await hub.ess.sender.deliver(delivery)
    assert await hub.run_until_state(delivery.uetr, "COMPLETED") == "COMPLETED"


# --------------------------------------------------------------- helpers
def _envelope_on(hub: Harness, topic: str, uetr: str) -> PaymentEnvelope:
    record = next(r for r in hub.bus.records(topic) if r.key == uetr)
    env = PaymentEnvelope()
    env.ParseFromString(record.value)
    return env
