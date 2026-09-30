"""The vertical slice from CLAUDE.md's build order.

One pacs.008, no screening hit, from ``ess send`` through the edge, the parser,
screening, routing, settlement, the dispatcher and the ACK matcher, to
COMPLETED on ``hub.pay.status``. Then the same for MT103.

These are the tests that say the platform works end to end.
"""

from __future__ import annotations

from hub_model import proto as pb
from hub_model import topics as tp
from hub_model.envelope import end_to_end_seconds
from hub_model.flows import FLOW_MT_FIN_103, FLOW_MX_SNF_PACS008, MT103, PACS_008
from hub_model.proto import DeliveryReceipt, PaymentEnvelope, StatusEvent

from tests.harness import Harness


async def test_pacs008_reaches_completed(hub: Harness) -> None:
    delivery = hub.ess.sender.build(PACS_008, flow=FLOW_MX_SNF_PACS008)
    receipt = await hub.ess.sender.deliver(delivery)
    assert receipt.status == DeliveryReceipt.ACCEPTED
    assert receipt.accepted_ns > 0, "the edge stamps T1 after the Kafka write"

    state = await hub.run_until_state(delivery.uetr, "COMPLETED")
    assert state == "COMPLETED", f"stuck in {state or 'no state'}"


async def test_mt103_reaches_completed(hub: Harness) -> None:
    delivery = hub.ess.sender.build(MT103, flow=FLOW_MT_FIN_103)
    receipt = await hub.ess.sender.deliver(delivery)
    assert receipt.status == DeliveryReceipt.ACCEPTED

    state = await hub.run_until_state(delivery.uetr, "COMPLETED")
    assert state == "COMPLETED", f"stuck in {state or 'no state'}"


async def test_pacs008_visits_every_stage_in_order(hub: Harness) -> None:
    """The journey a Kibana search on one UETR should show."""
    delivery = hub.ess.sender.build(PACS_008, flow=FLOW_MX_SNF_PACS008)
    await hub.ess.sender.deliver(delivery)
    await hub.run_until_state(delivery.uetr, "COMPLETED")

    states = [pb.state_name(e.state) for e in hub.status_events(delivery.uetr)]
    assert states == [
        "RECEIVED",
        "PARSED",
        "SCREENED",
        "ROUTED",
        "SETTLED",
        "DISPATCHED",
        "COMPLETED",
    ]


async def test_payment_reaches_every_topic_on_the_mx_lane(hub: Harness) -> None:
    delivery = hub.ess.sender.build(PACS_008, flow=FLOW_MX_SNF_PACS008)
    await hub.ess.sender.deliver(delivery)
    await hub.run_until_state(delivery.uetr, "COMPLETED")

    for topic in (
        tp.IN_MX_RAW,
        tp.PAY_CANONICAL,
        tp.PAY_SCREENED,
        tp.PAY_ROUTED,
        tp.OUT_MX,
        tp.NET_ACK,
        tp.PAY_STATUS,
        tp.PAY_STATE,
    ):
        keys = [record.key for record in hub.bus.records(topic)]
        assert delivery.uetr in keys, f"nothing for this payment on {topic}"

    # The MT lane must stay empty: MT and MX are separate until the canonical
    # topic and separate again at the outbound topics.
    assert hub.bus.records(tp.IN_FIN_RAW) == []
    assert hub.bus.records(tp.OUT_FIN) == []


async def test_mt103_stays_on_the_fin_lane(hub: Harness) -> None:
    delivery = hub.ess.sender.build(MT103, flow=FLOW_MT_FIN_103)
    await hub.ess.sender.deliver(delivery)
    await hub.run_until_state(delivery.uetr, "COMPLETED")

    assert [r.key for r in hub.bus.records(tp.IN_FIN_RAW)] == [delivery.uetr]
    assert [r.key for r in hub.bus.records(tp.OUT_FIN)] == [delivery.uetr]
    assert hub.bus.records(tp.IN_MX_RAW) == []
    assert hub.bus.records(tp.OUT_MX) == []


async def test_all_seven_timestamps_are_stamped(hub: Harness) -> None:
    """T0-T6 are what the performance framework measures stages with."""
    delivery = hub.ess.sender.build(PACS_008, flow=FLOW_MX_SNF_PACS008)
    await hub.ess.sender.deliver(delivery)
    await hub.run_until_state(delivery.uetr, "COMPLETED")

    final: StatusEvent = hub.status_events(delivery.uetr)[-1]
    timings = final.timings
    stamped = {
        "T0": timings.t0_network_sent_ns,
        "T1": timings.t1_edge_accepted_ns,
        "T2": timings.t2_canonical_ns,
        "T3": timings.t3_screen_reply_ns,
        "T4": timings.t4_handoff_ns,
        "T5": timings.t5_network_ack_ns,
        "T6": timings.t6_completed_ns,
    }
    missing = [name for name, value in stamped.items() if not value]
    assert not missing, f"unstamped: {missing}"

    ordered = list(stamped.values())
    assert ordered == sorted(ordered), f"timestamps out of order: {stamped}"
    elapsed = end_to_end_seconds(timings)
    assert elapsed is not None and elapsed > 0


async def test_canonical_payment_keeps_the_business_data(hub: Harness) -> None:
    """The MX parser's output is what every later stage works from."""
    delivery = hub.ess.sender.build(PACS_008, flow=FLOW_MX_SNF_PACS008)
    await hub.ess.sender.deliver(delivery)
    await hub.run_until_state(delivery.uetr, "COMPLETED")

    record = next(r for r in hub.bus.records(tp.PAY_CANONICAL) if r.key == delivery.uetr)
    env = PaymentEnvelope()
    env.ParseFromString(record.value)

    assert env.format == pb.MX
    assert env.ref.msg_type == PACS_008
    assert env.payment.debtor.name
    assert env.payment.creditor.name
    assert env.payment.interbank_settlement_amount.currency
    assert env.payment.interbank_settlement_amount.amount
    assert env.payment.end_to_end_id
    # Claim check: the canonical record points back at the raw topic rather
    # than carrying the XML again.
    assert env.payment.raw.topic == tp.IN_MX_RAW
    assert not env.payment.HasField("cover") or not env.payment.cover.partner_msg_type


async def test_outbound_message_is_built_and_sent(hub: Harness) -> None:
    delivery = hub.ess.sender.build(PACS_008, flow=FLOW_MX_SNF_PACS008)
    await hub.ess.sender.deliver(delivery)
    await hub.run_until_state(delivery.uetr, "COMPLETED")

    record = next(r for r in hub.bus.records(tp.OUT_MX) if r.key == delivery.uetr)
    outbound = pb.OutboundMessage()
    outbound.ParseFromString(record.value)
    assert outbound.format == pb.MX
    assert outbound.document.startswith(b"<?xml")
    assert b"<Document" in outbound.document
    assert outbound.app_hdr.startswith(b"<?xml")
    assert outbound.receiver_bic

    counters = hub.ess.recorder.counters()
    assert counters["by_method"].get("SnfGateway.SendMx", 0) == 1
    assert counters["by_method"].get("HubNetworkEvents.NotifyAck", 0) >= 1


async def test_two_payments_do_not_interfere(hub: Harness) -> None:
    mx = hub.ess.sender.build(PACS_008, flow=FLOW_MX_SNF_PACS008)
    mt = hub.ess.sender.build(MT103, flow=FLOW_MT_FIN_103)
    await hub.ess.sender.send([mx, mt])

    await hub.settle(
        until=lambda: hub.state_of(mx.uetr) == "COMPLETED" and hub.state_of(mt.uetr) == "COMPLETED",
        timeout_s=15.0,
    )
    assert hub.state_of(mx.uetr) == "COMPLETED"
    assert hub.state_of(mt.uetr) == "COMPLETED"
    assert mx.uetr != mt.uetr
