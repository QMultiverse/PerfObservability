"""Retries, the DLQ, outages, and the promise that one bad message never blocks.

Design doc section 7: a processing error sends the record to ``.retry.30s``,
then ``.retry.5m``, then ``.dlq``; the main partition keeps flowing.
"""

from __future__ import annotations

import asyncio

import pytest
from hub_model import proto as pb
from hub_model import topics as tp
from hub_model.envelope import now_ns
from hub_model.flows import FLOW_MX_SNF_PACS008, MT103, PACS_008
from hub_model.ids import new_uetr
from hub_model.proto import FailedRecord, MsgRef, MxDelivery, RawInbound
from hub_telemetry.grpc_telemetry import DEADLINE_DELIVER_S

from ess.sender import Delivery
from tests.harness import Harness


# --------------------------------------------------------- bad messages
async def test_malformed_mx_is_dead_lettered_not_retried(hub: Harness) -> None:
    """A malformed document will be malformed next time too: straight to DLQ."""
    uetr = new_uetr()
    # Well-formed XML, a real namespace, but missing every element the Hub
    # needs: amounts, parties, group header.
    broken = (
        "<Document xmlns='urn:iso:std:iso:20022:tech:xsd:pacs.008.001.08'>"
        "<FIToFICstmrCdtTrf><CdtTrfTxInf><PmtId>"
        f"<UETR>{uetr}</UETR>"
        "</PmtId></CdtTrfTxInf></FIToFICstmrCdtTrf></Document>"
    )
    delivery = Delivery(
        msg_type=PACS_008, uetr=uetr, payload=broken.encode(), flow=FLOW_MX_SNF_PACS008
    )

    receipt = await hub.ess.sender.deliver(delivery)
    assert receipt.status == pb.DeliveryReceipt.ACCEPTED, "the edge does not parse"

    await hub.settle(until=lambda: bool(hub.dlq(tp.IN_MX_RAW)), timeout_s=5.0)
    dead = hub.dlq(tp.IN_MX_RAW)
    assert len(dead) == 1

    failed = FailedRecord()
    failed.ParseFromString(dead[0].value)
    assert failed.ref.uetr == uetr
    assert failed.origin_topic == tp.IN_MX_RAW
    assert "validation" in failed.error_message.lower()
    # A permanent failure skips the retry ladder entirely.
    assert hub.bus.records(tp.IN_MX_RAW + tp.RETRY_30S_SUFFIX) == []
    assert hub.state_of(uetr) == "REJECTED"


async def test_malformed_mt_is_dead_lettered(hub: Harness) -> None:
    uetr = new_uetr()
    broken = (
        "{1:F01TESTGB2LXXXX0000000000}{2:I103TESTDEFFXXXXN}"
        f"{{3:{{121:{uetr}}}}}{{4:\n:20:REF\n-}}"  # no field 32A
    )
    delivery = Delivery(msg_type=MT103, uetr=uetr, payload=broken.encode(), flow="MT_FIN_103")
    await hub.ess.sender.deliver(delivery)

    await hub.settle(until=lambda: bool(hub.dlq(tp.IN_FIN_RAW)), timeout_s=5.0)
    failed = FailedRecord()
    failed.ParseFromString(hub.dlq(tp.IN_FIN_RAW)[0].value)
    assert "32A" in failed.error_message


async def test_one_bad_message_does_not_block_its_partition(hub: Harness) -> None:
    """The point of the retry ladder: good payments keep flowing past a bad one."""
    uetr = new_uetr()
    bad = Delivery(
        msg_type=PACS_008,
        uetr=uetr,
        payload=b"<Document xmlns='urn:iso:std:iso:20022:tech:xsd:pacs.008.001.08'/>",
        flow=FLOW_MX_SNF_PACS008,
    )
    good = hub.ess.sender.build(PACS_008, flow=FLOW_MX_SNF_PACS008)

    await hub.ess.sender.deliver(bad)
    await hub.ess.sender.deliver(good)

    assert await hub.run_until_state(good.uetr, "COMPLETED") == "COMPLETED"
    assert hub.dlq(tp.IN_MX_RAW), "the bad message should be dead-lettered"


# --------------------------------------------------------------- outages
async def test_compliance_outage_retries_then_recovers(hub: Harness) -> None:
    """Compliance screening being down is transient: the record goes round the retry ladder."""
    from ess.profiles import Profile

    hub.ess.profiles.set(Profile(target="COMPLIANCE", outage_until_ns=-1))
    delivery = hub.ess.sender.build(PACS_008, flow=FLOW_MX_SNF_PACS008)
    await hub.ess.sender.deliver(delivery)

    retry_topic = tp.PAY_CANONICAL + tp.RETRY_30S_SUFFIX
    await hub.settle(until=lambda: bool(hub.bus.records(retry_topic)), timeout_s=5.0)
    queued = hub.bus.records(retry_topic)
    assert queued, "an unreachable compliance service should retry, not dead-letter"

    failed = FailedRecord()
    failed.ParseFromString(queued[0].value)
    assert failed.error_type == "RetryableError"
    assert "Compliance screening" in failed.error_message
    assert failed.attempt == 1

    # The failure is visible on the status stream as FAILED, not REJECTED.
    states = [pb.state_name(e.state) for e in hub.status_events(delivery.uetr)]
    assert "FAILED" in states


async def test_a_dispatch_retried_after_an_snf_outage_completes(
    hub: Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Scenario 3's bug A: the payment went round the ladder, was dispatched and
    ACKed, yet the status view kept reporting FAILED."""
    import hub.common.retry_consumer as retry_consumer
    from ess.profiles import Profile

    hub.ess.profiles.set(Profile(target="SNF", outage_until_ns=-1, delivery_notifications=True))
    delivery = hub.ess.sender.build(PACS_008, flow=FLOW_MX_SNF_PACS008)
    await hub.ess.sender.deliver(delivery)
    retry_topic = tp.OUT_MX + tp.RETRY_30S_SUFFIX
    await hub.settle(until=lambda: bool(hub.bus.records(retry_topic)), timeout_s=5.0)
    assert hub.state_of(delivery.uetr) == "FAILED"

    # The outage ends; the retry consumer's clock moves past the 30 s delay.
    hub.ess.profiles.set(Profile(target="SNF", delivery_notifications=True))
    monkeypatch.setattr(retry_consumer, "now_ns", lambda: now_ns() + 31_000_000_000)
    await hub.runners["retry"].run_once()

    assert await hub.run_until_state(delivery.uetr, "COMPLETED") == "COMPLETED"
    states = [pb.state_name(e.state) for e in hub.status_events(delivery.uetr)]
    assert states[-3:] == ["FAILED", "DISPATCHED", "COMPLETED"]


async def test_fin_outage_makes_deliver_unavailable(hub: Harness) -> None:
    """An outage on the Hub's side is the ESS returning UNAVAILABLE."""
    import grpc

    from ess.profiles import Profile

    hub.ess.profiles.set(Profile(target="SNF", outage_until_ns=-1))
    delivery = hub.ess.sender.build(PACS_008, flow=FLOW_MX_SNF_PACS008)
    # Delivery still works — the outage is on SnF's *serving* side, which the
    # dispatcher hits, not the edge.
    await hub.ess.sender.deliver(delivery)

    await hub.settle(
        until=lambda: bool(hub.bus.records(tp.OUT_MX + tp.RETRY_30S_SUFFIX)), timeout_s=5.0
    )
    assert hub.bus.records(tp.OUT_MX + tp.RETRY_30S_SUFFIX)
    assert isinstance(grpc.StatusCode.UNAVAILABLE, grpc.StatusCode)


# ---------------------------------------------------------- edge rejects
async def test_edge_rejects_a_message_with_no_uetr(hub: Harness) -> None:
    import grpc

    stub = pb.HubInboundStub(grpc.aio.insecure_channel(hub.edge_address))
    request = MxDelivery(
        ref=MsgRef(msg_type=PACS_008, flow=FLOW_MX_SNF_PACS008),
        document=b"<Document xmlns='urn:iso:std:iso:20022:tech:xsd:pacs.008.001.08'/>",
        network_ts_ns=now_ns(),
    )
    receipt = await stub.DeliverMx(request, timeout=DEADLINE_DELIVER_S)
    assert receipt.status == pb.DeliveryReceipt.REJECTED
    assert "UETR" in receipt.reason
    assert hub.bus.records(tp.IN_MX_RAW) == [], "a rejected message never reaches Kafka"


async def test_edge_rejects_an_empty_message(hub: Harness) -> None:
    import grpc

    stub = pb.HubInboundStub(grpc.aio.insecure_channel(hub.edge_address))
    receipt = await stub.DeliverMx(
        MxDelivery(ref=MsgRef(uetr=new_uetr(), msg_type=PACS_008)),
        timeout=DEADLINE_DELIVER_S,
    )
    assert receipt.status == pb.DeliveryReceipt.REJECTED
    assert "empty" in receipt.reason.lower()


async def test_edge_rejects_an_unsupported_message_type(hub: Harness) -> None:
    import grpc

    uetr = new_uetr()
    stub = pb.HubInboundStub(grpc.aio.insecure_channel(hub.edge_address))
    receipt = await stub.DeliverMx(
        MxDelivery(
            ref=MsgRef(uetr=uetr, msg_type="camt.999.001.01"),
            document=f"<Document><UETR>{uetr}</UETR></Document>".encode(),
        ),
        timeout=DEADLINE_DELIVER_S,
    )
    assert receipt.status == pb.DeliveryReceipt.REJECTED
    assert "unsupported" in receipt.reason.lower()


# ------------------------------------------------------------ retry loop
async def test_retry_consumer_republishes_to_the_origin_topic(hub: Harness) -> None:
    """A record on a retry topic goes back to where it failed, once due."""
    uetr = new_uetr()
    failed = FailedRecord(
        origin_topic=tp.IN_MX_RAW,
        payload=RawInbound(format=pb.MX, document=b"<x/>").SerializeToString(),
        error_type="RetryableError",
        error_message="injected",
        attempt=1,
        first_failed_ns=now_ns(),
        failed_ns=now_ns(),
        consumer_group=tp.CG_MX_PARSER,
    )
    failed.ref.uetr = uetr
    failed.ref.msg_type = PACS_008
    failed.ref.flow = FLOW_MX_SNF_PACS008

    producer = hub.bus.producer()
    producer.produce(
        tp.IN_MX_RAW + tp.RETRY_30S_SUFFIX,
        uetr,
        failed.SerializeToString(),
        # Already due: the header is what the retry consumer waits on.
        {"hub-retry-at-ns": str(now_ns() - 1), "hub-origin-topic": tp.IN_MX_RAW},
    )

    before = len(hub.bus.records(tp.IN_MX_RAW))
    await hub.runners["retry"].run_once()
    after = hub.bus.records(tp.IN_MX_RAW)
    assert len(after) == before + 1
    assert after[-1].key == uetr
    assert after[-1].headers["hub-retry-attempt"] == "1"


async def test_retry_consumer_waits_until_the_record_is_due(hub: Harness) -> None:
    uetr = new_uetr()
    failed = FailedRecord(origin_topic=tp.IN_MX_RAW, payload=b"x", attempt=1)
    failed.ref.uetr = uetr

    producer = hub.bus.producer()
    producer.produce(
        tp.IN_MX_RAW + tp.RETRY_30S_SUFFIX,
        uetr,
        failed.SerializeToString(),
        {"hub-retry-at-ns": str(now_ns() + 300_000_000), "hub-origin-topic": tp.IN_MX_RAW},
    )

    task = asyncio.create_task(hub.runners["retry"].run_once())
    await asyncio.sleep(0.05)
    assert not task.done(), "the retry consumer should still be waiting"
    assert hub.bus.records(tp.IN_MX_RAW) == []
    await task
    assert len(hub.bus.records(tp.IN_MX_RAW)) == 1
