"""One payment end to end moves every series the performance dashboard reads."""

from __future__ import annotations

from hub_model.flows import FLOW_MX_SNF_PACS008, PACS_008
from hub_telemetry import metrics

from tests.harness import Harness


def sample(name: str, labels: dict[str, str] | None = None) -> float:
    return metrics.REGISTRY.get_sample_value(name, labels or {}) or 0.0


async def test_a_payment_moves_the_dashboard_series(hub: Harness) -> None:
    inbound = {"service": "hub.v1.HubInbound", "method": "DeliverMx", "status": "OK"}
    outbound = {"service": "ext.v1.SnfGateway", "method": "SendMx", "status": "OK"}
    e2e = {"format": "MX", "flow": FLOW_MX_SNF_PACS008}
    processed = {"service": "hub-routing", "outcome": "ok"}
    commits = {"service": "hub-routing"}
    names = (
        ("hub_grpc_server_calls_total", inbound),
        ("hub_grpc_client_calls_total", outbound),
        ("ess_e2e_latency_seconds_count", e2e),
        ("hub_records_processed_total", processed),
        ("hub_kafka_commit_seconds_count", commits),
        ("hub_process_latency_seconds_count", commits),
        ("hub_kafka_poll_seconds_count", commits),
        ("hub_post_commit_seconds_count", {"service": "hub-db-sink"}),
    )
    before = {name: sample(name, labels) for name, labels in names}

    delivery = hub.ess.sender.build(PACS_008, flow=FLOW_MX_SNF_PACS008)
    await hub.ess.sender.deliver(delivery)
    assert await hub.run_until_state(delivery.uetr, "COMPLETED") == "COMPLETED"

    for name, labels in names:
        assert sample(name, labels) > before[name], f"{name}{labels} did not move"
    assert sample("hub_kafka_commit_max_seconds", commits) > 0
    assert sample("hub_process_latency_max_seconds", commits) > 0
