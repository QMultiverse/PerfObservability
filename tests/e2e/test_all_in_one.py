"""The all-in-one runtime: every stage in one process, one thread each.

Same classes, same consumer groups, same topics as the per-stage deployment —
so a payment must behave identically. These tests run the threaded runtime for
real, rather than the harness's pumped loop, which is also the only place the
threading is exercised.
"""

from __future__ import annotations

import asyncio
import socket

import grpc
from hub_model import topics as tp
from hub_model.flows import FLOW_MT_FIN_103, FLOW_MX_SNF_PACS008, MT103, PACS_008
from hub_telemetry.events import Events
from hub_telemetry.grpc_telemetry import ServerTelemetryInterceptor
from hub_telemetry.memory_bus import MemoryBus
from hub_telemetry.tracked import TrackedProducer

from ess.server import EssSettings
from ess.server import build as build_ess
from hub.common import all_in_one
from hub.common.config import ExternalSettings, KafkaSettings, ServiceSettings
from hub.edge.server import register
from hub.status_api.processor import StatusApi
from tests.harness import local_specs


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


class RunningHub:
    """The all-in-one Hub plus an ESS, started for real."""

    def __init__(self) -> None:
        self.bus = MemoryBus(default_partitions=4)
        self.bus.ensure_topics(local_specs())
        self.edge_port = _free_port()
        self.ess_port = _free_port()
        self.settings = ServiceSettings(
            service_name="hub",
            grpc_port=self.edge_port,
            metrics_port=0,
            environment="test",
            kafka=KafkaSettings(
                bootstrap_servers="memory",
                partitions=4,
                replication=1,
                batch_size=100,
                # Short poll: with a thread each this only costs idle wakeups.
                poll_timeout_s=0.01,
            ),
            external=ExternalSettings(
                fin_target=f"localhost:{self.ess_port}",
                snf_target=f"localhost:{self.ess_port}",
                compliance_target=f"localhost:{self.ess_port}",
            ),
            pod_id="test",
        )
        self.events = Events("hub")
        self.hub = all_in_one.build(self.settings, self.bus)
        self.server: grpc.aio.Server | None = None
        self.producer: TrackedProducer | None = None
        self.ess = build_ess(
            EssSettings(
                mode="functional",
                port=self.ess_port,
                metrics_port=0,
                hub_target=f"localhost:{self.edge_port}",
                status_api_url="",
                seed=11,
            )
        )

    async def start(self) -> None:
        self.hub.start()
        self.producer = TrackedProducer(self.bus.producer(), self.events)
        self.server = grpc.aio.server(interceptors=[ServerTelemetryInterceptor(self.events)])
        register(self.server, self.settings, self.events, self.producer)
        self.server.add_insecure_port(f"[::]:{self.edge_port}")
        await self.server.start()
        await self.ess.start()

    async def stop(self) -> None:
        await self.ess.stop()
        self.hub.stop()
        if self.server is not None:
            await self.server.stop(grace=0.0)
        if self.producer is not None:
            self.producer.close()
        self.hub.join(timeout=5.0)
        self.bus.close()

    @property
    def status_view(self) -> StatusApi:
        processor = self.hub.stage("status-api").runner.processor  # type: ignore[union-attr]
        assert isinstance(processor, StatusApi)
        return processor

    def state_of(self, uetr: str) -> str:
        entry = self.status_view.view.get(uetr)
        return entry.state if entry else ""

    async def wait_for(self, uetr: str, state: str, timeout_s: float = 20.0) -> str:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout_s
        while loop.time() < deadline:
            if self.state_of(uetr) == state:
                return state
            await asyncio.sleep(0.05)
        return self.state_of(uetr)


async def test_every_stage_starts_on_its_own_thread() -> None:
    running = RunningHub()
    await running.start()
    try:
        assert len(running.hub.stages) == 11
        assert running.hub.dead() == [], "a stage thread died on start-up"
        names = {stage.thread.name for stage in running.hub.stages if stage.thread}
        assert len(names) == 11, "each stage needs its own thread"
    finally:
        await running.stop()


async def test_a_payment_completes_in_the_all_in_one_runtime() -> None:
    running = RunningHub()
    await running.start()
    try:
        delivery = running.ess.sender.build(PACS_008, flow=FLOW_MX_SNF_PACS008)
        receipt = await running.ess.sender.deliver(delivery)
        assert receipt.status == 1  # ACCEPTED

        state = await running.wait_for(delivery.uetr, "COMPLETED")
        assert state == "COMPLETED", f"stuck in {state or 'no state'}"
    finally:
        await running.stop()


async def test_both_lanes_work_in_one_process() -> None:
    running = RunningHub()
    await running.start()
    try:
        mx = running.ess.sender.build(PACS_008, flow=FLOW_MX_SNF_PACS008)
        mt = running.ess.sender.build(MT103, flow=FLOW_MT_FIN_103)
        await running.ess.sender.send([mx, mt])

        assert await running.wait_for(mx.uetr, "COMPLETED") == "COMPLETED"
        assert await running.wait_for(mt.uetr, "COMPLETED") == "COMPLETED"

        # The lanes stayed apart, exactly as in the per-stage deployment.
        assert [r.key for r in running.bus.records(tp.OUT_MX)] == [mx.uetr]
        assert [r.key for r in running.bus.records(tp.OUT_FIN)] == [mt.uetr]
    finally:
        await running.stop()


async def test_the_consumer_groups_are_unchanged() -> None:
    """Switching topology must not look different to Kafka."""
    running = RunningHub()
    await running.start()
    try:
        groups = {
            stage.runner.processor.group_id  # type: ignore[union-attr]
            for stage in running.hub.stages
            if stage.runner is not None
        }
        assert {
            tp.CG_FIN_PARSER,
            tp.CG_MX_PARSER,
            tp.CG_SCREENING,
            tp.CG_ROUTING,
            tp.CG_SETTLEMENT,
            tp.CG_DISPATCH_FIN,
            tp.CG_DISPATCH_MX,
            tp.CG_ACK_MATCHER,
            tp.CG_STATUS_API,
            tp.CG_DB_SINK,
        } <= groups
    finally:
        await running.stop()


async def test_a_narrowed_build_only_starts_what_was_asked_for() -> None:
    """``only`` exists so a test can run two stages without the other nine."""
    bus = MemoryBus(default_partitions=4)
    bus.ensure_topics(local_specs())
    settings = ServiceSettings(service_name="hub", metrics_port=0, pod_id="narrow")
    hub = all_in_one.build(settings, bus, only=["routing", "settlement"])
    assert hub.names() == ["routing", "settlement"]
