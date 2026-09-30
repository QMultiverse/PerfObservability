"""An in-process Payment Hub plus ESS, for end-to-end tests.

Runs the real edge, the real processors and the real simulator, wired over
:class:`~hub_telemetry.memory_bus.MemoryBus` instead of a broker and over
loopback gRPC instead of a network. Everything under test is production code:
the harness replaces only the two things a laptop should not need for a
functional test — a Kafka cluster and a container runtime.

The contract the harness keeps is the one the design doc states: the ESS
reaches the Hub only through gRPC, and nothing but the Hub writes to its Kafka.
"""

from __future__ import annotations

import asyncio
import contextlib
import socket
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import Any

import grpc
from hub_model import proto as pb
from hub_model import topics as tp
from hub_model.proto import StatusEvent
from hub_telemetry.events import Events
from hub_telemetry.grpc_telemetry import ServerTelemetryInterceptor
from hub_telemetry.memory_bus import MemoryBus
from hub_telemetry.messaging import TopicSpec
from hub_telemetry.otel import setup_tracing
from hub_telemetry.tracked import TrackedProducer

from ess.server import Ess, EssSettings
from ess.server import build as build_ess
from hub.ack_matcher.processor import AckMatcher
from hub.common.config import ExternalSettings, KafkaSettings, ServiceSettings
from hub.common.processor import Processor, ProcessorRunner
from hub.common.retry_consumer import RetryConsumer
from hub.db_sink.processor import DbSink
from hub.dispatcher.processor import FinDispatcher, MxDispatcher
from hub.edge.server import register
from hub.fin_parser.processor import FinParser
from hub.mx_parser.processor import MxParser
from hub.routing.processor import Routing
from hub.screening.processor import Screening
from hub.settlement.processor import Settlement
from hub.status_api.processor import StatusApi

#: Stages in pipeline order. Pumping them in this order lets one payment move
#: several hops per round, which keeps the tests fast.
STAGE_FACTORIES: tuple[tuple[str, Callable[..., Processor]], ...] = (
    ("fin-parser", FinParser),
    ("mx-parser", MxParser),
    ("screening", Screening),
    ("routing", Routing),
    ("settlement", Settlement),
    ("dispatcher-fin", FinDispatcher),
    ("dispatcher-mx", MxDispatcher),
    ("ack-matcher", AckMatcher),
    ("status-api", StatusApi),
    ("db-sink", DbSink),
    ("retry", RetryConsumer),
)


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def local_specs() -> list[TopicSpec]:
    """Four partitions is enough to prove keys land together, and is fast."""
    return [
        TopicSpec(
            spec.name,
            partitions=4,
            replication=1,
            cleanup_policy=spec.cleanup_policy,
            retention_ms=spec.retention_ms,
        )
        for spec in tp.topic_specs()
    ]


@dataclass
class Harness:
    """A running Hub and ESS."""

    bus: MemoryBus
    settings: ServiceSettings
    events: Events
    edge_server: grpc.aio.Server
    edge_producer: TrackedProducer
    edge_address: str
    ess: Ess
    runners: dict[str, ProcessorRunner] = field(default_factory=dict)

    #: The retry consumer sleeps until each record is due — 30 s for the first
    #: rung — so pumping it would stall every test that produces one. Tests
    #: that exercise the ladder drive ``runners["retry"]`` directly.
    excluded_from_pump: frozenset[str] = frozenset({"retry"})

    # ---------------------------------------------------------------- run
    async def pump(self, rounds: int = 1) -> int:
        """Run every pipeline stage once per round. Returns records handled."""
        handled = 0
        for _ in range(rounds):
            for name, runner in self.runners.items():
                if name in self.excluded_from_pump:
                    continue
                handled += await runner.run_once()
        return handled

    async def settle(
        self,
        *,
        timeout_s: float = 10.0,
        until: Callable[[], bool] | None = None,
        quiet_rounds: int = 3,
    ) -> bool:
        """Pump until ``until`` holds, or until nothing has moved for a while.

        The ESS replies through background tasks with real (small) delays, so
        the loop yields to the event loop between rounds rather than spinning.
        """
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout_s
        idle = 0
        while loop.time() < deadline:
            if until is not None and until():
                return True
            moved = await self.pump()
            idle = 0 if moved else idle + 1
            if until is None and idle >= quiet_rounds:
                return True
            await asyncio.sleep(0.01)
        return bool(until() if until is not None else False)

    async def run_until_state(self, uetr: str, state: str, *, timeout_s: float = 10.0) -> str:
        """Pump until ``uetr`` reaches ``state``. Returns the state it reached."""
        await self.settle(timeout_s=timeout_s, until=lambda: self.state_of(uetr) == state)
        return self.state_of(uetr)

    # --------------------------------------------------------------- read
    @property
    def status_api(self) -> StatusApi:
        processor = self.runners["status-api"].processor
        assert isinstance(processor, StatusApi)
        return processor

    def state_of(self, uetr: str) -> str:
        entry = self.status_api.view.get(uetr)
        return entry.state if entry else ""

    def history_of(self, uetr: str) -> list[str]:
        entry = self.status_api.view.get(uetr)
        return [step["state"] for step in entry.history] if entry else []

    def status_events(self, uetr: str = "") -> list[StatusEvent]:
        out: list[StatusEvent] = []
        for record in self.bus.records(tp.PAY_STATUS):
            if uetr and record.key != uetr:
                continue
            event = StatusEvent()
            event.ParseFromString(record.value)
            out.append(event)
        return out

    def records_on(self, topic: str) -> list[Any]:
        return self.bus.records(topic)

    def dlq(self, topic: str) -> list[Any]:
        return self.bus.records(tp.dlq_topic(topic))

    def processor(self, name: str) -> Processor:
        return self.runners[name].processor

    # ------------------------------------------------------------ teardown
    async def close(self) -> None:
        await self.ess.stop()
        for runner in self.runners.values():
            await runner.close()
        self.edge_producer.close()
        await self.edge_server.stop(grace=0.0)
        self.bus.close()


async def start_harness(
    *,
    tracking_mode: str = "functional",
    ess_mode: str = "functional",
    seed: int | None = 7,
    stages: Sequence[str] | None = None,
) -> Harness:
    """Bring up the Hub and the ESS, wired together."""
    setup_tracing("hub-test", endpoint=None, environment="test")

    bus = MemoryBus(default_partitions=4)
    bus.ensure_topics(local_specs())

    edge_port = free_port()
    ess_port = free_port()
    edge_address = f"localhost:{edge_port}"
    ess_address = f"localhost:{ess_port}"

    settings = ServiceSettings(
        service_name="hub-edge",
        grpc_port=edge_port,
        metrics_port=0,
        environment="test",
        kafka=KafkaSettings(
            bootstrap_servers="memory", partitions=4, replication=1, batch_size=100
        ),
        external=ExternalSettings(
            fin_target=ess_address, snf_target=ess_address, fcc_target=ess_address
        ),
        pod_id="test",
    )
    import os

    os.environ.setdefault("HUB_TRACKING_MODE", tracking_mode)
    settings.tracking_mode = type(settings.tracking_mode)(tracking_mode)

    events = Events("hub-edge")
    edge_producer = TrackedProducer(bus.producer(), events)
    edge_server = grpc.aio.server(interceptors=[ServerTelemetryInterceptor(events)])
    register(edge_server, settings, events, edge_producer)
    edge_server.add_insecure_port(f"[::]:{edge_port}")
    await edge_server.start()

    ess = build_ess(
        EssSettings(
            mode=ess_mode,
            port=ess_port,
            metrics_port=0,
            hub_target=edge_address,
            status_api_url="",  # the harness asserts on the in-process view
            seed=seed,
        )
    )
    await ess.start()

    wanted = set(stages) if stages is not None else None
    runners: dict[str, ProcessorRunner] = {}
    for name, factory in STAGE_FACTORIES:
        if wanted is not None and name not in wanted:
            continue
        stage_settings = _stage_settings(settings, name)
        processor = factory(stage_settings, Events(_service_name(name)))
        runners[name] = ProcessorRunner(processor, bus, stage_settings, events)

    return Harness(
        bus=bus,
        settings=settings,
        events=events,
        edge_server=edge_server,
        edge_producer=edge_producer,
        edge_address=edge_address,
        ess=ess,
        runners=runners,
    )


def _stage_settings(base: ServiceSettings, name: str) -> ServiceSettings:
    import dataclasses

    return dataclasses.replace(
        base, service_name=_service_name(name), pod_id=f"test-{name}", metrics_port=0
    )


def _service_name(name: str) -> str:
    return f"hub-{name}"


@contextlib.asynccontextmanager
async def harness(**kwargs: Any):  # type: ignore[no-untyped-def]
    """``async with harness() as hub:`` — starts and always tears down."""
    started = await start_harness(**kwargs)
    try:
        yield started
    finally:
        await started.close()


def uetrs_of(harness_: Harness, topic: str) -> list[str]:
    return [record.key for record in harness_.bus.records(topic)]


def state_names(events: Sequence[StatusEvent]) -> list[str]:
    return [pb.state_name(event.state) for event in events]
