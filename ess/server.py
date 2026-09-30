"""Assembles the ESS: three emulators, the sender, the recorder and the control API.

One image, one process, one port. The mode selects the default profiles and how
much the recorder keeps; everything else is identical, which is the point — the
ESS the developer runs is the ESS CI runs.
"""

from __future__ import annotations

import asyncio
import dataclasses
import logging
import os
import random
from dataclasses import dataclass
from typing import Final

import grpc
from hub_model import proto as pb
from hub_telemetry import metrics
from hub_telemetry.ecs import setup_logging
from hub_telemetry.events import Events
from hub_telemetry.grpc_telemetry import ServerTelemetryInterceptor
from hub_telemetry.otel import setup_tracing

from .batch import BatchRunner
from .cases import CaseRunner, StatusProbe
from .control import EssControl
from .emulators import FccEmulator, FinEmulator, Overrides, SnfEmulator
from .profiles import ProfileStore
from .recorder import Recorder
from .sender import InboundSender

log = logging.getLogger(__name__)

SERVICE_NAME: Final = "ess"
DEFAULT_PORT: Final = 9101
DEFAULT_METRICS_PORT: Final = 9464


@dataclass(slots=True)
class EssSettings:
    """Everything the simulator needs to start."""

    mode: str = "functional"
    port: int = DEFAULT_PORT
    metrics_port: int = DEFAULT_METRICS_PORT
    hub_target: str = "localhost:8443"
    status_api_url: str = "http://localhost:8080"
    log_level: str = "INFO"
    environment: str = "local"
    otlp_endpoint: str = ""
    run_id: str = ""
    seed: int | None = None
    record: bool = True

    @classmethod
    def from_env(cls) -> EssSettings:
        seed = os.environ.get("ESS_SEED", "")
        return cls(
            mode=os.environ.get("ESS_MODE", "functional").lower(),
            port=int(os.environ.get("ESS_PORT", DEFAULT_PORT)),
            metrics_port=int(os.environ.get("ESS_METRICS_PORT", DEFAULT_METRICS_PORT)),
            hub_target=os.environ.get("HUB_TARGET", "localhost:8443"),
            status_api_url=os.environ.get("HUB_STATUS_API", "http://localhost:8080"),
            log_level=os.environ.get("HUB_LOG_LEVEL", "INFO").upper(),
            environment=os.environ.get("HUB_ENV", "local"),
            otlp_endpoint=os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT", ""),
            run_id=os.environ.get("ESS_RUN_ID", ""),
            seed=int(seed) if seed.isdigit() else None,
            record=os.environ.get("ESS_RECORD", "1") != "0",
        )

    def replace(self, **changes: object) -> EssSettings:
        return dataclasses.replace(self, **changes)  # type: ignore[arg-type]


@dataclass(slots=True)
class Ess:
    """A running (or runnable) simulator and its parts."""

    settings: EssSettings
    profiles: ProfileStore
    recorder: Recorder
    overrides: Overrides
    events: Events
    fin: FinEmulator
    snf: SnfEmulator
    fcc: FccEmulator
    sender: InboundSender
    runner: CaseRunner
    batch: BatchRunner
    control: EssControl
    server: grpc.aio.Server | None = None

    async def start(self) -> str:
        """Start serving. Returns the bound address."""
        server = grpc.aio.server(
            interceptors=[ServerTelemetryInterceptor(self.events)],
            options=[
                ("grpc.max_receive_message_length", 16 * 1024 * 1024),
                ("grpc.keepalive_time_ms", 20_000),
                ("grpc.keepalive_permit_without_calls", 1),
            ],
        )
        pb.add_FinGatewayServicer_to_server(self.fin, server)
        pb.add_SnfGatewayServicer_to_server(self.snf, server)
        pb.add_FccScreeningServicer_to_server(self.fcc, server)
        pb.add_EssControlServicer_to_server(self.control, server)

        address = f"[::]:{self.settings.port}"
        bound = server.add_insecure_port(address)
        await server.start()
        self.server = server
        log.info(
            "ESS serving FIN, SnF, FCC and EssControl on port %d in %s mode (hub: %s)",
            bound,
            self.settings.mode,
            self.settings.hub_target,
        )
        return f"localhost:{bound}"

    async def stop(self, grace: float = 2.0) -> None:
        if self.server is not None:
            await self.server.stop(grace)
            self.server = None
        await asyncio.gather(
            self.fin.close(), self.snf.close(), self.fcc.close(), self.sender.close()
        )

    async def drain(self) -> None:
        """Wait for every scheduled callback. Used by cases and tests."""
        await asyncio.gather(self.fin.drain(), self.snf.drain(), self.fcc.drain())


def build(settings: EssSettings, *, events: Events | None = None) -> Ess:
    """Wire the simulator together without starting it."""
    profiles = ProfileStore(settings.mode)
    recorder = Recorder(enabled=settings.record)
    overrides = Overrides()
    bound_events = events or Events(SERVICE_NAME)
    rng = random.Random(settings.seed) if settings.seed is not None else random.Random()

    shared = {
        "profiles": profiles,
        "recorder": recorder,
        "events": bound_events,
        "hub_target": settings.hub_target,
        "overrides": overrides,
        "rng": rng,
    }
    fin = FinEmulator(**shared)  # type: ignore[arg-type]
    snf = SnfEmulator(**shared)  # type: ignore[arg-type]
    fcc = FccEmulator(**shared)  # type: ignore[arg-type]

    sender = InboundSender(settings.hub_target, bound_events, recorder, run_id=settings.run_id)
    probe = StatusProbe(settings.status_api_url) if settings.status_api_url else None
    runner = CaseRunner(sender, overrides, recorder, probe)
    batch = BatchRunner(sender, probe, rng=rng)
    control = EssControl(profiles, recorder, overrides, sender, runner, batch)

    return Ess(
        settings=settings,
        profiles=profiles,
        recorder=recorder,
        overrides=overrides,
        events=bound_events,
        fin=fin,
        snf=snf,
        fcc=fcc,
        sender=sender,
        runner=runner,
        batch=batch,
        control=control,
    )


def bootstrap(settings: EssSettings) -> Events:
    setup_logging(
        SERVICE_NAME,
        level=settings.log_level,
        environment=settings.environment,
        version=os.environ.get("HUB_VERSION", "0.1.0"),
    )
    setup_tracing(
        SERVICE_NAME,
        endpoint=settings.otlp_endpoint or None,
        environment=settings.environment,
    )
    if settings.metrics_port:
        metrics.serve_metrics(settings.metrics_port)
    return Events(SERVICE_NAME)


async def serve(settings: EssSettings) -> None:
    """Run the ESS until SIGTERM."""
    from hub.common.service import run_until_signal

    events = bootstrap(settings)
    ess = build(settings, events=events)
    await ess.start()

    stopped = asyncio.Event()

    async def wait() -> None:
        await stopped.wait()

    try:
        await run_until_signal(wait, stopped.set)
    finally:
        await ess.stop()
        log.info("ESS stopped")
