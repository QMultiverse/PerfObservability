"""Process startup, shared by every Hub service.

Sets up ECS logging, tracing, the metrics endpoint and the bus, then runs a
processor or a gRPC server until SIGTERM.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import signal
import sys
from collections.abc import Awaitable, Callable, Iterable

from hub_model import topics as tp
from hub_telemetry import metrics
from hub_telemetry.ecs import setup_logging
from hub_telemetry.events import Events
from hub_telemetry.messaging import Bus, TopicSpec
from hub_telemetry.otel import setup_tracing

from .config import ServiceSettings
from .processor import Processor, ProcessorRunner

log = logging.getLogger(__name__)


def build_bus(settings: ServiceSettings) -> Bus:
    """The real Kafka bus. Tests inject a :class:`MemoryBus` instead."""
    from hub_telemetry.kafka_bus import KafkaBus

    bus = KafkaBus(
        settings.kafka.bootstrap_servers,
        security=settings.kafka.security,
        client_id=settings.service_name,
        default_partitions=settings.kafka.partitions,
        default_replication=settings.kafka.replication,
    )
    if settings.kafka.create_topics:
        specs: Iterable[TopicSpec] = tp.topic_specs(
            partitions=settings.kafka.partitions,
            replication=settings.kafka.replication,
        )
        bus.ensure_topics(specs)
    return bus


def bootstrap(settings: ServiceSettings) -> Events:
    """Logging, tracing and metrics, in that order."""
    setup_logging(
        settings.service_name,
        level=settings.log_level,
        environment=settings.environment,
        version=settings.version,
    )
    setup_tracing(
        settings.service_name,
        endpoint=settings.otlp_endpoint or None,
        sample_ratio=settings.trace_sample_ratio,
        environment=settings.environment,
    )
    if settings.metrics_port:
        metrics.serve_metrics(settings.metrics_port)
    events = Events(settings.service_name)
    log.info(
        "%s starting",
        settings.service_name,
        extra={
            "hub.mode": settings.tracking_mode.value,
            "hub.env": settings.environment,
            "hub.pod": settings.pod_id,
        },
    )
    return events


async def run_until_signal(main: Callable[[], Awaitable[None]], stop: Callable[[], None]) -> None:
    """Run ``main`` until SIGTERM / SIGINT, then let it shut down cleanly.

    ``signal.add_signal_handler`` is POSIX-only; on Windows we fall back to
    ``signal.signal``, which is enough for a developer pressing Ctrl+C.
    """
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, stop)
        except (NotImplementedError, AttributeError, ValueError):
            with contextlib.suppress(ValueError, OSError):
                signal.signal(sig, lambda *_: stop())
    await main()


async def run_processor(
    factory: Callable[[ServiceSettings, Events], Processor],
    settings: ServiceSettings,
    *,
    bus: Bus | None = None,
) -> None:
    """Start one stage and run it until the process is told to stop."""
    events = bootstrap(settings)
    owned_bus = bus is None
    active_bus = bus if bus is not None else build_bus(settings)
    processor = factory(settings, events)
    runner = ProcessorRunner(processor, active_bus, settings, events)
    try:
        await run_until_signal(runner.run_forever, runner.stop)
    finally:
        await runner.close()
        if owned_bus:
            active_bus.close()
        log.info("%s stopped", settings.service_name)


def run(coro: Awaitable[None]) -> int:
    """Entry point wrapper: run ``coro``, turning Ctrl+C into a clean exit."""
    try:
        asyncio.run(coro)  # type: ignore[arg-type]
    except KeyboardInterrupt:
        return 130
    except Exception:
        logging.getLogger(__name__).exception("service failed")
        return 1
    return 0


def die(message: str) -> None:
    print(message, file=sys.stderr)  # noqa: T201 - before logging exists
    raise SystemExit(2)
