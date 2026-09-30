"""One entry point for every Hub service.

    python -m hub <service>
    hub-service <service>

The service names match the packages in CLAUDE.md's layout and the container
names in ``deploy/compose``.
"""

from __future__ import annotations

import asyncio
import sys
from collections.abc import Callable, Sequence

from hub_telemetry.events import Events

from .config import ServiceSettings
from .processor import Processor
from .service import run, run_processor

#: service name -> (module, factory attribute, default metrics port)
SERVICES: dict[str, tuple[str, str, int]] = {
    # Every stage in one process. The default for local runs; see
    # hub/common/all_in_one.py for why the stages get a thread each.
    "all": ("hub.common.all_in_one", "", 9464),
    "edge": ("hub.edge.__main__", "", 9464),
    "fin-parser": ("hub.fin_parser.processor", "build", 9465),
    "mx-parser": ("hub.mx_parser.processor", "build", 9466),
    "screening": ("hub.screening.processor", "build", 9467),
    "routing": ("hub.routing.processor", "build", 9468),
    "settlement": ("hub.settlement.processor", "build", 9469),
    "dispatcher-fin": ("hub.dispatcher.processor", "build_fin", 9470),
    "dispatcher-mx": ("hub.dispatcher.processor", "build_mx", 9471),
    "ack-matcher": ("hub.ack_matcher.processor", "build", 9472),
    "status-api": ("hub.status_api.processor", "build", 9473),
    "db-sink": ("hub.db_sink.processor", "build", 9474),
    "retry": ("hub.common.retry_consumer", "build", 9475),
}

SERVICE_NAMES: dict[str, str] = {
    "all": "hub",
    "fin-parser": "hub-fin-parser",
    "mx-parser": "hub-mx-parser",
    "screening": "hub-screening",
    "routing": "hub-routing",
    "settlement": "hub-settlement",
    "dispatcher-fin": "hub-dispatcher-fin",
    "dispatcher-mx": "hub-dispatcher-mx",
    "ack-matcher": "hub-ack-matcher",
    "status-api": "hub-status-api",
    "db-sink": "hub-db-sink",
    "retry": "hub-retry",
}

STATUS_API_HTTP_PORT = 8080
EDGE_PORT = 8443


def main(argv: Sequence[str] | None = None) -> int:
    args = list(argv if argv is not None else sys.argv[1:])
    if not args or args[0] in {"-h", "--help"}:
        return _usage()

    name = args[0]
    if name not in SERVICES:
        print(f"unknown service {name!r}", file=sys.stderr)  # noqa: T201
        return _usage()

    if name == "edge":
        from hub.edge.__main__ import main as edge_main

        return edge_main()

    if name == "all":
        settings = ServiceSettings.from_env("hub", default_grpc_port=EDGE_PORT)
        if not settings.metrics_port:
            settings.metrics_port = SERVICES["all"][2]
        return run(_run_everything(settings))

    module_name, attr, metrics_port = SERVICES[name]
    settings = ServiceSettings.from_env(SERVICE_NAMES[name])
    if not settings.metrics_port:
        settings.metrics_port = metrics_port

    import importlib

    module = importlib.import_module(module_name)
    factory: Callable[[ServiceSettings, Events], Processor] = getattr(module, attr)

    if name == "status-api":
        return run(_run_status_api(factory, settings))
    return run(run_processor(factory, settings))


async def _run_everything(settings: ServiceSettings) -> None:
    """The whole Hub in one process: the edge, every stage, the status API.

    Kafka sees exactly what it sees in the per-stage deployment — the same
    consumer groups reading the same topics — so switching between the two is a
    deployment choice, not a behaviour change.
    """
    import grpc
    from hub_telemetry.grpc_telemetry import ServerTelemetryInterceptor
    from hub_telemetry.tracked import TrackedProducer

    from hub.common import all_in_one
    from hub.edge.server import register
    from hub.status_api.processor import StatusApi, serve_http

    from .service import bootstrap, build_bus, run_until_signal

    events = bootstrap(settings)
    bus = build_bus(settings)

    hub = all_in_one.build(settings, bus)
    hub.start()

    # The status API's HTTP view lives on the stage that owns it.
    status_stage = hub.stage("status-api")
    status_processor = status_stage.runner.processor if status_stage.runner else None
    http_server = None
    if isinstance(status_processor, StatusApi):
        http_server = serve_http(status_processor.view, STATUS_API_HTTP_PORT)

    # The edge is unusual: it serves gRPC rather than consuming a topic, so it
    # stays on this thread's loop.
    edge_producer = TrackedProducer(bus.producer(), events)
    server = grpc.aio.server(
        interceptors=[ServerTelemetryInterceptor(events)],
        options=[
            ("grpc.max_receive_message_length", 16 * 1024 * 1024),
            ("grpc.keepalive_time_ms", 20_000),
            ("grpc.keepalive_permit_without_calls", 1),
        ],
    )
    register(server, settings, events, edge_producer)
    address = f"[::]:{settings.grpc_port or EDGE_PORT}"
    server.add_insecure_port(address)
    await server.start()

    import logging as _logging

    _logging.getLogger(__name__).info(
        "hub listening on %s with %d stages in-process", address, len(hub.stages)
    )

    stopped = asyncio.Event()

    async def wait() -> None:
        # Also give up if a stage thread dies: a half-running Hub silently
        # drops payments, which is worse than exiting and being restarted.
        while not stopped.is_set():
            dead = hub.dead()
            if dead:
                _logging.getLogger(__name__).error("stage(s) stopped: %s", ", ".join(dead))
                return
            try:
                await asyncio.wait_for(stopped.wait(), timeout=2.0)
            except TimeoutError:
                continue

    try:
        await run_until_signal(wait, stopped.set)
    finally:
        hub.stop()
        if http_server is not None:
            http_server.shutdown()
        await server.stop(grace=5.0)
        hub.join()
        edge_producer.close()
        bus.close()


async def _run_status_api(
    factory: Callable[[ServiceSettings, Events], Processor], settings: ServiceSettings
) -> None:
    """The status API needs its HTTP server alongside the consumer."""
    from hub.status_api.processor import StatusApi, serve_http

    from .processor import ProcessorRunner
    from .service import bootstrap, build_bus, run_until_signal

    events = bootstrap(settings)
    bus = build_bus(settings)
    processor = factory(settings, events)
    assert isinstance(processor, StatusApi)
    server = serve_http(processor.view, STATUS_API_HTTP_PORT)
    runner = ProcessorRunner(processor, bus, settings, events)
    try:
        await run_until_signal(runner.run_forever, runner.stop)
    finally:
        server.shutdown()
        await runner.close()
        bus.close()


def _usage() -> int:
    print("usage: python -m hub <service>", file=sys.stderr)  # noqa: T201
    print("services: " + ", ".join(sorted(SERVICES)), file=sys.stderr)  # noqa: T201
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
