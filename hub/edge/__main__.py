"""Run the Hub gRPC edge.

python -m hub.edge
"""

from __future__ import annotations

import asyncio
import logging

import grpc
from hub_telemetry import metrics
from hub_telemetry.grpc_telemetry import ServerTelemetryInterceptor
from hub_telemetry.messaging import Bus
from hub_telemetry.tracked import TrackedProducer

from hub.common.config import ServiceSettings
from hub.common.service import bootstrap, build_bus, run, run_until_signal

from .server import SERVICE_NAME, register

log = logging.getLogger(__name__)

DEFAULT_PORT = 8443


async def serve(settings: ServiceSettings, *, bus: Bus | None = None) -> None:
    events = bootstrap(settings)
    owned_bus = bus is None
    active_bus = bus if bus is not None else build_bus(settings)

    # The edge is not transactional: it writes one record per call with
    # acks=all and replies once that is confirmed. There is no consumer offset
    # to tie the write to, so a transaction would only add latency.
    producer = TrackedProducer(active_bus.producer(), events)

    server = grpc.aio.server(
        interceptors=[ServerTelemetryInterceptor(events)],
        options=[
            ("grpc.max_receive_message_length", 16 * 1024 * 1024),
            ("grpc.keepalive_time_ms", 20_000),
            ("grpc.keepalive_permit_without_calls", 1),
        ],
    )
    register(server, settings, events, producer)
    address = f"[::]:{settings.grpc_port or DEFAULT_PORT}"
    server.add_insecure_port(address)

    await server.start()
    log.info("%s listening on %s", SERVICE_NAME, address)
    metrics.held_payments.labels(SERVICE_NAME).set(0)

    stopped = asyncio.Event()

    async def wait() -> None:
        await stopped.wait()
        await server.stop(grace=5.0)

    try:
        await run_until_signal(wait, stopped.set)
    finally:
        producer.close()
        if owned_bus:
            active_bus.close()


def main() -> int:
    settings = ServiceSettings.from_env(SERVICE_NAME, default_grpc_port=DEFAULT_PORT)
    return run(serve(settings))


if __name__ == "__main__":
    raise SystemExit(main())
