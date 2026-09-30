"""Every Hub stage in one process, for local runs.

The production topology is one container per stage: that is what lets a
consumer group scale on its own, and ``deploy/compose`` still offers it under
the ``stages`` profile. But for developing and demonstrating, eleven containers
cost eleven times the memory and give eleven log streams to tail, for no gain.

This runs the gRPC edge, every processor and the status API in a single
process — the same classes, the same consumer groups, the same topics. From
Kafka's point of view nothing has changed.

**Each stage gets its own thread**, not a shared event loop.
``confluent_kafka``'s ``consume()`` blocks, so eleven stages polling a shared
loop with a 50 ms timeout would serialise into half a second of latency per
hop. A thread each keeps every stage's latency the same as its own container,
and the GIL is released during the Kafka and gRPC I/O that dominates.
"""

from __future__ import annotations

import asyncio
import logging
import threading
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field

from hub_telemetry.events import Events
from hub_telemetry.messaging import Bus

from .config import ServiceSettings
from .processor import Processor, ProcessorRunner

log = logging.getLogger(__name__)


@dataclass
class Stage:
    """One processor, running on its own thread with its own event loop."""

    name: str
    service_name: str
    factory: Callable[[ServiceSettings, Events], Processor]
    settings: ServiceSettings
    bus: Bus
    thread: threading.Thread | None = None
    runner: ProcessorRunner | None = None
    ready: threading.Event = field(default_factory=threading.Event)
    failure: BaseException | None = None
    _stop: threading.Event = field(default_factory=threading.Event)

    def start(self) -> None:
        self.thread = threading.Thread(target=self._run, name=self.name, daemon=True)
        self.thread.start()

    def _run(self) -> None:
        try:
            asyncio.run(self._serve())
        except Exception as exc:
            self.failure = exc
            log.exception("stage %s stopped", self.name)
        finally:
            self.ready.set()

    async def _serve(self) -> None:
        # The consumer and producer are created here, on the thread that will
        # use them: confluent_kafka handles are not for sharing across threads.
        events = Events(self.service_name)
        processor = self.factory(self.settings, events)
        runner = ProcessorRunner(processor, self.bus, self.settings, events)
        self.runner = runner
        self.ready.set()

        watcher = asyncio.create_task(self._watch_stop(runner))
        try:
            await runner.run_forever()
        finally:
            watcher.cancel()
            await runner.close()

    async def _watch_stop(self, runner: ProcessorRunner) -> None:
        """Bridge the threading stop flag into this thread's event loop."""
        await asyncio.to_thread(self._stop.wait)
        runner.stop()

    def stop(self) -> None:
        self._stop.set()

    def join(self, timeout: float = 10.0) -> None:
        if self.thread is not None:
            self.thread.join(timeout)

    @property
    def alive(self) -> bool:
        return self.thread is not None and self.thread.is_alive()


class AllInOne:
    """The whole Hub in one process."""

    def __init__(self, settings: ServiceSettings, bus: Bus) -> None:
        self.settings = settings
        self.bus = bus
        self.stages: list[Stage] = []

    def add(
        self,
        name: str,
        factory: Callable[[ServiceSettings, Events], Processor],
        *,
        service_name: str = "",
    ) -> None:
        import dataclasses

        stage_settings = dataclasses.replace(
            self.settings,
            service_name=service_name or f"hub-{name}",
            pod_id=f"{self.settings.pod_id}-{name}",
            # One /metrics endpoint for the process, served by the caller.
            metrics_port=0,
        )
        self.stages.append(
            Stage(
                name=name,
                service_name=service_name or f"hub-{name}",
                factory=factory,
                settings=stage_settings,
                bus=self.bus,
            )
        )

    def start(self, *, ready_timeout: float = 30.0) -> None:
        for stage in self.stages:
            stage.start()
        for stage in self.stages:
            if not stage.ready.wait(ready_timeout):
                raise TimeoutError(f"stage {stage.name} did not start within {ready_timeout}s")
            if stage.failure is not None:
                raise RuntimeError(f"stage {stage.name} failed to start") from stage.failure
        log.info("all %d stages running: %s", len(self.stages), ", ".join(self.names()))

    def names(self) -> list[str]:
        return [stage.name for stage in self.stages]

    def dead(self) -> list[str]:
        """Stages whose thread has exited. Used by the health check."""
        return [s.name for s in self.stages if not s.alive]

    def stop(self) -> None:
        for stage in self.stages:
            stage.stop()

    def join(self, timeout: float = 10.0) -> None:
        for stage in self.stages:
            stage.join(timeout)

    def stage(self, name: str) -> Stage:
        for candidate in self.stages:
            if candidate.name == name:
                return candidate
        raise KeyError(name)


def build(settings: ServiceSettings, bus: Bus, *, only: Sequence[str] | None = None) -> AllInOne:
    """Assemble every stage. ``only`` narrows it, for tests."""
    from hub.ack_matcher.processor import AckMatcher
    from hub.db_sink.processor import DbSink
    from hub.dispatcher.processor import FinDispatcher, MxDispatcher
    from hub.fin_parser.processor import FinParser
    from hub.mx_parser.processor import MxParser
    from hub.routing.processor import Routing
    from hub.screening.processor import Screening
    from hub.settlement.processor import Settlement
    from hub.status_api.processor import StatusApi

    from .retry_consumer import RetryConsumer

    catalogue: list[tuple[str, Callable[[ServiceSettings, Events], Processor]]] = [
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
    ]

    hub = AllInOne(settings, bus)
    wanted = set(only) if only is not None else None
    for name, factory in catalogue:
        if wanted is None or name in wanted:
            hub.add(name, factory)
    return hub
