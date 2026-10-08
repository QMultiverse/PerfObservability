"""A stage that holds no partitions must notice, say so, and rejoin its group.

Found by performance scenario 1: after the host slept, the broker dropped the
Hub's consumers and most never rejoined. They kept polling an empty
assignment, with no error, while the Hub reported healthy.
"""

from __future__ import annotations

from collections.abc import Sequence

from hub_model import topics as tp
from hub_telemetry import metrics
from hub_telemetry.events import Events
from hub_telemetry.memory_bus import MemoryBus
from hub_telemetry.messaging import Record

from hub.common.config import ServiceSettings
from hub.common.processor import Outbox, Processor, ProcessorRunner

SERVICE = "test-stall-stage"


class Idle(Processor):
    name = SERVICE
    stage = "test-stall"
    group_id = "cg-test-stall"
    input_topics = (tp.PAY_CANONICAL,)

    async def handle(self, record: Record, out: Outbox) -> None:
        return None


class Unassigned:
    """A consumer the broker has taken every partition from."""

    group_id = "cg-test-stall"

    def __init__(self) -> None:
        self.closed = False

    def consume(self, max_records: int = 500, timeout_s: float = 0.05) -> list[Record]:
        return []

    def assignment(self) -> Sequence[tuple[str, int]]:
        return []

    def close(self) -> None:
        self.closed = True


def runner(rejoin_after_s: float = 60.0) -> ProcessorRunner:
    settings = ServiceSettings(service_name=SERVICE)
    settings.kafka.rejoin_after_s = rejoin_after_s
    events = Events(SERVICE)
    return ProcessorRunner(Idle(settings, events), MemoryBus(), settings, events)


def stalled() -> float:
    return metrics.REGISTRY.get_sample_value("hub_stage_stalled", {"service": SERVICE}) or 0.0


def rejoins() -> float:
    labels = {"service": SERVICE}
    return metrics.REGISTRY.get_sample_value("hub_kafka_consumer_rejoins_total", labels) or 0.0


def test_a_stage_with_partitions_is_left_alone() -> None:
    run = runner()
    original = run.consumer
    assert run.check_assignment(now=1_000.0) is False
    assert run.check_assignment(now=2_000.0) is False
    assert run.consumer is original
    assert stalled() == 0


def test_a_stage_without_partitions_rejoins_after_the_timeout() -> None:
    run = runner(rejoin_after_s=60.0)
    lost = Unassigned()
    run.consumer = lost
    before = rejoins()

    assert run.check_assignment(now=1_000.0) is False, "first sighting only starts the clock"
    assert run.check_assignment(now=1_030.0) is False, "still inside the timeout"
    assert stalled() == 0

    assert run.check_assignment(now=1_061.0) is True
    assert lost.closed, "the stalled consumer is closed"
    assert run.consumer is not lost, "and replaced with a fresh one"
    assert rejoins() == before + 1
    assert stalled() == 1, "reported until partitions come back"

    # The fresh consumer (a MemoryBus one) is assigned partitions: healthy again.
    assert run.check_assignment(now=1_070.0) is False
    assert stalled() == 0


def test_the_check_is_rate_limited() -> None:
    run = runner()
    run.consumer = Unassigned()
    run.check_assignment(now=1_000.0)
    # Two seconds later is inside the 5 s check interval: nothing is looked at.
    assert run.check_assignment(now=1_002.0) is False
    assert run._unassigned_since == 1_000.0


def test_rejoin_can_be_switched_off() -> None:
    run = runner(rejoin_after_s=0)
    lost = Unassigned()
    run.consumer = lost
    for now in (1_000.0, 2_000.0, 3_000.0):
        assert run.check_assignment(now=now) is False
    assert not lost.closed
