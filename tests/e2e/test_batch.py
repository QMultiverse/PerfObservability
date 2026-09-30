"""Batch delivery: pacing, the message mix, and the settlement report."""

from __future__ import annotations

import time

import pytest
from hub_model import proto as pb
from hub_model import topics as tp
from hub_model.flows import MT103, MT202COV, PACS_008

from ess.batch import BatchRunner, BatchSpec, MixEntry, describe, parse_mix
from ess.cases import StatusProbe
from tests.harness import Harness


class HarnessProbe(StatusProbe):
    """Reads the in-process status view instead of the HTTP API."""

    def __init__(self, harness: Harness) -> None:
        super().__init__("")
        self.harness = harness

    def state_of(self, uetr: str) -> str:
        return self.harness.state_of(uetr)

    def end_to_end_ms(self, uetr: str) -> float | None:
        entry = self.harness.status_api.view.get(uetr)
        seconds = entry.end_to_end_s if entry else None
        return seconds * 1000.0 if seconds else None


async def _run(hub: Harness, spec: BatchSpec) -> object:
    """Run a batch while pumping the harness, so payments actually move."""
    import asyncio

    runner = BatchRunner(hub.ess.sender, HarnessProbe(hub))
    pumping = True

    async def pump() -> None:
        while pumping:
            await hub.pump()
            await asyncio.sleep(0.01)

    task = asyncio.create_task(pump())
    try:
        return await runner.run(spec)
    finally:
        pumping = False
        await task


# ---------------------------------------------------------------- sending
async def test_a_batch_delivers_every_payment(hub: Harness) -> None:
    report = await _run(hub, BatchSpec(count=12, settle_timeout_s=20.0))
    assert report.requested == 12
    assert report.accepted == 12
    assert report.rejected == 0
    assert report.errors == 0
    assert len(report.uetrs) == 12
    assert len(set(report.uetrs)) == 12, "each payment needs its own UETR"


async def test_a_batch_reports_how_they_settled(hub: Harness) -> None:
    report = await _run(hub, BatchSpec(count=8, settle_timeout_s=20.0))
    assert report.settled == 8
    assert report.unsettled == 0
    assert report.by_state == {"COMPLETED": 8}
    assert report.ok


async def test_the_report_carries_end_to_end_percentiles(hub: Harness) -> None:
    report = await _run(hub, BatchSpec(count=8, settle_timeout_s=20.0))
    assert len(report.end_to_end_ms) == 8
    assert report.p50_ms > 0
    assert report.p95_ms >= report.p50_ms
    assert report.max_ms >= report.p95_ms


async def test_a_mixed_batch_uses_both_lanes(hub: Harness) -> None:
    spec = BatchSpec(
        count=20,
        mix=(MixEntry(PACS_008, 1.0), MixEntry(MT103, 1.0)),
        settle_timeout_s=25.0,
    )
    report = await _run(hub, spec)
    assert report.accepted == 20

    mx = {r.key for r in hub.bus.records(tp.IN_MX_RAW)}
    mt = {r.key for r in hub.bus.records(tp.IN_FIN_RAW)}
    assert mx, "no payment took the MX lane"
    assert mt, "no payment took the MT lane"
    assert len(mx) + len(mt) == 20


async def test_a_cover_type_sends_both_legs(hub: Harness) -> None:
    """A cover leg alone never completes, so the batch must send the pair."""
    report = await _run(
        # A cover pair needs twice the hops, so give it room on a busy machine.
        hub,
        BatchSpec(count=3, mix=(MixEntry(MT202COV, 1.0),), settle_timeout_s=45.0),
    )
    assert report.accepted == 6, "three cover pairs means six deliveries"
    assert len(report.uetrs) == 3, "but only three payments"
    assert report.by_state == {"COMPLETED": 3}


async def test_a_sanctions_share_is_screened_as_a_hit(hub: Harness) -> None:
    """Every payment names a watchlist party, so every one must pass through HELD.

    They still complete: the functional FCC profile releases a hit after about
    100 ms. What matters is that the hit happened at all.
    """
    report = await _run(hub, BatchSpec(count=10, sanctions_hit_rate=1.0, settle_timeout_s=25.0))
    assert report.accepted == 10
    assert report.settled == 10
    assert report.by_state == {"COMPLETED": 10}

    for uetr in report.uetrs:
        assert "HELD" in hub.history_of(uetr), f"{uetr} was never held"


async def test_no_sanctions_share_means_no_hits(hub: Harness) -> None:
    report = await _run(hub, BatchSpec(count=10, sanctions_hit_rate=0.0, settle_timeout_s=25.0))
    assert report.by_state == {"COMPLETED": 10}
    for uetr in report.uetrs:
        assert "HELD" not in hub.history_of(uetr)


# ----------------------------------------------------------------- pacing
async def test_a_rate_limit_is_respected(hub: Harness) -> None:
    started = time.monotonic()
    report = await _run(hub, BatchSpec(count=10, rate_per_second=20.0, concurrency=4))
    elapsed = time.monotonic() - started

    # Ten payments at 20/s cannot finish faster than the ninth interval.
    assert elapsed >= 9 / 20.0 * 0.8, f"finished in {elapsed:.2f}s, too fast to be paced"
    assert report.accepted == 10
    assert report.achieved_rate > 0


async def test_an_unpaced_batch_is_not_slowed_down(hub: Harness) -> None:
    started = time.monotonic()
    await _run(hub, BatchSpec(count=10, rate_per_second=0.0, concurrency=10))
    assert time.monotonic() - started < 5.0


async def test_nothing_requested_is_not_an_error(hub: Harness) -> None:
    runner = BatchRunner(hub.ess.sender, HarnessProbe(hub))
    report = await runner.run(BatchSpec(count=0))
    assert report.requested == 0
    assert report.accepted == 0
    assert "nothing requested" in report.detail


# ------------------------------------------------------------------- mix
@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("pacs.008", ((PACS_008, 1.0),)),
        ("pacs.008=70,MT103=30", ((PACS_008, 70.0), (MT103, 30.0))),
        ("pacs008,mt103", ((PACS_008, 1.0), (MT103, 1.0))),
        (" pacs.008 = 2 , MT103 = 1 ", ((PACS_008, 2.0), (MT103, 1.0))),
    ],
)
def test_mix_parsing(text: str, expected: tuple[tuple[str, float], ...]) -> None:
    assert tuple((e.msg_type, e.share) for e in parse_mix(text)) == expected


@pytest.mark.parametrize("bad", ["", "   ", "pacs.008=notanumber"])
def test_a_bad_mix_is_rejected(bad: str) -> None:
    with pytest.raises(ValueError):
        parse_mix(bad)


def test_an_empty_mix_defaults_to_pacs008() -> None:
    assert BatchSpec(count=1).normalised_mix() == (MixEntry(PACS_008, 1.0),)


def test_the_summary_reads_sensibly(hub: Harness) -> None:
    from ess.batch import BatchReport

    report = BatchReport(requested=5, accepted=5, settled=5, send_duration_s=1.0)
    report.by_state = {"COMPLETED": 5}
    report.end_to_end_ms = [100.0, 120.0, 140.0, 160.0, 900.0]
    lines = describe(report)
    assert any("accepted 5" in line for line in lines)
    assert any("COMPLETED=5" in line for line in lines)
    assert any("p50" in line and "p95" in line for line in lines)


# ------------------------------------------------------- the control API
async def test_run_batch_through_the_control_api(hub: Harness) -> None:
    import asyncio

    hub.ess.batch.probe = HarnessProbe(hub)
    request = pb.BatchRequest(count=5, concurrency=5, run_id="BATCH-TEST", settle_timeout_ms=20_000)
    request.mix.append(pb.MixEntry(msg_type=PACS_008, share=1.0))

    pumping = True

    async def pump() -> None:
        while pumping:
            await hub.pump()
            await asyncio.sleep(0.01)

    task = asyncio.create_task(pump())
    try:
        result = await hub.ess.control.RunBatch(request, None)
    finally:
        pumping = False
        await task

    assert result.requested == 5
    assert result.accepted == 5
    assert result.rejected == 0
    assert result.settled == 5
    assert dict(result.by_state) == {"COMPLETED": 5}
    assert result.p50_end_to_end_ms > 0
    assert result.achieved_rate_per_second > 0


async def test_the_totals_always_add_up(hub: Harness) -> None:
    """requested == accepted + duplicate + rejected + errors.

    A delivery that never gets a receipt used to fall through every counter,
    so a batch could report 22 of 25 with nothing wrong.
    """
    report = await _run(hub, BatchSpec(count=15, settle_timeout_s=25.0))
    assert (
        report.accepted + report.duplicate + report.rejected + report.errors == report.requested
    ), report.detail
    assert "unaccounted" not in report.detail


async def test_a_failed_delivery_is_counted_as_an_error(hub: Harness) -> None:
    """With the edge unreachable, every delivery must show up as an error."""
    import grpc

    from ess.batch import BatchRunner

    class Broken:
        """A sender whose deliveries always fail at the transport."""

        run_id = ""

        def build(self, *a: object, **k: object) -> object:
            return object()

        def build_cover_pair(self, *a: object, **k: object) -> list[object]:
            return [object()]

        async def send(self, deliveries: list[object]) -> object:
            raise grpc.aio.AioRpcError(
                grpc.StatusCode.UNAVAILABLE, None, None, "edge is down", None
            )

    runner = BatchRunner(Broken(), None)  # type: ignore[arg-type]
    report = await runner.run(BatchSpec(count=5))
    assert report.errors == 5
    assert report.accepted == 0
    assert report.errors + report.accepted == report.requested
    assert report.failed_uetrs, "the failures need to be reported, not just counted"
