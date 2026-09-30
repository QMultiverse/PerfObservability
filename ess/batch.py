"""Batch delivery at a controlled rate, with a settlement report.

Drives the inbound sender for many payments instead of one, paces them, then
waits for each to reach a terminal state and reports what happened. That last
part is what makes it useful over a shell loop: you get a state breakdown and
end-to-end percentiles, not just "200 accepted".

This is still the **inbound sender**. Design doc section 8 is explicit that
high-rate load is paygen's job, not the ESS's, and CLAUDE.md caps local runs at
around 50 TPS. :data:`SOFT_RATE_LIMIT` warns past that rather than pretending
otherwise.
"""

from __future__ import annotations

import asyncio
import logging
import random
import time
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Final

import grpc
from hub_model.flows import PACS_008, flow_for, is_cover, normalise_msg_type

from .cases import StatusProbe
from .sender import InboundSender

log = logging.getLogger(__name__)

#: Above this, warn: the ESS is not a load generator and a laptop is not a
#: performance environment.
SOFT_RATE_LIMIT: Final = 50.0

#: Eight in flight against a laptop makes the 200 ms Deliver* deadline the
#: bottleneck: the retries cost more than the parallelism buys. Four is
#: measurably faster end to end.
DEFAULT_CONCURRENCY: Final = 4

#: Cap on the UETRs reported back for failures, so a bad run does not return
#: a megabyte of identifiers.
MAX_REPORTED_FAILURES: Final = 20

TERMINAL: Final = frozenset({"COMPLETED", "REJECTED", "BLOCKED"})


@dataclass(frozen=True, slots=True)
class MixEntry:
    """One message type and its relative weight in the batch."""

    msg_type: str
    share: float = 1.0
    flow: str = ""


@dataclass(slots=True)
class BatchSpec:
    """What to send."""

    count: int = 1
    rate_per_second: float = 0.0  # 0 = as fast as the Hub accepts
    concurrency: int = DEFAULT_CONCURRENCY
    mix: tuple[MixEntry, ...] = ()
    run_id: str = ""
    settle_timeout_s: float = 0.0  # 0 = deliver only, do not wait
    sanctions_hit_rate: float = 0.0

    def normalised_mix(self) -> tuple[MixEntry, ...]:
        if not self.mix:
            return (MixEntry(PACS_008, 1.0),)
        return tuple(
            MixEntry(normalise_msg_type(e.msg_type), max(0.0, e.share) or 1.0, e.flow)
            for e in self.mix
        )


@dataclass(slots=True)
class BatchReport:
    """What happened."""

    requested: int = 0
    accepted: int = 0
    rejected: int = 0
    duplicate: int = 0
    errors: int = 0
    send_duration_s: float = 0.0
    total_duration_s: float = 0.0
    settled: int = 0
    unsettled: int = 0
    by_state: dict[str, int] = field(default_factory=dict)
    end_to_end_ms: list[float] = field(default_factory=list)
    failed_uetrs: list[str] = field(default_factory=list)
    detail: str = ""
    uetrs: list[str] = field(default_factory=list)

    @property
    def achieved_rate(self) -> float:
        return self.accepted / self.send_duration_s if self.send_duration_s > 0 else 0.0

    def percentile(self, pct: float) -> float:
        """Nearest-rank percentile. Exact enough for tens or hundreds."""
        if not self.end_to_end_ms:
            return 0.0
        ordered = sorted(self.end_to_end_ms)
        index = min(len(ordered) - 1, round(pct / 100.0 * (len(ordered) - 1)))
        return ordered[index]

    @property
    def p50_ms(self) -> float:
        return self.percentile(50)

    @property
    def p95_ms(self) -> float:
        return self.percentile(95)

    @property
    def max_ms(self) -> float:
        return max(self.end_to_end_ms) if self.end_to_end_ms else 0.0

    @property
    def ok(self) -> bool:
        return self.rejected == 0 and self.errors == 0 and self.unsettled == 0


class BatchRunner:
    """Sends a batch through the inbound sender and reports on it."""

    def __init__(
        self,
        sender: InboundSender,
        probe: StatusProbe | None = None,
        *,
        rng: random.Random | None = None,
    ) -> None:
        self.sender = sender
        self.probe = probe
        self.rng = rng or random.Random()

    async def run(self, spec: BatchSpec) -> BatchReport:
        report = BatchReport(requested=max(0, spec.count))
        if report.requested == 0:
            report.detail = "nothing requested"
            return report

        if spec.rate_per_second > SOFT_RATE_LIMIT:
            log.warning(
                "%.0f TPS requested; the ESS inbound sender is not a load generator "
                "and CLAUDE.md caps local runs near %.0f TPS. Use paygen for real load.",
                spec.rate_per_second,
                SOFT_RATE_LIMIT,
            )

        started = time.monotonic()
        await self._deliver_all(spec, report)
        report.send_duration_s = time.monotonic() - started

        if spec.settle_timeout_s > 0 and report.uetrs:
            await self._await_settlement(spec, report)
        report.total_duration_s = time.monotonic() - started

        unaccounted = report.requested - (
            report.accepted + report.duplicate + report.rejected + report.errors
        )
        if unaccounted:
            report.detail = f"{unaccounted} payment(s) unaccounted for — this is a bug"
        elif not report.detail:
            report.detail = (
                f"{report.accepted}/{report.requested} accepted at {report.achieved_rate:.1f}/s"
            )
        return report

    # ------------------------------------------------------------- sending
    async def _deliver_all(self, spec: BatchSpec, report: BatchReport) -> None:
        """Deliver ``count`` payments, paced and bounded by ``concurrency``."""
        mix = spec.normalised_mix()
        weights = [entry.share for entry in mix]
        gate = asyncio.Semaphore(max(1, spec.concurrency))
        interval = 1.0 / spec.rate_per_second if spec.rate_per_second > 0 else 0.0
        lock = asyncio.Lock()
        origin = time.monotonic()

        async def one(index: int) -> None:
            # Pace on the wall clock rather than sleeping a fixed interval, so
            # a slow reply does not make the whole batch drift late.
            if interval:
                due = origin + index * interval
                delay = due - time.monotonic()
                if delay > 0:
                    await asyncio.sleep(delay)

            entry = self.rng.choices(mix, weights=weights, k=1)[0]
            hit = self.rng.random() < spec.sanctions_hit_rate
            async with gate:
                try:
                    deliveries = self._build(entry, sanctions_hit=hit)
                    outcome = await self.sender.send(deliveries)
                except grpc.aio.AioRpcError as exc:
                    async with lock:
                        report.errors += 1
                        if len(report.failed_uetrs) < MAX_REPORTED_FAILURES:
                            report.failed_uetrs.append(f"(send failed: {exc.code().name})")
                    return

            async with lock:
                report.accepted += outcome.accepted
                report.rejected += outcome.rejected
                report.duplicate += outcome.duplicate
                # A delivery that never got a receipt is a failure, not a
                # rounding error: without this the totals would not add up.
                report.errors += outcome.failed
                if outcome.uetrs:
                    # Cover pairs share a UETR; count the payment once.
                    report.uetrs.append(outcome.uetrs[0])
                for message in outcome.errors:
                    if len(report.failed_uetrs) < MAX_REPORTED_FAILURES:
                        report.failed_uetrs.append(message)

        previous_run_id = self.sender.run_id
        if spec.run_id:
            self.sender.run_id = spec.run_id
        try:
            await asyncio.gather(*(one(i) for i in range(spec.count)))
        finally:
            self.sender.run_id = previous_run_id

    def _build(self, entry: MixEntry, *, sanctions_hit: bool) -> list:  # type: ignore[type-arg]
        msg_type = entry.msg_type
        flow = entry.flow or flow_for(msg_type, cover=is_cover(msg_type))
        if is_cover(msg_type):
            # A cover leg on its own never completes; send the pair.
            partner = PACS_008 if msg_type.startswith("pacs") else "MT103"
            return self.sender.build_cover_pair(
                (partner, msg_type), flow=flow, sanctions_hit=sanctions_hit
            )
        return [self.sender.build(msg_type, flow=flow, sanctions_hit=sanctions_hit)]

    # ---------------------------------------------------------- settlement
    async def _await_settlement(self, spec: BatchSpec, report: BatchReport) -> None:
        """Poll the status API until every payment is terminal, or time runs out."""
        if self.probe is None:
            report.detail = "no status API configured; delivered without waiting"
            report.unsettled = len(report.uetrs)
            return

        pending = set(report.uetrs)
        deadline = time.monotonic() + spec.settle_timeout_s

        while pending and time.monotonic() < deadline:
            for uetr in list(pending):
                state = await asyncio.to_thread(self.probe.state_of, uetr)
                if state in TERMINAL:
                    pending.discard(uetr)
                    report.by_state[state] = report.by_state.get(state, 0) + 1
                    report.settled += 1
                    elapsed = await asyncio.to_thread(self.probe.end_to_end_ms, uetr)
                    if elapsed is not None:
                        report.end_to_end_ms.append(elapsed)
                    if state != "COMPLETED" and len(report.failed_uetrs) < MAX_REPORTED_FAILURES:
                        report.failed_uetrs.append(uetr)
            if pending:
                await asyncio.sleep(0.2)

        report.unsettled = len(pending)
        for uetr in list(pending)[: MAX_REPORTED_FAILURES - len(report.failed_uetrs)]:
            report.failed_uetrs.append(f"{uetr} (never settled)")
        if report.unsettled:
            report.detail = (
                f"{report.settled}/{len(report.uetrs)} settled; "
                f"{report.unsettled} still in flight after {spec.settle_timeout_s:g}s"
            )


def parse_mix(text: str) -> tuple[MixEntry, ...]:
    """Parse ``pacs.008=70,MT103=30`` into weighted entries.

    A bare list of types (``pacs.008,MT103``) weights them equally.
    """
    entries: list[MixEntry] = []
    for chunk in text.split(","):
        piece = chunk.strip()
        if not piece:
            continue
        if "=" in piece:
            name, _, weight = piece.partition("=")
            try:
                share = float(weight)
            except ValueError as exc:
                raise ValueError(f"not a weight: {weight!r} in {piece!r}") from exc
        else:
            name, share = piece, 1.0
        entries.append(MixEntry(normalise_msg_type(name.strip()), share))
    if not entries:
        raise ValueError("empty mix")
    return tuple(entries)


def describe(report: BatchReport) -> list[str]:
    """Human-readable summary lines, shared by the CLI and the control API."""
    lines = [
        f"requested {report.requested}  accepted {report.accepted}  "
        f"rejected {report.rejected}  duplicate {report.duplicate}  errors {report.errors}",
        f"sent in {report.send_duration_s:.2f}s at {report.achieved_rate:.1f}/s",
    ]
    if report.settled or report.unsettled:
        states = "  ".join(f"{k}={v}" for k, v in sorted(report.by_state.items()))
        lines.append(f"settled {report.settled}  unsettled {report.unsettled}  {states}")
    if report.end_to_end_ms:
        lines.append(
            f"end to end  p50 {report.p50_ms:.0f} ms  "
            f"p95 {report.p95_ms:.0f} ms  max {report.max_ms:.0f} ms"
        )
    if report.failed_uetrs:
        lines.append("needs a look: " + ", ".join(report.failed_uetrs[:5]))
    return lines


def spec_from_proto(request: object) -> BatchSpec:
    """Build a :class:`BatchSpec` from an ``ess.v1.BatchRequest``."""
    mix: Sequence[object] = getattr(request, "mix", ())
    return BatchSpec(
        count=int(getattr(request, "count", 0)) or 1,
        rate_per_second=float(getattr(request, "rate_per_second", 0.0)),
        concurrency=int(getattr(request, "concurrency", 0)) or DEFAULT_CONCURRENCY,
        mix=tuple(
            MixEntry(
                msg_type=str(getattr(e, "msg_type", "")),
                share=float(getattr(e, "share", 1.0)) or 1.0,
                flow=str(getattr(e, "flow", "")),
            )
            for e in mix
        ),
        run_id=str(getattr(request, "run_id", "")),
        settle_timeout_s=int(getattr(request, "settle_timeout_ms", 0)) / 1000.0,
        sanctions_hit_rate=float(getattr(request, "sanctions_hit_rate", 0.0)),
    )
