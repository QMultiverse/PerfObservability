"""Named test cases: the scenario engine.

A case is a scripted end-to-end scenario — send these messages, force these
outcomes, expect this final state. ``ess case run mt103_sanctions_hit`` runs
one; CI runs the whole catalogue against every Hub change.

Each case declares its expected terminal state, and the runner waits for that
state on the Hub's status API rather than sleeping a fixed time, so a case is a
real assertion and not a race.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
import urllib.error
import urllib.request
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Final

from hub_model.flows import (
    FLOW_MT_FIN_103,
    FLOW_MT_FIN_103_202COV,
    FLOW_MT_TO_MX_103,
    FLOW_MX_SNF_PACS008,
    FLOW_MX_SNF_PACS009,
    FLOW_MX_SNF_PACS009COV_PAIR,
    MT103,
    MT202COV,
    PACS_008,
    PACS_009,
    PACS_009_COV,
)

from .emulators import Override
from .recorder import Recorder
from .sender import Delivery, InboundSender

log = logging.getLogger(__name__)

DEFAULT_TIMEOUT_MS: Final = 30_000


@dataclass(frozen=True, slots=True)
class Case:
    """One scripted scenario."""

    case_id: str
    flow: str
    msg_types: tuple[str, ...]
    expected_state: str
    description: str
    cover_pair: bool = False
    override: Override | None = None
    sanctions_hit: bool = False
    timeout_ms: int = DEFAULT_TIMEOUT_MS


CASES: Final[dict[str, Case]] = {
    case.case_id: case
    for case in (
        Case(
            "pacs008_happy",
            FLOW_MX_SNF_PACS008,
            (PACS_008,),
            "COMPLETED",
            "One pacs.008, no screening hit, through to COMPLETED",
        ),
        Case(
            "mt103_happy",
            FLOW_MT_FIN_103,
            (MT103,),
            "COMPLETED",
            "One MT103 over FIN, through to COMPLETED",
        ),
        Case(
            "pacs009_happy",
            FLOW_MX_SNF_PACS009,
            (PACS_009,),
            "COMPLETED",
            "One pacs.009 FI transfer, no cover leg",
        ),
        Case(
            "mt103_sanctions_hit",
            FLOW_MT_FIN_103,
            (MT103,),
            "COMPLETED",
            "MT103 hits the watchlist, is HELD, then released by the analyst",
            override=Override(hit=True, release=True, decision_delay_s=0.2),
            sanctions_hit=True,
        ),
        Case(
            "pacs008_sanctions_hit",
            FLOW_MX_SNF_PACS008,
            (PACS_008,),
            "COMPLETED",
            "pacs.008 HELD on a hit, then released",
            override=Override(hit=True, release=True, decision_delay_s=0.2),
            sanctions_hit=True,
        ),
        Case(
            "pacs008_blocked",
            FLOW_MX_SNF_PACS008,
            (PACS_008,),
            "BLOCKED",
            "FCC returns BLOCK; the payment stops at screening",
            override=Override(block=True),
        ),
        Case(
            "pacs008_held_then_blocked",
            FLOW_MX_SNF_PACS008,
            (PACS_008,),
            "BLOCKED",
            "pacs.008 HELD on a hit, then blocked by the analyst",
            override=Override(hit=True, release=False, decision_delay_s=0.2),
        ),
        Case(
            "pacs008_nak",
            FLOW_MX_SNF_PACS008,
            (PACS_008,),
            "REJECTED",
            "SnF returns a NAK; the payment is REJECTED",
            override=Override(nak=True, nak_code="H50"),
        ),
        Case(
            "mt103_nak",
            FLOW_MT_FIN_103,
            (MT103,),
            "REJECTED",
            "FIN returns a NAK; the payment is REJECTED",
            override=Override(nak=True, nak_code="T13"),
        ),
        Case(
            "pacs008_duplicate_ack",
            FLOW_MX_SNF_PACS008,
            (PACS_008,),
            "COMPLETED",
            "The ACK is delivered twice; the Hub must not complete twice",
            override=Override(duplicate=True),
        ),
        Case(
            "mt103_202cov_pair",
            FLOW_MT_FIN_103_202COV,
            (MT103, MT202COV),
            "COMPLETED",
            "Cover method: MT103 and MT202 COV under one UETR, completing together",
            cover_pair=True,
        ),
        Case(
            "pacs009cov_pair",
            FLOW_MX_SNF_PACS009COV_PAIR,
            (PACS_008, PACS_009_COV),
            "COMPLETED",
            "Cover method: pacs.008 and pacs.009 COV under one UETR",
            cover_pair=True,
        ),
        Case(
            "mt103_to_pacs008",
            FLOW_MT_TO_MX_103,
            (MT103,),
            "COMPLETED",
            "MT103 in over FIN, translated in flow and sent on as pacs.008 over SnF",
        ),
        Case(
            "pacs008_send_rejected",
            FLOW_MX_SNF_PACS008,
            (PACS_008,),
            "REJECTED",
            "SnF rejects the send outright; the record is dead-lettered",
            override=Override(reject_send=True),
        ),
    )
}


@dataclass(slots=True)
class StepResult:
    name: str
    passed: bool
    detail: str = ""
    at_ns: int = 0


@dataclass(slots=True)
class CaseOutcome:
    case_id: str
    passed: bool
    uetrs: list[str] = field(default_factory=list)
    detail: str = ""
    steps: list[StepResult] = field(default_factory=list)
    duration_ms: int = 0


class StatusProbe:
    """Reads a payment's state from the Hub status API.

    A case needs to know when the Hub is *done*, and the status API is the
    read-only view built for exactly that. It is HTTP and synchronous, so it
    runs in a thread rather than blocking the event loop.
    """

    def __init__(self, base_url: str) -> None:
        self.base_url = base_url.rstrip("/")

    def _fetch(self, uetr: str) -> dict[str, object]:
        if not self.base_url:
            return {}
        try:
            with urllib.request.urlopen(
                f"{self.base_url}/payments/{uetr}", timeout=2.0
            ) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except (
            urllib.error.URLError,
            TimeoutError,
            json.JSONDecodeError,
            OSError,
            ValueError,
        ):
            return {}
        return dict(payload)

    def state_of(self, uetr: str) -> str:
        return str(self._fetch(uetr).get("state", ""))

    def end_to_end_ms(self, uetr: str) -> float | None:
        """T0 to T6 in milliseconds, or None while the payment is in flight.

        A subclass that reads the status view in-process overrides this
        alongside :meth:`state_of`.
        """
        seconds = self._fetch(uetr).get("end_to_end_s")
        return float(seconds) * 1000.0 if seconds else None  # type: ignore[arg-type]

    async def wait_for_state(self, uetr: str, expected: str, timeout_s: float) -> tuple[bool, str]:
        """Poll until the payment reaches ``expected`` or time runs out."""
        deadline = time.monotonic() + timeout_s
        last = ""
        while time.monotonic() < deadline:
            last = await asyncio.to_thread(self.state_of, uetr)
            if last == expected:
                return True, last
            if last in {"COMPLETED", "REJECTED", "BLOCKED"} and last != expected:
                return False, last  # terminal, and not what we wanted
            await asyncio.sleep(0.05)
        return False, last


class CaseRunner:
    """Runs a named case end to end."""

    def __init__(
        self,
        sender: InboundSender,
        overrides: object,
        recorder: Recorder,
        probe: StatusProbe | None,
    ) -> None:
        self.sender = sender
        self.overrides = overrides
        self.recorder = recorder
        self.probe = probe

    async def run(
        self,
        case_id: str,
        *,
        run_id: str = "",
        repeat: int = 1,
        timeout_ms: int = 0,
    ) -> CaseOutcome:
        case = CASES.get(case_id)
        if case is None:
            return CaseOutcome(case_id, False, detail=f"unknown case {case_id!r}")

        started = time.monotonic()
        outcome = CaseOutcome(case_id, True)
        limit_s = (timeout_ms or case.timeout_ms) / 1000.0

        for iteration in range(max(1, repeat)):
            deliveries = self._build(case)
            uetr = deliveries[0].uetr
            outcome.uetrs.append(uetr)

            if case.override is not None:
                self.overrides.set(uetr, case.override)  # type: ignore[attr-defined]
                outcome.steps.append(
                    StepResult(
                        f"override[{iteration}]", True, _describe(case.override), time.time_ns()
                    )
                )

            sent = await self.sender.send(deliveries)
            step = StepResult(
                f"deliver[{iteration}]",
                sent.ok,
                f"accepted={sent.accepted} duplicate={sent.duplicate} "
                f"rejected={sent.rejected} {'; '.join(sent.errors)}".strip(),
                time.time_ns(),
            )
            outcome.steps.append(step)
            if not sent.ok:
                outcome.passed = False
                outcome.detail = step.detail
                break

            if self.probe is None:
                outcome.steps.append(
                    StepResult(
                        f"await[{iteration}]",
                        True,
                        "no status API configured; delivery only",
                        time.time_ns(),
                    )
                )
                continue

            reached, actual = await self.probe.wait_for_state(uetr, case.expected_state, limit_s)
            outcome.steps.append(
                StepResult(
                    f"await[{iteration}]",
                    reached,
                    f"expected {case.expected_state}, saw {actual or '(nothing)'}",
                    time.time_ns(),
                )
            )
            if not reached:
                outcome.passed = False
                outcome.detail = (
                    f"{uetr} did not reach {case.expected_state} within {limit_s:g}s "
                    f"(last state: {actual or 'none'})"
                )
                break

        outcome.duration_ms = int((time.monotonic() - started) * 1000)
        if outcome.passed and not outcome.detail:
            outcome.detail = f"{case.description} — reached {case.expected_state}"
        return outcome

    def _build(self, case: Case) -> list[Delivery]:
        if case.cover_pair:
            return self.sender.build_cover_pair(
                (case.msg_types[0], case.msg_types[1]),
                flow=case.flow,
                sanctions_hit=case.sanctions_hit,
            )
        return [
            self.sender.build(case.msg_types[0], flow=case.flow, sanctions_hit=case.sanctions_hit)
        ]


def _describe(override: Override) -> str:
    parts = [
        name
        for name, value in (
            ("nak", override.nak),
            ("hit", override.hit),
            ("block", override.block),
            ("duplicate", override.duplicate),
            ("reject_send", override.reject_send),
        )
        if value
    ]
    if override.hit:
        parts.append("release" if override.release else "analyst-block")
    return ",".join(parts) or "none"


def case_list() -> Sequence[Case]:
    return list(CASES.values())
