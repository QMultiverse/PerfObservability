"""The ESS recorder: one row per call made or received.

In a performance run the Hub logs almost nothing, so the recorder is one of
the three measurements that remain (with Prometheus and the status tracker).
It has to be cheap: rows go to a bounded in-memory ring and are flushed to disk
in batches off the serving path.

Rows are written as JSON Lines by default. The design doc asks for Parquet;
``pyarrow`` is not a dependency of this repository (CLAUDE.md: no new
dependencies without asking), so Parquet is written only when ``pyarrow``
happens to be importable, and the format is reported either way.
"""

from __future__ import annotations

import json
import logging
import threading
import time
from collections import Counter, deque
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Final

from hub_telemetry.metrics import REGISTRY
from prometheus_client import Counter as PromCounter
from prometheus_client import Histogram

log = logging.getLogger(__name__)

MAX_BUFFERED: Final = 200_000

ess_calls = PromCounter(
    "ess_calls_total",
    "Calls the ESS served or made",
    ["method", "direction", "outcome"],
    registry=REGISTRY,
)
ess_latency = Histogram(
    "ess_call_latency_seconds",
    "ESS call duration",
    ["method", "direction"],
    buckets=(0.001, 0.005, 0.010, 0.025, 0.050, 0.100, 0.250, 0.500, 1.0, 2.0, 5.0),
    registry=REGISTRY,
)
ess_errors = PromCounter(
    "ess_errors_total",
    "ESS call failures",
    ["method", "error"],
    registry=REGISTRY,
)


@dataclass(slots=True)
class Row:
    """One recorded call."""

    uetr: str
    flow: str
    format: str
    msg_type: str
    method: str
    direction: str  # SERVED | MADE
    outcome: str
    started_ns: int
    duration_ns: int
    run_id: str = ""
    error: str = ""


class Recorder:
    """Bounded ring of rows, plus Prometheus counters."""

    def __init__(self, *, enabled: bool = True, max_rows: int = MAX_BUFFERED) -> None:
        self.enabled = enabled
        self._lock = threading.Lock()
        self._rows: deque[Row] = deque(maxlen=max_rows)
        self._by_method: Counter[str] = Counter()
        self._by_flow: Counter[str] = Counter()
        self._by_outcome: Counter[str] = Counter()
        self._by_run: dict[str, Counter[str]] = {}
        self.calls_served = 0
        self.calls_made = 0
        self.errors = 0
        self.dropped = 0

    def record(
        self,
        *,
        method: str,
        direction: str,
        outcome: str,
        started_ns: int,
        uetr: str = "",
        flow: str = "",
        fmt: str = "",
        msg_type: str = "",
        run_id: str = "",
        error: str = "",
    ) -> None:
        duration_ns = max(0, time.time_ns() - started_ns)
        ess_calls.labels(method, direction, outcome).inc()
        ess_latency.labels(method, direction).observe(duration_ns / 1e9)
        if error:
            ess_errors.labels(method, error).inc()

        with self._lock:
            if direction == "SERVED":
                self.calls_served += 1
            else:
                self.calls_made += 1
            if error:
                self.errors += 1
            self._by_method[method] += 1
            if flow:
                self._by_flow[flow] += 1
            self._by_outcome[outcome] += 1
            if run_id:
                bucket = self._by_run.setdefault(run_id, Counter())
                bucket[outcome] += 1
                bucket[method] += 1
            if not self.enabled:
                return
            if len(self._rows) == self._rows.maxlen:
                self.dropped += 1
            self._rows.append(
                Row(
                    uetr=uetr,
                    flow=flow,
                    format=fmt,
                    msg_type=msg_type,
                    method=method,
                    direction=direction,
                    outcome=outcome,
                    started_ns=started_ns,
                    duration_ns=duration_ns,
                    run_id=run_id,
                    error=error,
                )
            )

    # ---------------------------------------------------------- reporting
    def counters(self, run_id: str = "") -> dict[str, dict[str, int]]:
        with self._lock:
            if run_id:
                bucket = self._by_run.get(run_id, Counter())
                return {
                    "by_method": {k: v for k, v in bucket.items() if "." in k},
                    "by_flow": {},
                    "by_outcome": {k: v for k, v in bucket.items() if "." not in k},
                }
            return {
                "by_method": dict(self._by_method),
                "by_flow": dict(self._by_flow),
                "by_outcome": dict(self._by_outcome),
            }

    def totals(self) -> tuple[int, int, int]:
        with self._lock:
            return self.calls_served, self.calls_made, self.errors

    def rows(self) -> list[Row]:
        with self._lock:
            return list(self._rows)

    def __len__(self) -> int:
        with self._lock:
            return len(self._rows)

    # ------------------------------------------------------------ flushing
    def flush(self, path: str | Path, *, clear: bool = True) -> tuple[Path, str]:
        """Write the buffered rows out. Returns the path and the format used."""
        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        with self._lock:
            rows = list(self._rows)
            if clear:
                self._rows.clear()
        if not rows:
            return target, "empty"

        records = [asdict(row) for row in rows]
        if target.suffix == ".parquet":
            written = _write_parquet(target, records)
            if written:
                return target, "parquet"
            target = target.with_suffix(".jsonl")
        with target.open("a", encoding="utf-8") as handle:
            for record in records:
                handle.write(json.dumps(record, separators=(",", ":")) + "\n")
        return target, "jsonl"

    def reset(self) -> None:
        with self._lock:
            self._rows.clear()
            self._by_method.clear()
            self._by_flow.clear()
            self._by_outcome.clear()
            self._by_run.clear()
            self.calls_served = 0
            self.calls_made = 0
            self.errors = 0
            self.dropped = 0


def _write_parquet(target: Path, records: list[dict[str, Any]]) -> bool:
    try:
        import pyarrow as pa  # type: ignore[import-not-found]
        import pyarrow.parquet as pq  # type: ignore[import-not-found]
    except ImportError:
        log.info("pyarrow not installed; writing %s as JSON Lines instead", target.name)
        return False
    pq.write_table(pa.Table.from_pylist(records), target)
    return True
