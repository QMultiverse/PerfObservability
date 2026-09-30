"""Status API: serves the current state of a payment from ``hub.pay.status``.

Two parts:

* a consumer (``cg-status-api``) that folds every status event into an
  in-memory view, one entry per UETR;
* a small read-only HTTP server so an operator, a test or the performance
  framework can ask "where is this payment?" without querying PostgreSQL.

The system of record is still the database, filled by ``cg-db-sink``. This is
the hot path: days of history, keyed by UETR, answered in microseconds.
"""

from __future__ import annotations

import json
import logging
import threading
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Final
from urllib.parse import parse_qs, urlparse

from hub_model import proto as pb
from hub_model import topics as tp
from hub_model.envelope import TERMINAL_STATES, end_to_end_seconds, state_rank
from hub_model.proto import StatusEvent
from hub_telemetry.events import Events
from hub_telemetry.messaging import Record

from hub.common.config import ServiceSettings
from hub.common.processor import Outbox, Processor

log = logging.getLogger(__name__)

SERVICE_NAME: Final = "hub-status-api"

# Keep the view bounded. Older payments are still in PostgreSQL.
MAX_TRACKED: Final = 200_000


@dataclass(slots=True)
class PaymentStatus:
    """One payment, as the status API sees it."""

    uetr: str
    msg_type: str = ""
    flow: str = ""
    format: str = ""
    state: str = ""
    reason: str = ""
    error_code: str = ""
    service: str = ""
    run_id: str = ""
    updated_ns: int = 0
    #: Rank of the state currently reported, so a late event for an earlier
    #: stage cannot pull the payment backwards.
    rank: int = 0
    history: list[dict[str, Any]] = field(default_factory=list)
    timings: dict[str, int] = field(default_factory=dict)
    end_to_end_s: float | None = None

    def to_json(self) -> dict[str, Any]:
        return {
            "uetr": self.uetr,
            "msg_type": self.msg_type,
            "flow": self.flow,
            "format": self.format,
            "state": self.state,
            "reason": self.reason,
            "error_code": self.error_code,
            "service": self.service,
            "run_id": self.run_id,
            "updated_ns": self.updated_ns,
            "timings": self.timings,
            "end_to_end_s": self.end_to_end_s,
            "history": self.history,
        }


class StatusView:
    """Thread-safe view over the status stream."""

    def __init__(self, max_tracked: int = MAX_TRACKED) -> None:
        self._lock = threading.RLock()
        self._by_uetr: dict[str, PaymentStatus] = {}
        self._max = max_tracked

    def apply(self, event: StatusEvent) -> PaymentStatus:
        with self._lock:
            entry = self._by_uetr.get(event.ref.uetr)
            if entry is None:
                if len(self._by_uetr) >= self._max:
                    for stale in list(self._by_uetr)[: self._max // 10]:
                        self._by_uetr.pop(stale, None)
                entry = PaymentStatus(uetr=event.ref.uetr)
                self._by_uetr[event.ref.uetr] = entry

            state = pb.state_name(event.state)
            entry.msg_type = event.ref.msg_type or entry.msg_type
            entry.flow = event.ref.flow or entry.flow
            entry.format = pb.format_name(event.format)
            entry.run_id = event.run_id or entry.run_id
            # Everything that happened is kept, in arrival order.
            entry.history.append(
                {
                    "state": state,
                    "service": event.service,
                    "reason": event.reason,
                    "at_ns": event.emitted_ns,
                }
            )

            # Timings accumulate from every event, in whatever order they
            # arrive: each producer stamps only what it knew at the time, so a
            # late DISPATCHED still carries the only T4 anyone will report.
            entry.timings = _merge_timings(entry.timings, _timings(event))
            elapsed = end_to_end_seconds(event.timings)
            if elapsed is not None:
                entry.end_to_end_s = elapsed

            # The *current* state, though, only ever moves forward. A network
            # ACK can beat the dispatcher's own transaction commit, so
            # COMPLETED legitimately arrives before DISPATCHED; without this
            # guard the later, earlier-stage event would overwrite the
            # terminal one.
            rank = state_rank(event.state)
            if entry.rank and (
                rank < entry.rank
                or (entry.rank in _TERMINAL_RANKS and event.state not in TERMINAL_STATES)
            ):
                return entry

            entry.rank = rank
            entry.state = state
            entry.reason = event.reason
            entry.error_code = event.error_code
            entry.service = event.service
            entry.updated_ns = event.emitted_ns
            return entry

    def get(self, uetr: str) -> PaymentStatus | None:
        with self._lock:
            return self._by_uetr.get(uetr)

    def query(
        self,
        *,
        state: str = "",
        flow: str = "",
        run_id: str = "",
        limit: int = 100,
    ) -> list[PaymentStatus]:
        with self._lock:
            found: list[PaymentStatus] = []
            for entry in self._by_uetr.values():
                if state and entry.state != state.upper():
                    continue
                if flow and entry.flow != flow:
                    continue
                if run_id and entry.run_id != run_id:
                    continue
                found.append(entry)
                if len(found) >= limit:
                    break
            return found

    def counts(self, run_id: str = "") -> dict[str, int]:
        with self._lock:
            out: dict[str, int] = {}
            for entry in self._by_uetr.values():
                if run_id and entry.run_id != run_id:
                    continue
                out[entry.state] = out.get(entry.state, 0) + 1
            return out

    def __len__(self) -> int:
        with self._lock:
            return len(self._by_uetr)


_TERMINAL_RANKS = frozenset(state_rank(s) for s in TERMINAL_STATES)


def _merge_timings(existing: dict[str, int], incoming: dict[str, int]) -> dict[str, int]:
    """Keep every stamp seen. Events arrive out of order, and each carries only
    the timings its producer knew about."""
    merged = dict(existing)
    merged.update({k: v for k, v in incoming.items() if v})
    return merged


def _timings(event: StatusEvent) -> dict[str, int]:
    timings = event.timings
    return {
        name: value
        for name, value in (
            ("t0", timings.t0_network_sent_ns),
            ("t1", timings.t1_edge_accepted_ns),
            ("t2", timings.t2_canonical_ns),
            ("t3", timings.t3_screen_reply_ns),
            ("t4", timings.t4_handoff_ns),
            ("t5", timings.t5_network_ack_ns),
            ("t6", timings.t6_completed_ns),
        )
        if value
    }


class StatusApi(Processor):
    """Folds ``hub.pay.status`` into the view. Produces nothing."""

    name = SERVICE_NAME
    stage = "status"
    group_id = tp.CG_STATUS_API
    input_topics = (tp.PAY_STATUS,)

    def __init__(self, settings: ServiceSettings, events: Events) -> None:
        super().__init__(settings, events)
        self.view = StatusView()

    async def handle(self, record: Record, out: Outbox) -> None:
        event = StatusEvent()
        try:
            event.ParseFromString(record.value)
        except Exception:
            log.warning("undecodable StatusEvent at %s:%s", record.topic, record.offset)
            return
        self.view.apply(event)


class _Handler(BaseHTTPRequestHandler):
    view: StatusView

    def do_GET(self) -> None:
        url = urlparse(self.path)
        params = parse_qs(url.query)
        parts = [p for p in url.path.split("/") if p]

        if parts == ["healthz"]:
            return self._json({"ok": True, "tracked": len(self.view)})
        if parts == ["payments", "counts"]:
            return self._json(self.view.counts(_one(params, "run_id")))
        if len(parts) == 2 and parts[0] == "payments":
            entry = self.view.get(parts[1])
            if entry is None:
                return self._json({"error": "unknown UETR", "uetr": parts[1]}, status=404)
            return self._json(entry.to_json())
        if parts == ["payments"]:
            found = self.view.query(
                state=_one(params, "state"),
                flow=_one(params, "flow"),
                run_id=_one(params, "run_id"),
                limit=int(_one(params, "limit") or 100),
            )
            return self._json({"payments": [e.to_json() for e in found], "count": len(found)})
        self._json({"error": "not found"}, status=404)

    def _json(self, payload: object, status: int = 200) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt: str, *args: object) -> None:
        # Route access logs through our own logger, not stderr.
        log.debug(fmt, *args)


def _one(params: dict[str, list[str]], key: str) -> str:
    values = params.get(key)
    return values[0] if values else ""


def serve_http(view: StatusView, port: int) -> ThreadingHTTPServer:
    """Start the read-only HTTP server on a daemon thread."""
    handler = type("BoundHandler", (_Handler,), {"view": view})
    server = ThreadingHTTPServer(("0.0.0.0", port), handler)
    thread = threading.Thread(target=server.serve_forever, name="status-api", daemon=True)
    thread.start()
    log.info("status API listening on :%d", port)
    return server


def build(settings: ServiceSettings, events: Events) -> StatusApi:
    return StatusApi(settings, events)
