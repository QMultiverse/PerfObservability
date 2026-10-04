"""DB sink: ``hub.pay.status`` to PostgreSQL, the system of record.

Kafka keeps days of history for replay; the database keeps the years
regulation requires. Two tables:

* ``payment`` — one row per UETR, upserted to the latest state;
* ``payment_audit`` — one row per status event, append-only.

The upsert is guarded on ``updated_ns``, so a redelivered event cannot drag a
payment backwards. With no ``HUB_DATABASE_URL`` configured the sink runs in
dry-run mode and only counts — which is what local Compose and the tests use.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any, Final

from hub_model import proto as pb
from hub_model import topics as tp
from hub_model.envelope import state_rank
from hub_model.proto import StatusEvent
from hub_telemetry import metrics
from hub_telemetry.events import Events
from hub_telemetry.messaging import Record

from hub.common.config import ServiceSettings
from hub.common.processor import Outbox, Processor, RetryableError

log = logging.getLogger(__name__)

SERVICE_NAME: Final = "hub-db-sink"

SCHEMA: Final = """
CREATE TABLE IF NOT EXISTS payment (
    uetr            uuid PRIMARY KEY,
    msg_type        text        NOT NULL DEFAULT '',
    flow            text        NOT NULL DEFAULT '',
    format          text        NOT NULL DEFAULT '',
    state           text        NOT NULL,
    -- How far along `state` is, from hub_model.envelope.state_rank. Stored so
    -- the upsert can refuse to move a payment backwards using the same
    -- ordering the in-memory view uses.
    state_rank      smallint    NOT NULL DEFAULT 0,
    reason          text        NOT NULL DEFAULT '',
    error_code      text        NOT NULL DEFAULT '',
    run_id          text        NOT NULL DEFAULT '',
    t0_network_ns   bigint,
    t1_accepted_ns  bigint,
    t2_canonical_ns bigint,
    t3_screen_ns    bigint,
    t4_handoff_ns   bigint,
    t5_ack_ns       bigint,
    t6_completed_ns bigint,
    updated_ns      bigint      NOT NULL,
    updated_at      timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS payment_state_idx  ON payment (state);
CREATE INDEX IF NOT EXISTS payment_flow_idx   ON payment (flow);
CREATE INDEX IF NOT EXISTS payment_run_idx    ON payment (run_id) WHERE run_id <> '';
CREATE INDEX IF NOT EXISTS payment_updated_idx ON payment (updated_at DESC);

CREATE TABLE IF NOT EXISTS payment_audit (
    id         bigserial PRIMARY KEY,
    uetr       uuid        NOT NULL,
    state      text        NOT NULL,
    service    text        NOT NULL DEFAULT '',
    reason     text        NOT NULL DEFAULT '',
    error_code text        NOT NULL DEFAULT '',
    emitted_ns bigint      NOT NULL,
    emitted_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS payment_audit_uetr_idx ON payment_audit (uetr, emitted_ns);
"""

_UPSERT: Final = """
INSERT INTO payment (
    uetr, msg_type, flow, format, state, state_rank, reason, error_code, run_id,
    t0_network_ns, t1_accepted_ns, t2_canonical_ns, t3_screen_ns,
    t4_handoff_ns, t5_ack_ns, t6_completed_ns, updated_ns
) VALUES (
    %(uetr)s, %(msg_type)s, %(flow)s, %(format)s, %(state)s, %(state_rank)s,
    %(reason)s, %(error_code)s, %(run_id)s, %(t0)s, %(t1)s, %(t2)s, %(t3)s,
    %(t4)s, %(t5)s, %(t6)s, %(updated_ns)s
)
ON CONFLICT (uetr) DO UPDATE SET
    msg_type        = COALESCE(NULLIF(EXCLUDED.msg_type, ''), payment.msg_type),
    flow            = COALESCE(NULLIF(EXCLUDED.flow, ''), payment.flow),
    format          = COALESCE(NULLIF(EXCLUDED.format, ''), payment.format),
    state           = EXCLUDED.state,
    state_rank      = EXCLUDED.state_rank,
    reason          = EXCLUDED.reason,
    error_code      = EXCLUDED.error_code,
    run_id          = COALESCE(NULLIF(EXCLUDED.run_id, ''), payment.run_id),
    t0_network_ns   = COALESCE(EXCLUDED.t0_network_ns, payment.t0_network_ns),
    t1_accepted_ns  = COALESCE(EXCLUDED.t1_accepted_ns, payment.t1_accepted_ns),
    t2_canonical_ns = COALESCE(EXCLUDED.t2_canonical_ns, payment.t2_canonical_ns),
    t3_screen_ns    = COALESCE(EXCLUDED.t3_screen_ns, payment.t3_screen_ns),
    t4_handoff_ns   = COALESCE(EXCLUDED.t4_handoff_ns, payment.t4_handoff_ns),
    t5_ack_ns       = COALESCE(EXCLUDED.t5_ack_ns, payment.t5_ack_ns),
    t6_completed_ns = COALESCE(EXCLUDED.t6_completed_ns, payment.t6_completed_ns),
    updated_ns      = EXCLUDED.updated_ns,
    updated_at      = now()
-- The same rule as hub_model.envelope.supersedes. Once terminal, nothing
-- changes. FAILED is transient (the payment is on the retry ladder), so moving
-- into or out of it goes by time. Otherwise a payment only moves forward: a
-- network ACK can beat the dispatcher's own commit, so COMPLETED legitimately
-- arrives before DISPATCHED and must not be overwritten by it.
WHERE payment.state NOT IN ('COMPLETED', 'REJECTED', 'BLOCKED')
  AND CASE
        WHEN 'FAILED' IN (payment.state, EXCLUDED.state)
          THEN EXCLUDED.updated_ns >= payment.updated_ns
        ELSE EXCLUDED.state_rank >= payment.state_rank
      END;
"""

_AUDIT: Final = """
INSERT INTO payment_audit (uetr, state, service, reason, error_code, emitted_ns)
VALUES (%(uetr)s, %(state)s, %(service)s, %(reason)s, %(error_code)s, %(emitted_ns)s);
"""


class DbSink(Processor):
    """Persists every status event. Produces nothing to Kafka."""

    name = SERVICE_NAME
    stage = "persisted"
    group_id = tp.CG_DB_SINK
    input_topics = (tp.PAY_STATUS,)

    def __init__(self, settings: ServiceSettings, events: Events) -> None:
        super().__init__(settings, events)
        self._conn: Any = None
        self._pending: list[StatusEvent] = []
        self.written = 0
        self.dry_run = not settings.database_url

    def connection(self) -> Any:
        if self._conn is None:
            import psycopg

            with _db_call("connect"):
                self._conn = psycopg.connect(self.settings.database_url, autocommit=False)
                with self._conn.cursor() as cur:
                    cur.execute(SCHEMA)
                self._conn.commit()
            log.info("db sink connected")
        return self._conn

    async def handle(self, record: Record, out: Outbox) -> None:
        event = StatusEvent()
        try:
            event.ParseFromString(record.value)
        except Exception:
            log.warning("undecodable StatusEvent at %s:%s", record.topic, record.offset)
            return
        self._pending.append(event)

    async def on_batch_committed(self, count: int) -> None:
        """Write after the Kafka transaction commits.

        The database is outside the transaction, so the order matters: commit
        Kafka first, then write. A crash in between redelivers the batch, and
        the guarded upsert makes the repeat harmless.
        """
        if not self._pending:
            return
        batch, self._pending = self._pending, []
        if self.dry_run:
            self.written += len(batch)
            return
        try:
            conn = self.connection()
            _set_connections(active=1)
            with _db_call("write_batch"), conn.cursor() as cur:
                for event in batch:
                    params = _params(event)
                    with _db_call("upsert"):
                        cur.execute(_UPSERT, params)
                    with _db_call("audit_insert"):
                        cur.execute(_AUDIT, params)
                with _db_call("commit"):
                    conn.commit()
            self.written += len(batch)
            _set_connections(idle=1)
        except Exception as exc:
            if self._conn is not None:
                try:
                    with _db_call("rollback"):
                        self._conn.rollback()
                    self._conn.close()
                finally:
                    self._conn = None
            _set_connections()
            raise RetryableError(f"database write failed: {exc}", code="DB_WRITE") from exc

    async def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None
        _set_connections()


@contextmanager
def _db_call(operation: str) -> Iterator[None]:
    """Time and count one database call, as the JDBC panels expect."""
    started = time.perf_counter()
    outcome = "error"
    try:
        yield
        outcome = "ok"
    finally:
        metrics.db_calls.labels(SERVICE_NAME, operation, outcome).inc()
        metrics.db_call_latency.labels(SERVICE_NAME, operation).observe(
            time.perf_counter() - started
        )


def _set_connections(*, active: int = 0, idle: int = 0) -> None:
    """The sink holds one connection: idle between batches, active during one."""
    metrics.db_connections.labels(SERVICE_NAME, "active").set(active)
    metrics.db_connections.labels(SERVICE_NAME, "idle").set(idle)


def _params(event: StatusEvent) -> dict[str, Any]:
    timings = event.timings
    return {
        "uetr": event.ref.uetr,
        "msg_type": event.ref.msg_type,
        "flow": event.ref.flow,
        "format": pb.format_name(event.format),
        "state": pb.state_name(event.state),
        "state_rank": state_rank(event.state),
        "reason": event.reason[:500],
        "error_code": event.error_code,
        "service": event.service,
        "run_id": event.run_id,
        "emitted_ns": event.emitted_ns,
        "updated_ns": event.emitted_ns,
        "t0": timings.t0_network_sent_ns or None,
        "t1": timings.t1_edge_accepted_ns or None,
        "t2": timings.t2_canonical_ns or None,
        "t3": timings.t3_screen_reply_ns or None,
        "t4": timings.t4_handoff_ns or None,
        "t5": timings.t5_network_ack_ns or None,
        "t6": timings.t6_completed_ns or None,
    }


def build(settings: ServiceSettings, events: Events) -> DbSink:
    return DbSink(settings, events)
