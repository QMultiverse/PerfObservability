"""Helpers for moving a :class:`PaymentEnvelope` from one stage to the next.

Every processor does the same three things — advance the state, emit a status
event, and write the compacted state record — so that lives here rather than
being retyped in nine services.
"""

from __future__ import annotations

import time
from collections.abc import Iterable, Sequence

from . import proto as pb
from .proto import (
    Format,
    MsgRef,
    Payment,
    PaymentEnvelope,
    PaymentState,
    PaymentStateRecord,
    StatusEvent,
    Timings,
)

# Stage names recorded in PaymentStateRecord.stages_done. A processor checks
# these before doing its work, which is what makes the non-transactional calls
# (Screen, SendMt, SendMx) safe to repeat after a crash.
STAGE_PARSED = "parsed"
STAGE_SCREENED = "screened"
STAGE_ROUTED = "routed"
STAGE_SETTLED = "settled"
STAGE_DISPATCHED = "dispatched"
STAGE_ACKED = "acked"

# The order a payment moves through. Used to reject an out-of-order update
# rather than silently overwriting a later state with an earlier one.
STATE_ORDER: dict[int, int] = {
    pb.PAYMENT_STATE_UNSPECIFIED: 0,
    pb.RECEIVED: 1,
    pb.PARSED: 2,
    pb.AWAITING_COVER: 3,
    pb.HELD: 4,
    pb.SCREENED: 5,
    pb.ROUTED: 6,
    pb.SETTLED: 7,
    pb.DISPATCHED: 8,
    pb.COMPLETED: 9,
    # Terminal failures sort above everything; nothing follows them.
    pb.BLOCKED: 10,
    pb.REJECTED: 10,
    pb.FAILED: 10,
}

TERMINAL_STATES = frozenset({pb.COMPLETED, pb.REJECTED, pb.BLOCKED})


def now_ns() -> int:
    """Wall-clock nanoseconds.

    T0-T6 are compared across services and machines, so this must be the wall
    clock, not a monotonic counter.
    """
    return time.time_ns()


def make_ref(uetr: str, msg_type: str, flow: str) -> MsgRef:
    return MsgRef(uetr=uetr, msg_type=msg_type, flow=flow)


def new_envelope(
    ref: MsgRef,
    fmt: Format,
    *,
    payment: Payment | None = None,
    state: PaymentState = pb.RECEIVED,
    timings: Timings | None = None,
    trace_level: str = "",
    run_id: str = "",
) -> PaymentEnvelope:
    env = PaymentEnvelope(
        ref=ref,
        format=fmt,
        state=state,
        trace_level=trace_level,
        run_id=run_id,
    )
    if payment is not None:
        env.payment.CopyFrom(payment)
    if timings is not None:
        env.timings.CopyFrom(timings)
    return env


def advance(env: PaymentEnvelope, state: PaymentState) -> PaymentEnvelope:
    """Copy ``env`` with the state moved on. The input is left untouched.

    Protobuf messages are mutable, and a processor may hold a copy in its state
    store, so every transition returns a new envelope.
    """
    out = PaymentEnvelope()
    out.CopyFrom(env)
    out.state = state
    return out


def is_forward(current: int, proposed: int) -> bool:
    """Whether ``proposed`` is a later state than ``current``.

    Guards the compacted ``hub.pay.state`` topic against a redelivered record
    dragging a payment backwards.
    """
    if current in TERMINAL_STATES:
        return False
    return STATE_ORDER.get(proposed, 0) > STATE_ORDER.get(current, 0)


def state_rank(state: int) -> int:
    """How far along ``state`` is. Higher is later; terminals sort highest.

    The one ordering used everywhere a payment's state is stored, so the
    in-memory view, the compacted state topic and PostgreSQL all agree.
    """
    return STATE_ORDER.get(state, 0)


def status_event(
    env: PaymentEnvelope,
    *,
    service: str,
    reason: str = "",
    error_code: str = "",
    state: PaymentState | None = None,
    emitted_ns: int | None = None,
) -> StatusEvent:
    """Build the record for ``hub.pay.status``.

    This is what the DB sink persists, what the status API serves, and what the
    performance status tracker reads over its read-only ACL.
    """
    event = StatusEvent(
        ref=env.ref,
        format=env.format,
        state=state if state is not None else env.state,
        service=service,
        reason=reason,
        error_code=error_code,
        emitted_ns=emitted_ns if emitted_ns is not None else now_ns(),
        run_id=env.run_id,
    )
    event.timings.CopyFrom(env.timings)
    return event


def state_record(
    env: PaymentEnvelope,
    *,
    stages_done: Iterable[str] = (),
    updated_ns: int | None = None,
) -> PaymentStateRecord:
    """Build the record for the compacted ``hub.pay.state`` topic."""
    record = PaymentStateRecord(
        ref=env.ref,
        format=env.format,
        state=env.state,
        updated_ns=updated_ns if updated_ns is not None else now_ns(),
        stages_done=sorted(set(stages_done)),
    )
    record.envelope.CopyFrom(env)
    return record


def merge_stages(previous: Sequence[str], added: Iterable[str]) -> list[str]:
    return sorted(set(previous) | set(added))


def end_to_end_seconds(timings: Timings) -> float | None:
    """T0 to T6 in seconds, or None while the payment is still in flight."""
    if not timings.t0_network_sent_ns or not timings.t6_completed_ns:
        return None
    return (timings.t6_completed_ns - timings.t0_network_sent_ns) / 1e9


def stage_seconds(start_ns: int, end_ns: int) -> float | None:
    if not start_ns or not end_ns or end_ns < start_ns:
        return None
    return (end_ns - start_ns) / 1e9


def format_label(env: PaymentEnvelope) -> str:
    return pb.format_name(env.format)


def state_label(env: PaymentEnvelope) -> str:
    return pb.state_name(env.state)


def describe(env: PaymentEnvelope) -> str:
    """One-line summary for a log message."""
    return (
        f"{env.ref.uetr} {pb.format_name(env.format)} {env.ref.msg_type} "
        f"{env.ref.flow} {pb.state_name(env.state)}"
    )


__all__ = [
    "STAGE_ACKED",
    "STAGE_DISPATCHED",
    "STAGE_PARSED",
    "STAGE_ROUTED",
    "STAGE_SCREENED",
    "STAGE_SETTLED",
    "STATE_ORDER",
    "TERMINAL_STATES",
    "Format",
    "PaymentState",
    "advance",
    "describe",
    "end_to_end_seconds",
    "format_label",
    "is_forward",
    "make_ref",
    "merge_stages",
    "new_envelope",
    "now_ns",
    "stage_seconds",
    "state_label",
    "state_record",
    "status_event",
]
