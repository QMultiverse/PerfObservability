"""OpenTelemetry Baggage for payment tracking (design doc section 9).

Baggage is set once at the Hub gRPC edge and carried unchanged through every
gRPC call and Kafka record as the W3C ``baggage`` header, next to
``traceparent``.

Rules, enforced here:

* the whole header stays under 512 bytes;
* names, accounts and amounts never go in — only the seven keys below;
* it is stripped on calls to the real FIN, SnF and FCC.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping, MutableMapping
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Final

from opentelemetry import baggage as otel_baggage
from opentelemetry import context as otel_context
from opentelemetry import propagate
from opentelemetry.context import Context

# Baggage keys. Nothing else belongs in baggage.
UETR: Final = "payment.uetr"
FORMAT: Final = "payment.format"
MSG_TYPE: Final = "payment.msg_type"
FLOW: Final = "payment.flow"
BIZ_MSG_ID: Final = "payment.biz_msg_id"
TRACE_LEVEL: Final = "payment.trace"
RUN_ID: Final = "run.id"

PAYMENT_KEYS: Final = (UETR, FORMAT, MSG_TYPE, FLOW, BIZ_MSG_ID, TRACE_LEVEL, RUN_ID)

MAX_BAGGAGE_BYTES: Final = 512

# Headers that carry context between hops.
TRACEPARENT_HEADER: Final = "traceparent"
TRACESTATE_HEADER: Final = "tracestate"
BAGGAGE_HEADER: Final = "baggage"
CONTEXT_HEADERS: Final = (TRACEPARENT_HEADER, TRACESTATE_HEADER, BAGGAGE_HEADER)

# biz_msg_id is an operator lookup key, not an identity: truncate rather than
# drop the whole baggage if a counterparty sends something long.
_BIZ_MSG_ID_MAX: Final = 35


@dataclass(slots=True)
class PaymentBaggage:
    """The tracking identifiers for one payment."""

    uetr: str
    format: str = ""
    msg_type: str = ""
    flow: str = ""
    biz_msg_id: str = ""
    trace_level: str = ""
    run_id: str = ""

    def as_dict(self) -> dict[str, str]:
        """Only non-empty values; an empty key is not worth its bytes."""
        pairs = {
            UETR: self.uetr,
            FORMAT: self.format,
            MSG_TYPE: self.msg_type,
            FLOW: self.flow,
            BIZ_MSG_ID: self.biz_msg_id[:_BIZ_MSG_ID_MAX],
            TRACE_LEVEL: self.trace_level,
            RUN_ID: self.run_id,
        }
        return {k: v for k, v in pairs.items() if v}

    @classmethod
    def from_context(cls, ctx: Context | None = None) -> PaymentBaggage:
        values = get_all(ctx)
        return cls(
            uetr=values.get(UETR, ""),
            format=values.get(FORMAT, ""),
            msg_type=values.get(MSG_TYPE, ""),
            flow=values.get(FLOW, ""),
            biz_msg_id=values.get(BIZ_MSG_ID, ""),
            trace_level=values.get(TRACE_LEVEL, ""),
            run_id=values.get(RUN_ID, ""),
        )


@dataclass(slots=True)
class _Attached:
    """Handle returned by :func:`attach`, so callers can detach explicitly."""

    token: object
    baggage: PaymentBaggage = field(default_factory=lambda: PaymentBaggage(uetr=""))


def build_context(bag: PaymentBaggage, base: Context | None = None) -> Context:
    """Put ``bag`` into a new context, dropping keys that would overflow.

    Keys are added in :data:`PAYMENT_KEYS` order, so if the budget runs out it
    is the least important ones (``biz_msg_id``, ``run.id``) that go first.
    """
    ctx = base if base is not None else otel_context.get_current()
    used = 0
    for key, value in bag.as_dict().items():
        # "key=value" plus the "," separator for every entry after the first.
        cost = len(key) + 1 + len(value) + (1 if used else 0)
        if used + cost > MAX_BAGGAGE_BYTES:
            continue
        ctx = otel_baggage.set_baggage(key, value, context=ctx)
        used += cost
    return ctx


@contextmanager
def payment_context(bag: PaymentBaggage) -> Iterator[PaymentBaggage]:
    """Attach ``bag`` for the duration of the block."""
    token = otel_context.attach(build_context(bag))
    try:
        yield bag
    finally:
        otel_context.detach(token)


def capture() -> Context:
    """Snapshot the current context, to re-enter later.

    Used where work is recorded after the block that caused it has ended — a
    Kafka produce is logged only once its transaction commits, by which point
    the per-record context is long gone.
    """
    return otel_context.get_current()


@contextmanager
def attached(ctx: Context) -> Iterator[None]:
    """Re-enter a context captured by :func:`capture`."""
    token = otel_context.attach(ctx)
    try:
        yield
    finally:
        otel_context.detach(token)


@contextmanager
def restored_context(headers: Mapping[str, str]) -> Iterator[PaymentBaggage]:
    """Restore the context carried in ``headers`` for the duration of the block.

    Used by the Kafka consumer loop and by the gRPC server interceptor: the
    payment's baggage is put back before any processing happens, so every log
    line and span inside the block is searchable by UETR.
    """
    ctx = propagate.extract(dict(headers))
    token = otel_context.attach(ctx)
    try:
        yield PaymentBaggage.from_context()
    finally:
        otel_context.detach(token)


def inject(carrier: MutableMapping[str, str] | None = None) -> dict[str, str]:
    """Return the context headers for the current context.

    Produces ``traceparent`` and ``baggage`` (plus ``tracestate`` when set),
    ready for gRPC metadata or Kafka record headers.
    """
    out: dict[str, str] = dict(carrier) if carrier else {}
    propagate.inject(out)
    return out


def strip(headers: Mapping[str, str]) -> dict[str, str]:
    """Drop the context headers. Used on calls to the *real* FIN, SnF and FCC.

    ``traceparent`` goes too: the UETR is inside baggage, but a trace id is
    still internal routing information we do not hand to a third party.
    """
    lowered = {h.lower() for h in CONTEXT_HEADERS}
    return {k: v for k, v in headers.items() if k.lower() not in lowered}


def get_all(ctx: Context | None = None) -> dict[str, str]:
    """All baggage in ``ctx`` (or the current context) as plain strings."""
    return {str(k): str(v) for k, v in otel_baggage.get_all(context=ctx).items()}


def get(key: str, ctx: Context | None = None) -> str:
    value = otel_baggage.get_baggage(key, context=ctx)
    return "" if value is None else str(value)


def current_uetr() -> str:
    return get(UETR)


def current_trace_level() -> str:
    return get(TRACE_LEVEL)


def baggage_header_size(bag: PaymentBaggage) -> int:
    """Byte length of the header ``bag`` would produce. Used by tests."""
    items = bag.as_dict()
    if not items:
        return 0
    return sum(len(k) + 1 + len(v) for k, v in items.items()) + len(items) - 1
