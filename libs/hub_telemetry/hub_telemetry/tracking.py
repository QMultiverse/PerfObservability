"""Tracking levels and modes (design doc section 9.1).

Logging depth follows the mode. One setting, ``HUB_TRACKING_MODE``, is applied
to the Hub and the ESS; the edge turns it into a *level* and writes that level
into the ``payment.trace`` baggage field, so every service downstream makes the
same choice for a given payment.

Callers must check :func:`should_log` **before** building an event, so ``errors``
and ``none`` cost nothing on a healthy payment.
"""

from __future__ import annotations

import hashlib
import os
import re
from enum import StrEnum
from typing import Final


class Level(StrEnum):
    """How much of a payment's journey is logged."""

    FULL = "full"
    STANDARD = "standard"
    MINIMAL = "minimal"
    ERRORS = "errors"
    NONE = "none"

    @classmethod
    def parse(cls, value: str | None, default: Level = None) -> Level:  # type: ignore[assignment]
        if not value:
            return default if default is not None else Level.FULL
        try:
            return cls(value.strip().lower())
        except ValueError:
            return default if default is not None else Level.FULL


class Mode(StrEnum):
    """Deployment mode. Set once, per environment."""

    FUNCTIONAL = "functional"
    CI = "ci"
    PERFORMANCE = "performance"

    @classmethod
    def parse(cls, value: str | None) -> Mode:
        if not value:
            return cls.FUNCTIONAL
        try:
            return cls(value.strip().lower())
        except ValueError:
            return cls.FUNCTIONAL


# event.action values, one per touchpoint class (design doc section 9).
GRPC_SERVER_RECV: Final = "grpc.server.recv"
GRPC_SERVER_REPLY: Final = "grpc.server.reply"
GRPC_CLIENT_SEND: Final = "grpc.client.send"
GRPC_CLIENT_REPLY: Final = "grpc.client.reply"
KAFKA_PRODUCE: Final = "kafka.produce"
KAFKA_CONSUME: Final = "kafka.consume"
PAYMENT_STATE: Final = "payment.state"
PAYMENT_ERROR: Final = "payment.error"

ALL_ACTIONS: Final = frozenset(
    {
        GRPC_SERVER_RECV,
        GRPC_SERVER_REPLY,
        GRPC_CLIENT_SEND,
        GRPC_CLIENT_REPLY,
        KAFKA_PRODUCE,
        KAFKA_CONSUME,
        PAYMENT_STATE,
        PAYMENT_ERROR,
    }
)

_GRPC_ACTIONS: Final = frozenset(
    {GRPC_SERVER_RECV, GRPC_SERVER_REPLY, GRPC_CLIENT_SEND, GRPC_CLIENT_REPLY}
)

# What each level admits. ``errors`` additionally admits PAYMENT_STATE, but only
# for the failure states — see :func:`should_log`.
_ADMITTED: Final[dict[Level, frozenset[str]]] = {
    Level.FULL: ALL_ACTIONS,
    Level.STANDARD: _GRPC_ACTIONS | {PAYMENT_STATE, PAYMENT_ERROR},
    Level.MINIMAL: frozenset({PAYMENT_STATE, PAYMENT_ERROR}),
    Level.ERRORS: frozenset({PAYMENT_ERROR}),
    Level.NONE: frozenset(),
}

# States that count as a failure, so ``errors`` keeps the state change that
# caused the error alongside the error itself.
FAILURE_STATES: Final = frozenset({"HELD", "BLOCKED", "REJECTED", "FAILED"})

DEFAULT_LEVEL_FOR_MODE: Final[dict[Mode, Level]] = {
    Mode.FUNCTIONAL: Level.FULL,
    Mode.CI: Level.FULL,
    Mode.PERFORMANCE: Level.ERRORS,
}

_SPOT_CHECK_RE = re.compile(r"^(?P<level>\w+):(?P<pct>[\d.]+)%?$")


class TrackingPolicy:
    """Turns a mode into a level for each payment.

    Only the Hub edge needs this: it stamps the result into baggage, and every
    other service reads ``payment.trace`` instead of deciding again.
    """

    def __init__(
        self,
        mode: Mode = Mode.FUNCTIONAL,
        *,
        level_override: Level | None = None,
        spot_check: tuple[Level, float] | None = None,
    ) -> None:
        self.mode = mode
        self.base_level = level_override or DEFAULT_LEVEL_FOR_MODE[mode]
        self.spot_check = spot_check

    @classmethod
    def from_env(cls, env: dict[str, str] | None = None) -> TrackingPolicy:
        src = env if env is not None else dict(os.environ)
        mode = Mode.parse(src.get("HUB_TRACKING_MODE"))
        override = Level.parse(src.get("HUB_TRACKING_LEVEL"), DEFAULT_LEVEL_FOR_MODE[mode])
        explicit = src.get("HUB_TRACKING_LEVEL") is not None
        return cls(
            mode,
            level_override=override if explicit else None,
            spot_check=parse_spot_check(src.get("HUB_TRACKING_SPOT_CHECK")),
        )

    def choose_level(self, uetr: str) -> Level:
        """Pick the level for one payment.

        The choice is a deterministic function of the UETR, so re-running the
        same payment logs the same way and a spot check is reproducible.
        """
        if self.spot_check is not None:
            level, pct = self.spot_check
            if _bucket(uetr) < pct:
                return level
        return self.base_level


def parse_spot_check(value: str | None) -> tuple[Level, float] | None:
    """Parse ``minimal:0.1%`` into ``(Level.MINIMAL, 0.1)``."""
    if not value:
        return None
    match = _SPOT_CHECK_RE.match(value.strip())
    if not match:
        return None
    try:
        pct = float(match.group("pct"))
    except ValueError:
        return None
    return Level.parse(match.group("level")), pct


def _bucket(uetr: str) -> float:
    """Map a UETR onto [0, 100) deterministically."""
    digest = hashlib.blake2b(uetr.encode("utf-8"), digest_size=8).digest()
    return (int.from_bytes(digest, "big") % 1_000_000) / 10_000.0


def should_log(level: Level | str | None, action: str, *, state: str | None = None) -> bool:
    """Whether ``action`` is logged at ``level``.

    ``state`` is the payment state for a ``payment.state`` event; at the
    ``errors`` level only failure states are kept.
    """
    lvl = level if isinstance(level, Level) else Level.parse(level)
    if action in _ADMITTED[lvl]:
        return True
    if lvl is Level.ERRORS and action == PAYMENT_STATE:
        return state is not None and state.upper() in FAILURE_STATES
    return False
