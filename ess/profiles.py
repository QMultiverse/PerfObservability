"""Behaviour profiles: how the emulated FIN, SnF and FCC respond.

A profile is what turns one image into three modes. Functional runs use fixed,
near-zero latency and no faults; CI adds fault injection; performance draws
latency from a distribution.

Everything here is changed at runtime through ``EssControl.SetProfile`` or
``ess profile set``, so a test can make FIN go dark for two minutes without
restarting anything.
"""

from __future__ import annotations

import math
import random
import threading
import time
from dataclasses import dataclass, field, replace
from typing import Final

from hub_model.proto import BehaviourProfile, LatencyDist, Target

FIXED: Final = "fixed"
UNIFORM: Final = "uniform"
LOGNORMAL: Final = "lognormal"

TARGET_NAMES: Final = {
    int(Target.FIN): "FIN",
    int(Target.SNF): "SNF",
    int(Target.FCC): "FCC",
}


@dataclass(frozen=True, slots=True)
class Latency:
    """A latency model. ``p99`` is only meaningful for a drawn distribution."""

    kind: str = FIXED
    p50_ms: float = 1.0
    p99_ms: float = 1.0

    def sample(self, rng: random.Random) -> float:
        """Seconds to wait before replying."""
        if self.kind == FIXED or self.p99_ms <= self.p50_ms:
            return max(0.0, self.p50_ms) / 1000.0
        if self.kind == UNIFORM:
            return rng.uniform(self.p50_ms, self.p99_ms) / 1000.0
        # Lognormal fitted so the median is p50 and the 99th percentile is p99.
        # z(0.99) = 2.3263.
        mu = math.log(max(self.p50_ms, 1e-6))
        sigma = max(1e-6, (math.log(max(self.p99_ms, 1e-6)) - mu) / 2.3263)
        return max(0.0, rng.lognormvariate(mu, sigma)) / 1000.0

    @classmethod
    def from_proto(cls, dist: LatencyDist, fallback: Latency) -> Latency:
        if not dist.kind and not dist.p50_ms and not dist.p99_ms:
            return fallback
        return cls(
            kind=(dist.kind or FIXED).lower(),
            p50_ms=dist.p50_ms or fallback.p50_ms,
            p99_ms=dist.p99_ms or dist.p50_ms or fallback.p99_ms,
        )


@dataclass(slots=True)
class Profile:
    """One emulated system's behaviour."""

    target: str
    accept_latency: Latency = field(default_factory=Latency)
    ack_latency: Latency = field(default_factory=lambda: Latency(FIXED, 5.0, 5.0))
    nak_rate: float = 0.0
    hit_rate: float = 0.0
    block_rate: float = 0.0
    duplicate_rate: float = 0.0
    release_rate: float = 1.0
    delivery_notifications: bool = False
    outage_until_ns: int = 0

    def outage(self, now_ns: int | None = None) -> bool:
        if self.outage_until_ns == 0:
            return False
        if self.outage_until_ns < 0:  # indefinite, until cleared
            return True
        return (now_ns or time.time_ns()) < self.outage_until_ns

    def describe(self) -> str:
        parts = [
            f"accept={self.accept_latency.kind}:{self.accept_latency.p50_ms:g}ms",
            f"ack={self.ack_latency.kind}:{self.ack_latency.p50_ms:g}ms",
        ]
        for name, value in (
            ("nak", self.nak_rate),
            ("hit", self.hit_rate),
            ("block", self.block_rate),
            ("dup", self.duplicate_rate),
        ):
            if value:
                parts.append(f"{name}={value:g}")
        if self.outage():
            parts.append("OUTAGE")
        return " ".join(parts)


#: Functional mode: deterministic, fast, no faults. Named cases override this
#: per payment, so a developer sees a clean pipeline by default.
FUNCTIONAL_DEFAULTS: Final = {
    "FIN": Profile("FIN", Latency(FIXED, 1.0), Latency(FIXED, 5.0)),
    "SNF": Profile("SNF", Latency(FIXED, 1.0), Latency(FIXED, 5.0), delivery_notifications=True),
    "FCC": Profile("FCC", Latency(FIXED, 2.0), Latency(FIXED, 100.0)),
}

#: Performance mode: latency drawn from a distribution, with the small hit and
#: NAK rates a real day has. The SLO numbers these sit under are placeholders
#: in both design documents.
PERFORMANCE_DEFAULTS: Final = {
    "FIN": Profile(
        "FIN", Latency(LOGNORMAL, 8.0, 60.0), Latency(LOGNORMAL, 40.0, 400.0), nak_rate=0.001
    ),
    "SNF": Profile(
        "SNF",
        Latency(LOGNORMAL, 10.0, 80.0),
        Latency(LOGNORMAL, 50.0, 500.0),
        nak_rate=0.001,
        delivery_notifications=True,
    ),
    "FCC": Profile(
        "FCC",
        Latency(LOGNORMAL, 25.0, 300.0),
        Latency(FIXED, 300_000.0),  # analyst decisions take minutes
        hit_rate=0.02,
        block_rate=0.001,
        release_rate=0.95,
    ),
}


class ProfileStore:
    """Thread-safe profile registry, one entry per target."""

    def __init__(self, mode: str = "functional") -> None:
        self._lock = threading.RLock()
        self.mode = mode
        defaults = PERFORMANCE_DEFAULTS if mode == "performance" else FUNCTIONAL_DEFAULTS
        self._profiles = {name: replace(profile) for name, profile in defaults.items()}

    def get(self, target: str) -> Profile:
        with self._lock:
            return self._profiles[target.upper()]

    def all(self) -> dict[str, Profile]:
        with self._lock:
            return {name: replace(profile) for name, profile in self._profiles.items()}

    def set(self, profile: Profile) -> None:
        with self._lock:
            self._profiles[profile.target.upper()] = profile

    def apply(self, request: BehaviourProfile) -> list[str]:
        """Fold a ``SetProfile`` request into the store. Returns what changed."""
        targets = (
            list(self._profiles)
            if request.target in (int(Target.ALL), int(Target.TARGET_UNSPECIFIED))
            else [TARGET_NAMES[request.target]]
        )
        with self._lock:
            for name in targets:
                current = self._profiles[name]
                self._profiles[name] = Profile(
                    target=name,
                    accept_latency=Latency.from_proto(
                        request.accept_latency, current.accept_latency
                    ),
                    ack_latency=Latency.from_proto(request.ack_latency, current.ack_latency),
                    nak_rate=request.nak_rate,
                    hit_rate=request.hit_rate,
                    block_rate=request.block_rate,
                    duplicate_rate=request.duplicate_rate,
                    release_rate=request.release_rate or current.release_rate,
                    delivery_notifications=request.delivery_notifications
                    or current.delivery_notifications,
                    outage_until_ns=_outage_deadline(request, current),
                )
        return targets

    def reset(self) -> None:
        with self._lock:
            defaults = PERFORMANCE_DEFAULTS if self.mode == "performance" else FUNCTIONAL_DEFAULTS
            self._profiles = {name: replace(profile) for name, profile in defaults.items()}


def _outage_deadline(request: BehaviourProfile, current: Profile) -> int:
    if not request.outage:
        return 0
    if request.outage_for_ms <= 0:
        return -1  # until explicitly cleared
    return time.time_ns() + int(request.outage_for_ms) * 1_000_000


def profile_from_proto(request: BehaviourProfile, base: Profile) -> Profile:
    """Build a single profile from a request, for tests and the CLI."""
    return Profile(
        target=base.target,
        accept_latency=Latency.from_proto(request.accept_latency, base.accept_latency),
        ack_latency=Latency.from_proto(request.ack_latency, base.ack_latency),
        nak_rate=request.nak_rate,
        hit_rate=request.hit_rate,
        block_rate=request.block_rate,
        duplicate_rate=request.duplicate_rate,
        release_rate=request.release_rate or base.release_rate,
        delivery_notifications=request.delivery_notifications or base.delivery_notifications,
        outage_until_ns=_outage_deadline(request, base),
    )
