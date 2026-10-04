"""The local state store each stateful processor keeps.

Holds three things (design doc section 7):

* which stages a UETR has already completed — the idempotency check that makes
  ``Screen``, ``SendMt`` and ``SendMx`` safe to repeat after a crash;
* cover legs waiting for their partner;
* payments HELD for an FCC decision.

It is rebuilt from the compacted ``hub.pay.state`` topic on restart, so a
replica that loses its store recovers without replaying the whole pipeline.

The design doc offers RocksDB or Valkey. This implementation is in-memory with
the rebuild path built in, which is correct for a partition-assigned replica and
is what the local stack and the tests use. A Valkey-backed implementation can
replace it behind the same interface without touching a processor.
"""

from __future__ import annotations

import itertools
import threading
from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import Final

from hub_model import proto as pb
from hub_model.envelope import is_forward, merge_stages
from hub_model.proto import PaymentEnvelope, PaymentStateRecord


@dataclass(slots=True)
class Entry:
    uetr: str
    state: int = pb.PAYMENT_STATE_UNSPECIFIED
    stages_done: list[str] = field(default_factory=list)
    envelope: PaymentEnvelope | None = None
    updated_ns: int = 0
    # Cover pairs: legs seen so far, keyed by message type.
    cover_legs: dict[str, PaymentEnvelope] = field(default_factory=dict)
    case_id: str = ""


class StateStore:
    """Thread-safe; a processor may poll Kafka and serve a rebuild at once."""

    def __init__(self, completed_window: int = 100_000) -> None:
        self._lock = threading.RLock()
        self._entries: dict[str, Entry] = {}
        # Terminal payments leave the working set but are remembered, so a
        # repeated ACK or a redelivered record cannot complete a payment twice.
        self._completed: dict[str, int] = {}
        self._completed_window = completed_window

    # ----------------------------------------------------------- lookups
    def get(self, uetr: str) -> Entry | None:
        with self._lock:
            return self._entries.get(uetr)

    def envelope(self, uetr: str) -> PaymentEnvelope | None:
        entry = self.get(uetr)
        return entry.envelope if entry else None

    def already_done(self, uetr: str, stage: str) -> bool:
        """The idempotency check every processor runs before doing its work."""
        entry = self.get(uetr)
        return bool(entry and stage in entry.stages_done)

    def state_of(self, uetr: str) -> int:
        entry = self.get(uetr)
        return entry.state if entry else pb.PAYMENT_STATE_UNSPECIFIED

    def __len__(self) -> int:
        with self._lock:
            return len(self._entries)

    def size_stats(self, sample: int = 1_000) -> tuple[int, int, int]:
        """(working keys, remembered completed keys, approximate bytes).

        Read at every Prometheus scrape, so the byte figure is extrapolated
        from at most ``sample`` entries rather than walking a large store
        under the lock: serialised envelope sizes plus the UETR keys.
        """
        with self._lock:
            working = len(self._entries)
            completed = len(self._completed)
            measured = list(itertools.islice(self._entries.values(), sample))
        if not measured:
            return working, completed, completed * _UETR_BYTES
        sampled = sum(_entry_bytes(entry) for entry in measured)
        approx = sampled * working // len(measured) + completed * _UETR_BYTES
        return working, completed, approx

    def __iter__(self) -> Iterator[Entry]:
        with self._lock:
            return iter(list(self._entries.values()))

    # ----------------------------------------------------------- updates
    def record(
        self,
        env: PaymentEnvelope,
        *,
        stages: tuple[str, ...] = (),
        updated_ns: int = 0,
    ) -> Entry:
        """Remember ``env`` and the stages it has completed.

        A record that would move the payment backwards updates the stage set
        but leaves the state alone, so a redelivery cannot un-complete a
        payment.
        """
        with self._lock:
            entry = self._entries.get(env.ref.uetr)
            if entry is None:
                entry = Entry(uetr=env.ref.uetr)
                self._entries[env.ref.uetr] = entry
            if entry.state == pb.PAYMENT_STATE_UNSPECIFIED or is_forward(entry.state, env.state):
                entry.state = env.state
                entry.envelope = _copy(env)
            elif entry.envelope is None:
                entry.envelope = _copy(env)
            entry.stages_done = merge_stages(entry.stages_done, stages)
            entry.updated_ns = updated_ns or entry.updated_ns
            if env.screening.case_id:
                entry.case_id = env.screening.case_id
            return entry

    def apply_state_record(self, record: PaymentStateRecord) -> None:
        """Rebuild path: fold one ``hub.pay.state`` record into the store."""
        if not record.HasField("envelope"):
            with self._lock:
                entry = self._entries.setdefault(record.ref.uetr, Entry(uetr=record.ref.uetr))
                entry.state = record.state
                entry.stages_done = merge_stages(entry.stages_done, record.stages_done)
                entry.updated_ns = record.updated_ns
            return
        self.record(
            record.envelope,
            stages=tuple(record.stages_done),
            updated_ns=record.updated_ns,
        )

    def complete(self, uetr: str) -> None:
        """Retire a payment that has reached a terminal state.

        The working entry goes, but the UETR is remembered in a bounded
        window: a duplicate network callback must be recognised as a repeat,
        not treated as a payment we have never seen.
        """
        with self._lock:
            self._entries.pop(uetr, None)
            if len(self._completed) >= self._completed_window:
                for stale in list(self._completed)[: self._completed_window // 10]:
                    self._completed.pop(stale, None)
            self._completed[uetr] = 1

    def is_complete(self, uetr: str) -> bool:
        with self._lock:
            return uetr in self._completed

    def forget(self, uetr: str) -> None:
        """Drop a payment without remembering it. For tests and rebuilds."""
        with self._lock:
            self._entries.pop(uetr, None)

    # -------------------------------------------------------- cover pairs
    def add_cover_leg(self, env: PaymentEnvelope) -> dict[str, PaymentEnvelope]:
        """Record one leg and return every leg seen for this UETR so far.

        Both legs share the UETR, so Kafka has already put them on the same
        partition and one replica sees both.
        """
        with self._lock:
            entry = self._entries.get(env.ref.uetr)
            if entry is None:
                entry = Entry(uetr=env.ref.uetr)
                self._entries[env.ref.uetr] = entry
            entry.cover_legs[env.ref.msg_type] = _copy(env)
            return dict(entry.cover_legs)

    def cover_legs(self, uetr: str) -> dict[str, PaymentEnvelope]:
        entry = self.get(uetr)
        return dict(entry.cover_legs) if entry else {}

    def cover_leg(self, uetr: str, msg_type: str) -> PaymentEnvelope | None:
        """One leg of a cover pair.

        Both legs share the UETR, so the message type is what tells them
        apart — the compacted state topic keeps only the newest record per
        key, and that is not necessarily the leg being asked about.
        """
        entry = self.get(uetr)
        if entry is None:
            return None
        found = entry.cover_legs.get(msg_type)
        return _copy(found) if found is not None else None

    def clear_cover_legs(self, uetr: str) -> None:
        with self._lock:
            entry = self._entries.get(uetr)
            if entry is not None:
                entry.cover_legs.clear()

    def pending_cover_count(self) -> int:
        with self._lock:
            return sum(1 for e in self._entries.values() if e.cover_legs)

    # -------------------------------------------------------------- held
    def hold(self, env: PaymentEnvelope, case_id: str) -> None:
        with self._lock:
            entry = self._entries.get(env.ref.uetr)
            if entry is None:
                entry = Entry(uetr=env.ref.uetr)
                self._entries[env.ref.uetr] = entry
            entry.state = pb.HELD
            entry.envelope = _copy(env)
            entry.case_id = case_id

    def held(self, uetr: str) -> PaymentEnvelope | None:
        entry = self.get(uetr)
        if entry is None or entry.state != pb.HELD:
            return None
        return entry.envelope

    def count_in_state(self, state: int, *, updated_before_ns: int = 0) -> int:
        """Payments in ``state``; with ``updated_before_ns``, only those older."""
        with self._lock:
            return sum(
                1
                for e in self._entries.values()
                if e.state == state and (not updated_before_ns or e.updated_ns < updated_before_ns)
            )

    def held_count(self) -> int:
        with self._lock:
            return sum(1 for e in self._entries.values() if e.state == pb.HELD)

    def release(self, uetr: str) -> PaymentEnvelope | None:
        """Take the held envelope out of HELD so processing can resume."""
        with self._lock:
            entry = self._entries.get(uetr)
            if entry is None or entry.envelope is None:
                return None
            env = entry.envelope
            entry.state = pb.SCREENED
            return _copy(env)


_UETR_BYTES: Final = 36


def _entry_bytes(entry: Entry) -> int:
    size = _UETR_BYTES + sum(len(stage) for stage in entry.stages_done)
    if entry.envelope is not None:
        size += entry.envelope.ByteSize()
    return size + sum(leg.ByteSize() for leg in entry.cover_legs.values())


def _copy(env: PaymentEnvelope) -> PaymentEnvelope:
    out = PaymentEnvelope()
    out.CopyFrom(env)
    return out
