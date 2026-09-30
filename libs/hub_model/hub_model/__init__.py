"""hub-model — the canonical payment model and the names around it.

Drafted before the parsers, as CLAUDE.md requires: MT and MX are parsed into
:class:`~hub_model.proto.Payment` by separate services, and every stage
downstream of ``hub.pay.canonical`` works on this model alone.

Submodules:

* :mod:`hub_model.proto` — one import site for the generated protobuf types;
* :mod:`hub_model.topics` — topic names, consumer groups, the retry ladder;
* :mod:`hub_model.flows` — flow IDs and message types;
* :mod:`hub_model.ids` — UETR, BIC, IBAN and money;
* :mod:`hub_model.envelope` — stage transitions, status and state records.
"""

from __future__ import annotations

from . import envelope, flows, ids, proto, topics
from .envelope import advance, new_envelope, now_ns, state_record, status_event
from .ids import format_amount, is_valid_uetr, new_uetr, parse_amount, require_uetr
from .proto import MsgRef, Payment, PaymentEnvelope, StatusEvent

__all__ = [
    "MsgRef",
    "Payment",
    "PaymentEnvelope",
    "StatusEvent",
    "advance",
    "envelope",
    "flows",
    "format_amount",
    "ids",
    "is_valid_uetr",
    "new_envelope",
    "new_uetr",
    "now_ns",
    "parse_amount",
    "proto",
    "require_uetr",
    "state_record",
    "status_event",
    "topics",
]
