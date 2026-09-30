"""hub-format — SWIFT MT and ISO 20022 MX on the wire.

Kept separate from :mod:`hub_model` on purpose. ``hub_model`` is the
format-neutral core the whole pipeline shares; this library is the
format-specific cost the design doc isolates in the two parser services, the
dispatcher's outbound build, and the ESS.

* :mod:`hub_format.mt` — the in-house FIN block / tag parser and builder;
* :mod:`hub_format.mx` — ISO 20022 parsing, validation and building;
* :mod:`hub_format.map_mt` / :mod:`hub_format.map_mx` — to and from the
  canonical model;
* :mod:`hub_format.samples` — synthetic messages for tests and ``ess send``.
"""

from __future__ import annotations

from . import map_mt, map_mx, mt, mx
from .mt import MtMessage, MtParseError
from .mx import MxMessage, MxParseError, MxValidationError

__all__ = [
    "MtMessage",
    "MtParseError",
    "MxMessage",
    "MxParseError",
    "MxValidationError",
    "map_mt",
    "map_mx",
    "mt",
    "mx",
]
