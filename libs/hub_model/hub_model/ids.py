"""Identifiers: UETR, BIC, IBAN and money.

The UETR is the golden key — the Kafka message key on every topic, the gRPC
metadata value, and the primary search key in Kibana — so it is validated
wherever it enters the Hub rather than trusted.

Amounts are :class:`~decimal.Decimal` throughout. A float amount is a defect,
not a rounding inconvenience.
"""

from __future__ import annotations

import re
import uuid
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from typing import Final

# UUID v4, lower case, as SWIFT requires in MT field 121 and MX UETR.
_UETR_RE: Final = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
)
_BIC_RE: Final = re.compile(r"^[A-Z]{6}[A-Z0-9]{2}([A-Z0-9]{3})?$")
_CURRENCY_RE: Final = re.compile(r"^[A-Z]{3}$")

# Currencies with no minor unit; MT amounts for these carry no decimals.
ZERO_DECIMAL_CURRENCIES: Final = frozenset({"JPY", "KRW", "CLP", "ISK", "VND", "XOF", "XAF"})
THREE_DECIMAL_CURRENCIES: Final = frozenset({"BHD", "IQD", "JOD", "KWD", "LYD", "OMR", "TND"})


class InvalidIdentifier(ValueError):
    """A UETR, BIC, IBAN, currency or amount failed validation."""


def new_uetr() -> str:
    return str(uuid.uuid4())


def is_valid_uetr(value: str) -> bool:
    return bool(_UETR_RE.match(value or ""))


def require_uetr(value: str) -> str:
    if not is_valid_uetr(value):
        raise InvalidIdentifier(f"not a UUID v4 UETR: {value!r}")
    return value


def is_valid_bic(value: str) -> bool:
    return bool(_BIC_RE.match((value or "").strip().upper()))


def require_bic(value: str) -> str:
    bic = (value or "").strip().upper()
    if not is_valid_bic(bic):
        raise InvalidIdentifier(f"not a BIC: {value!r}")
    return bic


def bic8(value: str) -> str:
    """The institution part of a BIC11, for routing lookups."""
    return require_bic(value)[:8]


def is_valid_iban(value: str) -> bool:
    """Checksum-validated with ``schwifty``.

    Generated and sample data must pass this; CLAUDE.md requires it.
    """
    from schwifty import IBAN
    from schwifty.exceptions import SchwiftyException

    try:
        IBAN(value, validate_bban=False)
    except (SchwiftyException, ValueError):
        return False
    return True


def require_iban(value: str) -> str:
    normalised = (value or "").replace(" ", "").upper()
    if not is_valid_iban(normalised):
        raise InvalidIdentifier(f"IBAN failed checksum: {value!r}")
    return normalised


def is_valid_currency(value: str) -> bool:
    return bool(_CURRENCY_RE.match((value or "").strip().upper()))


def minor_units(currency: str) -> int:
    code = (currency or "").strip().upper()
    if code in ZERO_DECIMAL_CURRENCIES:
        return 0
    if code in THREE_DECIMAL_CURRENCIES:
        return 3
    return 2


def parse_amount(value: str, currency: str = "") -> Decimal:
    """Parse a decimal amount string. Never accepts a float.

    Rejects anything negative or with more decimals than the currency allows,
    because both mean the sending system built the message wrongly.
    """
    try:
        amount = Decimal(str(value).strip())
    except (InvalidOperation, ValueError) as exc:
        raise InvalidIdentifier(f"not a decimal amount: {value!r}") from exc
    if amount.is_nan() or amount.is_infinite():
        raise InvalidIdentifier(f"not a finite amount: {value!r}")
    if amount < 0:
        raise InvalidIdentifier(f"negative amount: {value!r}")
    if currency:
        allowed = minor_units(currency)
        if -amount.as_tuple().exponent > allowed:  # type: ignore[operator]
            raise InvalidIdentifier(f"{currency} allows {allowed} decimals, got {value!r}")
    return amount


def format_amount(amount: Decimal | str, currency: str = "") -> str:
    """Canonical string form: fixed decimals for the currency, no exponent."""
    value = amount if isinstance(amount, Decimal) else Decimal(str(amount))
    places = Decimal(1).scaleb(-minor_units(currency)) if currency else Decimal("0.01")
    return str(value.quantize(places, rounding=ROUND_HALF_UP))


def mt_amount(amount: Decimal | str, currency: str = "") -> str:
    """MT amount format: comma as the decimal separator, always present."""
    text = format_amount(amount, currency)
    if "." not in text:
        text += "."
    return text.replace(".", ",")


def from_mt_amount(text: str) -> Decimal:
    """Parse an MT amount, where the separator is a comma."""
    return parse_amount(text.replace(",", "."))
