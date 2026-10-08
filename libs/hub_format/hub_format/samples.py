"""Synthetic message generation.

Everything here is invented. Names come from a fixed fictional list, BICs use
the ``XXXX`` institution codes reserved for testing, and every IBAN is built so
it passes the mod-97 checksum ``schwifty`` enforces — CLAUDE.md requires both.

Used by ``ess send``, the scenario engine, the sample files under ``samples/``
and most of the tests.
"""

from __future__ import annotations

import datetime as dt
import random
import uuid
from dataclasses import dataclass
from typing import Final

from hub_model import proto as pb
from hub_model.flows import (
    MT103,
    MT202,
    MT202COV,
    PACS_008,
    PACS_009,
    PACS_009_COV,
    normalise_msg_type,
)
from hub_model.ids import format_amount, new_uetr
from hub_model.proto import Party, Payment

from . import map_mt, map_mx

# Test BICs. TESTxx2L-style codes are not issued to real institutions.
BANKS: Final = (
    ("TESTGB2LXXX", "GB", "TEST BANK LONDON", "LONDON"),
    ("TESTDEFFXXX", "DE", "TEST BANK FRANKFURT", "FRANKFURT"),
    ("TESTFRPPXXX", "FR", "TEST BANK PARIS", "PARIS"),
    ("TESTUS33XXX", "US", "TEST BANK NEW YORK", "NEW YORK"),
    ("TESTNL2AXXX", "NL", "TEST BANK AMSTERDAM", "AMSTERDAM"),
    ("TESTCHZZXXX", "CH", "TEST BANK ZURICH", "ZURICH"),
)

# name, street, town, country — the country is the customer's own, not their
# bank's, so a sample address is internally consistent.
CUSTOMERS: Final = (
    ("NORTHWIND TRADING LTD", "12 HARBOUR WAY", "LONDON", "GB"),
    ("ACME MANUFACTURING GMBH", "45 INDUSTRIESTRASSE", "FRANKFURT", "DE"),
    ("BLUEFIN LOGISTICS SA", "8 RUE DU COMMERCE", "PARIS", "FR"),
    ("CEDARWOOD SUPPLIES INC", "200 MARKET STREET", "NEW YORK", "US"),
    ("MERIDIAN EXPORTS BV", "31 HAVENKADE", "AMSTERDAM", "NL"),
)

# Party names the ESS compliance emulator treats as a sanctions hit. Entirely
# invented; they exist so a test can ask for a hit deterministically.
SANCTIONS_NAMES: Final = ("REDLIST HOLDINGS SA", "BLOCKED VENTURES LLC")

CURRENCIES: Final = ("EUR", "GBP", "USD", "CHF")

# Per-country account-code length and a test bank code, for IBAN generation.
# ``schwifty`` knows each country's BBAN structure, so we let it build the
# IBAN rather than guessing at national formats; US has no IBAN scheme.
_IBAN_SPEC: Final[dict[str, tuple[str, int]]] = {
    "GB": ("TEST", 8),
    "DE": ("37040044", 10),
    "FR": ("20041", 11),
    "NL": ("TEST", 10),
    "CH": ("00230", 12),
}


def has_iban(country: str) -> bool:
    """Whether this country is in the IBAN scheme at all."""
    return country in _IBAN_SPEC


def make_account(country: str, rng: random.Random) -> str:
    """A synthetic account number for a country with no IBAN scheme."""
    return "".join(str(rng.randint(0, 9)) for _ in range(10))


def make_iban(country: str, rng: random.Random) -> str:
    """A synthetic IBAN that passes the checksum ``schwifty`` enforces.

    Countries with no IBAN scheme (US) fall back to a plain account number —
    the caller must put that in ``other_id``, not ``iban``.
    """
    spec = _IBAN_SPEC.get(country)
    if spec is None:
        return make_account(country, rng)
    from schwifty import IBAN

    bank_code, digits = spec
    account = "".join(str(rng.randint(0, 9)) for _ in range(digits))
    return str(IBAN.generate(country, bank_code=bank_code, account_code=account).compact)


@dataclass(slots=True)
class SampleOptions:
    """Knobs a caller can set; everything else is drawn from ``seed``."""

    uetr: str = ""
    currency: str = ""
    amount: str = ""
    sender_bic: str = ""
    receiver_bic: str = ""
    debtor_name: str = ""
    creditor_name: str = ""
    sanctions_hit: bool = False
    value_date: str = ""
    seed: int | None = None
    biz_msg_id: str = ""
    # Pin the creation timestamp when the output must be byte-stable, as the
    # files under samples/ are.
    creation_dt: dt.datetime | None = None


def _rng(options: SampleOptions) -> random.Random:
    return random.Random(options.seed) if options.seed is not None else random.Random()


def seeded_uetr(rng: random.Random) -> str:
    """A valid UUID v4 drawn from ``rng``.

    ``uuid.uuid4()`` uses the OS entropy source, so a seeded sample would still
    get a different UETR every run. Drawing the bytes from the seeded generator
    and setting the version and variant nibbles by hand keeps a sample file
    byte-identical between runs.
    """
    value = uuid.UUID(int=rng.getrandbits(128), version=4)
    return str(value)


def make_payment(msg_type: str, options: SampleOptions | None = None) -> Payment:
    """Build a canonical payment, from which any format can be rendered."""
    opts = options or SampleOptions()
    rng = _rng(opts)
    canonical = normalise_msg_type(msg_type)

    sender = next((b for b in BANKS if b[0] == opts.sender_bic), None) or rng.choice(BANKS)
    receiver = next((b for b in BANKS if b[0] == opts.receiver_bic), None) or rng.choice(
        [b for b in BANKS if b[0] != sender[0]]
    )
    debtor_profile = rng.choice(CUSTOMERS)
    creditor_profile = rng.choice([c for c in CUSTOMERS if c[0] != debtor_profile[0]])

    currency = opts.currency or rng.choice(CURRENCIES)
    amount = opts.amount or format_amount(f"{rng.randint(100, 9_999_999) / 100:.2f}", currency)
    value_date = opts.value_date or dt.date.today().isoformat()
    uetr = opts.uetr or (seeded_uetr(rng) if opts.seed is not None else new_uetr())

    payment = Payment(
        format=pb.MX if canonical.startswith("pacs") else pb.MT,
        inbound_network=pb.SNF if canonical.startswith("pacs") else pb.FIN,
        direction=pb.INBOUND,
        biz_msg_id=opts.biz_msg_id or f"BIZ{rng.randint(10**9, 10**10 - 1)}",
        end_to_end_id=f"E2E{rng.randint(10**9, 10**10 - 1)}",
        instr_id=f"INS{rng.randint(10**9, 10**10 - 1)}",
        interbank_settlement_date=value_date,
        charge_bearer="SHAR",
        remittance_info=f"INVOICE {rng.randint(10000, 99999)}",
        purpose_code="GDDS",
        service_level="G001",
        settlement_method="COVE" if canonical in (MT202COV, PACS_009_COV) else "INDA",
        sender_bic=sender[0],
        receiver_bic=receiver[0],
    )
    payment.ref.uetr = uetr
    payment.ref.msg_type = canonical
    payment.interbank_settlement_amount.currency = currency
    payment.interbank_settlement_amount.amount = amount
    payment.instructed_amount.currency = currency
    payment.instructed_amount.amount = amount

    institution_leg = canonical in (MT202, MT202COV, PACS_009, PACS_009_COV)
    debtor_name = opts.debtor_name or (
        SANCTIONS_NAMES[0] if opts.sanctions_hit else debtor_profile[0]
    )
    creditor_name = opts.creditor_name or creditor_profile[0]

    if institution_leg:
        payment.debtor.CopyFrom(_bank_party(sender))
        payment.creditor.CopyFrom(_bank_party(receiver))
    else:
        payment.debtor.CopyFrom(_customer(debtor_name, debtor_profile, rng))
        payment.creditor.CopyFrom(_customer(creditor_name, creditor_profile, rng))

    payment.debtor_agent.CopyFrom(_bank_party(sender))
    payment.creditor_agent.CopyFrom(_bank_party(receiver))
    payment.instructing_agent.CopyFrom(_bank_party(sender))
    payment.instructed_agent.CopyFrom(_bank_party(receiver))

    if canonical in (MT202COV, PACS_009_COV):
        payment.cover.leg = pb.CoverLink.COVER
        payment.cover.partner_msg_type = MT103 if canonical == MT202COV else PACS_008
    return payment


def _bank_party(bank: tuple[str, str, str, str]) -> Party:
    party = Party(name=bank[2], bic=bank[0])
    party.address.country = bank[1]
    party.address.town = bank[3]
    return party


def _customer(name: str, profile: tuple[str, str, str, str], rng: random.Random) -> Party:
    country = profile[3]
    party = Party(name=name)
    if has_iban(country):
        party.account.iban = make_iban(country, rng)
    else:
        party.account.other_id = make_account(country, rng)
    party.address.country = country
    party.address.town = profile[2]
    party.address.address_line.append(profile[1])
    return party


# ----------------------------------------------------------------- render
def make_mx(msg_type: str, options: SampleOptions | None = None) -> tuple[bytes, bytes, Payment]:
    """Return ``(app_hdr, document, payment)`` for an MX sample."""
    opts = options or SampleOptions()
    canonical = normalise_msg_type(msg_type)
    payment = make_payment(canonical, opts)

    if canonical == PACS_009_COV:
        underlying = make_payment(
            PACS_008,
            SampleOptions(
                uetr=payment.ref.uetr,
                currency=payment.interbank_settlement_amount.currency,
                amount=payment.interbank_settlement_amount.amount,
                sender_bic=payment.sender_bic,
                receiver_bic=payment.receiver_bic,
                sanctions_hit=opts.sanctions_hit,
                seed=opts.seed,
                value_date=opts.value_date,
            ),
        )
        document = map_mx.build_cover_document(payment, underlying, creation_dt=opts.creation_dt)
    else:
        document = map_mx.build_document(payment, canonical, creation_dt=opts.creation_dt)

    app_hdr = map_mx.build_header_for(
        payment,
        canonical,
        from_bic=payment.sender_bic,
        to_bic=payment.receiver_bic,
        creation_dt=opts.creation_dt,
    )
    return app_hdr, document, payment


def make_mt(msg_type: str, options: SampleOptions | None = None) -> tuple[str, Payment]:
    """Return ``(fin_message, payment)`` for an MT sample."""
    opts = options or SampleOptions()
    canonical = normalise_msg_type(msg_type)
    payment = make_payment(canonical, opts)

    underlying: Payment | None = None
    if canonical == MT202COV:
        underlying = make_payment(
            MT103,
            SampleOptions(
                uetr=payment.ref.uetr,
                currency=payment.interbank_settlement_amount.currency,
                amount=payment.interbank_settlement_amount.amount,
                sender_bic=payment.sender_bic,
                receiver_bic=payment.receiver_bic,
                sanctions_hit=opts.sanctions_hit,
                seed=opts.seed,
            ),
        )

    text = map_mt.build_message(
        payment,
        canonical,
        sender_bic=payment.sender_bic,
        receiver_bic=payment.receiver_bic,
        underlying=underlying,
    )
    return text, payment


def make_cover_pair(
    msg_types: tuple[str, str], options: SampleOptions | None = None
) -> list[tuple[str, bytes, bytes, Payment]]:
    """Both legs of a cover pair, sharing one UETR.

    Returns ``(msg_type, app_hdr_or_empty, payload, payment)`` per leg. The
    shared UETR is what puts both legs on the same Kafka partition.
    """
    opts = options or SampleOptions()
    uetr = opts.uetr or new_uetr()
    legs: list[tuple[str, bytes, bytes, Payment]] = []
    for msg_type in msg_types:
        leg_opts = SampleOptions(
            uetr=uetr,
            currency=opts.currency,
            amount=opts.amount,
            sender_bic=opts.sender_bic,
            receiver_bic=opts.receiver_bic,
            sanctions_hit=opts.sanctions_hit,
            seed=opts.seed,
            value_date=opts.value_date,
            creation_dt=opts.creation_dt,
        )
        canonical = normalise_msg_type(msg_type)
        if canonical.startswith("pacs"):
            app_hdr, document, payment = make_mx(canonical, leg_opts)
            legs.append((canonical, app_hdr, document, payment))
        else:
            text, payment = make_mt(canonical, leg_opts)
            legs.append((canonical, b"", text.encode("utf-8"), payment))
    return legs
