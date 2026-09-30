"""MT (FIN) to the canonical payment model, and back.

MT103 and MT202 / MT202 COV map onto the same
:class:`~hub_model.proto.Payment` the MX parser produces, so everything
downstream of ``hub.pay.canonical`` is format-neutral.
"""

from __future__ import annotations

import datetime as dt
import re
from typing import Final

from hub_model import proto as pb
from hub_model.flows import MT103, MT202, MT202COV
from hub_model.ids import format_amount, from_mt_amount, mt_amount
from hub_model.proto import Party, Payment

from .mt import MtMessage, MtParseError, build, split_32a, split_party

# MT charge codes map straight onto ISO 20022 ChrgBr.
_CHARGE_BEARER: Final = {"OUR": "DEBT", "BEN": "CRED", "SHA": "SHAR"}
_CHARGE_BEARER_REVERSE: Final = {v: k for k, v in _CHARGE_BEARER.items()}

_BIC_LINE: Final = re.compile(r"^[A-Z]{6}[A-Z0-9]{2}([A-Z0-9]{3})?$")


class MtMappingError(ValueError):
    """The message parses, but is missing something the Hub needs."""


def to_payment(message: MtMessage) -> Payment:
    """Map a parsed MT103 / MT202 / MT202 COV to the canonical model."""
    msg_type = classify(message)
    sequence_a, sequence_b = message.sequences()
    head = message.sub(sequence_a) if sequence_b else message

    payment = Payment(
        format=pb.MT,
        inbound_network=pb.FIN,
        direction=pb.INBOUND,
        biz_msg_id=head.get("20"),
        end_to_end_id=head.get("21") or head.get("20"),
        sender_bic=message.header.sender_bic,
        receiver_bic=message.header.receiver_bic,
    )
    payment.ref.uetr = message.uetr
    payment.ref.msg_type = msg_type

    value_date, currency, amount = _amount(head)
    payment.interbank_settlement_amount.currency = currency
    payment.interbank_settlement_amount.amount = amount
    payment.interbank_settlement_date = _iso_date(value_date)

    if instructed := head.get("33B"):
        payment.instructed_amount.currency = instructed[:3]
        payment.instructed_amount.amount = format_amount(
            from_mt_amount(instructed[3:]), instructed[:3]
        )
    if rate := head.get("36"):
        payment.exchange_rate = rate.replace(",", ".")
    payment.charge_bearer = _CHARGE_BEARER.get(head.get("71A"), "")
    payment.local_instrument = head.get("23B")
    payment.remittance_info = head.get("70")
    payment.settlement_method = "COVE" if msg_type == MT202COV else "INDA"

    for instruction in head.get_all("72"):
        payment.instruction_for_next_agent.append(instruction)
    for instruction in head.get_all("23E"):
        payment.instruction_for_creditor_agent.append(instruction)

    if msg_type in (MT202, MT202COV):
        # An FI transfer: the parties are institutions.
        payment.debtor.CopyFrom(_party(head, "52", institution=True))
        payment.creditor.CopyFrom(_party(head, "58", institution=True))
        payment.debtor_agent.CopyFrom(_party(head, "53", institution=True))
        payment.creditor_agent.CopyFrom(_party(head, "57", institution=True))
    else:
        payment.debtor.CopyFrom(_party(head, "50", institution=False))
        payment.creditor.CopyFrom(_party(head, "59", institution=False))
        payment.debtor_agent.CopyFrom(_party(head, "52", institution=True))
        payment.creditor_agent.CopyFrom(_party(head, "57", institution=True))

    for tag in ("56",):
        agent = _party(head, tag, institution=True)
        if agent.bic or agent.name:
            payment.intermediary_agent.append(agent)

    if not payment.debtor_agent.bic:
        payment.debtor_agent.bic = message.header.sender_bic
    if not payment.creditor_agent.bic:
        payment.creditor_agent.bic = message.header.counterparty_bic

    if msg_type == MT202COV:
        payment.cover.leg = pb.CoverLink.COVER
        payment.cover.partner_msg_type = MT103
    return payment


def underlying_payment(message: MtMessage) -> Payment | None:
    """The customer transfer in sequence B of an MT202 COV."""
    _, sequence_b = message.sequences()
    if not sequence_b:
        return None
    leg = message.sub(sequence_b)
    payment = Payment(format=pb.MT, inbound_network=pb.FIN, direction=pb.INBOUND)
    payment.ref.msg_type = MT103
    payment.debtor.CopyFrom(_party(leg, "50", institution=False))
    payment.creditor.CopyFrom(_party(leg, "59", institution=False))
    payment.debtor_agent.CopyFrom(_party(leg, "52", institution=True))
    payment.creditor_agent.CopyFrom(_party(leg, "57", institution=True))
    payment.remittance_info = leg.get("70")
    if instructed := leg.get("33B"):
        payment.instructed_amount.currency = instructed[:3]
        payment.instructed_amount.amount = format_amount(
            from_mt_amount(instructed[3:]), instructed[:3]
        )
    return payment


def classify(message: MtMessage) -> str:
    """MT103, MT202 or MT202COV.

    An MT202 with sequence B present is a COV; there is no distinct message
    type on the wire.
    """
    msg_type = message.msg_type
    if msg_type == "103":
        return MT103
    if msg_type == "202":
        _, sequence_b = message.sequences()
        return MT202COV if sequence_b else MT202
    raise MtMappingError(f"MT{msg_type} is not in scope")


# ---------------------------------------------------------------- helpers
def _amount(message: MtMessage) -> tuple[str, str, str]:
    raw = message.get("32A")
    if not raw:
        raise MtMappingError("missing field 32A")
    value_date, currency, amount = split_32a(raw)
    return value_date, currency, format_amount(from_mt_amount(amount), currency)


def _iso_date(yymmdd: str) -> str:
    """MT dates are YYMMDD. The century window follows the SWIFT convention."""
    if len(yymmdd) != 6 or not yymmdd.isdigit():
        return ""
    year = int(yymmdd[:2])
    century = 2000 if year < 80 else 1900
    try:
        return dt.date(century + year, int(yymmdd[2:4]), int(yymmdd[4:6])).isoformat()
    except ValueError as exc:
        raise MtMappingError(f"invalid date in field 32A: {yymmdd!r}") from exc


def _to_mt_date(iso: str) -> str:
    if not iso:
        return dt.date.today().strftime("%y%m%d")
    return dt.date.fromisoformat(iso).strftime("%y%m%d")


def _party(message: MtMessage, tag: str, *, institution: bool) -> Party:
    """Build a party from an MT field, whichever option was used.

    Option A is a BIC, option K/D is name and address, option F is structured
    lines. All three end up in the same canonical shape.
    """
    party = Party()
    value = message.get(tag)
    if not value:
        return party
    option = message.option(tag)
    account, lines = split_party(value)

    if account:
        if _looks_like_iban(account):
            party.account.iban = account
        else:
            party.account.other_id = account

    if option == "A" or (not option and lines and _BIC_LINE.match(lines[0])):
        if lines:
            party.bic = lines[0].strip().upper()
        lines = lines[1:]
    elif institution and lines and _BIC_LINE.match(lines[0]):
        party.bic = lines[0].strip().upper()
        lines = lines[1:]

    if option == "F":
        party.name, lines, party.address.country = _parse_option_f(lines)

    if not party.name and lines:
        party.name = lines[0][:140]
        lines = lines[1:]
    for line in lines[:3]:
        party.address.address_line.append(line[:70])
    # Only option F carries a structured country (line code 2). Options A, D
    # and K are free text, so the country stays unset rather than guessed.
    return party


def _parse_option_f(lines: list[str]) -> tuple[str, list[str], str]:
    """Option F is ``n/content``: 1 is the name, 2 the address, 3 country/town."""
    name = ""
    country = ""
    address: list[str] = []
    for line in lines:
        if len(line) > 1 and line[0].isdigit() and line[1] == "/":
            code, content = line[0], line[2:].strip()
            if code == "1" and not name:
                name = content
            elif code == "3" and not country:
                country, _, town = content.partition("/")
                if town:
                    address.append(town)
            else:
                address.append(content)
        else:
            address.append(line)
    return name, address, country


def _looks_like_iban(value: str) -> bool:
    clean = value.replace(" ", "").upper()
    return len(clean) >= 15 and clean[:2].isalpha() and clean[2:4].isdigit()


# --------------------------------------------------------------- building
def build_message(
    payment: Payment,
    msg_type: str,
    *,
    sender_bic: str,
    receiver_bic: str,
    underlying: Payment | None = None,
) -> str:
    """Build an outbound MT103 / MT202 / MT202 COV from the canonical model."""
    if msg_type not in (MT103, MT202, MT202COV):
        raise MtMappingError(f"cannot build {msg_type}")

    amount = payment.interbank_settlement_amount
    tags: list[tuple[str, str]] = [("20", (payment.biz_msg_id or payment.ref.uetr)[:16])]

    if msg_type == MT103:
        tags.append(("23B", payment.local_instrument or "CRED"))
    else:
        tags.append(("21", (payment.end_to_end_id or payment.ref.uetr)[:16]))

    tags.append(
        (
            "32A",
            f"{_to_mt_date(payment.interbank_settlement_date)}"
            f"{amount.currency}{mt_amount(amount.amount, amount.currency)}",
        )
    )
    # 33B is a customer-transfer field: MT103 sequence A, or MT202 COV
    # sequence B. It has no place in an MT202 sequence A.
    if msg_type == MT103 and payment.instructed_amount.amount:
        instructed = payment.instructed_amount
        tags.append(
            ("33B", f"{instructed.currency}{mt_amount(instructed.amount, instructed.currency)}")
        )

    if msg_type == MT103:
        tags.extend(_party_tags("50", payment.debtor, institution=False))
        tags.extend(_party_tags("52", payment.debtor_agent, institution=True))
        tags.extend(_party_tags("57", payment.creditor_agent, institution=True))
        tags.extend(_party_tags("59", payment.creditor, institution=False))
        if payment.remittance_info:
            tags.append(("70", _wrap(payment.remittance_info, 35, 4)))
        tags.append(("71A", _CHARGE_BEARER_REVERSE.get(payment.charge_bearer, "SHA")))
    else:
        tags.extend(_party_tags("52", payment.debtor, institution=True))
        tags.extend(_party_tags("57", payment.creditor_agent, institution=True))
        tags.extend(_party_tags("58", payment.creditor, institution=True))

    for instruction in payment.instruction_for_next_agent[:1]:
        tags.append(("72", _wrap(instruction, 35, 6)))

    if msg_type == MT202COV:
        if underlying is None:
            raise MtMappingError("MT202COV needs the underlying customer transfer")
        tags.extend(_cover_sequence_b(underlying))

    return build(
        msg_type="103" if msg_type == MT103 else "202",
        sender_bic=sender_bic,
        receiver_bic=receiver_bic,
        tags=tags,
        uetr=payment.ref.uetr,
    )


def _cover_sequence_b(underlying: Payment) -> list[tuple[str, str]]:
    """Sequence B of an MT202 COV: the underlying customer credit transfer."""
    tags: list[tuple[str, str]] = []
    tags.extend(_party_tags("50", underlying.debtor, institution=False))
    tags.extend(_party_tags("52", underlying.debtor_agent, institution=True))
    tags.extend(_party_tags("57", underlying.creditor_agent, institution=True))
    tags.extend(_party_tags("59", underlying.creditor, institution=False))
    if underlying.remittance_info:
        tags.append(("70", _wrap(underlying.remittance_info, 35, 4)))
    if underlying.instructed_amount.amount:
        instructed = underlying.instructed_amount
        tags.append(
            ("33B", f"{instructed.currency}{mt_amount(instructed.amount, instructed.currency)}")
        )
    return tags


def _party_tags(tag: str, party: Party, *, institution: bool) -> list[tuple[str, str]]:
    """Option A when we have a BIC; the name-and-address option otherwise.

    The name-and-address option differs by field: ``:50K:`` for the ordering
    customer, ``:59:`` with no letter for the beneficiary, ``:52D:`` and
    friends for institutions.
    """
    if not (party.name or party.bic or party.account.iban or party.account.other_id):
        return []
    account = party.account.iban or party.account.other_id
    lines: list[str] = []
    if account:
        lines.append(f"/{account}")

    if party.bic:
        lines.append(party.bic)
        return [(tag + "A", "\n".join(lines))]

    lines.append(party.name[:35])
    lines.extend(line[:35] for line in party.address.address_line[:3])
    if institution:
        option = "D"
    elif tag == "59":
        option = ""  # the beneficiary has no option letter in this form
    else:
        option = "K"
    return [(tag + option, "\n".join(lines))]


def _wrap(text: str, width: int, max_lines: int) -> str:
    """Fold text into MT line width, dropping anything past ``max_lines``."""
    words = text.replace("\n", " ").split()
    lines: list[str] = []
    current = ""
    for word in words:
        candidate = f"{current} {word}".strip()
        if len(candidate) <= width:
            current = candidate
            continue
        if current:
            lines.append(current)
        current = word[:width]
        if len(lines) >= max_lines:
            break
    if current and len(lines) < max_lines:
        lines.append(current)
    return "\n".join(lines[:max_lines])


__all__ = [
    "MtMappingError",
    "MtParseError",
    "build_message",
    "classify",
    "to_payment",
    "underlying_payment",
]
