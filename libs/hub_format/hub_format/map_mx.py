"""MX (ISO 20022) to the canonical payment model, and back.

pacs.008 and pacs.009 map onto the same :class:`~hub_model.proto.Payment`:
a pacs.009 simply has financial institutions where a pacs.008 has customers,
and a pacs.009 COV carries the underlying customer transfer as well.
"""

from __future__ import annotations

import datetime as dt
from typing import Final

from hub_model import proto as pb
from hub_model.flows import HEAD_001, PACS_008, PACS_009, PACS_009_COV
from hub_model.ids import format_amount, parse_amount
from hub_model.proto import Account, Money, Party, Payment, PostalAddress
from lxml import etree

from .mx import (
    DocumentBuilder,
    MxMessage,
    MxValidationError,
    build_app_hdr,
    find,
    find_all,
    iso_datetime,
    parse_document,
    serialise,
    text_of,
)

# Where the parties live, per message type. pacs.009 has no separate debtor
# customer: the debtor *is* a financial institution.
_CUSTOMER_TYPES: Final = frozenset({PACS_008})


def to_payment(message: MxMessage, *, index: int = 0) -> Payment:
    """Map one transaction of an MX message to the canonical model.

    ``index`` selects the transaction; CBPR+ is one transaction per message,
    but the model does not assume it.
    """
    transactions = message.transactions()
    if not transactions:
        raise MxValidationError(f"{message.msg_type} has no transactions")
    if index >= len(transactions):
        raise MxValidationError(f"{message.msg_type} has no transaction at index {index}")
    tx = transactions[index]

    msg_type = effective_msg_type(message)
    payment = Payment(
        format=pb.MX,
        inbound_network=pb.SNF,
        direction=pb.INBOUND,
        biz_msg_id=message.app_hdr.biz_msg_id or text_of(message.group_header(), "MsgId"),
        instr_id=text_of(find(tx, "PmtId"), "InstrId"),
        end_to_end_id=text_of(find(tx, "PmtId"), "EndToEndId"),
        tx_id=text_of(find(tx, "PmtId"), "TxId"),
        interbank_settlement_date=text_of(tx, "IntrBkSttlmDt"),
        charge_bearer=text_of(tx, "ChrgBr"),
        exchange_rate=text_of(tx, "XchgRate"),
        remittance_info=_remittance(tx),
        purpose_code=text_of(find(tx, "Purp"), "Cd"),
        local_instrument=text_of(find(tx, "PmtTpInf/LclInstrm"), "Cd"),
        service_level=text_of(find(tx, "PmtTpInf/SvcLvl"), "Cd"),
        settlement_method=text_of(find(message.body, "GrpHdr/SttlmInf"), "SttlmMtd"),
        sender_bic=message.app_hdr.from_bic,
        receiver_bic=message.app_hdr.to_bic,
    )
    payment.ref.uetr = text_of(find(tx, "PmtId"), "UETR").lower()
    payment.ref.msg_type = msg_type

    _set_money(payment.interbank_settlement_amount, find(tx, "IntrBkSttlmAmt"))
    _set_money(payment.instructed_amount, find(tx, "InstdAmt"))
    for charge in find_all(tx, "ChrgsInf"):
        money = payment.charges.add()
        _set_money(money, find(charge, "Amt"))

    is_customer = msg_type in _CUSTOMER_TYPES
    payment.debtor.CopyFrom(_party(find(tx, "Dbtr"), find(tx, "DbtrAcct"), customer=is_customer))
    payment.creditor.CopyFrom(_party(find(tx, "Cdtr"), find(tx, "CdtrAcct"), customer=is_customer))
    payment.debtor_agent.CopyFrom(_agent(find(tx, "DbtrAgt")))
    payment.creditor_agent.CopyFrom(_agent(find(tx, "CdtrAgt")))
    payment.instructing_agent.CopyFrom(_agent(find(tx, "InstgAgt")))
    payment.instructed_agent.CopyFrom(_agent(find(tx, "InstdAgt")))
    for name in ("IntrmyAgt1", "IntrmyAgt2", "IntrmyAgt3"):
        node = find(tx, name)
        if node is not None:
            payment.intermediary_agent.append(_agent(node))

    for instruction in find_all(tx, "InstrForNxtAgt"):
        payment.instruction_for_next_agent.append(text_of(instruction, "InstrInf"))
    for instruction in find_all(tx, "InstrForCdtrAgt"):
        payment.instruction_for_creditor_agent.append(text_of(instruction, "InstrInf"))

    if msg_type == PACS_009_COV:
        payment.cover.leg = pb.CoverLink.COVER
        payment.cover.partner_msg_type = PACS_008
    return payment


def effective_msg_type(message: MxMessage) -> str:
    """pacs.009 carrying an underlying customer transfer is a COV."""
    if message.msg_type == PACS_009 and message.is_cover():
        return PACS_009_COV
    return message.msg_type


def underlying_payment(message: MxMessage, *, index: int = 0) -> Payment | None:
    """The customer transfer inside a pacs.009 COV, as a canonical payment."""
    transactions = message.transactions()
    if index >= len(transactions):
        return None
    underlying = find(transactions[index], "UndrlygCstmrCdtTrf")
    if underlying is None:
        return None
    payment = Payment(format=pb.MX, inbound_network=pb.SNF, direction=pb.INBOUND)
    payment.ref.msg_type = PACS_008
    payment.debtor.CopyFrom(
        _party(find(underlying, "Dbtr"), find(underlying, "DbtrAcct"), customer=True)
    )
    payment.creditor.CopyFrom(
        _party(find(underlying, "Cdtr"), find(underlying, "CdtrAcct"), customer=True)
    )
    payment.debtor_agent.CopyFrom(_agent(find(underlying, "DbtrAgt")))
    payment.creditor_agent.CopyFrom(_agent(find(underlying, "CdtrAgt")))
    _set_money(payment.interbank_settlement_amount, find(underlying, "IntrBkSttlmAmt"))
    _set_money(payment.instructed_amount, find(underlying, "InstdAmt"))
    payment.remittance_info = _remittance(underlying)
    return payment


# ---------------------------------------------------------------- helpers
def _set_money(target: Money, node: etree._Element | None) -> None:
    if node is None:
        return
    currency = node.get("Ccy", "")
    raw = (node.text or "").strip()
    if not raw:
        return
    target.currency = currency
    target.amount = format_amount(parse_amount(raw, currency), currency)


def _party(node: etree._Element | None, account: etree._Element | None, *, customer: bool) -> Party:
    party = Party()
    if node is not None:
        # pacs.009 wraps the party in FinInstnId; pacs.008 does not.
        wrapper = find(node, "FinInstnId")
        inner = wrapper if wrapper is not None else node
        party.name = text_of(inner, "Nm")
        party.bic = text_of(inner, "BICFI") or text_of(find(node, "Id/OrgId"), "AnyBIC")
        party.lei = text_of(inner, "LEI") or text_of(find(node, "Id/OrgId"), "LEI")
        party.clearing_system_id = text_of(find(inner, "ClrSysMmbId"), "MmbId")
        party.address.CopyFrom(_address(find(inner, "PstlAdr")))
    if account is not None:
        party.account.CopyFrom(_account(account))
    if customer and not party.name and node is not None:
        party.name = text_of(node, "Nm")
    return party


def _agent(node: etree._Element | None) -> Party:
    return _party(node, None, customer=False)


def _address(node: etree._Element | None) -> PostalAddress:
    address = PostalAddress()
    if node is None:
        return address
    address.country = text_of(node, "Ctry")
    address.town = text_of(node, "TwnNm")
    address.post_code = text_of(node, "PstCd")
    for line in find_all(node, "AdrLine"):
        if line.text:
            address.address_line.append(line.text.strip())
    if not address.address_line:
        street = text_of(node, "StrtNm")
        number = text_of(node, "BldgNb")
        if street:
            address.address_line.append(" ".join(p for p in (number, street) if p))
    return address


def _account(node: etree._Element | None) -> Account:
    account = Account()
    if node is None:
        return account
    identifier = find(node, "Id")
    if identifier is not None:
        account.iban = text_of(identifier, "IBAN")
        if not account.iban:
            account.other_id = text_of(find(identifier, "Othr"), "Id")
    account.currency = text_of(node, "Ccy")
    return account


def _remittance(tx: etree._Element) -> str:
    node = find(tx, "RmtInf")
    if node is None:
        return ""
    lines = [text_of(line) for line in find_all(node, "Ustrd")]
    if lines:
        return "\n".join(line for line in lines if line)
    structured = find(node, "Strd/CdtrRefInf/Ref")
    return text_of(structured) if structured is not None else ""


# --------------------------------------------------------------- building
def build_document(
    payment: Payment, msg_type: str, *, creation_dt: dt.datetime | None = None
) -> bytes:
    """Build a pacs.008 or pacs.009 Document from the canonical model."""
    if msg_type == PACS_009_COV:
        base_type = PACS_009
    elif msg_type in (PACS_008, PACS_009):
        base_type = msg_type
    else:
        raise MxValidationError(f"cannot build {msg_type} from the canonical model")

    moment = creation_dt or dt.datetime.now(dt.UTC)
    builder = DocumentBuilder(base_type)
    body = builder.child(
        builder.root,
        "FIToFICstmrCdtTrf" if base_type == PACS_008 else "FICdtTrf",
    )

    header = builder.child(body, "GrpHdr")
    builder.child(header, "MsgId", payment.biz_msg_id or payment.ref.uetr[:35])
    builder.child(header, "CreDtTm", iso_datetime(moment))
    builder.child(header, "NbOfTxs", "1")
    if payment.interbank_settlement_amount.amount:
        builder.child(
            header,
            "TtlIntrBkSttlmAmt",
            payment.interbank_settlement_amount.amount,
            Ccy=payment.interbank_settlement_amount.currency,
        )
    if payment.interbank_settlement_date:
        builder.child(header, "IntrBkSttlmDt", payment.interbank_settlement_date)
    settlement = builder.child(header, "SttlmInf")
    builder.child(settlement, "SttlmMtd", payment.settlement_method or "INDA")

    tx = builder.child(body, "CdtTrfTxInf")
    payment_id = builder.child(tx, "PmtId")
    if payment.instr_id:
        builder.child(payment_id, "InstrId", payment.instr_id)
    builder.child(payment_id, "EndToEndId", payment.end_to_end_id or payment.ref.uetr)
    if payment.tx_id:
        builder.child(payment_id, "TxId", payment.tx_id)
    builder.child(payment_id, "UETR", payment.ref.uetr)

    if payment.service_level or payment.local_instrument:
        type_info = builder.child(tx, "PmtTpInf")
        if payment.service_level:
            builder.child(builder.child(type_info, "SvcLvl"), "Cd", payment.service_level)
        if payment.local_instrument:
            builder.child(builder.child(type_info, "LclInstrm"), "Cd", payment.local_instrument)

    builder.child(
        tx,
        "IntrBkSttlmAmt",
        payment.interbank_settlement_amount.amount,
        Ccy=payment.interbank_settlement_amount.currency,
    )
    if payment.interbank_settlement_date:
        builder.child(tx, "IntrBkSttlmDt", payment.interbank_settlement_date)
    if payment.instructed_amount.amount:
        builder.child(
            tx, "InstdAmt", payment.instructed_amount.amount, Ccy=payment.instructed_amount.currency
        )
    if payment.charge_bearer:
        builder.child(tx, "ChrgBr", payment.charge_bearer)

    customer = base_type == PACS_008
    _write_agent(builder, tx, "InstgAgt", payment.instructing_agent)
    _write_agent(builder, tx, "InstdAgt", payment.instructed_agent)
    _write_party(builder, tx, "Dbtr", payment.debtor, customer=customer)
    _write_account(builder, tx, "DbtrAcct", payment.debtor)
    _write_agent(builder, tx, "DbtrAgt", payment.debtor_agent)
    for index, agent in enumerate(payment.intermediary_agent[:3], start=1):
        _write_agent(builder, tx, f"IntrmyAgt{index}", agent)
    _write_agent(builder, tx, "CdtrAgt", payment.creditor_agent)
    _write_party(builder, tx, "Cdtr", payment.creditor, customer=customer)
    _write_account(builder, tx, "CdtrAcct", payment.creditor)

    for instruction in payment.instruction_for_next_agent:
        builder.child(builder.child(tx, "InstrForNxtAgt"), "InstrInf", instruction)
    if payment.purpose_code:
        builder.child(builder.child(tx, "Purp"), "Cd", payment.purpose_code)
    if payment.remittance_info:
        remittance = builder.child(tx, "RmtInf")
        for line in payment.remittance_info.split("\n")[:4]:
            builder.child(remittance, "Ustrd", line[:140])

    return builder.to_bytes()


def build_cover_document(
    cover: Payment, underlying: Payment, *, creation_dt: dt.datetime | None = None
) -> bytes:
    """Build a pacs.009 COV: the FI transfer plus its underlying customer leg."""
    raw = build_document(cover, PACS_009, creation_dt=creation_dt)
    message = parse_document(raw)
    builder = DocumentBuilder(PACS_009)
    tx = message.transactions()[0]
    underlying_node = etree.SubElement(tx, f"{{{builder.ns}}}UndrlygCstmrCdtTrf")

    _write_party(builder, underlying_node, "Dbtr", underlying.debtor, customer=True)
    _write_account(builder, underlying_node, "DbtrAcct", underlying.debtor)
    _write_agent(builder, underlying_node, "DbtrAgt", underlying.debtor_agent)
    _write_agent(builder, underlying_node, "CdtrAgt", underlying.creditor_agent)
    _write_party(builder, underlying_node, "Cdtr", underlying.creditor, customer=True)
    _write_account(builder, underlying_node, "CdtrAcct", underlying.creditor)
    if underlying.instructed_amount.amount:
        builder.child(
            underlying_node,
            "InstdAmt",
            underlying.instructed_amount.amount,
            Ccy=underlying.instructed_amount.currency,
        )
    if underlying.remittance_info:
        remittance = builder.child(underlying_node, "RmtInf")
        for line in underlying.remittance_info.split("\n")[:4]:
            builder.child(remittance, "Ustrd", line[:140])

    return serialise(message.document)


def _write_party(
    builder: DocumentBuilder,
    parent: etree._Element,
    tag: str,
    party: Party,
    *,
    customer: bool,
) -> None:
    if not (party.name or party.bic or party.account.iban or party.account.other_id):
        return
    node = builder.child(parent, tag)
    target = node if customer else builder.child(node, "FinInstnId")
    if not customer and party.bic:
        builder.child(target, "BICFI", party.bic)
    if party.name:
        builder.child(target, "Nm", party.name)
    if customer and party.bic:
        builder.child(builder.path(target, "Id/OrgId"), "AnyBIC", party.bic)
    if party.address.country or party.address.address_line:
        address = builder.child(target, "PstlAdr")
        if party.address.town:
            builder.child(address, "TwnNm", party.address.town)
        if party.address.country:
            builder.child(address, "Ctry", party.address.country)
        for line in party.address.address_line[:2]:
            builder.child(address, "AdrLine", line[:70])


def _write_agent(builder: DocumentBuilder, parent: etree._Element, tag: str, agent: Party) -> None:
    if not (agent.bic or agent.name or agent.clearing_system_id):
        return
    node = builder.child(parent, tag)
    inner = builder.child(node, "FinInstnId")
    if agent.bic:
        builder.child(inner, "BICFI", agent.bic)
    if agent.name:
        builder.child(inner, "Nm", agent.name)
    if agent.clearing_system_id:
        builder.child(builder.child(inner, "ClrSysMmbId"), "MmbId", agent.clearing_system_id)


def _write_account(
    builder: DocumentBuilder, parent: etree._Element, tag: str, party: Party
) -> None:
    if not (party.account.iban or party.account.other_id):
        return
    node = builder.child(parent, tag)
    identifier = builder.child(node, "Id")
    if party.account.iban:
        builder.child(identifier, "IBAN", party.account.iban)
    else:
        builder.child(builder.child(identifier, "Othr"), "Id", party.account.other_id)
    if party.account.currency:
        builder.child(node, "Ccy", party.account.currency)


def build_header_for(
    payment: Payment,
    msg_type: str,
    *,
    from_bic: str,
    to_bic: str,
    creation_dt: dt.datetime | None = None,
) -> bytes:
    """The matching AppHdr for a Document built above."""
    base = PACS_009 if msg_type == PACS_009_COV else msg_type
    return build_app_hdr(
        from_bic=from_bic,
        to_bic=to_bic,
        biz_msg_id=payment.biz_msg_id or payment.ref.uetr[:35],
        msg_def_id=base,
        creation_dt=iso_datetime(creation_dt or dt.datetime.now(dt.UTC)),
    )


__all__ = [
    "HEAD_001",
    "build_cover_document",
    "build_document",
    "build_header_for",
    "effective_msg_type",
    "to_payment",
    "underlying_payment",
]
