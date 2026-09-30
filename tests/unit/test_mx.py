"""ISO 20022 parsing, validation, mapping and building."""

from __future__ import annotations

import pytest
from hub_format import map_mx
from hub_format import mx as mx_format
from hub_format.samples import SampleOptions, make_mx
from hub_model.flows import PACS_008, PACS_009, PACS_009_COV
from hub_model.ids import is_valid_iban

PACS008 = """<?xml version="1.0" encoding="UTF-8"?>
<Document xmlns="urn:iso:std:iso:20022:tech:xsd:pacs.008.001.08">
  <FIToFICstmrCdtTrf>
    <GrpHdr>
      <MsgId>BIZ0000000001</MsgId>
      <CreDtTm>2026-09-28T10:15:02Z</CreDtTm>
      <NbOfTxs>1</NbOfTxs>
      <SttlmInf><SttlmMtd>INDA</SttlmMtd></SttlmInf>
    </GrpHdr>
    <CdtTrfTxInf>
      <PmtId>
        <InstrId>INS0000000001</InstrId>
        <EndToEndId>E2E0000000001</EndToEndId>
        <UETR>eb6305c9-1f1d-4b3a-8a0f-2d3c4e5f6a7b</UETR>
      </PmtId>
      <IntrBkSttlmAmt Ccy="EUR">1234.56</IntrBkSttlmAmt>
      <IntrBkSttlmDt>2026-09-28</IntrBkSttlmDt>
      <InstdAmt Ccy="EUR">1234.56</InstdAmt>
      <ChrgBr>SHAR</ChrgBr>
      <Dbtr>
        <Nm>NORTHWIND TRADING LTD</Nm>
        <PstlAdr><TwnNm>LONDON</TwnNm><Ctry>GB</Ctry></PstlAdr>
      </Dbtr>
      <DbtrAcct><Id><IBAN>GB33BUKB20201555555555</IBAN></Id></DbtrAcct>
      <DbtrAgt><FinInstnId><BICFI>TESTGB2LXXX</BICFI></FinInstnId></DbtrAgt>
      <CdtrAgt><FinInstnId><BICFI>TESTDEFFXXX</BICFI></FinInstnId></CdtrAgt>
      <Cdtr>
        <Nm>ACME MANUFACTURING GMBH</Nm>
        <PstlAdr><TwnNm>FRANKFURT</TwnNm><Ctry>DE</Ctry></PstlAdr>
      </Cdtr>
      <CdtrAcct><Id><IBAN>DE89370400440532013000</IBAN></Id></CdtrAcct>
      <RmtInf><Ustrd>INVOICE 12345</Ustrd></RmtInf>
    </CdtTrfTxInf>
  </FIToFICstmrCdtTrf>
</Document>"""

APPHDR = """<?xml version="1.0" encoding="UTF-8"?>
<AppHdr xmlns="urn:iso:std:iso:20022:tech:xsd:head.001.001.02">
  <Fr><FIId><FinInstnId><BICFI>TESTGB2LXXX</BICFI></FinInstnId></FIId></Fr>
  <To><FIId><FinInstnId><BICFI>TESTDEFFXXX</BICFI></FinInstnId></FIId></To>
  <BizMsgIdr>BIZ0000000001</BizMsgIdr>
  <MsgDefIdr>pacs.008.001.08</MsgDefIdr>
  <BizSvc>swift.cbprplus.02</BizSvc>
  <CreDt>2026-09-28T10:15:02Z</CreDt>
</AppHdr>"""


def test_parse_identifies_the_message_type_from_the_namespace() -> None:
    message = mx_format.parse_document(PACS008, APPHDR)
    assert message.msg_type == PACS_008
    assert message.app_hdr.from_bic == "TESTGB2LXXX"
    assert message.app_hdr.to_bic == "TESTDEFFXXX"
    assert message.app_hdr.biz_msg_id == "BIZ0000000001"
    assert len(message.transactions()) == 1
    assert not message.is_cover()


def test_peek_reads_identifiers_without_building_a_tree() -> None:
    """The edge does light checks only; it must not parse a multi-kB document."""
    assert mx_format.peek_uetr(PACS008) == "eb6305c9-1f1d-4b3a-8a0f-2d3c4e5f6a7b"
    assert mx_format.peek_msg_type(PACS008) == PACS_008
    assert mx_format.peek_biz_msg_id(APPHDR) == "BIZ0000000001"


@pytest.mark.parametrize(
    "bad",
    ["", "   ", "<Document", "<Document><unclosed></Document>"],
)
def test_malformed_xml_is_rejected(bad: str) -> None:
    with pytest.raises(mx_format.MxParseError):
        mx_format.parse_document(bad)


def test_an_unknown_namespace_is_rejected() -> None:
    with pytest.raises(mx_format.MxParseError, match="namespace"):
        mx_format.parse_document('<Document xmlns="urn:example:nope"/>')


def test_a_non_document_root_is_rejected() -> None:
    with pytest.raises(mx_format.MxParseError, match="expected Document"):
        mx_format.parse_document('<NotADocument xmlns="urn:example"/>')


def test_external_entities_are_not_resolved() -> None:
    """An inbound payment is untrusted input: no XXE, no network fetches."""
    attack = (
        '<?xml version="1.0"?>'
        '<!DOCTYPE d [<!ENTITY xxe SYSTEM "file:///etc/passwd">]>'
        '<Document xmlns="urn:iso:std:iso:20022:tech:xsd:pacs.008.001.08">'
        "<FIToFICstmrCdtTrf><GrpHdr><MsgId>&xxe;</MsgId></GrpHdr></FIToFICstmrCdtTrf></Document>"
    )
    message = mx_format.parse_document(attack)
    assert "root:" not in mx_format.text_of(message.group_header(), "MsgId")


# ------------------------------------------------------------ validation
def test_a_complete_pacs008_validates() -> None:
    mx_format.validate_structure(mx_format.parse_document(PACS008, APPHDR))


def test_a_missing_required_element_is_reported() -> None:
    broken = PACS008.replace("<UETR>eb6305c9-1f1d-4b3a-8a0f-2d3c4e5f6a7b</UETR>", "")
    with pytest.raises(mx_format.MxValidationError) as caught:
        mx_format.validate_structure(mx_format.parse_document(broken))
    assert any("UETR" in error for error in caught.value.errors)


def test_a_negative_amount_is_rejected() -> None:
    broken = PACS008.replace(">1234.56</IntrBkSttlmAmt>", ">-1.00</IntrBkSttlmAmt>")
    with pytest.raises(mx_format.MxValidationError) as caught:
        mx_format.validate_structure(mx_format.parse_document(broken))
    assert any("positive" in error for error in caught.value.errors)


def test_a_bad_currency_is_rejected() -> None:
    broken = PACS008.replace(
        'Ccy="EUR">1234.56</IntrBkSttlmAmt>', 'Ccy="EURO">1234.56</IntrBkSttlmAmt>'
    )
    with pytest.raises(mx_format.MxValidationError) as caught:
        mx_format.validate_structure(mx_format.parse_document(broken))
    assert any("currency" in error for error in caught.value.errors)


def test_the_transaction_count_must_match() -> None:
    broken = PACS008.replace("<NbOfTxs>1</NbOfTxs>", "<NbOfTxs>7</NbOfTxs>")
    with pytest.raises(mx_format.MxValidationError) as caught:
        mx_format.validate_structure(mx_format.parse_document(broken))
    assert any("NbOfTxs" in error for error in caught.value.errors)


def test_an_empty_document_is_a_validation_error_not_a_surprise() -> None:
    """It must be classified, so the record is dead-lettered rather than retried."""
    with pytest.raises(mx_format.MxValidationError):
        mx_format.validate_structure(
            mx_format.parse_document(
                '<Document xmlns="urn:iso:std:iso:20022:tech:xsd:pacs.008.001.08"/>'
            )
        )


def test_xsd_validation_is_off_without_schemas() -> None:
    """CBPR+ schemas are licensed and not shipped; the validator no-ops."""
    validator = mx_format.XsdValidator(schema_dir=None)
    assert not validator.available
    validator.validate(mx_format.parse_document(PACS008))  # does nothing, raises nothing


# --------------------------------------------------------------- mapping
def test_pacs008_maps_to_the_canonical_model() -> None:
    payment = map_mx.to_payment(mx_format.parse_document(PACS008, APPHDR))
    assert payment.ref.uetr == "eb6305c9-1f1d-4b3a-8a0f-2d3c4e5f6a7b"
    assert payment.ref.msg_type == PACS_008
    assert payment.biz_msg_id == "BIZ0000000001"
    assert payment.end_to_end_id == "E2E0000000001"
    assert payment.interbank_settlement_amount.currency == "EUR"
    assert payment.interbank_settlement_amount.amount == "1234.56"
    assert payment.interbank_settlement_date == "2026-09-28"
    assert payment.charge_bearer == "SHAR"
    assert payment.settlement_method == "INDA"
    assert payment.debtor.name == "NORTHWIND TRADING LTD"
    assert payment.debtor.account.iban == "GB33BUKB20201555555555"
    assert payment.debtor.address.country == "GB"
    assert payment.creditor.name == "ACME MANUFACTURING GMBH"
    assert payment.debtor_agent.bic == "TESTGB2LXXX"
    assert payment.creditor_agent.bic == "TESTDEFFXXX"
    assert payment.remittance_info == "INVOICE 12345"


def test_pacs009_parties_are_institutions() -> None:
    _, document, _ = make_mx(PACS_009, SampleOptions(seed=5))
    payment = map_mx.to_payment(mx_format.parse_document(document))
    assert payment.debtor.bic, "a pacs.009 debtor is a financial institution"
    assert payment.creditor.bic


def test_a_pacs009_with_an_underlying_transfer_is_a_cov() -> None:
    _, document, _ = make_mx(PACS_009_COV, SampleOptions(seed=5))
    message = mx_format.parse_document(document)
    assert message.msg_type == PACS_009, "on the wire it is still a pacs.009"
    assert message.is_cover()
    assert map_mx.effective_msg_type(message) == PACS_009_COV

    underlying = map_mx.underlying_payment(message)
    assert underlying is not None
    assert underlying.ref.msg_type == PACS_008
    assert underlying.debtor.name
    assert underlying.creditor.name


# -------------------------------------------------------------- building
@pytest.mark.parametrize("msg_type", [PACS_008, PACS_009, PACS_009_COV])
def test_build_then_parse_preserves_the_payment(msg_type: str) -> None:
    header, document, original = make_mx(msg_type, SampleOptions(seed=42))
    message = mx_format.parse_document(document, header)
    mx_format.validate_structure(message)
    parsed = map_mx.to_payment(message)

    assert parsed.ref.uetr == original.ref.uetr
    assert parsed.interbank_settlement_amount == original.interbank_settlement_amount
    assert parsed.interbank_settlement_date == original.interbank_settlement_date
    assert parsed.end_to_end_id == original.end_to_end_id
    assert parsed.remittance_info == original.remittance_info


def test_a_built_document_declares_its_namespace() -> None:
    _, document, _ = make_mx(PACS_008, SampleOptions(seed=1))
    assert b"urn:iso:std:iso:20022:tech:xsd:pacs.008.001.08" in document
    assert document.startswith(b"<?xml")


def test_a_built_app_hdr_names_both_parties() -> None:
    header, _, payment = make_mx(PACS_008, SampleOptions(seed=1))
    parsed = mx_format.parse_app_hdr(header)
    assert parsed.from_bic == payment.sender_bic
    assert parsed.to_bic == payment.receiver_bic
    assert parsed.msg_def_id == PACS_008


def test_generated_ibans_pass_the_checksum() -> None:
    for seed in range(20):
        _, _, payment = make_mx(PACS_008, SampleOptions(seed=seed))
        for party in (payment.debtor, payment.creditor):
            if party.account.iban:
                assert is_valid_iban(party.account.iban), party.account.iban


def test_find_is_namespace_agnostic() -> None:
    """So a counterparty's minor version does not break the parser."""
    message = mx_format.parse_document(PACS008)
    tx = message.transactions()[0]
    assert mx_format.text_of(mx_format.find(tx, "PmtId"), "EndToEndId") == "E2E0000000001"
    assert mx_format.find(tx, "Does/Not/Exist") is None
    assert mx_format.find_all(tx, "Nope") == []
