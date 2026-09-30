"""The in-house MT parser, builder and mapper."""

from __future__ import annotations

import pytest
from hub_format import map_mt
from hub_format import mt as mt_format
from hub_format.samples import SampleOptions, make_mt
from hub_model.flows import MT103, MT202, MT202COV
from hub_model.ids import is_valid_iban

MT103_SAMPLE = (
    "{1:F01TESTGB2LAXXX0000000000}"
    "{2:I103TESTDEFFXXXXN}"
    "{3:{108:MYREF}{121:eb6305c9-1f1d-4b3a-8a0f-2d3c4e5f6a7b}}"
    """{4:
:20:REF123456
:23B:CRED
:32A:260928EUR1234,56
:33B:EUR1234,56
:50K:/GB33BUKB20201555555555
NORTHWIND TRADING LTD
12 HARBOUR WAY
:52A:TESTGB2LXXX
:57A:TESTDEFFXXX
:59:/DE89370400440532013000
ACME MANUFACTURING GMBH
45 INDUSTRIESTRASSE
:70:INVOICE 12345
:71A:SHA
-}{5:{CHK:0123456789AB}}"""
)


def test_parse_splits_all_five_blocks() -> None:
    message = mt_format.parse(MT103_SAMPLE)
    assert message.msg_type == "103"
    assert message.header.sender_lt == "TESTGB2LAXXX"
    assert message.header.sender_bic == "TESTGB2L"
    assert message.header.receiver_bic == "TESTDEFF"
    assert message.header.direction == "I"
    assert message.uetr == "eb6305c9-1f1d-4b3a-8a0f-2d3c4e5f6a7b"
    assert message.block3["108"] == "MYREF"
    assert message.block5["CHK"] == "0123456789AB"


def test_block4_fields_keep_their_order_and_continuation_lines() -> None:
    message = mt_format.parse(MT103_SAMPLE)
    assert [tag for tag, _ in message.tags] == [
        "20",
        "23B",
        "32A",
        "33B",
        "50K",
        "52A",
        "57A",
        "59",
        "70",
        "71A",
    ]
    assert message.get("50") == "/GB33BUKB20201555555555\nNORTHWIND TRADING LTD\n12 HARBOUR WAY"


def test_get_matches_any_option_letter() -> None:
    message = mt_format.parse(MT103_SAMPLE)
    assert message.get("50").startswith("/GB33")
    assert message.get("50K").startswith("/GB33")
    assert message.option("50") == "K"
    assert message.option("59") == ""
    assert message.get("99") == ""


def test_peek_reads_the_uetr_without_parsing() -> None:
    """The edge must not parse; it reads field 121 out of block 3."""
    assert mt_format.peek_uetr(MT103_SAMPLE) == "eb6305c9-1f1d-4b3a-8a0f-2d3c4e5f6a7b"
    assert mt_format.peek_msg_type(MT103_SAMPLE) == "MT103"
    # Still works when block 4 is nonsense, which is the point.
    broken = MT103_SAMPLE.replace(":32A:260928EUR1234,56", "GARBAGE")
    assert mt_format.peek_uetr(broken) == "eb6305c9-1f1d-4b3a-8a0f-2d3c4e5f6a7b"


@pytest.mark.parametrize(
    "bad",
    [
        "",
        "not a message",
        "{1:F01TESTGB2LAXXX0000000000}",  # no block 2 or 4
        "{1:BAD}{2:I103TESTDEFFXXXXN}{4:\n:20:X\n-}",
        "{1:F01TESTGB2LAXXX0000000000}{2:BAD}{4:\n:20:X\n-}",
        "{1:F01TESTGB2LAXXX0000000000}{2:I103TESTDEFFXXXXN}{4:\n-}",  # empty block 4
    ],
)
def test_malformed_messages_are_rejected(bad: str) -> None:
    with pytest.raises(mt_format.MtParseError):
        mt_format.parse(bad)


def test_unclosed_block_is_rejected() -> None:
    with pytest.raises(mt_format.MtParseError, match="not closed"):
        mt_format.parse("{1:F01TESTGB2LAXXX0000000000}{2:I103TESTDEFFXXXXN}{4:\n:20:X")


# --------------------------------------------------------------- mapping
def test_mt103_maps_to_the_canonical_model() -> None:
    payment = map_mt.to_payment(mt_format.parse(MT103_SAMPLE))
    assert payment.ref.msg_type == MT103
    assert payment.biz_msg_id == "REF123456"
    assert payment.interbank_settlement_amount.currency == "EUR"
    assert payment.interbank_settlement_amount.amount == "1234.56"
    assert payment.interbank_settlement_date == "2026-09-28"
    assert payment.charge_bearer == "SHAR"  # SHA -> SHAR
    assert payment.local_instrument == "CRED"
    assert payment.debtor.name == "NORTHWIND TRADING LTD"
    assert payment.debtor.account.iban == "GB33BUKB20201555555555"
    assert payment.creditor.name == "ACME MANUFACTURING GMBH"
    assert payment.creditor.account.iban == "DE89370400440532013000"
    assert payment.debtor_agent.bic == "TESTGB2LXXX"
    assert payment.creditor_agent.bic == "TESTDEFFXXX"
    assert payment.remittance_info == "INVOICE 12345"


def test_the_mt_comma_decimal_separator_is_handled() -> None:
    assert (
        map_mt.to_payment(mt_format.parse(MT103_SAMPLE)).interbank_settlement_amount.amount
        == "1234.56"
    )
    from hub_model.ids import from_mt_amount, mt_amount

    assert str(from_mt_amount("1234,56")) == "1234.56"
    assert mt_amount("1234.5", "EUR") == "1234,50"
    assert mt_amount("1234", "JPY") == "1234,"


@pytest.mark.parametrize(
    ("yymmdd", "iso"),
    [("260928", "2026-09-28"), ("991231", "1999-12-31"), ("000101", "2000-01-01")],
)
def test_mt_dates_use_the_swift_century_window(yymmdd: str, iso: str) -> None:
    sample = MT103_SAMPLE.replace(":32A:260928", f":32A:{yymmdd}")
    assert map_mt.to_payment(mt_format.parse(sample)).interbank_settlement_date == iso


def test_an_invalid_date_is_a_mapping_error() -> None:
    sample = MT103_SAMPLE.replace(":32A:260928", ":32A:261332")
    with pytest.raises(map_mt.MtMappingError):
        map_mt.to_payment(mt_format.parse(sample))


def test_a_missing_amount_is_a_mapping_error() -> None:
    sample = MT103_SAMPLE.replace(":32A:260928EUR1234,56\n", "")
    with pytest.raises(map_mt.MtMappingError, match="32A"):
        map_mt.to_payment(mt_format.parse(sample))


# -------------------------------------------------------------- MT202 COV
def test_an_mt202_with_sequence_b_is_a_cov() -> None:
    text, _ = make_mt(MT202COV, SampleOptions(seed=11))
    message = mt_format.parse(text)
    assert map_mt.classify(message) == MT202COV
    sequence_a, sequence_b = message.sequences()
    assert sequence_b, "sequence B must be present"
    assert next(tag for tag, _ in sequence_a) == "20"
    assert next(tag for tag, _ in sequence_b).startswith("50")


def test_an_mt202_without_sequence_b_is_not_a_cov() -> None:
    text, _ = make_mt(MT202, SampleOptions(seed=11))
    message = mt_format.parse(text)
    assert map_mt.classify(message) == MT202
    assert message.sequences()[1] == []


def test_an_mt103_is_never_split_into_sequences() -> None:
    """Field 50 is the ordering customer, not the start of a sequence B."""
    message = mt_format.parse(MT103_SAMPLE)
    sequence_a, sequence_b = message.sequences()
    assert sequence_b == []
    assert len(sequence_a) == 10


def test_the_cov_underlying_leg_is_recovered() -> None:
    text, _ = make_mt(MT202COV, SampleOptions(seed=12))
    message = mt_format.parse(text)
    underlying = map_mt.underlying_payment(message)
    assert underlying is not None
    assert underlying.ref.msg_type == MT103
    assert underlying.debtor.name
    assert underlying.creditor.name


# ------------------------------------------------------------- round trip
@pytest.mark.parametrize("msg_type", [MT103, MT202, MT202COV])
def test_build_then_parse_preserves_the_payment(msg_type: str) -> None:
    text, original = make_mt(msg_type, SampleOptions(seed=21))
    parsed = map_mt.to_payment(mt_format.parse(text))

    assert parsed.ref.uetr == original.ref.uetr
    assert parsed.interbank_settlement_amount.currency == (
        original.interbank_settlement_amount.currency
    )
    assert parsed.interbank_settlement_amount.amount == (
        original.interbank_settlement_amount.amount
    )
    assert parsed.interbank_settlement_date == original.interbank_settlement_date


def test_generated_ibans_pass_the_checksum() -> None:
    """CLAUDE.md: generated data must pass ``schwifty``."""
    for seed in range(20):
        _, payment = make_mt(MT103, SampleOptions(seed=seed))
        for party in (payment.debtor, payment.creditor):
            if party.account.iban:
                assert is_valid_iban(party.account.iban), party.account.iban


def test_the_lt_address_round_trips() -> None:
    for bic in ("TESTGB2L", "TESTGB2LXXX", "TESTGB2L123"):
        lt = mt_format._lt_from_bic(bic)
        assert len(lt) == 12
        recovered = mt_format._bic_from_lt(lt)
        assert recovered.startswith(bic[:8])


def test_field_59_has_no_option_letter_in_the_name_and_address_form() -> None:
    """:59: is the beneficiary; :59K: is not a valid field."""
    text, _ = make_mt(MT103, SampleOptions(seed=31))
    assert ":59:" in text
    assert ":59K:" not in text


def test_field_33b_is_not_emitted_in_an_mt202_sequence_a() -> None:
    text, _ = make_mt(MT202, SampleOptions(seed=32))
    message = mt_format.parse(text)
    sequence_a, _ = message.sequences()
    assert "33B" not in [tag for tag, _ in sequence_a]


def test_split_32a() -> None:
    assert mt_format.split_32a("260928EUR1234,56") == ("260928", "EUR", "1234,56")
    with pytest.raises(mt_format.MtParseError):
        mt_format.split_32a("short")


def test_split_party() -> None:
    account, lines = mt_format.split_party("/GB33BUKB20201555555555\nACME LTD\n1 HIGH ST")
    assert account == "GB33BUKB20201555555555"
    assert lines == ["ACME LTD", "1 HIGH ST"]
    assert mt_format.split_party("ACME LTD") == ("", ["ACME LTD"])
