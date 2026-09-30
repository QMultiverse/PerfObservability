"""The files under ``samples/``: valid, synthetic, and they go through the Hub.

CLAUDE.md forbids real customer data anywhere, so these tests assert on that
too — every IBAN must be checksum-valid and every BIC must be a test BIC.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from hub_format import map_mt, map_mx
from hub_format import mt as mt_format
from hub_format import mx as mx_format
from hub_format.samples import BANKS, SANCTIONS_NAMES
from hub_model.ids import is_valid_iban, is_valid_uetr

from ess.sender import Delivery
from tests.harness import Harness

SAMPLES = Path(__file__).resolve().parent.parent.parent / "samples"

MX_SAMPLES = sorted(p for p in SAMPLES.glob("*.xml") if not p.name.endswith(".apphdr.xml"))
MT_SAMPLES = sorted(SAMPLES.glob("*.fin"))
TEST_BICS = {bank[0] for bank in BANKS} | {bank[0][:8] for bank in BANKS}


def test_the_samples_directory_is_populated() -> None:
    assert MX_SAMPLES, "no MX samples; run python scripts/make_samples.py"
    assert MT_SAMPLES, "no MT samples; run python scripts/make_samples.py"
    assert (SAMPLES / "README.md").is_file()


@pytest.mark.parametrize("path", MX_SAMPLES, ids=lambda p: p.name)
def test_every_mx_sample_parses_and_validates(path: Path) -> None:
    header = path.with_suffix("").with_suffix(".apphdr.xml")
    assert header.is_file(), f"{path.name} has no AppHdr"

    message = mx_format.parse_document(path.read_bytes(), header.read_bytes())
    mx_format.validate_structure(message)
    payment = map_mx.to_payment(message)

    assert is_valid_uetr(payment.ref.uetr)
    assert payment.interbank_settlement_amount.currency
    assert payment.interbank_settlement_amount.amount


@pytest.mark.parametrize("path", MT_SAMPLES, ids=lambda p: p.name)
def test_every_mt_sample_parses_and_maps(path: Path) -> None:
    message = mt_format.parse(path.read_bytes())
    payment = map_mt.to_payment(message)

    assert is_valid_uetr(payment.ref.uetr)
    assert payment.interbank_settlement_amount.amount
    assert mt_format.peek_uetr(path.read_bytes()) == payment.ref.uetr


@pytest.mark.parametrize("path", MX_SAMPLES + MT_SAMPLES, ids=lambda p: p.name)
def test_no_sample_contains_real_looking_data(path: Path) -> None:
    """Test BICs only, and every IBAN must pass its checksum."""
    if path.suffix == ".fin":
        payment = map_mt.to_payment(mt_format.parse(path.read_bytes()))
    else:
        header = path.with_suffix("").with_suffix(".apphdr.xml")
        payment = map_mx.to_payment(
            mx_format.parse_document(path.read_bytes(), header.read_bytes())
        )

    for label, party in (
        ("debtor", payment.debtor),
        ("creditor", payment.creditor),
        ("debtor agent", payment.debtor_agent),
        ("creditor agent", payment.creditor_agent),
    ):
        if party.bic:
            assert party.bic in TEST_BICS, f"{label} BIC {party.bic} is not a test BIC"
        if party.account.iban:
            assert is_valid_iban(party.account.iban), (
                f"{label} IBAN {party.account.iban} fails its checksum"
            )


def test_the_sanctions_sample_names_a_watchlist_party() -> None:
    """The sample exists so a hit can be triggered without tuning rates."""
    path = SAMPLES / "pacs008_sanctions_hit.xml"
    payment = map_mx.to_payment(
        mx_format.parse_document(
            path.read_bytes(), (SAMPLES / "pacs008_sanctions_hit.apphdr.xml").read_bytes()
        )
    )
    names = {payment.debtor.name.upper(), payment.creditor.name.upper()}
    assert names & set(SANCTIONS_NAMES)


# ------------------------------------------------------------ end to end
async def test_a_sample_file_flows_through_the_hub(hub: Harness) -> None:
    """``ess send --file samples/pacs008_eur.xml``, in test form."""
    document = (SAMPLES / "pacs008_eur.xml").read_bytes()
    app_hdr = (SAMPLES / "pacs008_eur.apphdr.xml").read_bytes()
    uetr = mx_format.peek_uetr(document)

    delivery = Delivery(
        msg_type=mx_format.peek_msg_type(document),
        uetr=uetr,
        payload=document,
        app_hdr=app_hdr,
        flow="MX_SNF_PACS008",
    )
    await hub.ess.sender.deliver(delivery)
    assert await hub.run_until_state(uetr, "COMPLETED") == "COMPLETED"


async def test_an_mt_sample_file_flows_through_the_hub(hub: Harness) -> None:
    text = (SAMPLES / "mt103_gbp.fin").read_bytes()
    uetr = mt_format.peek_uetr(text)

    delivery = Delivery(msg_type="MT103", uetr=uetr, payload=text, flow="MT_FIN_103")
    await hub.ess.sender.deliver(delivery)
    assert await hub.run_until_state(uetr, "COMPLETED") == "COMPLETED"


async def test_the_sanctions_sample_is_held(hub: Harness) -> None:
    document = (SAMPLES / "pacs008_sanctions_hit.xml").read_bytes()
    app_hdr = (SAMPLES / "pacs008_sanctions_hit.apphdr.xml").read_bytes()
    uetr = mx_format.peek_uetr(document)

    delivery = Delivery(
        msg_type="pacs.008.001.08",
        uetr=uetr,
        payload=document,
        app_hdr=app_hdr,
        flow="MX_SNF_PACS008",
    )
    await hub.ess.sender.deliver(delivery)
    await hub.settle(until=lambda: hub.state_of(uetr) == "HELD", timeout_s=5.0)
    assert hub.state_of(uetr) == "HELD"
