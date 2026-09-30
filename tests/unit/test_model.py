"""hub-model: identifiers, money, flows and envelope transitions."""

from __future__ import annotations

from decimal import Decimal

import pytest
from hub_model import flows, ids
from hub_model import proto as pb
from hub_model.envelope import (
    STAGE_PARSED,
    STAGE_SCREENED,
    TERMINAL_STATES,
    advance,
    end_to_end_seconds,
    is_forward,
    make_ref,
    merge_stages,
    new_envelope,
    state_record,
    status_event,
)
from hub_model.ids import InvalidIdentifier

from hub.common.state_store import StateStore


# ----------------------------------------------------------------- UETR
def test_a_generated_uetr_is_a_valid_uuid_v4() -> None:
    for _ in range(50):
        assert ids.is_valid_uetr(ids.new_uetr())


@pytest.mark.parametrize(
    "bad",
    [
        "",
        "not-a-uuid",
        "EB6305C9-1F1D-4B3A-8A0F-2D3C4E5F6A7B",  # upper case
        "eb6305c9-1f1d-1b3a-8a0f-2d3c4e5f6a7b",  # version 1, not 4
        "eb6305c9-1f1d-4b3a-0a0f-2d3c4e5f6a7b",  # bad variant nibble
    ],
)
def test_bad_uetrs_are_rejected(bad: str) -> None:
    assert not ids.is_valid_uetr(bad)
    with pytest.raises(InvalidIdentifier):
        ids.require_uetr(bad)


# ------------------------------------------------------------------ BIC
@pytest.mark.parametrize("bic", ["TESTGB2L", "TESTGB2LXXX", "TESTGB2L123"])
def test_valid_bics(bic: str) -> None:
    assert ids.is_valid_bic(bic)
    assert ids.require_bic(bic.lower()) == bic


@pytest.mark.parametrize("bic", ["", "SHORT", "TEST12345", "TESTGB2LXXXX", "TEST-GB2L"])
def test_invalid_bics(bic: str) -> None:
    assert not ids.is_valid_bic(bic)


def test_bic8_truncates_a_bic11() -> None:
    assert ids.bic8("TESTGB2LXXX") == "TESTGB2L"


# ----------------------------------------------------------------- money
def test_amounts_parse_as_decimals() -> None:
    assert ids.parse_amount("1234.56", "EUR") == Decimal("1234.56")
    assert isinstance(ids.parse_amount("1", "EUR"), Decimal)


@pytest.mark.parametrize("bad", ["", "abc", "-1.00", "NaN", "Infinity"])
def test_bad_amounts_are_rejected(bad: str) -> None:
    with pytest.raises(InvalidIdentifier):
        ids.parse_amount(bad, "EUR")


def test_too_many_decimals_for_the_currency_is_rejected() -> None:
    with pytest.raises(InvalidIdentifier, match="decimals"):
        ids.parse_amount("1.005", "EUR")
    with pytest.raises(InvalidIdentifier):
        ids.parse_amount("1.5", "JPY")
    assert ids.parse_amount("1.005", "BHD") == Decimal("1.005")


@pytest.mark.parametrize(
    ("currency", "units"), [("EUR", 2), ("USD", 2), ("JPY", 0), ("BHD", 3), ("XXX", 2)]
)
def test_minor_units(currency: str, units: int) -> None:
    assert ids.minor_units(currency) == units


def test_formatting_uses_the_currency_precision() -> None:
    assert ids.format_amount("1234.5", "EUR") == "1234.50"
    assert ids.format_amount("1234", "JPY") == "1234"
    assert ids.format_amount("1.2345", "BHD") == "1.235", "half-up, not bankers' rounding"
    assert ids.format_amount("1.2344", "BHD") == "1.234"
    assert ids.format_amount(Decimal("1E+3"), "EUR") == "1000.00", "never an exponent"


def test_iban_validation_uses_schwifty() -> None:
    assert ids.is_valid_iban("GB33BUKB20201555555555")
    assert ids.is_valid_iban("gb33 bukb 2020 1555 5555 55"), "spacing and case are normalised"
    assert not ids.is_valid_iban("GB34BUKB20201555555555"), "wrong check digits"
    assert not ids.is_valid_iban("")


# ----------------------------------------------------------------- flows
@pytest.mark.parametrize(
    ("alias", "canonical"),
    [
        ("pacs008", flows.PACS_008),
        ("pacs.008", flows.PACS_008),
        ("PACS.008", flows.PACS_008),
        ("mt103", flows.MT103),
        ("MT103", flows.MT103),
        ("pacs.009cov", flows.PACS_009_COV),
    ],
)
def test_message_type_aliases(alias: str, canonical: str) -> None:
    assert flows.normalise_msg_type(alias) == canonical


def test_format_of_splits_the_two_lanes() -> None:
    assert flows.format_of("MT103") == int(pb.MT)
    assert flows.format_of("pacs.008") == int(pb.MX)
    with pytest.raises(ValueError):
        flows.format_of("nonsense")


def test_cover_types_are_recognised() -> None:
    assert flows.is_cover("MT202COV")
    assert flows.is_cover("pacs.009cov")
    assert not flows.is_cover("MT103")
    assert not flows.is_cover("pacs.008")


def test_the_default_flow_for_each_message_type() -> None:
    assert flows.flow_for("pacs.008") == flows.FLOW_MX_SNF_PACS008
    assert flows.flow_for("pacs.008", cover=True) == flows.FLOW_MX_SNF_PACS009COV_PAIR
    assert flows.flow_for("MT103") == flows.FLOW_MT_FIN_103
    assert flows.flow_for("MT202COV") == flows.FLOW_MT_FIN_103_202COV


def test_cover_flows_declare_two_legs() -> None:
    for flow_id in (flows.FLOW_MT_FIN_103_202COV, flows.FLOW_MX_SNF_PACS009COV_PAIR):
        flow = flows.get_flow(flow_id)
        assert flow is not None
        assert flow.cover_pair
        assert len(flow.inbound_types) == 2


def test_only_the_translation_flow_is_marked_translated() -> None:
    translated = [f.flow_id for f in flows.FLOWS.values() if f.translated]
    assert translated == [flows.FLOW_MT_TO_MX_103]


# -------------------------------------------------------------- envelope
def _envelope(state: int = int(pb.RECEIVED)) -> pb.PaymentEnvelope:
    return new_envelope(
        make_ref(ids.new_uetr(), flows.PACS_008, flows.FLOW_MX_SNF_PACS008),
        int(pb.MX),
        state=state,
        trace_level="full",
    )


def test_advance_returns_a_copy() -> None:
    original = _envelope()
    moved = advance(original, int(pb.PARSED))
    assert moved.state == int(pb.PARSED)
    assert original.state == int(pb.RECEIVED), "the input must not be mutated"


def test_state_order_only_moves_forward() -> None:
    assert is_forward(int(pb.RECEIVED), int(pb.PARSED))
    assert is_forward(int(pb.PARSED), int(pb.SCREENED))
    assert not is_forward(int(pb.SCREENED), int(pb.PARSED))
    assert not is_forward(int(pb.COMPLETED), int(pb.PARSED))


def test_nothing_follows_a_terminal_state() -> None:
    assert {int(pb.COMPLETED), int(pb.REJECTED), int(pb.BLOCKED)} == TERMINAL_STATES
    for terminal in TERMINAL_STATES:
        for state in (int(pb.PARSED), int(pb.COMPLETED), int(pb.DISPATCHED)):
            assert not is_forward(terminal, state)


def test_a_status_event_carries_the_timings() -> None:
    env = _envelope(int(pb.COMPLETED))
    env.timings.t0_network_sent_ns = 1_000
    env.timings.t6_completed_ns = 5_000
    event = status_event(env, service="hub-ack-matcher", reason="ACK")
    assert event.ref.uetr == env.ref.uetr
    assert event.state == int(pb.COMPLETED)
    assert event.service == "hub-ack-matcher"
    assert event.timings.t6_completed_ns == 5_000


def test_end_to_end_is_none_until_the_payment_completes() -> None:
    timings = pb.Timings(t0_network_sent_ns=1_000_000_000)
    assert end_to_end_seconds(timings) is None
    timings.t6_completed_ns = 3_000_000_000
    assert end_to_end_seconds(timings) == pytest.approx(2.0)


def test_state_records_carry_the_stage_list() -> None:
    env = _envelope(int(pb.SCREENED))
    record = state_record(env, stages_done=[STAGE_SCREENED, STAGE_PARSED, STAGE_PARSED])
    assert list(record.stages_done) == sorted({STAGE_PARSED, STAGE_SCREENED})
    assert record.envelope.ref.uetr == env.ref.uetr


def test_merge_stages_is_a_sorted_union() -> None:
    assert merge_stages(["b", "a"], ["c", "a"]) == ["a", "b", "c"]


# ----------------------------------------------------------- state store
def test_the_store_remembers_completed_stages() -> None:
    store = StateStore()
    env = _envelope(int(pb.PARSED))
    store.record(env, stages=(STAGE_PARSED,))
    assert store.already_done(env.ref.uetr, STAGE_PARSED)
    assert not store.already_done(env.ref.uetr, STAGE_SCREENED)


def test_the_store_does_not_move_a_payment_backwards() -> None:
    store = StateStore()
    env = _envelope(int(pb.COMPLETED))
    store.record(env)
    stale = advance(env, int(pb.PARSED))
    store.record(stale)
    assert store.state_of(env.ref.uetr) == int(pb.COMPLETED)


def test_a_completed_payment_is_remembered_after_it_leaves_the_working_set() -> None:
    """So a repeated network callback is recognised as a repeat."""
    store = StateStore()
    env = _envelope(int(pb.COMPLETED))
    store.record(env)
    store.complete(env.ref.uetr)
    assert store.envelope(env.ref.uetr) is None
    assert store.is_complete(env.ref.uetr)


def test_the_completed_window_is_bounded() -> None:
    store = StateStore(completed_window=100)
    for index in range(250):
        store.complete(f"uetr-{index}")
    assert store.is_complete("uetr-249")
    assert len(store._completed) <= 100


def test_cover_legs_are_kept_apart_by_message_type() -> None:
    store = StateStore()
    uetr = ids.new_uetr()
    for msg_type in (flows.PACS_008, flows.PACS_009_COV):
        env = new_envelope(make_ref(uetr, msg_type, flows.FLOW_MX_SNF_PACS009COV_PAIR), int(pb.MX))
        store.add_cover_leg(env)

    assert set(store.cover_legs(uetr)) == {flows.PACS_008, flows.PACS_009_COV}
    leg = store.cover_leg(uetr, flows.PACS_009_COV)
    assert leg is not None and leg.ref.msg_type == flows.PACS_009_COV
    assert store.cover_leg(uetr, "MT103") is None


def test_held_payments_are_tracked_and_released() -> None:
    store = StateStore()
    env = _envelope(int(pb.HELD))
    store.hold(env, "CASE-1")
    assert store.held_count() == 1
    assert store.held(env.ref.uetr) is not None

    released = store.release(env.ref.uetr)
    assert released is not None
    assert store.held_count() == 0
    assert store.held(env.ref.uetr) is None


def test_the_store_rebuilds_from_a_state_record() -> None:
    source = StateStore()
    env = _envelope(int(pb.DISPATCHED))
    source.record(env, stages=(STAGE_PARSED, STAGE_SCREENED))
    record = state_record(env, stages_done=[STAGE_PARSED, STAGE_SCREENED])

    rebuilt = StateStore()
    rebuilt.apply_state_record(record)
    assert rebuilt.state_of(env.ref.uetr) == int(pb.DISPATCHED)
    assert rebuilt.already_done(env.ref.uetr, STAGE_SCREENED)
    assert rebuilt.envelope(env.ref.uetr) is not None


# ------------------------------------------------- state never goes backwards
def test_state_rank_orders_the_pipeline() -> None:
    from hub_model.envelope import state_rank

    assert state_rank(int(pb.RECEIVED)) < state_rank(int(pb.PARSED))
    assert state_rank(int(pb.DISPATCHED)) < state_rank(int(pb.COMPLETED))
    assert state_rank(int(pb.COMPLETED)) > state_rank(int(pb.SETTLED))
    assert state_rank(999) == 0, "an unknown state ranks lowest, never highest"


def test_the_status_view_never_moves_a_payment_backwards() -> None:
    """A network ACK can beat the dispatcher's own transaction commit, so
    COMPLETED legitimately arrives before DISPATCHED on hub.pay.status.
    The reported state must stay COMPLETED.
    """
    from hub.status_api.processor import StatusView

    view = StatusView()
    uetr = ids.new_uetr()

    def event(state: int, service: str, emitted: int) -> pb.StatusEvent:
        e = pb.StatusEvent(state=state, service=service, emitted_ns=emitted, format=pb.MX)
        e.ref.uetr = uetr
        return e

    view.apply(event(int(pb.SETTLED), "hub-settlement", 100))
    view.apply(event(int(pb.COMPLETED), "hub-ack-matcher", 200))
    # Emitted later, but an earlier stage.
    view.apply(event(int(pb.DISPATCHED), "hub-dispatcher-mx", 300))

    entry = view.get(uetr)
    assert entry is not None
    assert entry.state == "COMPLETED", "a late DISPATCHED must not overwrite COMPLETED"
    # Everything that happened is still on the record.
    assert [h["state"] for h in entry.history] == ["SETTLED", "COMPLETED", "DISPATCHED"]


def test_the_status_view_keeps_timings_from_every_event() -> None:
    """Each event carries only the stamps its producer knew about."""
    from hub.status_api.processor import StatusView

    view = StatusView()
    uetr = ids.new_uetr()

    dispatched = pb.StatusEvent(state=int(pb.DISPATCHED), emitted_ns=300)
    dispatched.ref.uetr = uetr
    dispatched.timings.t0_network_sent_ns = 10
    dispatched.timings.t4_handoff_ns = 40

    completed = pb.StatusEvent(state=int(pb.COMPLETED), emitted_ns=200)
    completed.ref.uetr = uetr
    completed.timings.t0_network_sent_ns = 10
    completed.timings.t6_completed_ns = 60

    view.apply(completed)
    view.apply(dispatched)

    entry = view.get(uetr)
    assert entry is not None
    assert entry.state == "COMPLETED"
    assert entry.timings["t4"] == 40, "the late event's handoff stamp is still useful"
    assert entry.timings["t6"] == 60


def test_nothing_follows_a_terminal_state_in_the_view() -> None:
    from hub.status_api.processor import StatusView

    view = StatusView()
    uetr = ids.new_uetr()
    for state in (pb.REJECTED, pb.SCREENED, pb.COMPLETED):
        e = pb.StatusEvent(state=int(state), emitted_ns=1)
        e.ref.uetr = uetr
        view.apply(e)

    entry = view.get(uetr)
    assert entry is not None
    assert entry.state == "REJECTED"
