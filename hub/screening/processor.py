"""Screening: ``hub.pay.canonical`` + ``hub.compliance.decision`` to ``hub.pay.screened``.

The one stage with a synchronous call off the critical path's own topic: it
builds a screening request from the canonical payment and waits for the compliance service, with a
1 s deadline.

Three outcomes:

* ``NO_HIT`` — the payment moves on to ``hub.pay.screened``, T3 stamped.
* ``HIT_PENDING`` — the payment is parked in HELD in the local state store.
  Minutes later the compliance service calls ``NotifyComplianceDecision``; the edge writes it to
  ``hub.compliance.decision``, this stage reads it and resumes from the release.
* ``BLOCK`` — terminal. The payment is BLOCKED and goes no further.

**Cover pairs are matched here.** Both legs share the UETR, so Kafka has put
them on the same partition and one replica sees both. The design doc leaves the
matching stage open ("stateful steps keep a local state store... cover legs
waiting for their partner and payments HELD"); screening is the right place
because the business rule is that *both* legs must clear compliance before
either is released, and holding them together keeps AWAITING_COVER next to
HELD in one store. If that ownership moves, this module and
:mod:`hub.routing.processor` are the two that change.

``Screen`` is not part of the Kafka transaction. A crash after the call and
before the commit means the payment is screened again on redelivery, which is
why the compliance service de-duplicates on (UETR, message type, direction) and why we check
``stages_done`` before calling.
"""

from __future__ import annotations

import logging
from typing import Final

import grpc
from hub_model import proto as pb
from hub_model import topics as tp
from hub_model.envelope import STAGE_SCREENED, advance, now_ns
from hub_model.flows import get_flow
from hub_model.proto import (
    ComplianceDecisionRecord,
    PaymentEnvelope,
    ScreenParty,
    ScreenRequest,
    ScreenResult,
)
from hub_telemetry import metrics
from hub_telemetry.events import Events
from hub_telemetry.grpc_telemetry import DEADLINE_SCREEN_S, channel
from hub_telemetry.messaging import Record

from hub.common.config import ServiceSettings
from hub.common.processor import Outbox, PermanentError, Processor, RetryableError

log = logging.getLogger(__name__)

SERVICE_NAME: Final = "hub-screening"


class Screening(Processor):
    """Canonical in; screened, held or blocked out."""

    name = SERVICE_NAME
    stage = STAGE_SCREENED
    group_id = tp.CG_SCREENING
    input_topics = (tp.PAY_CANONICAL, tp.COMPLIANCE_DECISION)
    # HELD payments and half-matched cover pairs live in the local store; the
    # compacted state topic is what rebuilds them after a restart.
    reads_state = True

    def __init__(self, settings: ServiceSettings, events: Events) -> None:
        super().__init__(settings, events)
        self._channel: grpc.aio.Channel | None = None
        self._stub: pb.ComplianceScreeningStub | None = None

    # ------------------------------------------------------------- client
    def stub(self) -> pb.ComplianceScreeningStub:
        if self._stub is None:
            self._channel = channel(
                self.settings.external.compliance_target,
                self.events,
                strip_context=self.settings.external.real_networks,
            )
            self._stub = pb.ComplianceScreeningStub(self._channel)
        return self._stub

    async def close(self) -> None:
        if self._channel is not None:
            await self._channel.close()
            self._channel = None
            self._stub = None

    # ------------------------------------------------------------ dispatch
    async def handle(self, record: Record, out: Outbox) -> None:
        if record.topic.startswith(tp.COMPLIANCE_DECISION):
            await self._handle_decision(record, out)
        else:
            await self._handle_payment(record, out)

    # ---------------------------------------------------------- the payment
    async def _handle_payment(self, record: Record, out: Outbox) -> None:
        env = _decode(record)
        if self.done(env):
            log.debug("skipping %s: already screened", env.ref.uetr)
            return

        result = await self._screen(env)
        env.timings.t3_screen_reply_ns = now_ns()
        env.screening.screened_ns = env.timings.t3_screen_reply_ns
        env.screening.case_id = result.case_id
        env.screening.reason = result.reason
        for party in result.matched_parties:
            env.screening.matched_parties.append(party)

        if result.outcome == ScreenResult.BLOCK:
            env.screening.outcome = pb.Screening.BLOCK
            blocked = advance(env, pb.BLOCKED)
            # BLOCKED is terminal: a status event and the state record, but
            # nothing on hub.pay.screened, so the payment goes no further.
            stages = self.mark(blocked)
            out.emit_status(blocked, reason="Compliance returned BLOCK")
            out.emit_state(blocked, stages=stages)
            return

        if result.outcome == ScreenResult.HIT_PENDING:
            env.screening.outcome = pb.Screening.HIT_PENDING
            held = advance(env, pb.HELD)
            self.store.hold(held, result.case_id)
            out.emit_status(held, reason=f"Compliance hit, case {result.case_id}")
            out.emit_state(held, stages=())
            metrics.held_payments.labels(SERVICE_NAME).set(self.store.held_count())
            return

        env.screening.outcome = pb.Screening.NO_HIT
        await self._release(env, out, reason="Compliance returned NO_HIT")

    async def _screen(self, env: PaymentEnvelope) -> ScreenResult:
        request = build_screen_request(env)
        try:
            result: ScreenResult = await self.stub().Screen(request, timeout=DEADLINE_SCREEN_S)
            return result
        except grpc.aio.AioRpcError as exc:
            code = exc.code()
            if code in (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED):
                # Compliance screening is down or slow. The payment is not wrong; try again.
                raise RetryableError(
                    f"Compliance screening unreachable: {code.name}", code="COMPLIANCE_UNAVAILABLE"
                ) from exc
            if code == grpc.StatusCode.INVALID_ARGUMENT:
                raise PermanentError(
                    f"Compliance screening rejected the request: {exc.details()}",
                    code="COMPLIANCE_INVALID",
                ) from exc
            raise RetryableError(
                f"Compliance screening error {code.name}: {exc.details()}", code="COMPLIANCE_ERROR"
            ) from exc

    # ---------------------------------------------------------- the decision
    async def _handle_decision(self, record: Record, out: Outbox) -> None:
        decision = ComplianceDecisionRecord()
        try:
            decision.ParseFromString(record.value)
        except Exception as exc:
            raise PermanentError(
                f"undecodable ComplianceDecisionRecord: {exc}", code="COMPLIANCE_BAD_DECISION"
            ) from exc

        held = self.store.held(decision.ref.uetr)
        if held is None:
            # A decision for a payment this replica never held. It belongs to
            # another partition owner, or arrived before the hit was recorded;
            # either way, dropping it is correct, not an error.
            log.info(
                "no held payment for decision on %s (case %s)",
                decision.ref.uetr,
                decision.case_id,
            )
            return

        env = PaymentEnvelope()
        env.CopyFrom(held)
        env.screening.decided_ns = decision.decided_ns or now_ns()
        env.screening.reason = decision.reason

        if not decision.release:
            env.screening.outcome = pb.Screening.BLOCK
            blocked = advance(env, pb.BLOCKED)
            stages = self.mark(blocked)
            out.emit_status(blocked, reason=f"analyst blocked case {decision.case_id}")
            out.emit_state(blocked, stages=stages)
            metrics.held_payments.labels(SERVICE_NAME).set(self.store.held_count())
            return

        env.screening.outcome = pb.Screening.RELEASED
        self.store.release(decision.ref.uetr)
        metrics.held_payments.labels(SERVICE_NAME).set(self.store.held_count())
        await self._release(env, out, reason=f"analyst released case {decision.case_id}")

    # ------------------------------------------------------------- release
    async def _release(self, env: PaymentEnvelope, out: Outbox, *, reason: str) -> None:
        """Move a cleared payment on — once its cover partner is here too."""
        screened = advance(env, pb.SCREENED)
        flow = get_flow(screened.ref.flow)

        if flow is not None and flow.cover_pair:
            # This leg has cleared compliance: mark it done so its partner is
            # not mistaken for it, then see whether the pair is complete.
            stages = self.mark(screened)
            legs = self.store.add_cover_leg(screened)
            expected = set(flow.inbound_types)
            if not expected.issubset(legs):
                waiting = advance(screened, pb.AWAITING_COVER)
                missing = ", ".join(sorted(expected - set(legs)))
                out.emit_status(waiting, reason=f"waiting for {missing}")
                out.emit_state(waiting, stages=stages)
                self.store.record(waiting, stages=tuple(stages))
                metrics.pending_cover_legs.labels(SERVICE_NAME).set(
                    self.store.pending_cover_count()
                )
                return

            # Both legs are here and both have cleared. Release them together,
            # in the order the flow declares, so the instruction leg leads.
            for msg_type in flow.inbound_types:
                leg = legs[msg_type]
                leg.payment.cover.partner_present = True
                complete = advance(leg, pb.SCREENED)
                leg_stages = self.mark(complete)
                out.advance_to(
                    complete,
                    tp.PAY_SCREENED,
                    stages=leg_stages,
                    reason=f"{reason}; cover pair complete",
                )
            metrics.pending_cover_legs.labels(SERVICE_NAME).set(self.store.pending_cover_count())
            return

        stages = self.mark(screened)
        out.advance_to(screened, tp.PAY_SCREENED, stages=stages, reason=reason)


def build_screen_request(env: PaymentEnvelope) -> ScreenRequest:
    """Build the screening request.

    Names and amounts go in the *request*, never in baggage — they are exactly
    what section 9 forbids putting on every hop.
    """
    payment = env.payment
    request = ScreenRequest(
        currency=payment.interbank_settlement_amount.currency,
        amount=payment.interbank_settlement_amount.amount,
        purpose=payment.purpose_code,
        remittance_info=payment.remittance_info,
    )
    request.ref.CopyFrom(env.ref)

    for role, party in (
        ("DEBTOR", payment.debtor),
        ("CREDITOR", payment.creditor),
        ("DEBTOR_AGENT", payment.debtor_agent),
        ("CREDITOR_AGENT", payment.creditor_agent),
    ):
        if party.name or party.bic:
            request.parties.append(
                ScreenParty(
                    role=role,
                    name=party.name,
                    bic=party.bic,
                    country=party.address.country,
                )
            )
    for agent in payment.intermediary_agent:
        if agent.name or agent.bic:
            request.parties.append(
                ScreenParty(
                    role="INTERMEDIARY",
                    name=agent.name,
                    bic=agent.bic,
                    country=agent.address.country,
                )
            )
    return request


def _decode(record: Record) -> PaymentEnvelope:
    env = PaymentEnvelope()
    try:
        env.ParseFromString(record.value)
    except Exception as exc:
        raise PermanentError(f"undecodable PaymentEnvelope: {exc}", code="BAD_ENVELOPE") from exc
    return env


def build(settings: ServiceSettings, events: Events) -> Screening:
    return Screening(settings, events)


__all__ = ["SERVICE_NAME", "Screening", "build", "build_screen_request"]
