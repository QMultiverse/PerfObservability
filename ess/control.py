"""``EssControl``: change behaviour at runtime, run cases, read counters.

This is what the ``ess`` CLI, CI and the performance framework drive the
simulator through. Nothing here touches the Hub's Kafka — the ESS is an
external party and talks gRPC only, like the real systems it stands in for.
"""

from __future__ import annotations

import logging
from typing import Final

import grpc
from hub_model.proto import (
    Applied,
    BatchRequest,
    BatchResult,
    CaseInfo,
    CaseRequest,
    CaseResult,
    CaseStep,
    CounterRequest,
    Counters,
    EssControlServicer,
    ListCasesRequest,
    ListCasesResult,
    ResetRequest,
    SendInboundRequest,
    SendInboundResult,
)
from hub_model.proto import BehaviourProfile as BehaviourProfileMsg

from .batch import BatchRunner, describe, spec_from_proto
from .cases import CASES, CaseRunner
from .emulators import Overrides
from .profiles import ProfileStore
from .recorder import Recorder
from .sender import InboundSender

log = logging.getLogger(__name__)

SERVICE_NAME: Final = "ess-control"


class EssControl(EssControlServicer):
    """The ESS control plane."""

    def __init__(
        self,
        profiles: ProfileStore,
        recorder: Recorder,
        overrides: Overrides,
        sender: InboundSender,
        runner: CaseRunner,
        batch: BatchRunner | None = None,
    ) -> None:
        self.profiles = profiles
        self.recorder = recorder
        self.overrides = overrides
        self.sender = sender
        self.runner = runner
        self.batch = batch if batch is not None else BatchRunner(sender, runner.probe)

    async def SetProfile(  # noqa: N802 - gRPC method name
        self, request: BehaviourProfileMsg, context: grpc.aio.ServicerContext
    ) -> Applied:
        changed = self.profiles.apply(request)
        detail = "; ".join(f"{name}: {self.profiles.get(name).describe()}" for name in changed)
        log.info("profile applied to %s — %s", ", ".join(changed), detail)
        return Applied(applied=True, detail=detail)

    async def RunCase(  # noqa: N802 - gRPC method name
        self, request: CaseRequest, context: grpc.aio.ServicerContext
    ) -> CaseResult:
        if request.case_id not in CASES:
            return CaseResult(
                passed=False,
                case_id=request.case_id,
                detail=f"unknown case; known cases: {', '.join(sorted(CASES))}",
            )
        previous_run_id = self.sender.run_id
        if request.run_id:
            self.sender.run_id = request.run_id
        try:
            outcome = await self.runner.run(
                request.case_id,
                run_id=request.run_id,
                repeat=request.repeat or 1,
                timeout_ms=request.timeout_ms,
            )
        finally:
            self.sender.run_id = previous_run_id

        result = CaseResult(
            passed=outcome.passed,
            case_id=outcome.case_id,
            detail=outcome.detail,
            duration_ms=outcome.duration_ms,
        )
        result.uetrs.extend(outcome.uetrs)
        for step in outcome.steps:
            result.steps.append(
                CaseStep(name=step.name, passed=step.passed, detail=step.detail, at_ns=step.at_ns)
            )
        return result

    async def ListCases(  # noqa: N802 - gRPC method name
        self, request: ListCasesRequest, context: grpc.aio.ServicerContext
    ) -> ListCasesResult:
        result = ListCasesResult()
        for case in CASES.values():
            result.cases.append(
                CaseInfo(
                    case_id=case.case_id,
                    flow=case.flow,
                    description=case.description,
                    expected_state=case.expected_state,
                )
            )
        return result

    async def SendInbound(  # noqa: N802 - gRPC method name
        self, request: SendInboundRequest, context: grpc.aio.ServicerContext
    ) -> SendInboundResult:
        previous_run_id = self.sender.run_id
        if request.run_id:
            self.sender.run_id = request.run_id
        try:
            result = SendInboundResult()
            for _ in range(max(1, request.count)):
                delivery = self.sender.build(
                    request.msg_type,
                    uetr=request.uetr,
                    payload=request.payload,
                    app_hdr=request.app_hdr,
                    flow=request.flow,
                )
                outcome = await self.sender.send([delivery])
                result.uetrs.extend(outcome.uetrs)
                result.accepted += outcome.accepted
                result.rejected += outcome.rejected
                result.duplicate += outcome.duplicate
                if outcome.errors:
                    result.detail = "; ".join(outcome.errors)
            return result
        except grpc.aio.AioRpcError as exc:
            return SendInboundResult(detail=f"delivery failed: {exc.code().name}")
        finally:
            self.sender.run_id = previous_run_id

    async def RunBatch(  # noqa: N802 - gRPC method name
        self, request: BatchRequest, context: grpc.aio.ServicerContext
    ) -> BatchResult:
        spec = spec_from_proto(request)
        log.info(
            "batch: %d payment(s) at %s/s, concurrency %d",
            spec.count,
            f"{spec.rate_per_second:g}" if spec.rate_per_second else "unpaced",
            spec.concurrency,
        )
        report = await self.batch.run(spec)
        for line in describe(report):
            log.info("batch: %s", line)

        result = BatchResult(
            requested=report.requested,
            accepted=report.accepted,
            rejected=report.rejected,
            duplicate=report.duplicate,
            errors=report.errors,
            send_duration_ms=int(report.send_duration_s * 1000),
            achieved_rate_per_second=report.achieved_rate,
            settled=report.settled,
            unsettled=report.unsettled,
            p50_end_to_end_ms=report.p50_ms,
            p95_end_to_end_ms=report.p95_ms,
            max_end_to_end_ms=report.max_ms,
            total_duration_ms=int(report.total_duration_s * 1000),
            detail=report.detail,
        )
        result.by_state.update(report.by_state)
        result.failed_uetrs.extend(report.failed_uetrs)
        return result

    async def GetCounters(  # noqa: N802 - gRPC method name
        self, request: CounterRequest, context: grpc.aio.ServicerContext
    ) -> Counters:
        buckets = self.recorder.counters(request.run_id)
        served, made, errors = self.recorder.totals()
        counters = Counters(calls_served=served, calls_made=made, errors=errors)
        counters.by_method.update(buckets["by_method"])
        counters.by_flow.update(buckets["by_flow"])
        counters.by_outcome.update(buckets["by_outcome"])
        return counters

    async def Reset(  # noqa: N802 - gRPC method name
        self, request: ResetRequest, context: grpc.aio.ServicerContext
    ) -> Applied:
        done: list[str] = []
        # An empty request resets everything: between tests that is what you
        # want, and it is the least surprising reading of "Reset".
        everything = not (
            request.clear_counters or request.clear_profiles or request.clear_recorder
        )
        if request.clear_profiles or everything:
            self.profiles.reset()
            self.overrides.clear()
            done.append("profiles and overrides")
        if request.clear_counters or request.clear_recorder or everything:
            self.recorder.reset()
            done.append("counters and recorder")
        detail = ", ".join(done) or "nothing"
        log.info("reset: %s", detail)
        return Applied(applied=True, detail=f"cleared {detail}")
