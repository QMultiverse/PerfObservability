"""The gRPC contract, checked on both sides.

CLAUDE.md: "Contract tests run on both sides, so the Hub and the ESS can't
drift apart." These tests read the generated descriptors, so they fail if a
proto changes without both implementations following.
"""

from __future__ import annotations

import inspect

import grpc
import pytest
from hub_model import proto as pb
from hub_proto.ess.v1 import control_pb2
from hub_proto.ext.v1 import networks_pb2
from hub_proto.hub.v1 import common_pb2, hub_edge_pb2, payment_pb2

from ess.control import EssControl
from ess.emulators import FccEmulator, FinEmulator, SnfEmulator
from hub.edge.server import HubComplianceService, HubInboundService, HubNetworkEventsService
from tests.harness import Harness

# The seven business RPCs of design doc section 4, plus the ESS control plane.
HUB_SERVED = {
    "hub.v1.HubInbound": ("DeliverFin", "DeliverMx"),
    "hub.v1.HubNetworkEvents": ("NotifyAck", "NotifyDeliveryNotification"),
    "hub.v1.HubCompliance": ("NotifyFccDecision",),
}
EXT_SERVED = {
    "ext.v1.FinGateway": ("SendMt",),
    "ext.v1.SnfGateway": ("SendMx",),
    "ext.v1.FccScreening": ("Screen",),
}
ESS_SERVED = {
    "ess.v1.EssControl": (
        "SetProfile",
        "RunCase",
        "GetCounters",
        "Reset",
        "SendInbound",
        "ListCases",
        "RunBatch",
    ),
}

IMPLEMENTATIONS = {
    "hub.v1.HubInbound": HubInboundService,
    "hub.v1.HubNetworkEvents": HubNetworkEventsService,
    "hub.v1.HubCompliance": HubComplianceService,
    "ext.v1.FinGateway": FinEmulator,
    "ext.v1.SnfGateway": SnfEmulator,
    "ext.v1.FccScreening": FccEmulator,
    "ess.v1.EssControl": EssControl,
}

_FILES = {
    "hub/v1/common.proto": common_pb2,
    "hub/v1/hub_edge.proto": hub_edge_pb2,
    "hub/v1/payment.proto": payment_pb2,
    "ext/v1/networks.proto": networks_pb2,
    "ess/v1/control.proto": control_pb2,
}


def _services() -> dict[str, list[str]]:
    """Every service in the contract, with its method names."""
    found: dict[str, list[str]] = {}
    for module in _FILES.values():
        descriptor = module.DESCRIPTOR
        for service in descriptor.services_by_name.values():
            found[service.full_name] = [m.name for m in service.methods]
    return found


# ------------------------------------------------------- the proto itself
def test_every_expected_service_exists() -> None:
    found = _services()
    for full_name, methods in {**HUB_SERVED, **EXT_SERVED, **ESS_SERVED}.items():
        assert full_name in found, f"{full_name} is missing from the contract"
        assert set(methods) <= set(found[full_name]), (
            f"{full_name} is missing {set(methods) - set(found[full_name])}"
        )


def test_the_contract_has_no_unexpected_services() -> None:
    """A new service is a deliberate act, not a drive-by."""
    expected = set(HUB_SERVED) | set(EXT_SERVED) | set(ESS_SERVED)
    assert set(_services()) == expected


def test_every_business_rpc_is_unary() -> None:
    """Section 4: "seven business RPCs, all unary"."""
    for module in _FILES.values():
        for service in module.DESCRIPTOR.services_by_name.values():
            for method in service.methods:
                assert not method.client_streaming, f"{method.full_name} streams from the client"
                assert not method.server_streaming, f"{method.full_name} streams to the client"


def test_mt_and_mx_use_different_methods() -> None:
    """So every gRPC metric splits by format without an extra label."""
    inbound = _services()["hub.v1.HubInbound"]
    assert "DeliverFin" in inbound and "DeliverMx" in inbound
    assert "ext.v1.FinGateway" in _services()
    assert "ext.v1.SnfGateway" in _services()
    assert "SendMt" not in _services()["ext.v1.SnfGateway"]
    assert "SendMx" not in _services()["ext.v1.FinGateway"]


def test_msg_ref_carries_the_correlation_key() -> None:
    fields = {f.name for f in common_pb2.MsgRef.DESCRIPTOR.fields}
    assert fields == {"uetr", "msg_type", "flow"}


def test_amounts_are_strings_never_floats() -> None:
    """A float amount is a defect. The contract must make it impossible."""
    from google.protobuf.descriptor import FieldDescriptor

    floats = []
    for module in (payment_pb2, networks_pb2):
        for message in module.DESCRIPTOR.message_types_by_name.values():
            for field in message.fields:
                is_float = field.type in (
                    FieldDescriptor.TYPE_FLOAT,
                    FieldDescriptor.TYPE_DOUBLE,
                )
                if is_float and ("amount" in field.name or "amt" in field.name):
                    floats.append(field.full_name)
    assert floats == [], f"floating-point money in the contract: {floats}"


# ----------------------------------------------------- both sides implement
@pytest.mark.parametrize(
    ("full_name", "methods"), list({**HUB_SERVED, **EXT_SERVED, **ESS_SERVED}.items())
)
def test_the_implementation_provides_every_method(full_name: str, methods: tuple[str, ...]) -> None:
    implementation = IMPLEMENTATIONS[full_name]
    for method in methods:
        handler = getattr(implementation, method, None)
        assert handler is not None, f"{implementation.__name__} does not implement {method}"
        assert inspect.iscoroutinefunction(handler), f"{full_name}.{method} must be async"
        signature = inspect.signature(handler)
        assert list(signature.parameters) == ["self", "request", "context"], (
            f"{full_name}.{method} has an unexpected signature"
        )


async def test_the_hub_actually_serves_its_side(hub: Harness) -> None:
    """Not just implemented — reachable over the wire."""
    async with grpc.aio.insecure_channel(hub.edge_address) as channel:
        receipt = await pb.HubInboundStub(channel).DeliverMx(
            pb.MxDelivery(ref=pb.MsgRef(uetr="not-a-uetr")), timeout=1.0
        )
        assert receipt.status == pb.DeliveryReceipt.REJECTED

        received = await pb.HubNetworkEventsStub(channel).NotifyAck(
            pb.NetworkAck(
                ref=pb.MsgRef(uetr="00000000-0000-4000-8000-000000000000"),
                network=pb.SNF,
                ack=True,
            ),
            timeout=1.0,
        )
        assert received.received

        decided = await pb.HubComplianceStub(channel).NotifyFccDecision(
            pb.FccDecision(
                ref=pb.MsgRef(uetr="00000000-0000-4000-8000-000000000000"),
                case_id="CASE-1",
                decision=pb.FccDecision.RELEASE,
            ),
            timeout=1.0,
        )
        assert decided.received


async def test_the_ess_actually_serves_its_side(hub: Harness) -> None:
    address = f"localhost:{hub.ess.settings.port}"
    async with grpc.aio.insecure_channel(address) as channel:
        accepted = await pb.FinGatewayStub(channel).SendMt(
            pb.SendMtRequest(
                ref=pb.MsgRef(uetr="00000000-0000-4000-8000-000000000000", msg_type="MT103"),
                fin_message=b"{1:F01TESTGB2LXXXX0000000000}",
                receiver_bic="TESTDEFFXXX",
            ),
            timeout=1.0,
        )
        assert accepted.status == pb.SendAccepted.ACCEPTED
        assert accepted.send_ref

        result = await pb.FccScreeningStub(channel).Screen(
            pb.ScreenRequest(
                ref=pb.MsgRef(uetr="00000000-0000-4000-8000-000000000001"),
                parties=[pb.ScreenParty(role="DEBTOR", name="NORTHWIND TRADING LTD")],
                currency="EUR",
                amount="1.00",
            ),
            timeout=1.0,
        )
        assert result.outcome == pb.ScreenResult.NO_HIT

        listed = await pb.EssControlStub(channel).ListCases(pb.ListCasesRequest(), timeout=1.0)
        assert listed.cases


# ------------------------------------------------------------- deadlines
def test_deadlines_match_the_design_doc() -> None:
    from hub_telemetry.grpc_telemetry import (
        DEADLINE_DELIVER_S,
        DEADLINE_SCREEN_S,
        DEADLINE_SEND_S,
    )

    assert DEADLINE_DELIVER_S == 0.200
    assert DEADLINE_SEND_S == 0.500
    assert DEADLINE_SCREEN_S == 1.000


def test_only_unavailable_and_deadline_exceeded_are_retried() -> None:
    from hub_telemetry.grpc_telemetry import MAX_ATTEMPTS, RETRYABLE

    assert {grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED} == RETRYABLE
    assert grpc.StatusCode.INVALID_ARGUMENT not in RETRYABLE
    assert MAX_ATTEMPTS == 3
