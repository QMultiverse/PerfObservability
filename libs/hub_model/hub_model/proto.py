"""One import site for the generated protobuf types.

Service code does ``from hub_model.proto import PaymentEnvelope`` rather than
reaching into ``hub_proto.hub.v1.payment_pb2``. If the contract is reorganised,
only this module changes.
"""

from __future__ import annotations

from hub_proto.ess.v1 import control_pb2 as _ess
from hub_proto.ess.v1 import control_pb2_grpc as _ess_grpc
from hub_proto.ext.v1 import networks_pb2 as _ext
from hub_proto.ext.v1 import networks_pb2_grpc as _ext_grpc
from hub_proto.hub.v1 import common_pb2 as _common
from hub_proto.hub.v1 import hub_edge_pb2 as _edge
from hub_proto.hub.v1 import hub_edge_pb2_grpc as _edge_grpc
from hub_proto.hub.v1 import payment_pb2 as _pay

# ------------------------------------------------------------------ common
MsgRef = _common.MsgRef
Received = _common.Received
RawRef = _common.RawRef
Format = _common.Format
Network = _common.Network
Direction = _common.Direction

FORMAT_UNSPECIFIED = _common.FORMAT_UNSPECIFIED
MT = _common.MT
MX = _common.MX
NETWORK_UNSPECIFIED = _common.NETWORK_UNSPECIFIED
FIN = _common.FIN
SNF = _common.SNF
INBOUND = _common.INBOUND
OUTBOUND = _common.OUTBOUND

# ----------------------------------------------------------------- payment
Money = _pay.Money
PostalAddress = _pay.PostalAddress
Account = _pay.Account
Party = _pay.Party
Payment = _pay.Payment
CoverLink = _pay.CoverLink
Screening = _pay.Screening
Route = _pay.Route
Settlement = _pay.Settlement
Dispatch = _pay.Dispatch
Timings = _pay.Timings
PaymentEnvelope = _pay.PaymentEnvelope
OutboundMessage = _pay.OutboundMessage
StatusEvent = _pay.StatusEvent
PaymentStateRecord = _pay.PaymentStateRecord
RawInbound = _pay.RawInbound
NetworkAckRecord = _pay.NetworkAckRecord
FccDecisionRecord = _pay.FccDecisionRecord
FailedRecord = _pay.FailedRecord

PaymentState = _pay.PaymentState
PAYMENT_STATE_UNSPECIFIED = _pay.PAYMENT_STATE_UNSPECIFIED
RECEIVED = _pay.RECEIVED
PARSED = _pay.PARSED
SCREENED = _pay.SCREENED
HELD = _pay.HELD
BLOCKED = _pay.BLOCKED
AWAITING_COVER = _pay.AWAITING_COVER
ROUTED = _pay.ROUTED
SETTLED = _pay.SETTLED
DISPATCHED = _pay.DISPATCHED
COMPLETED = _pay.COMPLETED
REJECTED = _pay.REJECTED
FAILED = _pay.FAILED

# -------------------------------------------------------------- hub edge
FinDelivery = _edge.FinDelivery
MxDelivery = _edge.MxDelivery
DeliveryReceipt = _edge.DeliveryReceipt
NetworkAck = _edge.NetworkAck
DeliveryNotification = _edge.DeliveryNotification
FccDecision = _edge.FccDecision

HubInboundServicer = _edge_grpc.HubInboundServicer
HubInboundStub = _edge_grpc.HubInboundStub
HubNetworkEventsServicer = _edge_grpc.HubNetworkEventsServicer
HubNetworkEventsStub = _edge_grpc.HubNetworkEventsStub
HubComplianceServicer = _edge_grpc.HubComplianceServicer
HubComplianceStub = _edge_grpc.HubComplianceStub
add_HubInboundServicer_to_server = _edge_grpc.add_HubInboundServicer_to_server
add_HubNetworkEventsServicer_to_server = _edge_grpc.add_HubNetworkEventsServicer_to_server
add_HubComplianceServicer_to_server = _edge_grpc.add_HubComplianceServicer_to_server

# ------------------------------------------------------------- ext (ESS)
SendMtRequest = _ext.SendMtRequest
SendMxRequest = _ext.SendMxRequest
SendAccepted = _ext.SendAccepted
ScreenParty = _ext.ScreenParty
ScreenRequest = _ext.ScreenRequest
ScreenResult = _ext.ScreenResult

FinGatewayServicer = _ext_grpc.FinGatewayServicer
FinGatewayStub = _ext_grpc.FinGatewayStub
SnfGatewayServicer = _ext_grpc.SnfGatewayServicer
SnfGatewayStub = _ext_grpc.SnfGatewayStub
FccScreeningServicer = _ext_grpc.FccScreeningServicer
FccScreeningStub = _ext_grpc.FccScreeningStub
add_FinGatewayServicer_to_server = _ext_grpc.add_FinGatewayServicer_to_server
add_SnfGatewayServicer_to_server = _ext_grpc.add_SnfGatewayServicer_to_server
add_FccScreeningServicer_to_server = _ext_grpc.add_FccScreeningServicer_to_server

# ------------------------------------------------------------- ess control
Target = _ess.Target
LatencyDist = _ess.LatencyDist
BehaviourProfile = _ess.BehaviourProfile
Applied = _ess.Applied
CaseRequest = _ess.CaseRequest
CaseResult = _ess.CaseResult
CaseStep = _ess.CaseStep
CaseInfo = _ess.CaseInfo
ListCasesRequest = _ess.ListCasesRequest
ListCasesResult = _ess.ListCasesResult
SendInboundRequest = _ess.SendInboundRequest
SendInboundResult = _ess.SendInboundResult
BatchRequest = _ess.BatchRequest
BatchResult = _ess.BatchResult
MixEntry = _ess.MixEntry
CounterRequest = _ess.CounterRequest
Counters = _ess.Counters
ResetRequest = _ess.ResetRequest
CallRecord = _ess.CallRecord

EssControlServicer = _ess_grpc.EssControlServicer
EssControlStub = _ess_grpc.EssControlStub
add_EssControlServicer_to_server = _ess_grpc.add_EssControlServicer_to_server


def state_name(state: int) -> str:
    """``COMPLETED`` for :data:`COMPLETED`; used in logs and metric labels."""
    return str(PaymentState.Name(state))


def format_name(fmt: int) -> str:
    """``MT`` / ``MX``; ``UNKNOWN`` rather than raising on an unset field."""
    try:
        name = str(Format.Name(fmt))
    except ValueError:
        return "UNKNOWN"
    return "UNKNOWN" if name == "FORMAT_UNSPECIFIED" else name


def network_name(net: int) -> str:
    try:
        name = str(Network.Name(net))
    except ValueError:
        return "UNKNOWN"
    return "UNKNOWN" if name == "NETWORK_UNSPECIFIED" else name
