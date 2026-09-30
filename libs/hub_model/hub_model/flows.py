"""Flow IDs and message types.

A *flow* names one end-to-end route through the Hub. It is stamped into
baggage at the edge, labels every metric, and is what a test case and a
performance profile are written against.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

from .proto import FIN, MT, MX, SNF, Format, Network

# ------------------------------------------------------------ message types
MT103: Final = "MT103"
MT202: Final = "MT202"
MT202COV: Final = "MT202COV"

PACS_008: Final = "pacs.008.001.08"
PACS_009: Final = "pacs.009.001.08"
PACS_009_COV: Final = "pacs.009.001.08COV"
PACS_002: Final = "pacs.002.001.10"
PACS_004: Final = "pacs.004.001.09"
CAMT_056: Final = "camt.056.001.08"
CAMT_029: Final = "camt.029.001.09"
HEAD_001: Final = "head.001.001.02"

MT_TYPES: Final = frozenset({MT103, MT202, MT202COV})
MX_TYPES: Final = frozenset(
    {PACS_008, PACS_009, PACS_009_COV, PACS_002, PACS_004, CAMT_056, CAMT_029}
)

# Short names accepted on the ``ess send`` command line.
ALIASES: Final = {
    "mt103": MT103,
    "mt202": MT202,
    "mt202cov": MT202COV,
    "pacs.008": PACS_008,
    "pacs008": PACS_008,
    "pacs.009": PACS_009,
    "pacs009": PACS_009,
    "pacs.009cov": PACS_009_COV,
    "pacs009cov": PACS_009_COV,
    "pacs.002": PACS_002,
    "pacs.004": PACS_004,
    "camt.056": CAMT_056,
    "camt.029": CAMT_029,
}


def normalise_msg_type(value: str) -> str:
    """``pacs008`` / ``MT103`` / a full ISO name -> the canonical form."""
    key = value.strip()
    if key in MT_TYPES or key in MX_TYPES:
        return key
    return ALIASES.get(key.lower(), key)


def format_of(msg_type: str) -> Format:
    canonical = normalise_msg_type(msg_type)
    if canonical in MT_TYPES:
        return MT
    if canonical in MX_TYPES:
        return MX
    raise ValueError(f"unknown message type {msg_type!r}")


def is_cover(msg_type: str) -> bool:
    canonical = normalise_msg_type(msg_type)
    return canonical in (MT202COV, PACS_009_COV)


# ------------------------------------------------------------------ flows
FLOW_MT_FIN_103: Final = "MT_FIN_103"
FLOW_MT_FIN_103_202COV: Final = "MT_FIN_103_202COV"
FLOW_MT_TO_MX_103: Final = "MT_TO_MX_103"
FLOW_MX_SNF_PACS008: Final = "MX_SNF_PACS008"
FLOW_MX_SNF_PACS009: Final = "MX_SNF_PACS009"
FLOW_MX_SNF_PACS009COV_PAIR: Final = "MX_SNF_PACS009COV_PAIR"


@dataclass(frozen=True, slots=True)
class Flow:
    flow_id: str
    inbound_format: Format
    inbound_network: Network
    inbound_types: tuple[str, ...]
    outbound_format: Format
    outbound_network: Network
    cover_pair: bool = False
    translated: bool = False
    description: str = ""


FLOWS: Final[dict[str, Flow]] = {
    FLOW_MT_FIN_103: Flow(
        FLOW_MT_FIN_103,
        MT,
        FIN,
        (MT103,),
        MT,
        FIN,
        description="Single MT103 in and out over FIN",
    ),
    FLOW_MT_FIN_103_202COV: Flow(
        FLOW_MT_FIN_103_202COV,
        MT,
        FIN,
        (MT103, MT202COV),
        MT,
        FIN,
        cover_pair=True,
        description="Cover method: MT103 plus its MT202 COV, one UETR",
    ),
    FLOW_MT_TO_MX_103: Flow(
        FLOW_MT_TO_MX_103,
        MT,
        FIN,
        (MT103,),
        MX,
        SNF,
        translated=True,
        description="MT103 in over FIN, translated and sent on as pacs.008 over SnF",
    ),
    FLOW_MX_SNF_PACS008: Flow(
        FLOW_MX_SNF_PACS008,
        MX,
        SNF,
        (PACS_008,),
        MX,
        SNF,
        description="pacs.008 in and out over SnF; the highest-volume flow",
    ),
    FLOW_MX_SNF_PACS009: Flow(
        FLOW_MX_SNF_PACS009,
        MX,
        SNF,
        (PACS_009,),
        MX,
        SNF,
        description="FI transfer, no cover leg",
    ),
    FLOW_MX_SNF_PACS009COV_PAIR: Flow(
        FLOW_MX_SNF_PACS009COV_PAIR,
        MX,
        SNF,
        (PACS_008, PACS_009_COV),
        MX,
        SNF,
        cover_pair=True,
        description="Cover method: pacs.008 plus its pacs.009 COV, one UETR",
    ),
}


def flow_for(msg_type: str, *, cover: bool = False) -> str:
    """The default flow for a message type.

    The edge stamps this into baggage when the caller did not name a flow.
    A cover leg is only known to be part of a pair once both legs are seen,
    so ``cover`` is how a test declares its intent up front.
    """
    canonical = normalise_msg_type(msg_type)
    if canonical == MT103:
        return FLOW_MT_FIN_103_202COV if cover else FLOW_MT_FIN_103
    if canonical in (MT202, MT202COV):
        return FLOW_MT_FIN_103_202COV
    if canonical == PACS_008:
        return FLOW_MX_SNF_PACS009COV_PAIR if cover else FLOW_MX_SNF_PACS008
    if canonical == PACS_009:
        return FLOW_MX_SNF_PACS009
    if canonical == PACS_009_COV:
        return FLOW_MX_SNF_PACS009COV_PAIR
    return FLOW_MX_SNF_PACS008 if format_of(canonical) == MX else FLOW_MT_FIN_103


def get_flow(flow_id: str) -> Flow | None:
    return FLOWS.get(flow_id)
