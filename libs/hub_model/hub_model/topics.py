"""Topic names, consumer groups and the retry / DLQ ladder.

Every name in the design doc appears exactly once, here. Services import the
constant; nothing builds a topic name from a string literal.
"""

from __future__ import annotations

from typing import Final

from hub_telemetry.messaging import TopicSpec

DAY_MS: Final = 24 * 60 * 60 * 1000

# --------------------------------------------------------------- topics
IN_FIN_RAW: Final = "hub.in.fin.raw"
IN_MX_RAW: Final = "hub.in.mx.raw"
PAY_CANONICAL: Final = "hub.pay.canonical"
PAY_SCREENED: Final = "hub.pay.screened"
PAY_ROUTED: Final = "hub.pay.routed"
OUT_FIN: Final = "hub.out.fin"
OUT_MX: Final = "hub.out.mx"
NET_ACK: Final = "hub.net.ack"
FCC_DECISION: Final = "hub.fcc.decision"
PAY_STATUS: Final = "hub.pay.status"
PAY_STATE: Final = "hub.pay.state"

ALL_TOPICS: Final = (
    IN_FIN_RAW,
    IN_MX_RAW,
    PAY_CANONICAL,
    PAY_SCREENED,
    PAY_ROUTED,
    OUT_FIN,
    OUT_MX,
    NET_ACK,
    FCC_DECISION,
    PAY_STATUS,
    PAY_STATE,
)

# -------------------------------------------------------- consumer groups
CG_FIN_PARSER: Final = "cg-fin-parser"
CG_MX_PARSER: Final = "cg-mx-parser"
CG_SCREENING: Final = "cg-screening"
CG_ROUTING: Final = "cg-routing"
CG_SETTLEMENT: Final = "cg-settlement"
CG_DISPATCH_FIN: Final = "cg-dispatch-fin"
CG_DISPATCH_MX: Final = "cg-dispatch-mx"
CG_ACK_MATCHER: Final = "cg-ack-matcher"
CG_STATUS_API: Final = "cg-status-api"
CG_DB_SINK: Final = "cg-db-sink"

# --------------------------------------------------------- retry ladder
RETRY_30S_SUFFIX: Final = ".retry.30s"
RETRY_5M_SUFFIX: Final = ".retry.5m"
DLQ_SUFFIX: Final = ".dlq"

RETRY_DELAYS_S: Final = (30.0, 300.0)


def retry_topic(topic: str, attempt: int) -> str:
    """Where a record goes after its ``attempt``-th failure (1-based).

    One failing message must never block a partition, so the record leaves the
    main topic immediately and comes back later — or lands in the DLQ.
    """
    if attempt <= 1:
        return topic + RETRY_30S_SUFFIX
    if attempt == 2:
        return topic + RETRY_5M_SUFFIX
    return topic + DLQ_SUFFIX


def dlq_topic(topic: str) -> str:
    return topic + DLQ_SUFFIX


def is_retry_topic(topic: str) -> bool:
    return topic.endswith((RETRY_30S_SUFFIX, RETRY_5M_SUFFIX))


def origin_topic(topic: str) -> str:
    """Strip a retry / DLQ suffix back to the topic the record came from."""
    for suffix in (RETRY_30S_SUFFIX, RETRY_5M_SUFFIX, DLQ_SUFFIX):
        if topic.endswith(suffix):
            return topic[: -len(suffix)]
    return topic


def retry_delay_s(topic: str) -> float:
    if topic.endswith(RETRY_30S_SUFFIX):
        return RETRY_DELAYS_S[0]
    if topic.endswith(RETRY_5M_SUFFIX):
        return RETRY_DELAYS_S[1]
    return 0.0


# --------------------------------------------------------- topic catalogue
def topic_specs(partitions: int = 24, replication: int = 3) -> list[TopicSpec]:
    """The full catalogue from design doc section 7.

    Every payment-flow topic gets the same partition count, so one UETR maps to
    the same partition number everywhere. ``hub.pay.state`` is compacted: it
    holds the latest state per UETR, not a history.
    """
    retention = {
        IN_FIN_RAW: 7 * DAY_MS,  # the replay source
        IN_MX_RAW: 7 * DAY_MS,
        PAY_CANONICAL: 3 * DAY_MS,
        PAY_SCREENED: 3 * DAY_MS,
        PAY_ROUTED: 3 * DAY_MS,
        OUT_FIN: 3 * DAY_MS,
        OUT_MX: 3 * DAY_MS,
        NET_ACK: 3 * DAY_MS,
        FCC_DECISION: 7 * DAY_MS,
        PAY_STATUS: 7 * DAY_MS,
    }

    specs = [
        TopicSpec(name, partitions=partitions, replication=replication, retention_ms=ms)
        for name, ms in retention.items()
    ]
    specs.append(
        TopicSpec(
            PAY_STATE,
            partitions=partitions,
            replication=replication,
            cleanup_policy="compact",
        )
    )
    # The retry ladder mirrors every topic a consumer can fail on.
    for name in (
        IN_FIN_RAW,
        IN_MX_RAW,
        PAY_CANONICAL,
        PAY_SCREENED,
        PAY_ROUTED,
        OUT_FIN,
        OUT_MX,
        NET_ACK,
        FCC_DECISION,
    ):
        for suffix in (RETRY_30S_SUFFIX, RETRY_5M_SUFFIX, DLQ_SUFFIX):
            specs.append(
                TopicSpec(
                    name + suffix,
                    partitions=partitions,
                    replication=replication,
                    retention_ms=14 * DAY_MS,
                )
            )
    return specs


def local_topic_specs() -> list[TopicSpec]:
    """The same catalogue sized for one broker on a laptop."""
    return [
        TopicSpec(
            spec.name,
            partitions=4,
            replication=1,
            cleanup_policy=spec.cleanup_policy,
            retention_ms=spec.retention_ms,
        )
        for spec in topic_specs()
    ]
