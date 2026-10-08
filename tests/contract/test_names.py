"""The names in CLAUDE.md are the contract too.

Topic names, consumer groups and flow IDs appear in the design docs, in
operational runbooks and in Kibana queries. A rename is a breaking change, so
these tests pin them.
"""

from __future__ import annotations

import pytest
from hub_model import flows
from hub_model import topics as tp
from hub_telemetry.messaging import TopicSpec

from hub.ack_matcher.processor import AckMatcher
from hub.common.retry_consumer import RetryConsumer
from hub.db_sink.processor import DbSink
from hub.dispatcher.processor import FinDispatcher, MxDispatcher
from hub.fin_parser.processor import FinParser
from hub.mx_parser.processor import MxParser
from hub.routing.processor import Routing
from hub.screening.processor import Screening
from hub.settlement.processor import Settlement
from hub.status_api.processor import StatusApi

# Exactly the table in CLAUDE.md.
EXPECTED_TOPICS = (
    "hub.in.fin.raw",
    "hub.in.mx.raw",
    "hub.pay.canonical",
    "hub.pay.screened",
    "hub.pay.routed",
    "hub.out.fin",
    "hub.out.mx",
    "hub.net.ack",
    "hub.compliance.decision",
    "hub.pay.status",
    "hub.pay.state",
)

EXPECTED_GROUPS = (
    "cg-fin-parser",
    "cg-mx-parser",
    "cg-screening",
    "cg-routing",
    "cg-settlement",
    "cg-dispatch-fin",
    "cg-dispatch-mx",
    "cg-ack-matcher",
    "cg-status-api",
    "cg-db-sink",
)

EXPECTED_FLOWS = (
    "MT_FIN_103",
    "MT_FIN_103_202COV",
    "MT_TO_MX_103",
    "MX_SNF_PACS008",
    "MX_SNF_PACS009",
    "MX_SNF_PACS009COV_PAIR",
)


def test_topic_names() -> None:
    assert tp.ALL_TOPICS == EXPECTED_TOPICS


def test_consumer_group_names() -> None:
    assert (
        tp.CG_FIN_PARSER,
        tp.CG_MX_PARSER,
        tp.CG_SCREENING,
        tp.CG_ROUTING,
        tp.CG_SETTLEMENT,
        tp.CG_DISPATCH_FIN,
        tp.CG_DISPATCH_MX,
        tp.CG_ACK_MATCHER,
        tp.CG_STATUS_API,
        tp.CG_DB_SINK,
    ) == EXPECTED_GROUPS


def test_flow_ids() -> None:
    assert tuple(flows.FLOWS) == EXPECTED_FLOWS


def test_every_processor_uses_its_declared_consumer_group() -> None:
    assert FinParser.group_id == tp.CG_FIN_PARSER
    assert MxParser.group_id == tp.CG_MX_PARSER
    assert Screening.group_id == tp.CG_SCREENING
    assert Routing.group_id == tp.CG_ROUTING
    assert Settlement.group_id == tp.CG_SETTLEMENT
    assert FinDispatcher.group_id == tp.CG_DISPATCH_FIN
    assert MxDispatcher.group_id == tp.CG_DISPATCH_MX
    assert AckMatcher.group_id == tp.CG_ACK_MATCHER
    assert StatusApi.group_id == tp.CG_STATUS_API
    assert DbSink.group_id == tp.CG_DB_SINK
    assert RetryConsumer.group_id == "cg-retry"


def test_the_pipeline_reads_the_topics_the_design_doc_says() -> None:
    """Touchpoints 5-6, 8, 13-14, 16, 18, 23, 25 of section 6."""
    assert FinParser.input_topics == (tp.IN_FIN_RAW,)
    assert MxParser.input_topics == (tp.IN_MX_RAW,)
    assert Screening.input_topics == (tp.PAY_CANONICAL, tp.COMPLIANCE_DECISION)
    assert Routing.input_topics == (tp.PAY_SCREENED,)
    assert Settlement.input_topics == (tp.PAY_ROUTED,)
    assert FinDispatcher.input_topics == (tp.OUT_FIN,)
    assert MxDispatcher.input_topics == (tp.OUT_MX,)
    assert AckMatcher.input_topics == (tp.NET_ACK,)
    assert StatusApi.input_topics == (tp.PAY_STATUS,)
    assert DbSink.input_topics == (tp.PAY_STATUS,)


def test_only_pay_state_is_compacted() -> None:
    specs = {spec.name: spec for spec in tp.topic_specs()}
    compacted = [name for name, spec in specs.items() if spec.cleanup_policy == "compact"]
    assert compacted == [tp.PAY_STATE]


def test_every_payment_flow_topic_has_the_same_partition_count() -> None:
    """One UETR must map to the same partition number everywhere."""
    specs = {spec.name: spec for spec in tp.topic_specs(partitions=24)}
    counts = {specs[name].partitions for name in EXPECTED_TOPICS}
    assert counts == {24}


def test_retention_matches_the_topic_catalogue() -> None:
    specs = {spec.name: spec for spec in tp.topic_specs()}
    day = tp.DAY_MS
    assert specs[tp.IN_FIN_RAW].retention_ms == 7 * day, "the replay source keeps 7 days"
    assert specs[tp.IN_MX_RAW].retention_ms == 7 * day
    assert specs[tp.PAY_CANONICAL].retention_ms == 3 * day
    assert specs[tp.COMPLIANCE_DECISION].retention_ms == 7 * day
    assert specs[tp.PAY_STATUS].retention_ms == 7 * day


def test_every_failable_topic_has_a_full_retry_ladder() -> None:
    names = {spec.name for spec in tp.topic_specs()}
    for topic in (
        tp.IN_FIN_RAW,
        tp.IN_MX_RAW,
        tp.PAY_CANONICAL,
        tp.PAY_SCREENED,
        tp.PAY_ROUTED,
        tp.OUT_FIN,
        tp.OUT_MX,
        tp.NET_ACK,
        tp.COMPLIANCE_DECISION,
    ):
        for suffix in (tp.RETRY_30S_SUFFIX, tp.RETRY_5M_SUFFIX, tp.DLQ_SUFFIX):
            assert topic + suffix in names, f"{topic}{suffix} is missing"


@pytest.mark.parametrize(
    ("attempt", "expected"),
    [
        (1, "hub.pay.canonical.retry.30s"),
        (2, "hub.pay.canonical.retry.5m"),
        (3, "hub.pay.canonical.dlq"),
        (9, "hub.pay.canonical.dlq"),
    ],
)
def test_the_retry_ladder_ends_in_the_dlq(attempt: int, expected: str) -> None:
    assert tp.retry_topic(tp.PAY_CANONICAL, attempt) == expected


def test_a_retry_topic_resolves_back_to_its_origin() -> None:
    for suffix in (tp.RETRY_30S_SUFFIX, tp.RETRY_5M_SUFFIX, tp.DLQ_SUFFIX):
        assert tp.origin_topic(tp.PAY_ROUTED + suffix) == tp.PAY_ROUTED
    assert tp.origin_topic(tp.PAY_ROUTED) == tp.PAY_ROUTED


def test_local_specs_fit_one_broker() -> None:
    local = tp.local_topic_specs()
    assert all(spec.replication == 1 for spec in local)
    assert all(spec.partitions == 4 for spec in local)


def test_compacted_topics_carry_compaction_config() -> None:
    spec = next(s for s in tp.topic_specs() if s.name == tp.PAY_STATE)
    config = spec.config()
    assert config["cleanup.policy"] == "compact"
    assert "retention.ms" not in config, "a compacted topic is not time-limited"


def test_delete_topics_carry_retention_config() -> None:
    spec = TopicSpec("x", retention_ms=1000)
    assert spec.config() == {"cleanup.policy": "delete", "retention.ms": "1000"}
