"""The retry consumer: puts delayed records back on their origin topic.

A stage that fails a record writes a :class:`FailedRecord` to
``<topic>.retry.30s`` (then ``.retry.5m``), with the earliest time it may be
retried in the ``hub-retry-at-ns`` header. This service waits for that time and
republishes the original payload to the origin topic, attempt count carried
forward.

Waiting *here* rather than in the failing stage is what keeps the promise that
one bad message never blocks a partition: the main topic's offset has already
moved on.

Records in ``.dlq`` are never republished. They are for operators.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Sequence
from typing import Final

from hub_model import proto as pb
from hub_model import topics as tp
from hub_model.envelope import now_ns
from hub_model.proto import FailedRecord
from hub_telemetry import metrics
from hub_telemetry.events import Events
from hub_telemetry.messaging import Record

from .config import ServiceSettings
from .processor import (
    HDR_ATTEMPT,
    HDR_FLOW,
    HDR_MSG_TYPE,
    HDR_ORIGIN,
    HDR_RETRY_AT,
    Outbox,
    PermanentError,
    Processor,
)

log = logging.getLogger(__name__)

SERVICE_NAME: Final = "hub-retry"
GROUP_ID: Final = "cg-retry"

#: Topics whose retry ladders this service drains.
RETRYABLE_TOPICS: Final = (
    tp.IN_FIN_RAW,
    tp.IN_MX_RAW,
    tp.PAY_CANONICAL,
    tp.PAY_SCREENED,
    tp.PAY_ROUTED,
    tp.OUT_FIN,
    tp.OUT_MX,
    tp.NET_ACK,
    tp.FCC_DECISION,
)


def retry_topics(topics: Sequence[str] = RETRYABLE_TOPICS) -> list[str]:
    return [
        topic + suffix for topic in topics for suffix in (tp.RETRY_30S_SUFFIX, tp.RETRY_5M_SUFFIX)
    ]


class RetryConsumer(Processor):
    """Delayed republish from the retry topics to their origins."""

    name = SERVICE_NAME
    stage = "retry"
    group_id = GROUP_ID
    input_topics = tuple(retry_topics())

    def __init__(self, settings: ServiceSettings, events: Events) -> None:
        super().__init__(settings, events)
        self.republished = 0

    async def handle(self, record: Record, out: Outbox) -> None:
        failed = FailedRecord()
        try:
            failed.ParseFromString(record.value)
        except Exception as exc:
            raise PermanentError(f"undecodable FailedRecord: {exc}", code="RETRY_BAD") from exc

        origin = (
            failed.origin_topic or record.headers.get(HDR_ORIGIN) or tp.origin_topic(record.topic)
        )
        await self._wait_until_due(record)

        headers = {
            HDR_ATTEMPT: str(failed.attempt),
            HDR_ORIGIN: origin,
            HDR_MSG_TYPE: failed.ref.msg_type,
            HDR_FLOW: failed.ref.flow,
            "hub-first-failed-ns": str(failed.first_failed_ns or now_ns()),
        }
        out.send(origin, record.key or failed.ref.uetr, failed.payload, headers)
        self.republished += 1
        metrics.payments_retried.labels(SERVICE_NAME, origin).inc()
        log.info(
            "republished %s to %s (attempt %d, %s)",
            failed.ref.uetr,
            origin,
            failed.attempt,
            failed.error_type,
        )

    async def _wait_until_due(self, record: Record) -> None:
        """Sleep until the record's retry time.

        The batch is small and ordered by delay, so sleeping inside the
        transaction is simpler than a timer wheel and costs nothing but a held
        transaction — which is why the batch size for this stage is low.
        """
        raw = record.headers.get(HDR_RETRY_AT)
        if not raw:
            return
        try:
            due_ns = int(raw)
        except ValueError:
            return
        wait_s = (due_ns - now_ns()) / 1e9
        if wait_s > 0:
            await asyncio.sleep(min(wait_s, tp.RETRY_DELAYS_S[-1]))


def build(settings: ServiceSettings, events: Events) -> RetryConsumer:
    # A long delay per record means small batches; a big one would hold the
    # transaction open past the broker's transaction timeout.
    settings.kafka.batch_size = min(settings.kafka.batch_size, 20)
    return RetryConsumer(settings, events)


__all__ = ["GROUP_ID", "RETRYABLE_TOPICS", "RetryConsumer", "build", "pb", "retry_topics"]
