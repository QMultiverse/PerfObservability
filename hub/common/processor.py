"""The processing loop every Hub stage runs.

One batch is read, processed and written in a single Kafka transaction, with
the consumer offsets committed inside it. A crash mid-batch therefore leaves
nothing half-done: the batch is simply redelivered.

A record that fails goes to ``<topic>.retry.30s``, then ``.retry.5m``, then
``.dlq``, inside that same transaction — so the offset advances and one bad
message never blocks its partition.
"""

from __future__ import annotations

import abc
import asyncio
import logging
import time
from collections.abc import Mapping, Sequence
from typing import Final

from hub_model import proto as pb
from hub_model import topics as tp
from hub_model.envelope import (
    TERMINAL_STATES,
    merge_stages,
    now_ns,
    state_record,
    status_event,
)
from hub_model.flows import get_flow
from hub_model.proto import FailedRecord, PaymentEnvelope, PaymentStateRecord, StatusEvent
from hub_telemetry import metrics
from hub_telemetry.events import Events
from hub_telemetry.messaging import Bus, BusConsumer, Record, TransactionAborted
from hub_telemetry.tracked import TrackedProducer, carry, consumed

from .config import ServiceSettings
from .state_store import StateStore

log = logging.getLogger(__name__)

# Headers the Hub adds on top of the context headers.
HDR_FORMAT: Final = "format"
HDR_MSG_TYPE: Final = "msg-type"
HDR_FLOW: Final = "flow"
HDR_ATTEMPT: Final = "hub-retry-attempt"
HDR_ORIGIN: Final = "hub-origin-topic"
HDR_RETRY_AT: Final = "hub-retry-at-ns"


class PermanentError(Exception):
    """The record can never succeed. Goes straight to the DLQ, no retries.

    Malformed content, a failed schema check, an unknown message type — none of
    those get better on a second attempt.
    """

    def __init__(self, message: str, *, code: str = "PERMANENT") -> None:
        super().__init__(message)
        self.code = code


class RetryableError(Exception):
    """A transient failure. Goes round the retry ladder."""

    def __init__(self, message: str, *, code: str = "TRANSIENT") -> None:
        super().__init__(message)
        self.code = code


class Outbox:
    """What a processor writes through.

    Everything produced here joins the open transaction, so outputs, status,
    state and offsets all commit together or not at all.
    """

    def __init__(self, producer: TrackedProducer, service: str, events: Events) -> None:
        self._producer = producer
        self._service = service
        self._events = events
        self.produced: list[tuple[str, str]] = []  # (topic, uetr), for tests

    def send(
        self,
        topic: str,
        key: str,
        value: bytes,
        headers: Mapping[str, str] | None = None,
    ) -> None:
        self._producer.produce(topic, key, value, headers)
        self.produced.append((topic, key))
        metrics.kafka_produced.labels(self._service, topic).inc()

    def send_envelope(
        self,
        topic: str,
        env: PaymentEnvelope,
        headers: Mapping[str, str] | None = None,
    ) -> None:
        self.send(topic, env.ref.uetr, env.SerializeToString(), envelope_headers(env, headers))

    def emit_status(
        self,
        env: PaymentEnvelope,
        *,
        reason: str = "",
        error_code: str = "",
        state: pb.PaymentState | None = None,
    ) -> StatusEvent:
        event = status_event(
            env, service=self._service, reason=reason, error_code=error_code, state=state
        )
        self.send(tp.PAY_STATUS, env.ref.uetr, event.SerializeToString(), envelope_headers(env))
        state_name = pb.state_name(event.state)
        metrics.payments_state.labels(self._service, pb.format_name(env.format), state_name).inc()
        # The status topic is for machines; this is the same transition as a
        # searchable ECS event, which is what makes the Kibana payment journey
        # show state changes alongside the hops.
        self._events.payment_state(state_name, reason=reason, **{"error.code": error_code or None})
        return event

    def emit_state(self, env: PaymentEnvelope, *, stages: Sequence[str] = ()) -> None:
        record = state_record(env, stages_done=stages)
        self.send(tp.PAY_STATE, env.ref.uetr, record.SerializeToString(), envelope_headers(env))

    def advance_to(
        self,
        env: PaymentEnvelope,
        topic: str,
        *,
        stages: Sequence[str] = (),
        reason: str = "",
    ) -> None:
        """The usual move: next topic, a status event and the state record."""
        self.send_envelope(topic, env)
        self.emit_status(env, reason=reason)
        self.emit_state(env, stages=stages)


def envelope_headers(
    env: PaymentEnvelope, extra: Mapping[str, str] | None = None
) -> dict[str, str]:
    """Headers that let a consumer split MT from MX without deserialising."""
    headers = {
        HDR_FORMAT: pb.format_name(env.format),
        HDR_MSG_TYPE: env.ref.msg_type,
        HDR_FLOW: env.ref.flow,
    }
    if extra:
        headers.update(extra)
    return headers


class Processor(abc.ABC):
    """One stage. Subclasses implement :meth:`handle` and nothing else."""

    #: Service name, e.g. ``hub-screening``. Used for logs and metric labels.
    name: str = "hub-processor"
    #: Stage marker written into ``stages_done`` for idempotency.
    stage: str = ""
    #: Consumer group, from the catalogue in CLAUDE.md.
    group_id: str = ""
    #: Topics this stage reads.
    input_topics: Sequence[str] = ()
    #: Whether this stage also reads the compacted ``hub.pay.state`` topic to
    #: rebuild its local store — cover legs waiting for a partner, payments
    #: HELD for a decision, payments dispatched and awaiting an ACK. Records
    #: from that topic are folded in *before* the batch is processed, so an
    #: ACK that arrives in the same batch as its payment still matches.
    reads_state: bool = False

    def __init__(self, settings: ServiceSettings, events: Events) -> None:
        self.settings = settings
        self.events = events
        self.store = StateStore()

    @abc.abstractmethod
    async def handle(self, record: Record, out: Outbox) -> None:
        """Process one record, writing everything through ``out``.

        Raise :class:`RetryableError` for something that may succeed later, and
        :class:`PermanentError` for something that never will.
        """

    def stage_for(self, env: PaymentEnvelope) -> str:
        """The idempotency marker for this stage *and this message*.

        A cover pair is two messages under one UETR, so a bare stage name
        would let the first leg mark the stage done and the second be skipped.
        On a cover flow the marker carries the message type; everywhere else
        it is the plain stage name.
        """
        flow = get_flow(env.ref.flow)
        if flow is not None and flow.cover_pair and env.ref.msg_type:
            return f"{self.stage}:{env.ref.msg_type}"
        return self.stage

    def done(self, env: PaymentEnvelope) -> bool:
        """Whether this stage has already handled this message."""
        return self.store.already_done(env.ref.uetr, self.stage_for(env))

    def mark(self, env: PaymentEnvelope, *extra: str) -> list[str]:
        """Record the stage as done and return the accumulated stage list.

        The returned list goes into the compacted state record, so
        ``stages_done`` accumulates across the pipeline instead of each stage
        overwriting the last one's work.
        """
        stages = (self.stage_for(env), *extra)
        entry = self.store.record(env, stages=stages)
        return list(entry.stages_done)

    def handle_state(self, record: Record) -> None:
        """Fold one ``hub.pay.state`` record into the local store.

        Terminal payments are skipped: they need no further work, and keeping
        them would grow the store without bound.
        """
        state = PaymentStateRecord()
        try:
            state.ParseFromString(record.value)
        except Exception:
            log.warning("undecodable PaymentStateRecord on %s", record.topic)
            return
        if state.state in TERMINAL_STATES:
            self.store.complete(state.ref.uetr)
            return
        if self.store.is_complete(state.ref.uetr):
            # A network ACK can beat the dispatcher's commit, so a payment's
            # DISPATCHED record may arrive after it COMPLETED. Applying it would
            # put a finished payment back in the store as waiting for an ACK.
            return
        self.store.apply_state_record(state)
        flow = get_flow(state.ref.flow)
        if flow is not None and flow.cover_pair and state.HasField("envelope"):
            # Both legs share the UETR and overwrite each other on the
            # compacted topic, so keep them apart by message type.
            self.store.add_cover_leg(state.envelope)

    async def on_batch_start(self) -> None:  # noqa: B027 - an optional hook
        """Hook for work that belongs once per batch, not once per record."""

    async def on_batch_committed(self, count: int) -> None:  # noqa: B027 - optional
        """Hook that runs only after the transaction actually committed."""

    async def close(self) -> None:  # noqa: B027 - optional; most stages hold nothing
        """Release anything the stage opened (channels, pools)."""


class ProcessorRunner:
    """Drives a :class:`Processor` against a bus."""

    def __init__(
        self,
        processor: Processor,
        bus: Bus,
        settings: ServiceSettings,
        events: Events,
        *,
        max_retries: int = 3,
    ) -> None:
        self.processor = processor
        self.bus = bus
        self.settings = settings
        self.events = events
        self.max_retries = max_retries
        self._stopping = asyncio.Event()

        topics = list(processor.input_topics)
        if processor.reads_state and tp.PAY_STATE not in topics:
            topics.append(tp.PAY_STATE)
        self.topics = topics
        self.consumer: BusConsumer = bus.consumer(processor.group_id, topics)
        self.producer = TrackedProducer(
            bus.producer(settings.transactional_id(processor.stage or processor.group_id)),
            processor.events,
        )
        metrics.state_stores.register(processor.name, processor.store)

    # ------------------------------------------------------------- driving
    async def run_forever(self) -> None:
        log.info(
            "%s consuming %s as %s",
            self.processor.name,
            ", ".join(self.topics),
            self.processor.group_id,
        )
        while not self._stopping.is_set():
            handled = await self.run_once()
            if handled == 0:
                # Nothing to do; yield rather than spin the CPU. Idle time is
                # waiting for records too, so it counts towards IO wait.
                idle = self.settings.kafka.poll_timeout_s
                await asyncio.sleep(idle)
                metrics.kafka_io_wait.labels(self.processor.name).inc(idle)

    async def run_once(self) -> int:
        """Read, process and commit one batch. Returns the record count."""
        name = self.processor.name
        polled = time.perf_counter()
        batch = self.consumer.consume(
            max_records=self.settings.kafka.batch_size,
            timeout_s=self.settings.kafka.poll_timeout_s,
        )
        poll_s = time.perf_counter() - polled
        metrics.kafka_poll.labels(name).observe(poll_s)
        metrics.kafka_io_wait.labels(name).inc(poll_s)
        if not batch:
            return 0

        started = time.perf_counter()
        await self.processor.on_batch_start()

        # State records first: a payment and the ACK that completes it can
        # arrive in one batch, and the ACK matcher needs the payment already
        # in its store when it gets there.
        work: list[Record] = []
        for record in batch:
            if self.processor.reads_state and record.topic == tp.PAY_STATE:
                self.processor.handle_state(record)
            else:
                work.append(record)

        self.producer.begin()
        outbox = Outbox(self.producer, self.processor.name, self.processor.events)

        for record in work:
            await self._handle_one(record, outbox)

        committing = time.perf_counter()
        try:
            self.producer.commit(self.consumer)
        except TransactionAborted as exc:
            metrics.kafka_transactions_aborted.labels(self.processor.name, "commit").inc()
            log.error("%s transaction aborted: %s", self.processor.name, exc)
            return 0

        commit_s = time.perf_counter() - committing
        metrics.kafka_commit.labels(name).observe(commit_s)
        metrics.kafka_commit_max.observe((name,), commit_s)
        metrics.kafka_transaction_latency.labels(self.processor.name).observe(
            time.perf_counter() - started
        )
        for record in batch:
            metrics.kafka_consumed.labels(
                self.processor.name, record.topic, self.processor.group_id
            ).inc()
            if record.lag is not None:
                metrics.kafka_consumer_lag.labels(
                    self.processor.name,
                    record.topic,
                    self.processor.group_id,
                    str(record.partition),
                ).set(record.lag)
        # Work done after the commit (the DB sink's PostgreSQL write) is not
        # in process latency, so it is timed on its own: without it a slow
        # database is invisible on the process panels (perf scenario 2).
        post_started = time.perf_counter()
        await self.processor.on_batch_committed(len(batch))
        post_s = time.perf_counter() - post_started
        metrics.post_commit.labels(name).observe(post_s)
        metrics.post_commit_max.observe((name,), post_s)
        return len(batch)

    async def _handle_one(self, record: Record, outbox: Outbox) -> None:
        name = self.processor.name
        started = time.perf_counter()
        with consumed(record, self.events, self.processor.group_id):
            try:
                await self.processor.handle(record, outbox)
                outcome = "ok"
            except PermanentError as exc:
                self._dead_letter(record, outbox, exc, exc.code, permanent=True)
                outcome = "dlq"
            except RetryableError as exc:
                outcome = self._retry(record, outbox, exc, exc.code)
            except Exception as exc:
                log.exception("%s failed on %s", self.processor.name, record.topic)
                outcome = self._retry(record, outbox, exc, "UNEXPECTED")
        elapsed = time.perf_counter() - started
        metrics.process_latency.labels(name).observe(elapsed)
        metrics.process_latency_max.observe((name,), elapsed)
        metrics.records_processed.labels(name, outcome).inc()

    # --------------------------------------------------------- retry ladder
    def _retry(self, record: Record, outbox: Outbox, exc: Exception, code: str) -> str:
        """Send ``record`` down the ladder; returns ``retry`` or ``dlq``."""
        attempt = _attempt_of(record) + 1
        origin = record.headers.get(HDR_ORIGIN) or tp.origin_topic(record.topic)
        if attempt > self.max_retries:
            self._dead_letter(record, outbox, exc, code, permanent=False)
            return "dlq"
        target = tp.retry_topic(origin, attempt)
        self._publish_failure(record, outbox, exc, code, target, attempt, origin)
        return "retry"

    def _dead_letter(
        self,
        record: Record,
        outbox: Outbox,
        exc: Exception,
        code: str,
        *,
        permanent: bool,
    ) -> None:
        origin = record.headers.get(HDR_ORIGIN) or tp.origin_topic(record.topic)
        attempt = _attempt_of(record) + 1
        self._publish_failure(
            record, outbox, exc, code, tp.dlq_topic(origin), attempt, origin, permanent=permanent
        )

    def _publish_failure(
        self,
        record: Record,
        outbox: Outbox,
        exc: Exception,
        code: str,
        target: str,
        attempt: int,
        origin: str,
        *,
        permanent: bool = False,
    ) -> None:
        failed = FailedRecord(
            origin_topic=origin,
            payload=record.value,
            error_type=type(exc).__name__,
            error_message=str(exc)[:2000],
            attempt=attempt,
            first_failed_ns=_first_failed_ns(record),
            failed_ns=now_ns(),
            consumer_group=self.processor.group_id,
        )
        failed.ref.uetr = record.key
        failed.ref.msg_type = record.headers.get(HDR_MSG_TYPE, "")
        failed.ref.flow = record.headers.get(HDR_FLOW, "")

        headers = carry(
            record,
            {
                HDR_ATTEMPT: str(attempt),
                HDR_ORIGIN: origin,
                HDR_RETRY_AT: str(now_ns() + int(tp.retry_delay_s(target) * 1e9)),
            },
        )
        outbox.send(target, record.key, failed.SerializeToString(), headers)

        metrics.payments_errors.labels(self.processor.name, self.processor.stage, code).inc()
        metrics.payments_retried.labels(self.processor.name, target).inc()
        self.processor.events.payment_error(
            code,
            message=f"{type(exc).__name__}: {exc}",
            error_type=type(exc).__name__,
            retry_target=target,
            attempt=attempt,
            exc_info=not permanent,
        )
        self._emit_failure_status(record, outbox, code, target, permanent)

    def _emit_failure_status(
        self,
        record: Record,
        outbox: Outbox,
        code: str,
        target: str,
        permanent: bool,
    ) -> None:
        """Keep the journey complete: a failed payment still gets a status."""
        env = PaymentEnvelope()
        env.ref.uetr = record.key
        env.ref.msg_type = record.headers.get(HDR_MSG_TYPE, "")
        env.ref.flow = record.headers.get(HDR_FLOW, "")
        env.format = _format_of(record)
        env.state = pb.REJECTED if permanent else pb.FAILED
        outbox.emit_status(env, reason=f"sent to {target}", error_code=code)

    # ------------------------------------------------------------ lifecycle
    def stop(self) -> None:
        self._stopping.set()

    async def close(self) -> None:
        self.stop()
        metrics.state_stores.unregister(self.processor.name)
        await self.processor.close()
        self.producer.close()
        self.consumer.close()


def _attempt_of(record: Record) -> int:
    try:
        return int(record.headers.get(HDR_ATTEMPT, "0"))
    except ValueError:
        return 0


def _first_failed_ns(record: Record) -> int:
    raw = record.headers.get("hub-first-failed-ns")
    try:
        return int(raw) if raw else now_ns()
    except ValueError:
        return now_ns()


def _format_of(record: Record) -> pb.Format:
    name = record.headers.get(HDR_FORMAT, "")
    if name == "MT":
        return pb.MT
    if name == "MX":
        return pb.MX
    return pb.FORMAT_UNSPECIFIED


def mark_done(store: StateStore, env: PaymentEnvelope, stage: str) -> list[str]:
    """Record a completed stage and return the full stage list for the state topic."""
    entry = store.record(env, stages=(stage,))
    return merge_stages(entry.stages_done, (stage,))
