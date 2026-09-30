"""ECS JSON logging to stdout, with baggage on every line.

Every event is one JSON line in Elastic Common Schema format, picked up from
container stdout by Filebeat and shipped into the ``logs-payments-*`` data
stream.

Two things keep logging off the payment's critical path:

* a :class:`~logging.handlers.QueueHandler` in front of the stdout handler, so
  a slow writer never blocks a processor;
* :func:`~hub_telemetry.tracking.should_log`, checked by the caller before an
  event is even built.
"""

from __future__ import annotations

import atexit
import datetime as dt
import json
import logging
import logging.handlers
import os
import queue
import sys
import threading
from typing import Any, Final, TextIO

from opentelemetry import trace

from . import baggage

# Standard LogRecord attributes; anything else on a record is ours and is
# promoted to a top-level ECS field.
_RESERVED: Final = frozenset(
    {
        "args",
        "asctime",
        "created",
        "exc_info",
        "exc_text",
        "filename",
        "funcName",
        "levelname",
        "levelno",
        "lineno",
        "module",
        "msecs",
        "message",
        "msg",
        "name",
        "pathname",
        "process",
        "processName",
        "relativeCreated",
        "stack_info",
        "taskName",
        "thread",
        "threadName",
    }
)

_LEVEL_TO_ECS: Final = {
    "DEBUG": "debug",
    "INFO": "info",
    "WARNING": "warning",
    "ERROR": "error",
    "CRITICAL": "critical",
}

_listener_lock = threading.Lock()
_listener: logging.handlers.QueueListener | None = None


class BaggageFilter(logging.Filter):
    """Copy the current baggage onto every record.

    Ordinary business logs become searchable by UETR without the caller
    passing identifiers around.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        for key, value in baggage.get_all().items():
            if not hasattr(record, key):
                setattr(record, key, value)
        return True


class TraceContextFilter(logging.Filter):
    """Add ``trace.id`` and ``span.id`` so logs and traces join in Kibana."""

    def filter(self, record: logging.LogRecord) -> bool:
        span = trace.get_current_span()
        ctx = span.get_span_context()
        if ctx.is_valid:
            record.__dict__.setdefault("trace.id", format(ctx.trace_id, "032x"))
            record.__dict__.setdefault("span.id", format(ctx.span_id, "016x"))
        return True


class EcsFormatter(logging.Formatter):
    """Render a record as one ECS JSON line."""

    def __init__(self, service_name: str, *, environment: str = "", version: str = "") -> None:
        super().__init__()
        self.service_name = service_name
        self.environment = environment
        self.version = version

    def format(self, record: logging.LogRecord) -> str:
        doc: dict[str, Any] = {
            "@timestamp": _iso_ts(record.created),
            "log.level": _LEVEL_TO_ECS.get(record.levelname, record.levelname.lower()),
            "service.name": self.service_name,
            "message": record.getMessage(),
        }
        if self.environment:
            doc["service.environment"] = self.environment
        if self.version:
            doc["service.version"] = self.version

        for key, value in record.__dict__.items():
            if key in _RESERVED or key.startswith("_"):
                continue
            if value is None:
                continue
            # An event that names its own service wins over the process
            # default; everything else is added once.
            if key in doc and key != "service.name":
                continue
            doc[key] = value

        if record.exc_info:
            exc_type, exc, _ = record.exc_info
            doc["error.type"] = exc_type.__name__ if exc_type else ""
            doc["error.message"] = str(exc)
            doc["error.stack_trace"] = self.formatException(record.exc_info)
        if record.stack_info:
            doc["error.stack_trace"] = record.stack_info

        doc.setdefault("log.logger", record.name)
        return json.dumps(doc, default=_fallback, separators=(",", ":"))


def _iso_ts(created: float) -> str:
    moment = dt.datetime.fromtimestamp(created, tz=dt.UTC)
    return moment.strftime("%Y-%m-%dT%H:%M:%S.") + f"{moment.microsecond // 1000:03d}Z"


def _fallback(value: object) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8", "replace")
    return repr(value)


def setup_logging(
    service_name: str,
    *,
    level: int | str = logging.INFO,
    stream: TextIO | None = None,
    asynchronous: bool = True,
    environment: str = "",
    version: str = "",
) -> logging.Logger:
    """Configure root logging for a Hub service or the ESS.

    Idempotent: calling it twice replaces the handlers rather than doubling
    every line.
    """
    global _listener

    root = logging.getLogger()
    _shutdown_listener()
    for existing in list(root.handlers):
        root.removeHandler(existing)
        existing.close()

    formatter = EcsFormatter(service_name, environment=environment, version=version)
    sink: logging.Handler = logging.StreamHandler(stream if stream is not None else sys.stdout)
    sink.setFormatter(formatter)

    handler: logging.Handler
    if asynchronous:
        log_queue: queue.Queue[logging.LogRecord] = queue.Queue(maxsize=20_000)
        # The queue is bounded: under a log storm we would rather drop lines
        # than add latency to the payment path.
        handler = _NonBlockingQueueHandler(log_queue)
        with _listener_lock:
            _listener = logging.handlers.QueueListener(log_queue, sink, respect_handler_level=False)
            _listener.start()
        atexit.register(_shutdown_listener)
    else:
        handler = sink

    handler.addFilter(BaggageFilter())
    handler.addFilter(TraceContextFilter())
    root.addHandler(handler)
    root.setLevel(level)

    # Chatty third parties, muted so they cannot swamp payment events.
    for noisy in ("grpc", "urllib3", "asyncio", "elasticsearch"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    return logging.getLogger(service_name)


class _NonBlockingQueueHandler(logging.handlers.QueueHandler):
    """Drop the record instead of blocking when the queue is full."""

    def __init__(self, log_queue: queue.Queue[logging.LogRecord]) -> None:
        super().__init__(log_queue)
        self.dropped = 0

    def enqueue(self, record: logging.LogRecord) -> None:
        try:
            self.queue.put_nowait(record)
        except queue.Full:
            self.dropped += 1


def _shutdown_listener() -> None:
    global _listener
    with _listener_lock:
        if _listener is not None:
            _listener.stop()
            _listener = None


def setup_from_env(service_name: str, *, stream: TextIO | None = None) -> logging.Logger:
    """Configure logging from ``HUB_LOG_LEVEL`` / ``HUB_ENV`` / ``HUB_VERSION``."""
    return setup_logging(
        service_name,
        level=os.environ.get("HUB_LOG_LEVEL", "INFO").upper(),
        stream=stream,
        asynchronous=os.environ.get("HUB_LOG_ASYNC", "1") != "0",
        environment=os.environ.get("HUB_ENV", ""),
        version=os.environ.get("HUB_VERSION", ""),
    )
