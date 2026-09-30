"""The one place a touchpoint event is emitted.

Each method maps to an ``event.action`` from design doc section 9 and checks
the payment's tracking level *first*, so ``errors`` and ``none`` cost nothing
on a healthy payment.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Any

from . import baggage, tracking
from .tracking import Level


class Events:
    """Emitter bound to one service name."""

    def __init__(self, service: str, logger: logging.Logger | None = None) -> None:
        self.service = service
        self._log = logger or logging.getLogger(service)

    # ------------------------------------------------------------ helpers
    def _level(self) -> Level:
        return Level.parse(baggage.current_trace_level(), Level.FULL)

    def _emit(
        self,
        action: str,
        message: str,
        fields: Mapping[str, Any] | None = None,
        *,
        outcome: str = "success",
        level: int = logging.INFO,
        state: str | None = None,
    ) -> None:
        """Emit one event, if the payment level admits this action.

        ``fields`` is a mapping rather than ``**kwargs`` because every ECS key
        here contains dots, and so can never be a Python keyword.
        """
        if not tracking.should_log(self._level(), action, state=state):
            return
        extra: dict[str, Any] = {
            "event.action": action,
            "event.outcome": outcome,
            # The emitting service, stamped per event rather than taken from
            # the process: in a test harness several stages share a process.
            "service.name": self.service,
        }
        extra.update({k: v for k, v in (fields or {}).items() if v is not None})
        self._log.log(level, message, extra=extra)

    # -------------------------------------------------------------- gRPC
    def grpc_server_recv(self, method: str, *, peer: str = "", bytes_in: int | None = None) -> None:
        self._emit(
            tracking.GRPC_SERVER_RECV,
            f"recv {method}",
            {
                "rpc.method": method,
                "client.address": peer or None,
                "http.request.body.size": bytes_in,
            },
        )

    def grpc_server_reply(
        self,
        method: str,
        *,
        status_code: str = "OK",
        duration_ns: int = 0,
        outcome: str = "success",
    ) -> None:
        self._emit(
            tracking.GRPC_SERVER_REPLY,
            f"reply {method} {status_code}",
            {
                "rpc.method": method,
                "rpc.grpc.status_code": status_code,
                "event.duration": duration_ns,
            },
            outcome=outcome,
            level=logging.INFO if outcome == "success" else logging.WARNING,
        )

    def grpc_client_send(self, method: str, *, target: str = "", attempt: int = 1) -> None:
        self._emit(
            tracking.GRPC_CLIENT_SEND,
            f"send {method}",
            {
                "rpc.method": method,
                "server.address": target or None,
                "rpc.retry_count": attempt - 1,
            },
        )

    def grpc_client_reply(
        self,
        method: str,
        *,
        status_code: str = "OK",
        duration_ns: int = 0,
        attempt: int = 1,
        outcome: str = "success",
    ) -> None:
        self._emit(
            tracking.GRPC_CLIENT_REPLY,
            f"reply {method} {status_code}",
            {
                "rpc.method": method,
                "rpc.grpc.status_code": status_code,
                "rpc.retry_count": attempt - 1,
                "event.duration": duration_ns,
            },
            outcome=outcome,
            level=logging.INFO if outcome == "success" else logging.WARNING,
        )

    # ------------------------------------------------------------- Kafka
    def kafka_produce(
        self,
        topic: str,
        *,
        partition: int = -1,
        offset: int = -1,
        size_bytes: int = 0,
        duration_ns: int = 0,
    ) -> None:
        self._emit(
            tracking.KAFKA_PRODUCE,
            f"produce {topic}",
            {
                "messaging.system": "kafka",
                "messaging.destination": topic,
                "messaging.kafka.partition": partition,
                "messaging.kafka.offset": offset,
                "messaging.message.body.size": size_bytes,
                "event.duration": duration_ns,
            },
        )

    def kafka_consume(
        self,
        topic: str,
        *,
        partition: int = -1,
        offset: int = -1,
        consumer_group: str = "",
        lag: int | None = None,
        size_bytes: int = 0,
    ) -> None:
        self._emit(
            tracking.KAFKA_CONSUME,
            f"consume {topic}",
            {
                "messaging.system": "kafka",
                "messaging.source": topic,
                "messaging.kafka.partition": partition,
                "messaging.kafka.offset": offset,
                "messaging.kafka.consumer.group": consumer_group or None,
                "messaging.kafka.consumer.lag": lag,
                "messaging.message.body.size": size_bytes,
            },
        )

    # ----------------------------------------------------------- payment
    def payment_state(self, state: str, *, reason: str = "", **fields: Any) -> None:
        failed = state.upper() in tracking.FAILURE_STATES
        self._emit(
            tracking.PAYMENT_STATE,
            f"state {state}",
            {"payment.state": state, "payment.state_reason": reason or None, **fields},
            state=state,
            outcome="failure" if failed else "success",
            level=logging.WARNING if failed else logging.INFO,
        )

    def payment_error(
        self,
        error_code: str,
        *,
        message: str = "",
        error_type: str = "",
        retry_target: str = "",
        attempt: int | None = None,
        exc_info: bool = False,
    ) -> None:
        # payment.error is never suppressed except at level ``none``.
        if not tracking.should_log(self._level(), tracking.PAYMENT_ERROR):
            return
        self._log.error(
            message or error_code,
            exc_info=exc_info,
            extra={
                "event.action": tracking.PAYMENT_ERROR,
                "event.outcome": "failure",
                "service.name": self.service,
                "error.code": error_code,
                "error.type": error_type or None,
                "payment.retry_target": retry_target or None,
                "payment.retry_attempt": attempt,
            },
        )
