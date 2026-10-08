"""gRPC interceptors, deadlines and the retry policy.

Server side: read the baggage from metadata, restore the context, log
``grpc.server.recv`` / ``grpc.server.reply``.

Client side: inject baggage and ``traceparent``, apply the deadline, retry only
``UNAVAILABLE`` and ``DEADLINE_EXCEEDED`` (at most three times, exponential
backoff with jitter), log ``grpc.client.send`` / ``grpc.client.reply``.

Both sides are ``grpc.aio``; the Hub edge, the processors and the ESS are all
async.
"""

from __future__ import annotations

import asyncio
import os
import random
import time
from collections.abc import Awaitable, Callable, Mapping, Sequence
from typing import Any, Final

import grpc
from grpc.aio import ClientCallDetails, ServerInterceptor, UnaryUnaryClientInterceptor
from opentelemetry import trace
from opentelemetry.trace import SpanKind

from . import baggage, metrics
from .events import Events

# Deadlines from design doc section 4.
#
# HUB_DEADLINE_SCALE multiplies them all and is 1 everywhere that matters. The
# test suite sets it higher: its calls are real gRPC over loopback, and on a
# loaded laptop or a shared CI runner a 200 ms deadline is missed for reasons
# that have nothing to do with the code under test.
_SCALE: Final = float(os.environ.get("HUB_DEADLINE_SCALE") or 1.0)
DEADLINE_DELIVER_S: Final = 0.200 * _SCALE
DEADLINE_NOTIFY_S: Final = 0.200 * _SCALE
DEADLINE_SEND_S: Final = 0.500 * _SCALE
DEADLINE_SCREEN_S: Final = 1.000 * _SCALE

MAX_ATTEMPTS: Final = 3
BACKOFF_BASE_S: Final = 0.020
BACKOFF_MAX_S: Final = 0.500

# Only these are safe to retry. INVALID_ARGUMENT never is: the message will
# fail identically the second time, and a retry storm makes it worse.
RETRYABLE: Final = frozenset({grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED})


def backoff_delay(attempt: int, *, rng: random.Random | None = None) -> float:
    """Exponential backoff with full jitter for ``attempt`` (1-based)."""
    source = rng or random
    ceiling = min(BACKOFF_MAX_S, BACKOFF_BASE_S * (2 ** (attempt - 1)))
    return source.uniform(0.0, ceiling)


def metadata_for_call(extra: Mapping[str, str] | None = None) -> list[tuple[str, str]]:
    """Build the metadata every outbound call carries.

    ``uetr`` and ``flow`` are duplicated out of baggage as their own keys, so a
    gateway or a network capture can route on them without parsing baggage.
    """
    bag = baggage.PaymentBaggage.from_context()
    meta: dict[str, str] = {}
    if bag.uetr:
        meta["uetr"] = bag.uetr
    if bag.flow:
        meta["flow"] = bag.flow
    if bag.run_id:
        meta["run-id"] = bag.run_id
    meta.update(baggage.inject())
    if extra:
        meta.update(extra)
    return list(meta.items())


def metadata_to_headers(metadata: Sequence[tuple[str, Any]] | None) -> dict[str, str]:
    """Flatten incoming gRPC metadata to a string mapping."""
    if not metadata:
        return {}
    out: dict[str, str] = {}
    for key, value in metadata:
        out[key] = value.decode("utf-8", "replace") if isinstance(value, bytes) else str(value)
    return out


class ServerTelemetryInterceptor(ServerInterceptor):
    """Restores the payment context and logs both ends of every served call."""

    def __init__(self, events: Events) -> None:
        self._events = events

    async def intercept_service(  # type: ignore[override]
        self,
        continuation: Callable[[Any], Awaitable[Any]],
        handler_call_details: Any,
    ) -> Any:
        handler = await continuation(handler_call_details)
        if handler is None or not handler.unary_unary:
            return handler

        method = handler_call_details.method
        inbound = metadata_to_headers(handler_call_details.invocation_metadata)
        events = self._events
        inner = handler.unary_unary

        tracer = trace.get_tracer(__name__)

        async def wrapper(request: Any, context: Any) -> Any:
            with (
                baggage.restored_context(inbound),
                tracer.start_as_current_span(
                    method, kind=SpanKind.SERVER, attributes={"rpc.method": method}
                ),
            ):
                started = time.perf_counter()
                events.grpc_server_recv(method, peer=_peer(context))
                try:
                    response = await inner(request, context)
                except grpc.RpcError as exc:
                    code_of = getattr(exc, "code", lambda: grpc.StatusCode.UNKNOWN)
                    status = _status_name(code_of())
                    _observe_server(method, status, started)
                    events.grpc_server_reply(
                        method,
                        status_code=status,
                        duration_ns=_elapsed_ns(started),
                        outcome="failure",
                    )
                    raise
                except Exception:
                    _observe_server(method, "INTERNAL", started)
                    events.grpc_server_reply(
                        method,
                        status_code="INTERNAL",
                        duration_ns=_elapsed_ns(started),
                        outcome="failure",
                    )
                    raise
                code = context.code() or grpc.StatusCode.OK
                _observe_server(method, _status_name(code), started)
                events.grpc_server_reply(
                    method,
                    status_code=code.name,
                    duration_ns=_elapsed_ns(started),
                    outcome="success" if code is grpc.StatusCode.OK else "failure",
                )
                return response

        return grpc.unary_unary_rpc_method_handler(
            wrapper,
            request_deserializer=handler.request_deserializer,
            response_serializer=handler.response_serializer,
        )


class ClientTelemetryInterceptor(UnaryUnaryClientInterceptor):
    """Injects context, retries what is safe to retry, logs both ends."""

    def __init__(
        self,
        events: Events,
        *,
        target: str = "",
        max_attempts: int = MAX_ATTEMPTS,
        strip_context: bool = False,
        rng: random.Random | None = None,
    ) -> None:
        self._events = events
        self._target = target
        self._max_attempts = max_attempts
        # True when the peer is the *real* FIN / SnF / compliance service: baggage and
        # traceparent are internal and must not leave the bank.
        self._strip_context = strip_context
        self._rng = rng

    async def intercept_unary_unary(
        self,
        continuation: Callable[[ClientCallDetails, Any], Any],
        client_call_details: ClientCallDetails,
        request: Any,
    ) -> Any:
        method = _method_name(client_call_details.method)
        existing = metadata_to_headers(list(client_call_details.metadata or []))
        context_meta = {} if self._strip_context else dict(metadata_for_call())
        merged = {**existing, **context_meta}
        if self._strip_context:
            merged = baggage.strip(merged)

        tracer = trace.get_tracer(__name__)
        last_error: grpc.aio.AioRpcError | None = None
        for attempt in range(1, self._max_attempts + 1):
            with tracer.start_as_current_span(
                method, kind=SpanKind.CLIENT, attributes={"rpc.method": method}
            ):
                # The span must be current before the metadata is built: that
                # is what puts a traceparent on the wire.
                merged = {**existing, **({} if self._strip_context else dict(metadata_for_call()))}
                if self._strip_context:
                    merged = baggage.strip(merged)
                details = _with_metadata(client_call_details, merged)
            started = time.perf_counter()
            self._events.grpc_client_send(method, target=self._target, attempt=attempt)
            try:
                call = await continuation(details, request)
                response = await call if hasattr(call, "__await__") else call
            except grpc.aio.AioRpcError as exc:
                last_error = exc
                _observe_client(method, exc.code().name, started)
                self._events.grpc_client_reply(
                    method,
                    status_code=exc.code().name,
                    duration_ns=_elapsed_ns(started),
                    attempt=attempt,
                    outcome="failure",
                )
                if exc.code() not in RETRYABLE or attempt == self._max_attempts:
                    raise
                service, short = split_method(method)
                metrics.grpc_client_retries.labels(service, short, exc.code().name).inc()
                await asyncio.sleep(backoff_delay(attempt, rng=self._rng))
                continue
            _observe_client(method, "OK", started)
            self._events.grpc_client_reply(
                method,
                status_code="OK",
                duration_ns=_elapsed_ns(started),
                attempt=attempt,
            )
            return response

        assert last_error is not None
        raise last_error


def _with_metadata(details: ClientCallDetails, metadata: Mapping[str, str]) -> ClientCallDetails:
    """Return ``details`` with our metadata substituted.

    ``grpc.aio.ClientCallDetails`` is an immutable namedtuple, so this replaces
    the field rather than assigning to it.
    """
    replaced: ClientCallDetails = details._replace(  # type: ignore[attr-defined]
        metadata=grpc.aio.Metadata(*metadata.items())
    )
    return replaced


def split_method(method: str) -> tuple[str, str]:
    """``/hub.v1.HubInbound/DeliverMx`` -> (``hub.v1.HubInbound``, ``DeliverMx``)."""
    service, _, short = method.lstrip("/").rpartition("/")
    return service or "unknown", short or method


def _status_name(code: Any) -> str:
    name = getattr(code, "name", None)
    return str(name) if name else str(code)


def _observe_server(method: str, status: str, started: float) -> None:
    service, short = split_method(method)
    metrics.grpc_server_calls.labels(service, short, status).inc()
    metrics.grpc_server_latency.labels(service, short).observe(time.perf_counter() - started)


def _observe_client(method: str, status: str, started: float) -> None:
    """One attempt. A retried call counts once per attempt, as the network sees it."""
    service, short = split_method(method)
    metrics.grpc_client_calls.labels(service, short, status).inc()
    metrics.grpc_client_latency.labels(service, short).observe(time.perf_counter() - started)


def _method_name(method: str | bytes) -> str:
    return method.decode() if isinstance(method, bytes) else method


def _peer(context: Any) -> str:
    try:
        return str(context.peer())
    except Exception:
        return ""


def _elapsed_ns(started: float) -> int:
    return int((time.perf_counter() - started) * 1e9)


def channel(
    target: str,
    events: Events,
    *,
    credentials: grpc.ChannelCredentials | None = None,
    strip_context: bool = False,
    options: Sequence[tuple[str, Any]] | None = None,
    max_attempts: int = MAX_ATTEMPTS,
) -> grpc.aio.Channel:
    """A long-lived channel with keep-alive, round-robin and our interceptor.

    ``credentials`` is None only for local Compose and tests; shared and
    production environments pass mTLS credentials. ``max_attempts=1`` is for a
    caller with its own retry policy: two layers of retries multiply.
    """
    opts: list[tuple[str, Any]] = [
        ("grpc.keepalive_time_ms", 20_000),
        ("grpc.keepalive_timeout_ms", 10_000),
        ("grpc.keepalive_permit_without_calls", 1),
        ("grpc.http2.max_pings_without_data", 0),
        ("grpc.lb_policy_name", "round_robin"),
        ("grpc.enable_retries", 0),  # we retry ourselves, with our own policy
        ("grpc.max_receive_message_length", 16 * 1024 * 1024),
        ("grpc.max_send_message_length", 16 * 1024 * 1024),
        *(options or []),
    ]
    interceptors = [
        ClientTelemetryInterceptor(
            events, target=target, strip_context=strip_context, max_attempts=max_attempts
        )
    ]
    if credentials is None:
        return grpc.aio.insecure_channel(target, options=opts, interceptors=interceptors)
    return grpc.aio.secure_channel(target, credentials, options=opts, interceptors=interceptors)
