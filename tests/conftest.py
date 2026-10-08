"""Shared fixtures."""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator, Iterator

import pytest
from hub_telemetry.ecs import setup_logging
from hub_telemetry.otel import setup_tracing

from .harness import Harness, start_harness


@pytest.fixture(scope="session", autouse=True)
def _telemetry() -> Iterator[None]:
    """Install tracing once, and keep test output quiet.

    ``setup_tracing`` with no endpoint is what a service does locally: spans
    are created and propagated, but nothing exports them. Without a real
    tracer provider there would be no ``traceparent`` to carry.

    Logging is configured synchronously so assertions see records immediately;
    the async queue handler would deliver them after the assertion.
    """
    setup_tracing("tests", endpoint=None, environment="test")
    setup_logging("tests", level=logging.WARNING, asynchronous=False)
    yield


@pytest.fixture(autouse=True)
def _fast_redelivery(monkeypatch: pytest.MonkeyPatch) -> None:
    """Redeliver within milliseconds, not the simulator's 2 to 60 seconds.

    The ESS backs off like a real network when the Hub misses a notification.
    In a test that only turns one slow call into a wait of seconds or minutes.
    """
    import ess.emulators as emulators

    monkeypatch.setattr(emulators, "REDELIVERY_FIRST_S", 0.02)
    monkeypatch.setattr(emulators, "REDELIVERY_MAX_S", 0.2)
    monkeypatch.setattr(emulators, "REDELIVERY_RATE_PER_S", 200.0)


@pytest.fixture
async def hub() -> AsyncIterator[Harness]:
    """A running Hub and ESS, torn down after the test."""
    harness = await start_harness()
    try:
        yield harness
    finally:
        await harness.close()


@pytest.fixture
async def perf_hub() -> AsyncIterator[Harness]:
    """The same, in performance mode: tracking at ``errors``, drawn latency."""
    harness = await start_harness(tracking_mode="performance", ess_mode="performance")
    try:
        yield harness
    finally:
        await harness.close()
