"""The ``ess`` command line.

    ess serve --mode functional --hub hub-edge.hub.svc:8443
    ess send --type pacs.008 --file samples/pacs008_eur.xml
    ess send --type MT103 --file samples/mt103_gbp.fin
    ess case run mt103_sanctions_hit
    ess profile set fcc --hit-rate 0.02 --decision-delay 5m
    ess profile set fin --outage --for 2m
    ess counters --run-id S02-2026-09-28-01

Every subcommand except ``serve`` is a thin gRPC client of ``EssControl``, so
the same commands work against a simulator in Docker, in Kubernetes or on a
laptop.
"""

from __future__ import annotations

import asyncio
import json
import re
import sys
from pathlib import Path
from typing import Annotated, Any

import grpc
import typer
from hub_model import proto as pb
from hub_model.flows import normalise_msg_type
from hub_telemetry.ecs import setup_logging
from hub_telemetry.otel import setup_propagators_only

from .batch import parse_mix
from .cases import CASES
from .server import DEFAULT_PORT, EssSettings, serve

app = typer.Typer(
    add_completion=False,
    help="External Systems Simulator — plays FIN, SnF and FCC for the Payment Hub.",
    no_args_is_help=True,
)
profile_app = typer.Typer(help="Change emulator behaviour at runtime.", no_args_is_help=True)
case_app = typer.Typer(help="Run and list scripted test cases.", no_args_is_help=True)
app.add_typer(profile_app, name="profile")
app.add_typer(case_app, name="case")

_DURATION = re.compile(r"^(?P<value>[\d.]+)(?P<unit>ms|s|m|h)?$")

TARGETS = {"fin": pb.Target.FIN, "snf": pb.Target.SNF, "fcc": pb.Target.FCC, "all": pb.Target.ALL}


def parse_duration(text: str) -> float:
    """``5m`` -> 300.0 seconds. Bare numbers are seconds."""
    match = _DURATION.match(text.strip().lower())
    if match is None:
        raise typer.BadParameter(f"cannot read a duration from {text!r} (try 500ms, 30s, 5m)")
    value = float(match.group("value"))
    return value * {"ms": 0.001, "s": 1.0, "m": 60.0, "h": 3600.0}[match.group("unit") or "s"]


def _control_target(target: str) -> str:
    return target if ":" in target else f"{target}:{DEFAULT_PORT}"


async def _call(target: str, fn: Any) -> Any:
    """Open a plain channel to EssControl and run one call.

    Deliberately without the telemetry interceptor: the CLI is an operator
    tool, not a payment hop, and should not invent baggage.
    """
    async with grpc.aio.insecure_channel(_control_target(target)) as chan:
        stub = pb.EssControlStub(chan)
        return await fn(stub)


def _run(coro: Any) -> Any:
    try:
        return asyncio.run(coro)
    except grpc.aio.AioRpcError as exc:
        typer.secho(
            f"ESS call failed: {exc.code().name} — {exc.details()}", fg=typer.colors.RED, err=True
        )
        raise typer.Exit(1) from exc
    except KeyboardInterrupt as exc:
        raise typer.Exit(130) from exc


# ------------------------------------------------------------------ serve
def serve_cmd(
    mode: Annotated[
        str, typer.Option("--mode", help="functional | ci | performance")
    ] = "functional",
    hub: Annotated[str, typer.Option("--hub", help="Hub gRPC edge address")] = "localhost:8443",
    port: Annotated[int, typer.Option("--port", help="Port to serve on")] = DEFAULT_PORT,
    status_api: Annotated[
        str, typer.Option("--status-api", help="Hub status API base URL, for case assertions")
    ] = "http://localhost:8080",
    metrics_port: Annotated[int, typer.Option("--metrics-port")] = 9464,
    seed: Annotated[int, typer.Option("--seed", help="Make behaviour reproducible")] = -1,
    run_id: Annotated[str, typer.Option("--run-id")] = "",
    log_level: Annotated[str, typer.Option("--log-level")] = "INFO",
) -> None:
    """Run the simulator."""
    settings = EssSettings(
        mode=mode.lower(),
        port=port,
        metrics_port=metrics_port,
        hub_target=hub,
        status_api_url=status_api,
        log_level=log_level.upper(),
        run_id=run_id,
        seed=seed if seed >= 0 else None,
    )
    _run(serve(settings))


# Registered by hand so the command is `serve` while the function name stays
# clear of the imported `serve` coroutine.
app.command(name="serve", help="Run the simulator.")(serve_cmd)


# ------------------------------------------------------------------- send
@app.command()
def send(
    msg_type: Annotated[
        str, typer.Option("--type", "-t", help="pacs.008 | pacs.009 | MT103 | MT202 | MT202COV")
    ],
    file: Annotated[
        Path | None, typer.Option("--file", "-f", help="Message to send; generated if omitted")
    ] = None,
    app_hdr: Annotated[
        Path | None, typer.Option("--app-hdr", help="head.001 AppHdr for an MX message")
    ] = None,
    uetr: Annotated[str, typer.Option("--uetr", help="Use this UETR instead of a new one")] = "",
    flow: Annotated[str, typer.Option("--flow", help="Flow ID for baggage and metrics")] = "",
    count: Annotated[int, typer.Option("--count", "-n", help="How many to send")] = 1,
    run_id: Annotated[str, typer.Option("--run-id")] = "",
    ess: Annotated[str, typer.Option("--ess", help="ESS control address")] = "localhost",
) -> None:
    """Deliver one inbound message (or several) into the Hub."""
    payload = file.read_bytes() if file else b""
    header = app_hdr.read_bytes() if app_hdr else b""

    request = pb.SendInboundRequest(
        msg_type=normalise_msg_type(msg_type),
        payload=payload,
        app_hdr=header,
        uetr=uetr,
        flow=flow,
        run_id=run_id,
        count=max(1, count),
    )
    result = _run(_call(ess, lambda stub: stub.SendInbound(request)))
    for sent in result.uetrs:
        typer.echo(sent)

    # A DUPLICATE means a retried delivery whose first attempt already landed:
    # the payment is in the Hub exactly once, which is a success.
    delivered = result.accepted + result.duplicate
    summary = f"accepted={result.accepted} rejected={result.rejected}"
    if result.duplicate:
        summary += f" duplicate={result.duplicate} (retried; already accepted)"
    if result.detail:
        summary += f" — {result.detail}"
    ok = result.rejected == 0 and delivered > 0
    typer.secho(summary, fg=typer.colors.GREEN if ok else typer.colors.RED)
    if not ok:
        raise typer.Exit(1)


# ------------------------------------------------------------------ cases
@case_app.command("run")
def case_run(
    case_id: Annotated[str, typer.Argument(help="Case to run, e.g. mt103_sanctions_hit")],
    repeat: Annotated[int, typer.Option("--repeat", "-n")] = 1,
    run_id: Annotated[str, typer.Option("--run-id")] = "",
    trace: Annotated[str, typer.Option("--trace", help="Per-run tracking level override")] = "",
    timeout: Annotated[str, typer.Option("--timeout")] = "30s",
    ess: Annotated[str, typer.Option("--ess")] = "localhost",
) -> None:
    """Run a scripted end-to-end case and assert its outcome."""
    request = pb.CaseRequest(
        case_id=case_id,
        run_id=run_id,
        trace_level=trace,
        repeat=max(1, repeat),
        timeout_ms=int(parse_duration(timeout) * 1000),
    )
    result = _run(_call(ess, lambda stub: stub.RunCase(request)))

    for step in result.steps:
        mark = "ok  " if step.passed else "FAIL"
        typer.echo(f"  {mark} {step.name}: {step.detail}")
    for sent in result.uetrs:
        typer.echo(f"  uetr {sent}")
    typer.secho(
        f"{'PASS' if result.passed else 'FAIL'} {result.case_id} "
        f"({result.duration_ms} ms) — {result.detail}",
        fg=typer.colors.GREEN if result.passed else typer.colors.RED,
    )
    if not result.passed:
        raise typer.Exit(1)


@case_app.command("list")
def case_list(
    ess: Annotated[str, typer.Option("--ess")] = "",
) -> None:
    """List the known cases. Reads the local catalogue unless --ess is given."""
    if ess:
        result = _run(_call(ess, lambda stub: stub.ListCases(pb.ListCasesRequest())))
        rows = [(c.case_id, c.flow, c.expected_state, c.description) for c in result.cases]
    else:
        rows = [(c.case_id, c.flow, c.expected_state, c.description) for c in CASES.values()]

    width = max((len(row[0]) for row in rows), default=10)
    for case_id, flow, expected, description in rows:
        typer.echo(f"{case_id.ljust(width)}  {expected.ljust(10)}  {flow}")
        typer.echo(f"{' ' * width}  {description}")


@case_app.command("run-all")
def case_run_all(
    run_id: Annotated[str, typer.Option("--run-id")] = "",
    timeout: Annotated[str, typer.Option("--timeout")] = "30s",
    ess: Annotated[str, typer.Option("--ess")] = "localhost",
) -> None:
    """Run every case. This is what CI calls on a Hub change."""
    timeout_ms = int(parse_duration(timeout) * 1000)
    failures: list[str] = []

    async def run_them() -> list[Any]:
        async with grpc.aio.insecure_channel(_control_target(ess)) as chan:
            stub = pb.EssControlStub(chan)
            listed = await stub.ListCases(pb.ListCasesRequest())
            out = []
            for case in listed.cases:
                out.append(
                    await stub.RunCase(
                        pb.CaseRequest(case_id=case.case_id, run_id=run_id, timeout_ms=timeout_ms)
                    )
                )
            return out

    for result in _run(run_them()):
        colour = typer.colors.GREEN if result.passed else typer.colors.RED
        typer.secho(
            f"{'PASS' if result.passed else 'FAIL'}  {result.case_id.ljust(28)} "
            f"{result.duration_ms:>6} ms  {result.detail}",
            fg=colour,
        )
        if not result.passed:
            failures.append(result.case_id)

    if failures:
        typer.secho(f"\n{len(failures)} case(s) failed: {', '.join(failures)}", fg=typer.colors.RED)
        raise typer.Exit(1)
    typer.secho("\nall cases passed", fg=typer.colors.GREEN)


# --------------------------------------------------------------- profiles
@profile_app.command("set")
def profile_set(
    target: Annotated[str, typer.Argument(help="fin | snf | fcc | all")],
    accept_latency: Annotated[
        str, typer.Option("--accept-latency", help="Synchronous reply time, e.g. 10ms")
    ] = "",
    ack_latency: Annotated[
        str, typer.Option("--ack-latency", help="Async ACK delay, e.g. 50ms")
    ] = "",
    decision_delay: Annotated[
        str, typer.Option("--decision-delay", help="FCC analyst decision delay, e.g. 5m")
    ] = "",
    p99: Annotated[str, typer.Option("--p99", help="99th percentile for a drawn latency")] = "",
    kind: Annotated[str, typer.Option("--kind", help="fixed | uniform | lognormal")] = "",
    nak_rate: Annotated[float, typer.Option("--nak-rate")] = 0.0,
    hit_rate: Annotated[float, typer.Option("--hit-rate")] = 0.0,
    block_rate: Annotated[float, typer.Option("--block-rate")] = 0.0,
    duplicate_rate: Annotated[float, typer.Option("--duplicate-rate")] = 0.0,
    release_rate: Annotated[float, typer.Option("--release-rate")] = 0.0,
    outage: Annotated[bool, typer.Option("--outage", help="Return UNAVAILABLE")] = False,
    outage_for: Annotated[
        str, typer.Option("--for", help="How long the outage lasts, e.g. 2m")
    ] = "",
    ess: Annotated[str, typer.Option("--ess")] = "localhost",
) -> None:
    """Change one emulator's behaviour without restarting it."""
    if target.lower() not in TARGETS:
        raise typer.BadParameter(f"target must be one of {', '.join(sorted(TARGETS))}")

    request = pb.BehaviourProfile(
        target=TARGETS[target.lower()],
        nak_rate=nak_rate,
        hit_rate=hit_rate,
        block_rate=block_rate,
        duplicate_rate=duplicate_rate,
        release_rate=release_rate,
        outage=outage,
        outage_for_ms=int(parse_duration(outage_for) * 1000) if outage_for else 0,
    )
    if accept_latency:
        ms = parse_duration(accept_latency) * 1000
        request.accept_latency.CopyFrom(
            pb.LatencyDist(
                kind=kind or "fixed",
                p50_ms=ms,
                p99_ms=parse_duration(p99) * 1000 if p99 else ms,
            )
        )
    # --decision-delay is the FCC-shaped name for the same field.
    async_latency = ack_latency or decision_delay
    if async_latency:
        ms = parse_duration(async_latency) * 1000
        request.ack_latency.CopyFrom(pb.LatencyDist(kind=kind or "fixed", p50_ms=ms, p99_ms=ms))

    result = _run(_call(ess, lambda stub: stub.SetProfile(request)))
    typer.secho(result.detail or "applied", fg=typer.colors.GREEN)


# ------------------------------------------------------------- counters
@app.command()
def counters(
    run_id: Annotated[str, typer.Option("--run-id", help="Limit to one test run")] = "",
    as_json: Annotated[bool, typer.Option("--json", help="Machine-readable output")] = False,
    ess: Annotated[str, typer.Option("--ess")] = "localhost",
) -> None:
    """Show what the simulator has served and sent."""
    result = _run(_call(ess, lambda stub: stub.GetCounters(pb.CounterRequest(run_id=run_id))))
    payload = {
        "calls_served": result.calls_served,
        "calls_made": result.calls_made,
        "errors": result.errors,
        "by_method": dict(result.by_method),
        "by_flow": dict(result.by_flow),
        "by_outcome": dict(result.by_outcome),
    }
    if as_json:
        typer.echo(json.dumps(payload, indent=2, sort_keys=True))
        return

    typer.echo(
        f"served={payload['calls_served']} made={payload['calls_made']} errors={payload['errors']}"
    )
    for title, values in (
        ("by method", payload["by_method"]),
        ("by flow", payload["by_flow"]),
        ("by outcome", payload["by_outcome"]),
    ):
        if not values:
            continue
        typer.echo(f"\n{title}:")
        for key in sorted(values):
            typer.echo(f"  {key:<40} {values[key]}")


@app.command()
def reset(
    counters_only: Annotated[bool, typer.Option("--counters-only")] = False,
    profiles_only: Annotated[bool, typer.Option("--profiles-only")] = False,
    ess: Annotated[str, typer.Option("--ess")] = "localhost",
) -> None:
    """Clear state between tests."""
    request = pb.ResetRequest(
        clear_counters=counters_only,
        clear_recorder=counters_only,
        clear_profiles=profiles_only,
    )
    result = _run(_call(ess, lambda stub: stub.Reset(request)))
    typer.secho(result.detail, fg=typer.colors.GREEN)


# ------------------------------------------------------------------ batch
@app.command()
def batch(
    count: Annotated[int, typer.Option("--count", "-n", help="How many payments")] = 10,
    rate: Annotated[
        float, typer.Option("--rate", "-r", help="Payments per second; 0 = unpaced")
    ] = 0.0,
    msg_type: Annotated[
        str, typer.Option("--type", "-t", help="One message type; ignored when --mix is given")
    ] = "pacs.008",
    mix: Annotated[
        str,
        typer.Option("--mix", help='Weighted mix, e.g. "pacs.008=70,MT103=30"'),
    ] = "",
    concurrency: Annotated[
        int, typer.Option("--concurrency", "-c", help="Deliveries in flight")
    ] = 4,
    wait: Annotated[
        str, typer.Option("--wait", help="How long to wait for terminal states; 0 to skip")
    ] = "60s",
    hit_rate: Annotated[
        float, typer.Option("--hit-rate", help="Share drawn with a watchlist party")
    ] = 0.0,
    run_id: Annotated[str, typer.Option("--run-id")] = "",
    as_json: Annotated[bool, typer.Option("--json", help="Machine-readable output")] = False,
    ess: Annotated[str, typer.Option("--ess")] = "localhost",
) -> None:
    """Send a batch of payments and report how they settled."""
    entries = parse_mix(mix) if mix else parse_mix(msg_type)
    wait_s = parse_duration(wait) if wait and wait != "0" else 0.0

    request = pb.BatchRequest(
        count=max(1, count),
        rate_per_second=max(0.0, rate),
        concurrency=max(1, concurrency),
        run_id=run_id,
        settle_timeout_ms=int(wait_s * 1000),
        sanctions_hit_rate=hit_rate,
    )
    for entry in entries:
        request.mix.append(pb.MixEntry(msg_type=entry.msg_type, share=entry.share))

    result = _run(_call(ess, lambda stub: stub.RunBatch(request)))

    if as_json:
        typer.echo(
            json.dumps(
                {
                    "requested": result.requested,
                    "accepted": result.accepted,
                    "rejected": result.rejected,
                    "duplicate": result.duplicate,
                    "errors": result.errors,
                    "send_duration_ms": result.send_duration_ms,
                    "achieved_rate_per_second": round(result.achieved_rate_per_second, 2),
                    "settled": result.settled,
                    "unsettled": result.unsettled,
                    "by_state": dict(result.by_state),
                    "p50_end_to_end_ms": round(result.p50_end_to_end_ms, 1),
                    "p95_end_to_end_ms": round(result.p95_end_to_end_ms, 1),
                    "max_end_to_end_ms": round(result.max_end_to_end_ms, 1),
                    "total_duration_ms": result.total_duration_ms,
                    "failed_uetrs": list(result.failed_uetrs),
                },
                indent=2,
                sort_keys=True,
            )
        )
    else:
        typer.echo(
            f"requested {result.requested}  accepted {result.accepted}  "
            f"rejected {result.rejected}  duplicate {result.duplicate}  "
            f"errors {result.errors}"
        )
        typer.echo(
            f"sent in {result.send_duration_ms / 1000:.2f}s "
            f"at {result.achieved_rate_per_second:.1f}/s"
        )
        if result.settled or result.unsettled:
            states = "  ".join(f"{k}={v}" for k, v in sorted(result.by_state.items()))
            typer.echo(f"settled {result.settled}  unsettled {result.unsettled}  {states}")
        if result.p50_end_to_end_ms:
            typer.echo(
                f"end to end  p50 {result.p50_end_to_end_ms:.0f} ms  "
                f"p95 {result.p95_end_to_end_ms:.0f} ms  "
                f"max {result.max_end_to_end_ms:.0f} ms"
            )
        for uetr in list(result.failed_uetrs)[:10]:
            typer.secho(f"  needs a look: {uetr}", fg=typer.colors.YELLOW)

    ok = result.rejected == 0 and result.errors == 0 and result.unsettled == 0
    typer.secho(
        "batch ok" if ok else "batch had failures",
        fg=typer.colors.GREEN if ok else typer.colors.RED,
    )
    if not ok:
        raise typer.Exit(1)


@app.callback()
def main() -> None:
    """Set up logging and the W3C propagators for every subcommand."""
    setup_logging("ess-cli", level="WARNING", stream=sys.stderr, asynchronous=False)
    setup_propagators_only()


if __name__ == "__main__":
    app()
