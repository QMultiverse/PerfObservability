"""Common tasks, the same on Windows, macOS and Linux.

    python scripts/dev.py check          # stubs, ruff, mypy and pytest
    python scripts/dev.py doctor         # is this machine ready to run the stack?
    python scripts/dev.py up             # build the image once, start the stack
    python scripts/dev.py ready          # is the stack ready for a test run?
    python scripts/dev.py send pacs.008  # deliver one payment into it
    python scripts/dev.py trace          # send one payment and follow it everywhere

Standard library only, on purpose: ``install`` has to work on a fresh clone,
before anything has been installed.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import shutil
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent

# Host ports, each overridable in .env (see .env.example). Container ports are fixed.
PORTS = {
    "HUB_GRPC_HOST_PORT": 8443,
    "HUB_STATUS_HOST_PORT": 8080,
    "HUB_METRICS_HOST_PORT": 9464,
    "ESS_HOST_PORT": 9101,
    "KAFKA_HOST_PORT": 29092,
    "KAFKA_JMX_HOST_PORT": 9404,
    "POSTGRES_HOST_PORT": 5432,
    "PROMETHEUS_HOST_PORT": 9090,
    "GRAFANA_HOST_PORT": 3000,
    "ELASTICSEARCH_HOST_PORT": 9200,
    "KIBANA_HOST_PORT": 5601,
}
CONSUMER_GROUPS = 11
TERMINAL = {"COMPLETED", "REJECTED", "BLOCKED"}


# ------------------------------------------------------------------ helpers
def python() -> str:
    """The project's virtualenv interpreter if there is one, else this one."""
    for candidate in (ROOT / ".venv" / "Scripts" / "python.exe", ROOT / ".venv" / "bin" / "python"):
        if candidate.exists():
            return str(candidate)
    return sys.executable


def env() -> dict[str, str]:
    """``.env`` overlaid with the real environment, as Docker Compose reads it."""
    values: dict[str, str] = {}
    dotenv = ROOT / ".env"
    if dotenv.exists():
        for line in dotenv.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, _, value = line.partition("=")
                values[key.strip()] = value.strip().strip('"').strip("'")
    values.update(os.environ)
    return values


def port(name: str) -> int:
    try:
        return int(env().get(name, "") or PORTS[name])
    except ValueError:
        return PORTS[name]


def url(port_name: str, path: str = "") -> str:
    return f"http://localhost:{port(port_name)}{path}"


def say(text: str) -> None:
    print(text, flush=True)


def step(name: str, cmd: Sequence[str], *, check: bool = True) -> int:
    say(f"==> {name}")
    code = subprocess.run(list(cmd), cwd=ROOT, check=False).returncode
    if check and code != 0:
        raise SystemExit(f"{name} failed with exit code {code}")
    return code


def capture(cmd: Sequence[str], *, timeout: float = 60.0) -> tuple[int, str]:
    try:
        done = subprocess.run(
            list(cmd), cwd=ROOT, capture_output=True, text=True, timeout=timeout, check=False
        )
    except (FileNotFoundError, subprocess.TimeoutExpired) as exc:
        return 1, str(exc)
    return done.returncode, (done.stdout or "") + (done.stderr or "")


def compose(*args: str) -> list[str]:
    # compose.yaml sits at the repository root, so no -f is needed.
    return ["docker", "compose", *args]


def get_json(
    address: str, *, data: bytes | None = None, headers: dict[str, str] | None = None
) -> Any:
    request = urllib.request.Request(address, data=data, headers=headers or {})
    with urllib.request.urlopen(request, timeout=10) as response:
        return json.load(response)


def prom(query: str) -> float | None:
    """One number from Prometheus, or None if it has no answer."""
    address = url(
        "PROMETHEUS_HOST_PORT", "/api/v1/query?" + urllib.parse.urlencode({"query": query})
    )
    try:
        result = get_json(address)["data"]["result"]
    except (urllib.error.URLError, OSError, KeyError, ValueError):
        return None
    return float(result[0]["value"][1]) if result else None


# -------------------------------------------------------------------- tasks
def task_install(_: argparse.Namespace) -> None:
    # In this order. pyproject.toml maps the hub_proto package onto gen/, which
    # is generated and not committed, so on a fresh clone the project cannot be
    # installed until the stubs exist, and the stubs need protoc.
    step("protoc", [python(), "-m", "pip", "install", "grpcio-tools>=1.68", "protobuf>=5.28"])
    step("proto", [python(), "scripts/gen_proto.py"])
    step("install", [python(), "-m", "pip", "install", "-e", ".[dev]"])


def task_check(_: argparse.Namespace) -> None:
    step("stubs", [python(), "scripts/gen_proto.py", "--check"])
    step("ruff", [python(), "-m", "ruff", "check", "."])
    step("mypy", [python(), "-m", "mypy", "hub", "ess", "libs"])
    step("tests", [python(), "-m", "pytest", "-q"])
    say("all checks passed")


def task_up(args: argparse.Namespace) -> None:
    if not (ROOT / ".env").exists():
        shutil.copyfile(ROOT / ".env.example", ROOT / ".env")
        say("created .env from .env.example")
    if not args.no_build:
        # Built once, by name. The Hub, the ESS and the topic job share this
        # image, and letting `up --build` build it for each in parallel has
        # failed on DNS inside the build.
        step("build", compose("build", "hub"))
    step("up", compose("up", "-d"))
    say("next: python scripts/dev.py ready --wait 300")


def task_down(_: argparse.Namespace) -> None:
    say("this wipes all local logs, metrics and payment data")
    step("down", compose("down"))


def task_send(args: argparse.Namespace) -> None:
    step(
        "send", compose("exec", "-T", "ess", "python", "-m", "ess.cli", "send", "--type", args.type)
    )


def simple(
    name: str, cmd: Callable[[argparse.Namespace], Sequence[str]]
) -> Callable[[argparse.Namespace], None]:
    def run(args: argparse.Namespace) -> None:
        raise SystemExit(step(name, cmd(args), check=False))

    return run


# ------------------------------------------------------------------- doctor
def task_doctor(_: argparse.Namespace) -> None:
    """Is this machine ready to run the stack? Prints what to fix."""
    problems = 0
    settings = env()
    profiles = settings.get("COMPOSE_PROFILES", "solo,obs,elk")
    wants_elk = "elk" in profiles

    def report(ok: bool | None, text: str, fix: str = "", *, warn: bool = False) -> None:
        nonlocal problems
        mark = {True: "ok  ", False: "warn" if warn else "FAIL", None: "note"}[ok]
        say(f"  [{mark}] {text}")
        if ok is False:
            problems += 0 if warn else 1
            if fix:
                say(f"         fix: {fix}")

    system = platform.system()
    say(f"platform: {system} {platform.machine()}, Python {platform.python_version()}")
    report(sys.version_info >= (3, 12), "Python 3.12 or later", "install Python 3.12+")

    code, out = capture(["docker", "version", "--format", "{{.Server.Version}}"])
    engine = code == 0 and bool(out.strip())
    report(
        engine,
        f"Docker engine reachable ({out.strip() if engine else 'no'})",
        "start Docker Desktop, or the docker service on Linux",
    )
    code, out = capture(["docker", "compose", "version", "--short"])
    report(
        code == 0,
        f"Docker Compose v2 ({out.strip() if code == 0 else 'missing'})",
        "install the Compose plugin (it ships with Docker Desktop)",
    )

    if engine:
        _, info = capture(
            [
                "docker",
                "info",
                "--format",
                "{{.MemTotal}} {{.NCPU}} {{.Architecture}} {{.OperatingSystem}}",
            ]
        )
        parts = info.split(maxsplit=3)
        if len(parts) >= 3 and parts[0].isdigit():
            gib = int(parts[0]) / 2**30
            need = 9.0 if wants_elk else 4.5
            desktop = "Docker Desktop" in info
            where = (
                "Docker Desktop: Settings, Resources "
                r"(on Windows with WSL 2: %UserProfile%\.wslconfig)"
                if desktop
                else "this is the host's memory; drop the elk profile or free memory"
            )
            report(
                gib >= need,
                f"Docker has {gib:.1f} GiB of memory (profiles '{profiles}' want about {need:g})",
                where,
                warn=True,  # it runs with less, but slowly and close to the limit
            )
            report(
                None,
                f"{parts[1]} CPUs, {parts[2]}"
                + (
                    " (Apple Silicon / arm64: all images are multi-arch)"
                    if parts[2] in ("aarch64", "arm64")
                    else ""
                ),
            )

        if wants_elk:
            count: int | None = None
            if (
                system == "Linux"
                and Path("/proc/sys/vm/max_map_count").exists()
                and "microsoft" not in platform.release().lower()
            ):
                count = int(Path("/proc/sys/vm/max_map_count").read_text().strip())
                fix = "sudo sysctl -w vm.max_map_count=262144   (persist it in /etc/sysctl.d/)"
            else:
                # Docker Desktop: the setting lives in its VM, so ask a container.
                code, out = capture(
                    [
                        "docker",
                        "run",
                        "--rm",
                        "--entrypoint",
                        "cat",
                        "curlimages/curl:8.10.1",
                        "/proc/sys/vm/max_map_count",
                    ],
                    timeout=120,
                )
                count = int(out.strip()) if code == 0 and out.strip().isdigit() else None
                fix = (
                    "in WSL: sudo sysctl -w vm.max_map_count=262144"
                    if system == "Windows"
                    else "Docker Desktop normally sets this; restart it"
                )
            if count is None:
                report(None, "could not read vm.max_map_count (Elasticsearch needs 262144)")
            else:
                report(
                    count >= 262144,
                    f"vm.max_map_count is {count} (Elasticsearch needs 262144)",
                    fix,
                )

    report((ROOT / ".env").exists(), ".env exists", "copy .env.example to .env (dev.py up does it)")

    _, running = capture(compose("ps", "-q"))
    if running.strip():
        report(None, "the stack is already running, so its ports are in use by it")
    else:
        for name in PORTS:
            if name in ("ELASTICSEARCH_HOST_PORT", "KIBANA_HOST_PORT") and not wants_elk:
                continue
            number = port(name)
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
                probe.settimeout(0.5)
                busy = probe.connect_ex(("127.0.0.1", number)) == 0
            report(
                not busy, f"port {number} is free ({name})", f"set {name} to a free port in .env"
            )

    report(
        None,
        "laptops: turn off sleep while testing. A sleeping host freezes Docker's VM "
        "and Kafka drops the Hub's consumers",
    )
    if problems:
        raise SystemExit(f"{problems} problem(s) to fix")
    say("this machine is ready")


# -------------------------------------------------------------------- ready
def task_ready(args: argparse.Namespace) -> None:
    """The readiness gate: running is not the same as ready for a test."""
    deadline = time.monotonic() + args.wait
    while True:
        checks = [
            ("Prometheus answers", prom("vector(1)") == 1.0, ""),
            (
                f"all {CONSUMER_GROUPS} consumer groups hold partitions",
                prom("count(hub_kafka_assigned_partitions > 0)") == float(CONSUMER_GROUPS),
                "a stage holds none; it rejoins by itself within a minute, or restart the hub",
            ),
            ("no stage is stalled", (prom("sum(hub_stage_stalled)") or 0.0) == 0.0, ""),
            (
                "consumer lag is zero",
                (prom("sum(clamp_min(kafka_consumergroup_lag, 0))") or 0.0) == 0.0,
                "work left from an earlier run is still draining",
            ),
            (
                "no payment is waiting too long for an ACK",
                (prom('sum(hub_awaiting_ack{overdue="true"})') or 0.0) == 0.0,
                "",
            ),
        ]
        if all(ok for _, ok, _ in checks) or time.monotonic() >= deadline:
            break
        time.sleep(5)
    for text, ok, hint in checks:
        say(f"  [{'ok  ' if ok else 'FAIL'}] {text}" + (f"  ({hint})" if hint and not ok else ""))
    if not all(ok for _, ok, _ in checks):
        raise SystemExit("not ready")
    say("ready. Before a measured run, also pass a warm-up batch:")
    say("  docker compose exec -T ess python -m ess.cli batch -n 30 -r 1 --run-id warmup")


# -------------------------------------------------------------------- trace
def task_trace(args: argparse.Namespace) -> None:
    """Send one payment and follow it through the Hub, Kafka, PostgreSQL and the logs."""

    def heading(text: str) -> None:
        say(f"\n=== {text} ===")

    heading(f"1. send a {args.type} into the Hub")
    code, out = capture(
        compose(
            "exec",
            "-T",
            "ess",
            "python",
            "-m",
            "ess.cli",
            "send",
            "--type",
            args.type,
            "--run-id",
            args.run_id,
        )
    )
    lines = [line for line in out.splitlines() if line.strip()]
    for line in lines:
        say(f"  {line}")
    if code != 0 or not lines:
        raise SystemExit("no UETR came back; is the stack up?")
    uetr = lines[0].strip()

    heading("2. where the Hub thinks it is")
    status: dict[str, Any] = {}
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        time.sleep(0.4)
        try:
            status = get_json(url("HUB_STATUS_HOST_PORT", f"/payments/{uetr}"))
        except (urllib.error.URLError, OSError, ValueError):
            continue
        if status.get("state") in TERMINAL:
            break
    if not status:
        raise SystemExit(f"the status API has nothing for {uetr}")
    elapsed = (status.get("end_to_end_s") or 0) * 1000
    say(
        f"  {status.get('uetr')}  {status.get('format')}  {status.get('state')}"
        f"  ({elapsed:,.0f} ms end to end)"
    )
    say("\n  journey:")
    for hop in status.get("history", []):
        say(f"    {hop.get('state', ''):<12} {hop.get('service', ''):<22} {hop.get('reason', '')}")

    heading(f"3. the record on {args.topic}")
    # The console consumer exits non-zero when --timeout-ms runs out, which is how it stops.
    _, out = capture(
        [
            "docker",
            "exec",
            "-e",
            "KAFKA_OPTS=",
            "hub-kafka",
            "/opt/kafka/bin/kafka-console-consumer.sh",
            "--bootstrap-server",
            "localhost:9092",
            "--topic",
            args.topic,
            "--from-beginning",
            "--timeout-ms",
            "12000",
            "--property",
            "print.key=true",
            "--property",
            "print.partition=true",
            "--property",
            "print.offset=true",
            "--property",
            "key.separator= | ",
        ],
        timeout=60,
    )
    found = next((line for line in out.splitlines() if uetr in line), "")
    if found:
        say(f"  {found[:120]}")
        say("  (the value is Protobuf, so it reads as binary with the message legible inside)")
    else:
        say(f"  nothing found on {args.topic} for this UETR")

    heading("4. the system of record")
    _, out = capture(
        [
            "docker",
            "exec",
            "-e",
            "PGPASSWORD=hub",
            "hub-postgres",
            "psql",
            "-U",
            "hub",
            "-d",
            "payments",
            "-t",
            "-c",
            "SELECT state, service, reason FROM payment_audit "
            f"WHERE uetr = '{uetr}' ORDER BY emitted_ns;",
        ]
    )
    for line in out.splitlines():
        if line.strip():
            say(f"  {line.strip()}")

    if not args.skip_elastic:
        heading("5. Elasticsearch")
        say("  waiting for the index to refresh...")
        time.sleep(12)
        body = json.dumps(
            {
                "query": {"term": {"payment.uetr": uetr}},
                "sort": [{"@timestamp": "asc"}],
                "size": 200,
            }
        ).encode()
        try:
            hits = get_json(
                url("ELASTICSEARCH_HOST_PORT", "/logs-payments-*/_search"),
                data=body,
                headers={"Content-Type": "application/json"},
            )["hits"]["hits"]
        except (urllib.error.URLError, OSError, KeyError, ValueError):
            hits = []
            say("  Elasticsearch did not answer (is the elk profile running?)")
        say(f"  {len(hits)} events for this payment")
        for hit in hits:
            source = hit.get("_source", {})
            service = (source.get("service") or {}).get("name", "")
            action = (source.get("event") or {}).get("action", "")
            say(f"    {service:<20} {action:<20} {source.get('message', '')}")

        heading("6. the same thing in Kibana")
        query = urllib.parse.quote(f'payment.uetr : "{uetr}"')
        say(
            "  "
            + url(
                "KIBANA_HOST_PORT",
                "/app/discover#/?_g=(time:(from:now-1h,to:now))"
                f"&_a=(query:(language:kuery,query:'{query}'))",
            )
        )
        say("  (choose the logs-payments-* data view if Kibana asks)")
    say(f"\nUETR: {uetr}\n")


# --------------------------------------------------------------------- main
def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog="dev.py", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    sub = parser.add_subparsers(dest="task", metavar="task")

    def add(
        name: str, func: Callable[[argparse.Namespace], None], help_: str
    ) -> argparse.ArgumentParser:
        command = sub.add_parser(name, help=help_)
        command.set_defaults(func=func)
        return command

    add("install", task_install, "install the project and generate the gRPC stubs")
    add(
        "proto",
        simple("proto", lambda a: [python(), "scripts/gen_proto.py"]),
        "regenerate the gRPC stubs",
    )
    add(
        "samples",
        simple("samples", lambda a: [python(), "scripts/make_samples.py"]),
        "regenerate samples/",
    )
    add(
        "topics",
        simple("topics", lambda a: [python(), "scripts/create_topics.py", "--list"]),
        "list the topic catalogue",
    )
    add("lint", simple("ruff", lambda a: [python(), "-m", "ruff", "check", "."]), "ruff check")
    add(
        "format", simple("format", lambda a: [python(), "-m", "ruff", "format", "."]), "ruff format"
    )
    add(
        "types",
        simple("mypy", lambda a: [python(), "-m", "mypy", "hub", "ess", "libs"]),
        "mypy --strict",
    )
    test = add(
        "test",
        simple("tests", lambda a: [python(), "-m", "pytest", *a.rest]),
        "pytest; extra arguments are passed on",
    )
    test.add_argument("rest", nargs=argparse.REMAINDER)
    add("check", task_check, "stubs, ruff, mypy and pytest")
    add("doctor", task_doctor, "check this machine can run the stack")
    up = add("up", task_up, "build the image once and start the stack")
    up.add_argument("--no-build", action="store_true", help="start without rebuilding the image")
    add("down", task_down, "stop the stack and wipe its data")
    add("stop", simple("stop", lambda a: compose("stop")), "stop the stack, keeping its data")
    add("ps", simple("ps", lambda a: compose("ps")), "container status")
    logs = add(
        "logs",
        simple("logs", lambda a: compose("logs", "-f", *a.rest)),
        "follow logs; optionally name services",
    )
    logs.add_argument("rest", nargs=argparse.REMAINDER)
    ready = add("ready", task_ready, "check the running stack is ready for a test run")
    ready.add_argument("--wait", type=float, default=0, help="seconds to keep trying")
    send = add("send", task_send, "deliver one payment")
    send.add_argument("type", nargs="?", default="pacs.008")
    add(
        "cases",
        simple(
            "cases",
            lambda a: compose("exec", "-T", "ess", "python", "-m", "ess.cli", "case", "run-all"),
        ),
        "run every scripted ESS case",
    )
    trace = add("trace", task_trace, "send one payment and follow it everywhere")
    trace.add_argument("--type", default="pacs.008")
    trace.add_argument("--run-id", default="TRACE")
    trace.add_argument(
        "--topic", default="hub.in.mx.raw", help="topic to show the payment's record from"
    )
    trace.add_argument("--skip-elastic", action="store_true")

    args = parser.parse_args(argv)
    if not getattr(args, "func", None):
        parser.print_help()
        return
    args.func(args)


if __name__ == "__main__":
    main()
