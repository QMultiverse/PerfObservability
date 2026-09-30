"""Create the Hub's topic catalogue, then exit.

Runs once at the start of the local stack, before any service. Partitions and
replication come from ``KAFKA_PARTITIONS`` / ``KAFKA_REPLICATION``, so the same
script serves one broker on a laptop and three brokers in a shared environment.

    python scripts/create_topics.py
    python scripts/create_topics.py --list
"""

from __future__ import annotations

import argparse
import logging
import sys
import time

from hub_model import topics as tp
from hub_telemetry.ecs import setup_logging

from hub.common.config import KafkaSettings

log = logging.getLogger("create-topics")


def wait_for_broker(bootstrap: str, timeout_s: float = 60.0) -> None:
    """Block until the broker answers a metadata request."""
    from confluent_kafka.admin import AdminClient

    admin = AdminClient({"bootstrap.servers": bootstrap})
    deadline = time.monotonic() + timeout_s
    last: Exception | None = None
    while time.monotonic() < deadline:
        try:
            admin.list_topics(timeout=5)
            return
        except Exception as exc:
            last = exc
            time.sleep(2.0)
    raise SystemExit(f"broker at {bootstrap} did not come up: {last}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--list", action="store_true", help="print the catalogue and exit")
    args = parser.parse_args()

    settings = KafkaSettings.from_env()
    specs = tp.topic_specs(partitions=settings.partitions, replication=settings.replication)

    if args.list:
        for spec in specs:
            retention = f"{spec.retention_ms // 86_400_000}d" if spec.retention_ms else "-"
            print(
                f"{spec.name:<44} {spec.partitions:>3}p "
                f"rf={spec.replication} {spec.cleanup_policy:<8} {retention}"
            )
        return 0

    setup_logging("create-topics", asynchronous=False)
    log.info("waiting for %s", settings.bootstrap_servers)
    wait_for_broker(settings.bootstrap_servers)

    from hub_telemetry.kafka_bus import KafkaBus

    bus = KafkaBus(
        settings.bootstrap_servers,
        security=settings.security,
        client_id="create-topics",
        default_partitions=settings.partitions,
        default_replication=settings.replication,
    )
    bus.ensure_topics(specs)
    log.info(
        "topic catalogue ready: %d topics, %d partitions each, replication %d",
        len(specs),
        settings.partitions,
        settings.replication,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
