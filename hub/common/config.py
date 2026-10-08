"""Configuration, read once from the environment.

Compose and Helm set the same variables, so a service does not know or care
which one started it.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any

from hub_telemetry.tracking import Level, Mode, TrackingPolicy, parse_spot_check


def _env(name: str, default: str = "") -> str:
    return os.environ.get(name, default).strip()


def _env_int(name: str, default: int) -> int:
    raw = _env(name)
    try:
        return int(raw) if raw else default
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    raw = _env(name)
    try:
        return float(raw) if raw else default
    except ValueError:
        return default


def _env_bool(name: str, default: bool = False) -> bool:
    raw = _env(name).lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "on"}


@dataclass(slots=True)
class KafkaSettings:
    bootstrap_servers: str = "localhost:9092"
    partitions: int = 24
    replication: int = 3
    batch_size: int = 500
    poll_timeout_s: float = 0.05
    #: A stage that has held no partitions for this long drops its consumer and
    #: joins its group again. 0 turns it off, which a deployment with more
    #: replicas than partitions needs: there an idle replica is normal.
    rejoin_after_s: float = 60.0
    create_topics: bool = False
    security: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_env(cls) -> KafkaSettings:
        security: dict[str, Any] = {}
        protocol = _env("KAFKA_SECURITY_PROTOCOL")
        if protocol:
            security["security.protocol"] = protocol
            for key, var in (
                ("sasl.mechanisms", "KAFKA_SASL_MECHANISM"),
                ("sasl.username", "KAFKA_SASL_USERNAME"),
                ("sasl.password", "KAFKA_SASL_PASSWORD"),
                ("ssl.ca.location", "KAFKA_SSL_CA"),
                ("ssl.certificate.location", "KAFKA_SSL_CERT"),
                ("ssl.key.location", "KAFKA_SSL_KEY"),
            ):
                value = _env(var)
                if value:
                    security[key] = value
        return cls(
            bootstrap_servers=_env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            partitions=_env_int("KAFKA_PARTITIONS", 24),
            replication=_env_int("KAFKA_REPLICATION", 3),
            batch_size=_env_int("HUB_BATCH_SIZE", 500),
            poll_timeout_s=_env_float("HUB_POLL_TIMEOUT_S", 0.05),
            rejoin_after_s=_env_float("HUB_REJOIN_AFTER_S", 60.0),
            create_topics=_env_bool("KAFKA_CREATE_TOPICS", False),
            security=security,
        )


@dataclass(slots=True)
class ExternalSettings:
    """Where the FIN, SnF and compliance screening gateways live.

    In every test environment all three point at the ESS. Switching to the real
    systems is these three addresses plus ``real_networks=True``, which makes
    the client interceptors strip baggage and traceparent.
    """

    fin_target: str = "localhost:9101"
    snf_target: str = "localhost:9101"
    compliance_target: str = "localhost:9101"
    real_networks: bool = False

    @classmethod
    def from_env(cls) -> ExternalSettings:
        ess = _env("ESS_TARGET", "localhost:9101")
        return cls(
            fin_target=_env("FIN_TARGET", ess),
            snf_target=_env("SNF_TARGET", ess),
            compliance_target=_env("COMPLIANCE_TARGET", ess),
            real_networks=_env_bool("HUB_REAL_NETWORKS", False),
        )


@dataclass(slots=True)
class ServiceSettings:
    """Everything one Hub service needs to start."""

    service_name: str
    grpc_port: int = 0
    metrics_port: int = 0
    log_level: str = "INFO"
    environment: str = "local"
    version: str = "0.1.0"
    otlp_endpoint: str = ""
    trace_sample_ratio: float = 1.0
    tracking_mode: Mode = Mode.FUNCTIONAL
    tracking_level_override: Level | None = None
    tracking_spot_check: tuple[Level, float] | None = None
    kafka: KafkaSettings = field(default_factory=KafkaSettings)
    external: ExternalSettings = field(default_factory=ExternalSettings)
    database_url: str = ""
    pod_id: str = ""

    @classmethod
    def from_env(cls, service_name: str, *, default_grpc_port: int = 0) -> ServiceSettings:
        mode = Mode.parse(_env("HUB_TRACKING_MODE"))
        override = _env("HUB_TRACKING_LEVEL")
        return cls(
            service_name=service_name,
            grpc_port=_env_int("HUB_GRPC_PORT", default_grpc_port),
            metrics_port=_env_int("HUB_METRICS_PORT", 9464),
            log_level=_env("HUB_LOG_LEVEL", "INFO").upper(),
            environment=_env("HUB_ENV", "local"),
            version=_env("HUB_VERSION", "0.1.0"),
            otlp_endpoint=_env("OTEL_EXPORTER_OTLP_ENDPOINT"),
            trace_sample_ratio=_env_float("OTEL_TRACES_SAMPLER_ARG", 1.0),
            tracking_mode=mode,
            tracking_level_override=Level.parse(override) if override else None,
            tracking_spot_check=parse_spot_check(_env("HUB_TRACKING_SPOT_CHECK")),
            kafka=KafkaSettings.from_env(),
            external=ExternalSettings.from_env(),
            database_url=_env("HUB_DATABASE_URL"),
            # Kubernetes sets HOSTNAME to the pod name; it is what makes each
            # replica's transactional.id unique.
            pod_id=_env("HUB_POD_ID") or _env("HOSTNAME") or f"local-{os.getpid()}",
        )

    def tracking_policy(self) -> TrackingPolicy:
        return TrackingPolicy(
            self.tracking_mode,
            level_override=self.tracking_level_override,
            spot_check=self.tracking_spot_check,
        )

    def transactional_id(self, stage: str) -> str:
        """Stable per replica, unique across replicas — Kafka requires both."""
        return f"{stage}-{self.pod_id}"
