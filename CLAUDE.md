# CLAUDE.md — Payment Hub

Context for Claude Code. Read this first, then the design docs:

- `docs/platform-design.md`: the Payment Hub, its gRPC contract, Kafka design, the External Systems Simulator (ESS) and payment tracking with ELK. **This is the source of truth for the platform.**
- `docs/performance-testing.md`: the performance testing infrastructure (perfctl, paygen, SLO gate, observability). This comes later; the platform comes first.

If code and docs disagree, stop and ask. When a design decision changes, update the relevant doc in the same change.

## What we are building

A **Payment Hub** that ingests, validates, screens, routes and settles cross-border payments arriving as SWIFT **MT** over FIN (MT103, MT202, MT202 COV) or **MX** / ISO 20022 over SWIFTNet Store-and-Forward (pacs.008, pacs.009 incl. COV, pacs.002, pacs.004, camt.056 / camt.029). It also includes the **External Systems Simulator (ESS)**, which plays FIN, SnF and FCC (financial crime compliance) so the Hub can be developed and tested.

## Names and conventions (use these exactly)

| Thing | Name |
| --- | --- |
| System under test | Payment Hub ("the Hub") |
| Mock of FIN, SnF and FCC | External Systems Simulator (ESS) |
| gRPC services the Hub serves | `HubInbound` (`DeliverFin`, `DeliverMx`), `HubNetworkEvents` (`NotifyAck`, `NotifyDeliveryNotification`), `HubCompliance` (`NotifyFccDecision`) |
| gRPC services the Hub calls | `FinGateway.SendMt`, `SnfGateway.SendMx`, `FccScreening.Screen` |
| ESS control API | `EssControl` (`SetProfile`, `RunCase`, `ListCases`, `SendInbound`, `RunBatch`, `GetCounters`, `Reset`) and the `ess` CLI |
| Proto packages | `hub.v1` (`proto/hub/v1/hub_edge.proto`), `ext.v1` (`proto/ext/v1/networks.proto`), `ess.v1` (`proto/ess/v1/control.proto`) |
| Kafka topics | `hub.in.fin.raw`, `hub.in.mx.raw`, `hub.pay.canonical`, `hub.pay.screened`, `hub.pay.routed`, `hub.out.fin`, `hub.out.mx`, `hub.net.ack`, `hub.fcc.decision`, `hub.pay.status`, `hub.pay.state` (compacted), plus `<topic>.retry.30s`, `.retry.5m`, `.dlq` |
| Consumer groups | `cg-fin-parser`, `cg-mx-parser`, `cg-screening`, `cg-routing`, `cg-settlement`, `cg-dispatch-fin`, `cg-dispatch-mx`, `cg-ack-matcher`, `cg-status-api`, `cg-db-sink`, `cg-retry` (drains the retry topics) |
| Correlation key | UETR (UUID v4). It is the Kafka key on every topic and travels in gRPC metadata. Cover pairs share one UETR. |
| Flow IDs | `MT_FIN_103`, `MT_FIN_103_202COV`, `MT_TO_MX_103`, `MX_SNF_PACS008`, `MX_SNF_PACS009`, `MX_SNF_PACS009COV_PAIR` |
| Shared telemetry library | `hub-telemetry` |
| Shared wire-format library | `hub-format` (MT and MX on the wire; kept out of `hub-model`, which is format-neutral) |

## Architecture rules (do not break)

1. **gRPC is the only external boundary.** Nothing outside the Hub, including the ESS and test tools, writes to the Hub's Kafka. The one exception is the performance status tracker, which reads `hub.pay.status` with a read-only ACL.
2. **Durable before acknowledged.** `DeliverFin` / `DeliverMx` reply `ACCEPTED` only after the Kafka write (`acks=all`) succeeds. No parsing on the edge path.
3. **MT and MX stay separate** until `hub.pay.canonical` (separate RPCs, raw topics and parsers), then split again at `hub.out.fin` / `hub.out.mx`. Records on shared topics carry a `format` header (MT / MX).
4. **Each processor reads, processes and writes in one Kafka transaction** (read_committed, transactional producer, offsets committed in the transaction). Calls outside Kafka (`Screen`, `SendMt`, `SendMx`) are not transactional, so every receiver de-duplicates on (UETR, message type, direction).
5. **Claim-check:** raw MT/MX bytes travel only on `*.raw` topics. Later topics carry the canonical model plus a pointer (topic, partition, offset).
6. **Every gRPC call sets a deadline** (proposed: `Deliver*` 200 ms, `Send*` 500 ms, `Screen` 1 s). Retry only `UNAVAILABLE` / `DEADLINE_EXCEEDED`, with exponential backoff and jitter, at most 3 times.
7. **Failures go to retry topics, then the DLQ.** One bad message must never block a partition.
8. **Screening hits** (`HIT_PENDING`) park the payment in HELD. `NotifyFccDecision` → `hub.fcc.decision` → processing resumes.
9. **Cover pairs** are matched in a state store (rebuilt from `hub.pay.state`) and complete together. **Decided:** the matching stage is **screening** (`hub/screening/processor.py`), because both legs must clear compliance before either is released, and that keeps `AWAITING_COVER` next to `HELD` in one store. The ACK matcher matches again on the way out, so the pair is COMPLETED only once both ACKs arrive. The store is in-memory with the `hub.pay.state` rebuild path built in (`hub/common/state_store.py`); a Valkey-backed implementation can replace it behind the same interface.

## Payment tracking (ELK) and baggage

- OpenTelemetry **Baggage** is set once at the Hub edge and carried as the W3C `baggage` header, in gRPC metadata and in Kafka record headers, alongside `traceparent`.
- Baggage keys: `payment.uetr`, `payment.format`, `payment.msg_type`, `payment.flow`, `payment.biz_msg_id`, `payment.trace`, and `run.id` (test only). Keep it under 512 bytes. **Never** put names, accounts or amounts in baggage. Strip it on calls to the real FIN / SnF / FCC.
- `hub-telemetry` provides gRPC server and client interceptors, Kafka produce/consume wrappers, a logging filter that copies baggage into every log line, a baggage span processor and asynchronous (queue) logging. Events are one JSON line in ECS format, with `event.action` one of `grpc.server.recv|reply`, `grpc.client.send|reply`, `kafka.produce`, `kafka.consume`, `payment.state`, `payment.error`.
- **Tracking levels:** `full` / `standard` / `minimal` / `errors` / `none`. The mode is set by `HUB_TRACKING_MODE`:
  - `functional` and `ci` → `full` for every payment.
  - `performance` → `errors` (or `none`), optionally `minimal` for 0.1% of payments.
  - The edge writes the level into `payment.trace`, and every service must check it **before** building an event.

## Tech stack

- **Language and tools:** Python 3.12 or later (`requires-python = ">=3.12"`; the local venv is 3.13), with `ruff`, `mypy --strict` and `pytest`. Dependencies are declared in `pyproject.toml` and installed with `pip install -e ".[dev]"`; `uv` works too but nothing depends on it.
- **gRPC:** `grpcio` / `grpc.aio`, with stubs from `proto/` via `grpcio-tools` or `buf`.
- **Kafka:** `confluent-kafka`. Serialisation is Protobuf with a schema registry (Confluent Schema Registry or Apicurio — still open).
- **Observability:** OpenTelemetry SDK. Logs go to ELK (Filebeat → Elasticsearch → Kibana); traces to the OTel Collector (Elastic APM optional); metrics to Prometheus.
- **Storage:** PostgreSQL is the system of record (filled from `hub.pay.status` by `cg-db-sink`).
- **CLIs:** Typer, for `ess` and later `perfctl` / `paygen`.
- **Message handling:** `lxml` + `xmlschema` for MX (ISO 20022 XSDs; CBPR+ schemas come from Swift MyStandards). The MT parser is in-house.

## Local environment (Windows, 16 GB RAM)

- **Setup:** Docker Desktop on WSL2. PyCharm, with the interpreter inside WSL2 or Docker. For the Claude Code plugin with WSL, set the Claude command to `wsl -d Ubuntu -- bash -lic "claude"`.
- **`%UserProfile%\.wslconfig`:** `memory=10GB`, `swap=4GB`, plus `[experimental] autoMemoryReclaim=gradual`. Inside WSL2, also set `vm.max_map_count=262144`.
- **Memory limits:**

  | Component | Limit | Notes |
  | --- | --- | --- |
  | Elasticsearch | 3 GB | Heap 1.5 GB, single node, 1 shard, 0 replicas, ML off, `best_compression` |
  | Kibana | 1 GB | |
  | Filebeat | 256 MB | No Logstash locally |
  | Kafka | 1.5 GB | 1 broker, KRaft |
  | Hub services + ESS | 2.5 GB | One replica each |

- **Throwaway data:** the ELK compose stack uses **no named volumes**, so `docker compose down` wipes all logs, and `docker compose stop` keeps them. The index lifecycle policy rolls over at 2 GB and deletes after 1 day.
- **Load:** keep local loads at or below ~50 TPS. Peak performance runs belong in a shared environment.

## Repo layout

```
payment-hub/
├── CLAUDE.md
├── docs/                     # platform-design.md, performance-testing.md
├── proto/                    # hub/v1, ext/v1, ess/v1 (the contract; versioned)
├── libs/hub_telemetry/       # interceptors, Kafka wrappers, baggage, ECS logging, tracking levels
├── libs/hub_model/           # canonical payment model, topic names, flow IDs, identifiers
├── libs/hub_format/          # MT parser/builder, ISO 20022 handling, both mappers, synthetic samples
├── hub/                      # one package per service:
│   ├── edge/                 #   gRPC edge: HubInbound, HubNetworkEvents, HubCompliance
│   ├── fin_parser/  mx_parser/
│   ├── screening/  routing/  settlement/
│   ├── dispatcher/  ack_matcher/
│   ├── status_api/  db_sink/
│   └── common/              #   config, the transactional loop, state store, retry consumer
├── ess/                      # External Systems Simulator: emulators, sender, scenario engine, recorder, EssControl, `ess` CLI
├── samples/                  # sample pacs.008 / pacs.009 / MT103 / MT202 messages (synthetic data only)
├── compose.yaml              # the single local stack; profiles solo | stages | elk
├── deploy/compose/           # the config compose.yaml mounts (filebeat, elasticsearch)
├── deploy/helm/              # later
├── scripts/                  # gen_proto, create_topics, make_samples, dev.ps1
├── gen/                      # generated gRPC stubs; not committed
└── tests/                    # unit/, contract/ (proto contract, both sides), e2e/ (via ESS)
```

`gen/` is produced by `python scripts/gen_proto.py` and is gitignored. `samples/`
is produced by `python scripts/make_samples.py`; both are generated, never
hand-edited.

The performance framework (perfctl, paygen, analyser) comes later, per `docs/performance-testing.md`.

## Build order (agreed)

1. **Foundations:**
   - Repo skeleton and the `proto/` contract with code generation.
   - `hub-telemetry`.
   - The local Docker Compose stack.
   - ESS **functional mode**: emulators, `ess send`, `ess case run`.
2. **Canonical payment model:** draft `libs/hub_model` before the parsers.
3. **Vertical slice:** one **pacs.008, no screening hit**, from `ess send` through the edge, parser, screening, pass-through routing/settlement, dispatcher and ACK matcher to COMPLETED on `hub.pay.status`. It must be visible in Kibana by UETR. Then do the same for MT103.
4. **Remaining flows:** cover pairs, screening hits (HELD → decision), NAKs, rejects, retries and DLQ, plus CI contract tests.
5. **Performance:** ESS performance mode, then the performance framework.

## Execution model

Two ways to run the Hub, same code and same consumer groups either way:

| | Command | Containers | Use |
| --- | --- | --- | --- |
| All-in-one | `python -m hub all`, profile `solo` | 1 | Local development and demos |
| Per stage | `python -m hub <stage>`, profile `stages` | 11 | Production shape; scaling a group on its own |

The all-in-one runtime (`hub/common/all_in_one.py`) gives **each stage its own
thread**, not a shared event loop: `confluent_kafka`'s `consume()` blocks, so
eleven stages sharing one loop with a 50 ms poll would serialise into half a
second of added latency per hop.

`compose.yaml` at the repository root is the only Compose file; profiles pick
the topology, set in `.env` as `COMPOSE_PROFILES`. `solo` and `stages` are
alternatives — running both just splits the partitions between them.

Driving payments is the `ess` CLI: `send` for one, `batch` for many (paced, with
a settlement report and end-to-end percentiles), `case run` for a scripted
scenario. `batch` is the inbound sender, not a load generator — about 10/s is
the local ceiling, and paygen owns real load.

## Build status

Steps 1 to 4 of the build order are implemented and tested end to end
(`pytest tests/e2e`). Step 5, the performance framework, is not started.

Decisions the implementation had to make, each recorded next to the code:

| Decision | Where | Why |
| --- | --- | --- |
| Completion is the **network ACK** | `COMPLETION_RULE` in `hub/ack_matcher/processor.py` | It is what section 5 stamps T6 on. The other candidates stay open. |
| Cover pairs match in **screening**, and again in the ACK matcher | `hub/screening/processor.py` | Both legs must clear compliance together; both ACKs must arrive before COMPLETED. |
| The idempotency marker is **per leg** on a cover flow | `Processor.stage_for` in `hub/common/processor.py` | One UETR carries two messages, so a bare stage name would let the first leg mark the stage done and the second be skipped. |
| `hub.pay.status` carries **RECEIVED** from the edge | `hub/edge/server.py` | The topic is every state change, and RECEIVED is the first one. Written without flushing: the durability promise is the raw write. |
| The retry ladder is drained by its **own service** | `hub/common/retry_consumer.py`, group `cg-retry` | Waiting in the failing stage would hold its partition, which is what the ladder exists to avoid. |
| XSD validation is **opt-in** | `HUB_MX_SCHEMA_DIR`, `hub_format.mx.XsdValidator` | CBPR+ schemas are licensed through Swift MyStandards and cannot be committed. Structural validation always runs. |
| The ESS recorder writes **JSON Lines** | `ess/recorder.py` | The doc asks for Parquet; `pyarrow` is not a dependency. Parquet is written when `pyarrow` happens to be importable. |
| Tests run against an **in-process Kafka** | `hub_telemetry.memory_bus`, `tests/harness.py` | The whole suite runs in seconds with no broker. Service code only ever sees the `Bus` interface; `KafkaBus` is the real one. |
| A payment's state **never moves backwards**, except out of FAILED | `supersedes` in `hub_model/envelope.py`; the guards in `hub/status_api` and `hub/db_sink` (same rule in SQL) | A network ACK can beat the dispatcher's own transaction commit, so COMPLETED legitimately arrives before DISPATCHED on `hub.pay.status`. Both sinks rank the state; timings still accumulate from every event. FAILED means "on the retry ladder", not terminal, so moving into or out of it goes by event time: a payment retried and then dispatched must not stay FAILED (found by perf scenario 3). |
| The ESS **redelivers** network notifications; the Hub **reports** a missing ACK | `notify_reliably` in `ess/emulators.py`; `ACK_OVERDUE_S` and `hub_awaiting_ack` in `hub/ack_matcher/processor.py` | A real FIN / SnF interface keeps an undelivered ACK queued. Giving up after one call left payments DISPATCHED forever, unnoticed (perf scenarios 1, 3, 4). Redelivery is **bounded**: one call per round (the channel does not retry underneath), backoff 2 s to 60 s with jitter, at most 5 redeliveries a second per emulator, for up to 10 minutes; the Hub de-duplicates repeats. Unbounded, it caused a retry storm (perf scenario 2). What the Hub *does* about an overdue ACK is a `TODO(business-rules)`. |

## Open questions (don't guess — ask or leave a clear TODO)

- **Completion:** what counts as completed — outbound handoff, network ACK, or final pacs.002? (Implemented as the network ACK; see `COMPLETION_RULE`.)
- **FCC:** is the real FCC call synchronous on the payment path, or event-based?
- **Volumes:** what is the MT vs MX split and peak TPS? The SLO numbers in the docs are placeholders.
- **Production inbound path:** a gRPC edge, or an MQ / Alliance Access adapter?
- **Tooling:** Confluent Schema Registry or Apicurio? And the database schema for the payment store.
- **Business rules** for routing (FIN vs SnF, cut-offs), settlement / ledger posting, and MT→MX translation. (Placeholders marked `TODO(business-rules)` in `hub/routing/processor.py` and `hub/settlement/processor.py`.)
- **NAK and reject code catalogue**, and the retry rules for each error. (The ladder is built; which errors are permanent is currently decided per stage by `PermanentError` vs `RetryableError`.)

## Working agreements for Claude

- Keep changes small and runnable; add or adjust tests with every change (`pytest`), and run `ruff` and `mypy`.
- Never use real customer data. Samples and generated data must be synthetic; IBANs must pass checksum via `schwifty`.
- Regenerate gRPC stubs from `proto/`; never hand-edit generated code.
- Don't add paid or restrictively licensed dependencies without asking (see `docs/performance-testing.md` §4.1).
- Explain Windows / WSL2 specifics when giving commands (PowerShell vs WSL bash).
