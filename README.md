# Payment Hub

A cross-border payment hub that ingests, validates, screens, routes and settles
payments arriving as SWIFT **MT** over FIN (MT103, MT202, MT202 COV) or **MX** /
ISO 20022 over SWIFTNet Store-and-Forward (pacs.008, pacs.009 incl. COV), plus
the **External Systems Simulator (ESS)** that plays FIN, SnF and compliance screening so the Hub
can be built and tested.

The design is in `docs/platform-design.md`; how it gets performance-tested is in
`docs/performance-testing.md`. `CLAUDE.md` holds the conventions. Those three
documents are the source of truth — if the code and a doc disagree, that is a
bug in one of them.

## What is here

| Part | Where | What it is |
| --- | --- | --- |
| The contract | `proto/` | `hub.v1`, `ext.v1`, `ess.v1`. Seven business RPCs, all unary. |
| Canonical model | `libs/hub_model/` | The format-neutral payment, topic names, flow IDs, identifiers. |
| Wire formats | `libs/hub_format/` | The in-house MT parser, ISO 20022 handling, both mappers, synthetic samples. |
| Telemetry | `libs/hub_telemetry/` | Baggage, ECS logging, gRPC interceptors, Kafka wrappers, tracking levels, metrics. |
| The Hub | `hub/` | One package per stage: `edge`, `fin_parser`, `mx_parser`, `screening`, `routing`, `settlement`, `dispatcher`, `ack_matcher`, `status_api`, `db_sink`. |
| The simulator | `ess/` | FIN, SnF and compliance screening emulators, the inbound sender, the scenario engine, the recorder, `EssControl` and the `ess` CLI. |
| Local stack | `compose.yaml` | One file, profiles for one-container or per-stage. Config in `deploy/compose/`. |
| Tests | `tests/` | `unit/`, `contract/` (the proto, both sides), `e2e/` (whole payments). |

## The pipeline

```
FIN ─DeliverFin─┐                                          ┌─SendMt─→ FIN
                ├→ gRPC edge ─→ hub.in.{fin,mx}.raw ─→ parsers ─→ hub.pay.canonical
SnF ─DeliverMx──┘                                              │
                                                               ↓
                            Compliance ←─Screen── screening ──→ hub.pay.screened
                                    │                               ↓
                         NotifyComplianceDecision                routing
                                    │                               ↓
                                    ↓                           settlement
                         hub.compliance.decision                    ↓
                                                        hub.out.fin / hub.out.mx
                                                                    ↓
                                                               dispatcher ─SendMx─→ SnF
                                                                    ↓
                        hub.pay.status ←─ ACK matcher ←─ hub.net.ack ←─ NotifyAck
```

Four rules hold everywhere, and the tests enforce them:

- **gRPC is the only external boundary.** Nothing outside the Hub writes to its
  Kafka — not the ESS, not a test tool.
- **Durable before acknowledged.** `Deliver*` replies `ACCEPTED` only after the
  Kafka write with `acks=all` is confirmed. No parsing on that path.
- **One transaction per stage.** Each processor reads, processes and writes in a
  single Kafka transaction, offsets committed inside it.
- **MT and MX stay apart** until `hub.pay.canonical`, and split again at
  `hub.out.fin` / `hub.out.mx`.

## Getting started

Everything below works the same on Windows, macOS and Linux. Common tasks go
through one script, `python scripts/dev.py <task>`, so there is nothing
shell-specific to remember; run it with no arguments to list the tasks.

### Prerequisites

| | Windows | macOS (Intel or Apple Silicon) | Linux |
| --- | --- | --- | --- |
| Python | 3.12 or later | 3.12 or later | 3.12 or later |
| Containers | Docker Desktop (WSL 2 backend) | Docker Desktop | Docker Engine with the Compose plugin, or Docker Desktop |
| Memory for Docker | 10 GB: `%UserProfile%\.wslconfig` (below) | 10 GB: Docker Desktop, Settings, Resources | The host's own memory; about 9 GB free |
| Elasticsearch setting | Set by Docker Desktop | Set by Docker Desktop | `sudo sysctl -w vm.max_map_count=262144` |

Supported container runtimes are Docker Desktop and Docker Engine. Podman,
Colima and rootless Docker are not tested: Filebeat reads the Docker socket and
the containers' log files, which those expose differently. Every image the
stack uses is published for both `amd64` and `arm64`.

On Windows, give WSL 2 enough memory in `%UserProfile%\.wslconfig`, then run
`wsl --shutdown` and restart Docker Desktop:

```ini
[wsl2]
memory=10GB
swap=4GB

[experimental]
autoMemoryReclaim=gradual
```

On Linux, keep the Elasticsearch setting across reboots:

```sh
echo "vm.max_map_count=262144" | sudo tee /etc/sysctl.d/99-elasticsearch.conf
sudo sysctl --system
```

Without the `elk` profile neither the 10 GB nor `vm.max_map_count` is needed.

`python scripts/dev.py doctor` checks all of this on your machine and says what
to fix, including which host ports are already taken (each can be changed in
`.env`).

### Install and generate the stubs

Create a virtual environment, then let the script do the rest:

```sh
python -m venv .venv            # python3 on macOS and Linux
```

Debian and Ubuntu ship Python without the `venv` module; install it first with
`sudo apt install python3-venv`.

Activate the environment: `.\.venv\Scripts\Activate.ps1` in PowerShell,
`source .venv/bin/activate` in bash or zsh. Then:

```sh
python scripts/dev.py install   # the gRPC stubs, then pip install -e ".[dev]"
```

Use this rather than `pip install -e .` on a fresh clone: generated code is
never committed, and the project cannot be installed until `gen/` has been
built from `proto/`. `install` does the two in the right order.

### Run the tests

The whole suite runs in seconds with no broker and no containers: the e2e tests
drive the real edge, the real processors and the real simulator over an
in-process Kafka stand-in (`hub_telemetry.memory_bus`).

```sh
python scripts/dev.py check     # stubs, ruff, mypy and pytest: what CI runs
python scripts/dev.py test tests/e2e
python scripts/dev.py lint
python scripts/dev.py types
```

CI (`.github/workflows/ci.yml`) runs `check` on Linux, macOS and Windows, and
brings the real stack up on Linux to push payments through it.

The tests make real gRPC calls over loopback, so the suite multiplies every
gRPC deadline by 10 (`HUB_DEADLINE_SCALE`, set in `tests/__init__.py`) to stay
independent of the machine's speed. Leave it unset everywhere else.

Known issue: the full suite occasionally hangs instead of finishing (roughly one run in three to five on a 16 GB Windows laptop under memory pressure; cause not yet found).
Stop the run and start it again. Every open problem is listed in
[`docs/known-issues.md`](docs/known-issues.md).

### Run the stack

One file, `compose.yaml`, at the repository root, so no `-f` is needed:

```sh
python scripts/dev.py doctor          # is this machine ready?
python scripts/dev.py up              # build the image once, start everything
python scripts/dev.py ready --wait 300  # running is not the same as ready
python scripts/dev.py logs hub
```

`up` copies `.env.example` to `.env` the first time, and builds the shared image
once before starting: the Hub, the ESS and the topic job all use it, and
building it for each in parallel has failed on DNS inside the build.
`docker compose up -d` works too, once the image exists.

`ready` checks what "all containers are up" does not: every consumer group
holds partitions, nothing is stalled, and lag is zero. A stage that loses its
partitions (a laptop that slept, for instance) rejoins its group by itself
after a minute and reports itself on `hub_stage_stalled` until it has.

That is Kafka, the topic catalogue, PostgreSQL, the whole Hub in one container
and the ESS, plus metrics (Prometheus, Grafana, kafka-exporter) and logs
(Elasticsearch, Kibana, Filebeat): **ten long-running containers**. Which
services start is one line in `.env`:

| `COMPOSE_PROFILES` | What runs | When to use it |
| --- | --- | --- |
| `solo,obs,elk` (default) | Hub in one container, dashboards and Kibana | Developing, demos, performance scenarios |
| `solo,obs` | The same without ELK | About 4 GB lighter; metrics only |
| `solo` | Just the Hub, the ESS, Kafka and PostgreSQL | Fastest start, lowest memory |
| `stages,obs,elk` | One container per stage | Watching a consumer group scale, or anything topology-shaped |

Grafana is at <http://localhost:3000> (no login), Prometheus at
<http://localhost:9090> and Kibana at <http://localhost:5601>.
`docs/observability/Payment-Hub-Observability-Guide.docx` explains every
dashboard panel and walks through five fault-injection scenarios.

`solo` and `stages` are alternatives, not additive: both join the same consumer
groups, so running them together just splits the partitions. Switching to
`stages` also means pointing the ESS at the split edge:

```ini
COMPOSE_PROFILES=stages,obs,elk
HUB_EDGE=hub-edge:8443
HUB_STATUS_API=http://hub-status-api:8080
```

The all-in-one container runs the gRPC edge, all eleven stages and the status
API in one process, **each stage on its own thread** — `confluent_kafka`'s
`consume()` blocks, so a shared event loop would serialise eleven 50 ms polls
into half a second of latency per hop. Kafka sees no difference between the two
profiles: same consumer groups, same topics, same transactions.

### Run payments

One command sends one payment; another sends a batch and tells you how it went.

```sh
# one payment
docker compose exec ess python -m ess.cli send --type pacs.008
docker compose exec ess python -m ess.cli send --type MT103 --run-id MYRUN

# a batch, paced, with a settlement report
docker compose exec ess python -m ess.cli batch --count 50 --rate 10
docker compose exec ess python -m ess.cli batch --count 100 --rate 10 --mix "pacs.008=70,MT103=30"
docker compose exec ess python -m ess.cli batch --count 20 --rate 5 --hit-rate 0.1
docker compose exec ess python -m ess.cli batch --count 50 --json     # for a script

# a scripted scenario, asserted end to end
docker compose exec ess python -m ess.cli case run mt103_sanctions_hit
docker compose exec ess python -m ess.cli case run-all                # what CI runs
```

A batch reports what actually happened, not just what was sent:

```
requested 30  accepted 30  rejected 0  duplicate 0  errors 0
sent in 2.95s at 10.2/s
settled 30  unsettled 0  COMPLETED=30
end to end  p50 743 ms  p95 1199 ms  max 1253 ms
batch ok
```

`requested` always equals `accepted + duplicate + rejected + errors`, so a
payment can never quietly disappear. A `duplicate` is a retried delivery whose
first attempt already landed — the idempotency rule working, and a success.

**This is the inbound sender, not a load generator.** Design doc section 8 gives
high-rate load to paygen; about **10/s is the comfortable ceiling** for one
laptop running ELK as well, and past that the 200 ms `Deliver*` deadline starts
costing retries. Lower `--concurrency` before raising `--rate`: fewer
deliveries in flight means fewer retries and, counter-intuitively, a higher
achieved rate.

### Follow one payment

`python scripts/dev.py trace` does the whole walkthrough in one command: it
sends a payment, waits for it to settle, then shows it in the status API, Kafka,
PostgreSQL and Elasticsearch, and prints a Kibana link to its journey:

```sh
python scripts/dev.py trace                             # a pacs.008
python scripts/dev.py trace --type MT103 --topic hub.in.fin.raw
python scripts/dev.py trace --topic hub.pay.canonical
```

#### Doing it by hand (PowerShell)

Run these from the repository root. A bash and zsh version follows.

**1. Send the payment and keep its UETR.** The UETR is the correlation key: the
Kafka message key on every topic, and the search key in Kibana.

```powershell
$out  = docker compose exec -T ess `
          python -m ess.cli send --type pacs.008 --run-id MYRUN
$out
$uetr = [string]($out | Select-Object -First 1).Trim()
```

**2. Ask the Hub where it got to.**

```powershell
Invoke-RestMethod "http://localhost:8080/payments/$uetr" | Format-List
```

`state : COMPLETED`, the seven T0-T6 timestamps, and the seven-hop history.

**3. Find it in Kafka.** Swap the topic to follow it along the pipeline:
`hub.in.mx.raw` (or `hub.in.fin.raw` for MT), `hub.pay.canonical`,
`hub.pay.screened`, `hub.pay.routed`, `hub.out.mx`, `hub.net.ack`,
`hub.pay.status`.

```powershell
docker exec hub-kafka /opt/kafka/bin/kafka-console-consumer.sh `
    --bootstrap-server localhost:9092 `
    --topic hub.in.mx.raw --from-beginning --timeout-ms 12000 `
    --property print.key=true --property print.partition=true --property print.offset=true `
    --property key.separator=" | " 2>$null |
  Select-String -SimpleMatch $uetr
```

Prints `Partition:0 | Offset:4 | <uetr> | <record>`. To see which topics hold
anything at all:

```powershell
docker exec hub-kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 |
  Where-Object { $_ -match '^hub\.' -and $_ -notmatch ':0$' } | Sort-Object
```

**4. Count its events in Elasticsearch.** Around 65 for one clean pacs.008 at
the `full` tracking level.

```powershell
Start-Sleep -Seconds 12    # the index refreshes every 10s; see the notes below

$body = "{`"query`":{`"term`":{`"payment.uetr`":`"$uetr`"}}}"
(Invoke-RestMethod -Method Post -ContentType "application/json" `
   -Uri "http://localhost:9200/logs-payments-*/_count" -Body $body).count
```

**5. Read the whole journey, in order.**

```powershell
$body = "{`"query`":{`"term`":{`"payment.uetr`":`"$uetr`"}},`"sort`":[{`"@timestamp`":`"asc`"}],`"size`":200}"
(Invoke-RestMethod -Method Post -ContentType "application/json" `
   -Uri "http://localhost:9200/logs-payments-*/_search" -Body $body).hits.hits._source |
  Select-Object @{n='service';e={$_.service.name}}, @{n='action';e={$_.event.action}}, message |
  Format-Table -AutoSize
```

**6. The same thing in Kibana.** <http://localhost:5601> → Discover → the
`logs-payments-*` data view → search `payment.uetr : "<uetr>"`, sorted oldest
first, with `service.name`, `event.action` and `message` as columns.

Create the data view once, if the script has not already:

```powershell
$h = @{ "kbn-xsrf" = "true"; "Content-Type" = "application/json" }
$body = '{"data_view":{"title":"logs-payments-*","name":"Payment tracking","timeFieldName":"@timestamp"}}'
Invoke-RestMethod -Method Post -Uri "http://localhost:5601/api/data_views/data_view" -Headers $h -Body $body
```

**7. And in PostgreSQL**, the system of record:

```powershell
docker exec -e PGPASSWORD=hub hub-postgres psql -U hub -d payments -c `
  "SELECT state, service, reason FROM payment_audit WHERE uetr = '$uetr' ORDER BY emitted_ns;"
```

#### Doing it by hand (bash or zsh)

The same steps on macOS, Linux or WSL:

```sh
# 1. send the payment and keep its UETR
uetr=$(docker compose exec -T ess python -m ess.cli send --type pacs.008 --run-id MYRUN | head -1 | tr -d '\r')

# 2. ask the Hub where it got to
curl -s "http://localhost:8080/payments/$uetr" | python -m json.tool

# 3. find it in Kafka
docker exec -e KAFKA_OPTS= hub-kafka /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 --topic hub.in.mx.raw --from-beginning \
    --timeout-ms 12000 --property print.key=true --property print.partition=true \
    --property print.offset=true --property key.separator=" | " 2>/dev/null | grep -a "$uetr"

# 4 and 5. count its events in Elasticsearch, then read the journey
sleep 12
curl -s -H 'Content-Type: application/json' "http://localhost:9200/logs-payments-*/_count" \
    -d "{\"query\":{\"term\":{\"payment.uetr\":\"$uetr\"}}}"

# 7. and in PostgreSQL
docker exec -e PGPASSWORD=hub hub-postgres psql -U hub -d payments -c \
    "SELECT state, service, reason FROM payment_audit WHERE uetr = '$uetr' ORDER BY emitted_ns;"
```

`KAFKA_OPTS=` is cleared because the broker's JVM agent would otherwise start a
second time inside the command-line tool and fail on its port.

#### Three things that will otherwise catch you out

- **Elasticsearch is not instant.** The index template sets
  `refresh_interval: 10s`, so a payment that already shows COMPLETED on the
  status API is not searchable for a few seconds more — a query run too early
  returns 0, not an error. The status API and Kafka are immediate.
- **Kafka values are Protobuf**, so `kafka-console-consumer.sh` prints them as
  binary with the identifiers and any XML legible inside. The record *key* is
  the UETR in plain text, which is what makes `Select-String` or `grep` work.
- **On Windows, do not use Git Bash for `docker exec`** unless you set
  `MSYS_NO_PATHCONV=1`. Git Bash rewrites `/opt/kafka/...` into a Windows path,
  and the exec fails with a misleading "no such file or directory". PowerShell,
  WSL, macOS and Linux shells are unaffected, and so is `scripts/dev.py`.

**The local ELK stack is throwaway.** There are no named volumes anywhere in the
Compose file, so `docker compose down` removes the containers and all the logs
with them. `docker compose stop` keeps them for when you want to come back.

### Drive the simulator

```bash
ess send --type pacs.008 --file samples/pacs008_eur.xml \
         --app-hdr samples/pacs008_eur.apphdr.xml
ess send --type MT103 --file samples/mt103_gbp.fin
ess case list                                   # the scenario catalogue
ess case run mt103_sanctions_hit                # one scripted case
ess case run-all                                # what CI runs on a Hub change
ess profile set compliance --hit-rate 0.02 --decision-delay 5m
ess profile set fin --outage --for 2m           # fault injection
ess counters --run-id S02-2026-09-28-01
```

Every subcommand but `serve` is a thin `EssControl` client, so the same commands
work against a simulator in Docker, in Kubernetes or on a laptop. Add
`--ess <host>:<port>` to point somewhere else.

## Payment tracking

A payment carries seven identifiers as OpenTelemetry **Baggage**, set once at the
edge and travelling on every gRPC call and every Kafka record next to
`traceparent`:

`payment.uetr`, `payment.format`, `payment.msg_type`, `payment.flow`,
`payment.biz_msg_id`, `payment.trace`, and `run.id` in test environments.

The header stays under 512 bytes, names and accounts and amounts never go in it,
and it is stripped on calls to the real FIN, SnF and compliance screening. `tests/e2e/test_tracking.py`
holds each of those to account.

How much is logged depends on `HUB_TRACKING_MODE`:

| Mode | Level | Events per clean payment |
| --- | --- | --- |
| `functional` | `full` | ~30 — every touchpoint and every state change |
| `ci` | `full` | ~30 |
| `performance` | `errors` | 0 |

The level is checked before an event is built, so a performance run costs
nothing in logging. Add `HUB_TRACKING_SPOT_CHECK=minimal:0.1%` to sanity-check a
few journeys under load.

## Development

| Task | Command |
| --- | --- |
| Regenerate the gRPC stubs | `python scripts/gen_proto.py` |
| Check the stubs are current | `python scripts/gen_proto.py --check` |
| Regenerate the samples | `python scripts/make_samples.py` |
| Print the topic catalogue | `python scripts/create_topics.py --list` |
| Everything a pull request must pass | `python scripts/dev.py check` |
| Check this machine can run the stack | `python scripts/dev.py doctor` |
| Check the running stack is ready for a test | `python scripts/dev.py ready` |
| Follow one payment through the stack | `python scripts/dev.py trace` |
| Send a batch | `docker compose exec ess python -m ess.cli batch --count 50 --rate 10` |
| Run the whole Hub in one process | `python -m hub all` |
| Run one Hub service | `python -m hub <service>` |
| Run the simulator | `python -m ess.cli serve --mode functional` |

Never hand-edit anything under `gen/` or `samples/` — both are generated, and
`gen/` is not committed. Samples are synthetic by construction: test BICs,
invented company names, and IBANs built to pass the `schwifty` checksum.

## What is not built yet

The build order in `CLAUDE.md` runs to five steps. Steps 1 to 4 are done:
foundations, the canonical model, the vertical slice, and the remaining flows
with CI contract tests. Step 5, the performance framework (`perfctl`, `paygen`,
the analyser and the SLO gate from `docs/performance-testing.md`), is not
started — the ESS has its performance mode, which is the prerequisite.

Several things are placeholders because the documents list them as open
questions, and each is marked with a `TODO(business-rules)` comment next to the
code that will change:

- **Routing rules** — correspondent selection and cut-off windows. The flows and
  a small currency cut-off table are implemented; the bank's real rules are not.
- **Settlement** — ledger posting records a deterministic reference and value
  date. There is no ledger interface yet.
- **MX schema validation** — structural checks always run. Official CBPR+ XSDs
  are licensed through Swift MyStandards and are not in this repository; point
  `HUB_MX_SCHEMA_DIR` at a directory of `<msg_type>.xsd` files to turn XSD
  validation on.
- **Completion** — the Hub treats the network ACK as completion. See
  `COMPLETION_RULE` in `hub/ack_matcher/processor.py`; the other candidates are
  the outbound handoff and the final pacs.002.
- **Schema registry** — records are plain Protobuf on Kafka. Whether the
  registry is Confluent or Apicurio is still open, so nothing depends on either.
- **Helm charts** — `deploy/helm/` is empty; Compose is the only deployment.
