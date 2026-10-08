// Chapters 1-4: start here, concepts, setup, stack tour.
const { p, h1, h2, h3, bullets, numbered, code, callout, figure, table } = require("./lib");

module.exports = (DIAG, SHOTS) => [
  // ------------------------------------------------------------ 1. Start here
  h1("1. Start here"),
  p("This guide explains how the Payment Hub is observed: how to bring up the local stack, what each component does, what every metric and dashboard panel means, and how those panels were used to find real problems in five fault-injection scenarios."),
  p("It is written for someone new to observability. Every concept is introduced through the Hub's own metrics, so by the end you should be able to open the Grafana dashboards during a test and explain what you are looking at."),
  h2("1.1 What you will learn"),
  bullets([
    "What observability is, and the difference between **metrics**, **logs** and **traces**.",
    "How to set up and start the stack on Windows, macOS or Linux.",
    "What each part of the stack does: the Hub, the External Systems Simulator (ESS), Kafka, PostgreSQL, Prometheus, Grafana, the exporters and the ELK log stack.",
    "What each of the 40 panels on the performance dashboard measures, what _normal_ looks like, and what _bad_ looks like.",
    "How to run a performance test properly: readiness checks, a warm-up gate, injecting a fault and marking it on the dashboards.",
    "A repeatable method for reading the dashboards top-down to find a root cause.",
    "Five worked investigations, each with the screenshots that led to the answer.",
  ]),
  h2("1.2 How to read this guide"),
  table(
    ["If you want to…", "Read"],
    [
      ["Understand the ideas first", "Chapter 2 (concepts), then chapter 4 (stack tour)"],
      ["Get the stack running", "Chapter 3 (setup), then Appendix A (commands)"],
      ["Know what a panel means", "Chapter 5 (dashboards and panels) and Appendix B (metric catalogue)"],
      ["Run a test yourself", "Chapter 6 (running a test) and chapter 7 (triage method)"],
      ["Learn by example", "Chapters 8 to 12, one per scenario"],
      ["See what was found and fixed", "Chapter 13 (findings)"],
    ],
    [4, 6]
  ),
  h2("1.3 About this version"),
  p("This guide describes branch `main` as of 8 October 2026. That is later than commit `4e215b2`, the most recent commit when it was written: the later fixes from the scenario runs, the compliance rename and the cross-platform `scripts/dev.py` are described here but were not yet committed. Commands and ports may change after that; the repository's `README.md`, `compose.yaml` and `CLAUDE.md` are the source of truth for exact values. All payment data is synthetic."),
  callout("warn", "The screenshots were taken before the compliance screening service was renamed in the code. A few legends and method names in them still show its earlier abbreviation; they refer to the same service. The text of this guide uses the current names throughout."),
  callout("key", [
    "Observability is the ability to answer questions about a running system from the outside, without changing its code to ask them. A good set of metrics, logs and dashboards lets you go from **\"something is slow\"** to **\"this stage is waiting on that dependency\"** in a few minutes.",
  ]),

  // ------------------------------------------------------------ 2. Concepts
  h1("2. Observability concepts"),
  p("This chapter covers the ideas you need to read the dashboards. Each idea is shown with a real metric from the Hub."),
  h2("2.1 Metrics, logs and traces"),
  table(
    ["Signal", "What it is", "In this stack", "Best at answering"],
    [
      ["**Metrics**", "Numbers sampled over time, already aggregated (a count, a rate, a percentile).", "Prometheus scrapes them; Grafana draws them.", "Is something wrong? Where? Since when? How much?"],
      ["**Logs**", "One line per event, with full detail (which payment, which step, what error).", "Every service writes one JSON line per event; Filebeat ships them to Elasticsearch; Kibana searches them.", "What exactly happened to _this_ payment?"],
      ["**Traces**", "The path of one request across services, as timed spans.", "The Hub creates OpenTelemetry spans and can export them, but the local stack runs no trace store.", "Which hop of one request was slow?"],
    ],
    [1.3, 3, 3.4, 2.6], { caption: "The three signals. In this guide, metrics find the problem and logs confirm it." }
  ),
  p("Metrics are cheap because they are aggregated: the Hub does not record a separate number for each payment, it records running counts and timings for each stage. That is why metrics can run at full load while detailed logging is switched down in performance mode."),
  p("Logs are the opposite: rich but expensive. Every payment carries a **UETR** (its unique end-to-end reference), and every log line about it includes `payment.uetr`, so you can search one payment's full journey in Kibana."),
  callout("key", "A useful rule: **metrics tell you THAT something is wrong and WHERE; the per-payment log trace tells you WHAT happened.** Every scenario in this guide follows that path."),

  h2("2.2 How Prometheus collects metrics"),
  p("Prometheus uses a **pull** model. Each service exposes a plain-text page at `/metrics`, and Prometheus fetches (\"scrapes\") it every 5 seconds. Each line on that page is one **time series**: a metric name plus a set of **labels**."),
  code([
    "# One series: records processed by the routing stage that succeeded",
    "hub_records_processed_total{service=\"hub-routing\",outcome=\"ok\"} 2017",
    "",
    "# Same metric, different labels = a different series",
    "hub_records_processed_total{service=\"hub-routing\",outcome=\"retry\"} 3",
  ]),
  p("Labels are how you slice a metric: by stage (`service`), by format (`format=\"MT\"` or `\"MX\"`), by consumer group, and so on. Each distinct combination of labels is stored separately, so labels must have few possible values. That is why the test run ID is added once per scrape as an external label, never as a label on every metric."),

  h2("2.3 The three metric types"),
  table(
    ["Type", "Behaviour", "Hub example", "How you read it"],
    [
      ["**Counter**", "Only ever goes up (resets to 0 when the process restarts).", "`hub_records_processed_total`", "Never look at the raw value; take its rate: `rate(...[1m])` gives records per second."],
      ["**Gauge**", "Goes up and down; a reading of the current value.", "`hub_kafka_consumer_lag`, `hub_held_payments`, `process_resident_memory_bytes`", "Read it directly: \"lag is 345 right now\"."],
      ["**Histogram**", "Counts observations into buckets (≤ 5 ms, ≤ 10 ms, … ≤ 30 s).", "`hub_grpc_server_latency_seconds`, `ess_e2e_latency_seconds`", "Use `histogram_quantile(0.95, …)` to estimate a percentile."],
    ],
    [1.3, 2.6, 3.2, 3.6]
  ),
  callout("warn", [
    "A histogram can only report up to its highest bucket. When scenario 1 drove end-to-end latency past 30 seconds, every panel showed exactly **30 s**, because 30 s was the top bucket. The real value was higher. The end-to-end histograms were extended to 10 minutes before scenario 3 so the retry tail could be measured.",
  ]),

  h2("2.4 rate(), increase() and why a counter's first value is invisible"),
  p("`rate(x[1m])` is the average per-second increase of counter `x` over the last minute. `increase(x[5m])` is the total increase over five minutes. Both need at least two samples: a counter that is born at 1 and never changes shows an increase of **0**, because Prometheus never saw it at 0."),
  p("This caught out the **Rebalance** panel: every consumer group rebalanced once at start-up, but `increase()` showed zero. The panel now shows the cumulative count since start-up instead."),

  h2("2.5 Percentiles, and why averages mislead"),
  p("The **p95** (95th percentile) latency is the value that 95 % of payments were faster than. **p50** is the median, **p99** the slowest 1 % boundary. Percentiles show what most users experience and how bad the tail is; an average hides both."),
  p("Example from scenario 5: p95 end-to-end latency was **0.97 s**, but the slowest payment took **8.3 s**. An SLO that only checks p95 passes that run; the max shows that some payments were badly delayed."),

  h2("2.6 Kafka in four ideas"),
  bullets([
    "**Topic:** a named, append-only log of records. The Hub uses one topic per step, for example `hub.in.mx.raw`, `hub.pay.canonical`, `hub.out.mx`.",
    "**Partition:** a topic is split into partitions so several consumers can share the work. Records with the same key (the UETR) always land on the same partition, so one payment's records stay in order.",
    "**Consumer group:** the set of consumers sharing a topic. Each Hub stage is one group, for example `cg-screening`. Kafka assigns each partition to exactly one member of the group.",
    "**Offset and lag:** each record has a position (offset). A group's **lag** is how many records are waiting: the end of the log minus the group's committed position. Rising lag means work is arriving faster than the stage can finish it.",
  ]),
  p("The Hub processes each batch of records inside a **Kafka transaction**: it reads, processes, writes its output and commits its position in one atomic step. Downstream stages only read committed data. That makes processing safe after a crash, and it also means a record is invisible downstream until the whole batch it belongs to commits (this matters in scenario 1)."),

  h2("2.7 Two views of lag"),
  table(
    ["Panel", "Measured by", "Strength", "Weakness"],
    [
      ["Consumer Lag (Consumer)", "Each Hub stage, when it reads a record", "Cheap, per partition", "Only updates when a record arrives, so a stuck stage freezes at its last value"],
      ["Consumer Lag (Broker)", "kafka-exporter, asking the broker for each group's committed offset", "Keeps updating even if the consumer is stuck or gone", "Can briefly read slightly negative on transactional topics (clamped to 0)"],
    ],
    [2.4, 3, 2.6, 3.4]
  ),

  h2("2.8 Saturation and the IO wait ratio"),
  p("Each Hub stage is one thread that loops: poll Kafka for records, process them, commit. The **IO wait ratio** is the share of that thread's time spent waiting for records. Near **100 %** means the stage is idle and has plenty of spare capacity. Falling towards **0 %** means it is saturated: as soon as it finishes one batch, the next is already waiting."),
  p("This one number tells you which stage is the bottleneck. In scenario 1, only screening fell to 0.5 %; in scenario 4, every stage fell at once."),

  h2("2.9 RED and USE"),
  p("Two checklists help decide what to measure:"),
  bullets([
    "**RED**, for services: **R**ate (requests per second), **E**rrors, **D**uration (latency). The gRPC panels are RED for every call the Hub makes and serves.",
    "**USE**, for resources: **U**tilisation, **S**aturation, **E**rrors. IO wait, lag, connection usage and memory are USE for stages, Kafka, the database and the processes.",
  ]),

  h2("2.10 Annotations"),
  p("An **annotation** is a marker on the time axis of every Grafana panel, with a short text. Each scenario posted one when it started, when the fault went in, when it was removed and when it ended. They turn a dashboard into a story: you can see exactly what changed after the fault marker."),

  // ------------------------------------------------------------ 3. Setup
  h1("3. Setting up (Windows, macOS, Linux)"),
  p("The stack runs the same on all three: every component is a Linux container. Only a few host settings differ, and one command checks them for you."),
  h2("3.1 What you need"),
  table(
    ["", "Windows", "macOS (Intel or Apple Silicon)", "Linux"],
    [
      ["Containers", "Docker Desktop (WSL 2 backend)", "Docker Desktop", "Docker Engine with the Compose plugin, or Docker Desktop"],
      ["Memory for Docker", "10 GB, in `%UserProfile%\\.wslconfig`", "10 GB, in Docker Desktop: Settings, Resources", "The host's own memory: about 9 GB free"],
      ["Elasticsearch setting", "Set by Docker Desktop", "Set by Docker Desktop", "`sudo sysctl -w vm.max_map_count=262144`"],
      ["Python 3.12 or later", "For the helper script and the tests", "Same", "Same"],
      ["Git, a browser", "Yes", "Yes", "Yes"],
    ],
    [2.2, 2.8, 2.8, 3]
  ),
  p("The full stack uses about 9 GB. Without the `elk` profile (logs) it needs about half that, and the Elasticsearch setting does not matter. Every image is published for both Intel/AMD and ARM processors, so Apple Silicon Macs run it natively."),
  callout("warn", "Supported container runtimes are Docker Desktop and Docker Engine. Podman, Colima and rootless Docker are not tested: Filebeat reads the Docker socket and the containers' log files, which those expose differently."),
  h2("3.2 Give Docker enough memory"),
  p("**Windows:** Docker Desktop's VM gets half the machine's RAM by default. Create `%UserProfile%\\.wslconfig` with the lines below, run `wsl --shutdown`, and start Docker Desktop again."),
  code(["[wsl2]", "memory=10GB", "swap=4GB", "", "[experimental]", "autoMemoryReclaim=gradual"]),
  p("**macOS:** Docker Desktop, Settings, Resources: set Memory to 10 GB and apply."),
  p("**Linux:** containers use the host's memory directly. Set the Elasticsearch kernel setting once, and keep it across reboots:"),
  code([
    "echo \"vm.max_map_count=262144\" | sudo tee /etc/sysctl.d/99-elasticsearch.conf",
    "sudo sysctl --system",
  ]),
  h2("3.3 First start"),
  p("These commands are the same in PowerShell, Terminal on macOS, and any Linux shell. Run them from the repository folder. On macOS and Linux the interpreter may be called `python3`."),
  code([
    "python scripts/dev.py doctor            # is this machine ready? says what to fix",
    "python scripts/dev.py up                # build the image once, start everything",
    "python scripts/dev.py ready --wait 300  # wait until it is really ready",
  ]),
  p("`doctor` checks Docker, memory, the Elasticsearch setting and whether the stack's ports are free. `up` creates `.env` from `.env.example` the first time, builds the shared image once and starts the stack. `ready` waits for the conditions described in chapter 6."),
  callout("key", "`python scripts/dev.py` with no arguments lists every task: `install`, `check`, `doctor`, `up`, `ready`, `stop`, `down`, `logs`, `send`, `trace` and more. It uses only Python's standard library, so it works on a fresh clone."),
  p("Which services start is set by `COMPOSE_PROFILES` in `.env`:"),
  table(
    ["Profile", "Starts", "Use"],
    [
      ["`solo`", "The whole Hub in one container", "Local development and these tests (the default)"],
      ["`stages`", "One container per stage (11)", "Production shape; do not combine with `solo`"],
      ["`obs`", "Prometheus, Grafana, kafka-exporter", "Metrics and dashboards"],
      ["`elk`", "Elasticsearch, Kibana, Filebeat", "Logs (about 4 GB of RAM)"],
    ],
    [1.5, 3.5, 4]
  ),
  p("If another program already uses one of the stack's ports, change it in `.env`, for example `GRAFANA_HOST_PORT=3001`. `.env.example` lists them all."),
  h2("3.4 Check it works"),
  numbered([
    "Open **http://localhost:9090/targets**. The `hub`, `ess`, `kafka` and `kafka-exporter` targets should show **UP**. The `hub-stages` targets show DOWN, which is expected: that is the per-stage topology, and it is not running.",
    "Open **http://localhost:3000** (no login). Go to Dashboards, then the **Payment Hub** folder.",
    "Send a few payments: `docker compose exec -T ess python -m ess.cli batch -n 30 -r 1`. All 30 should report COMPLETED.",
    "Open **3 · Performance deep-dive** and set the time range to the last 15 minutes. The panels should show the traffic.",
    "Follow one payment everywhere with `python scripts/dev.py trace`.",
  ]),
  figure(`${SHOTS}/prom_targets.png`, "Prometheus's targets page. Every scrape target the stack defines, and whether Prometheus can reach it. `hub-stages` is DOWN by design when the `solo` profile runs.", 560),
  h2("3.5 Stopping and cleaning up"),
  table(
    ["Command", "Effect"],
    [
      ["`python scripts/dev.py stop`", "Stops the containers and keeps all data, metrics and logs"],
      ["`python scripts/dev.py up --no-build`", "Starts them again"],
      ["`python scripts/dev.py down`", "**Deletes** the containers and all their data. Nothing in this stack uses named volumes"],
    ],
    [3.8, 5.7]
  ),
  h2("3.6 Problems we hit, and how to recognise them"),
  p("These were found on a Windows laptop. The first applies to any laptop: Docker Desktop on macOS pauses its VM on sleep in the same way."),
  table(
    ["Symptom", "Cause", "Fix"],
    [
      ["Payments accepted but never complete; no errors anywhere", "The computer slept. Kafka removed the Hub's consumers from their groups and most never rejoined", "Fixed in the Hub: a stage that holds no partitions for a minute rejoins by itself, and the Hub reports unhealthy meanwhile (`hub_stage_stalled`). `python scripts/dev.py ready` shows it"],
      ["Image build hangs on \"load metadata\" or fails with \"Temporary failure in name resolution\"", "DNS inside Docker Desktop's VM stopped working, usually after sleep (seen on Windows)", "Restart Docker Desktop, then `python scripts/dev.py up`. A fixed DNS server in Docker Desktop's Docker Engine settings should prevent it (suggested, not yet tested)"],
      ["A container cannot start: \"port is already allocated\", but nothing is using the port", "A stale reservation inside Docker after a stack was torn down (seen on Windows)", "Restart Docker Desktop"],
      ["Very high latency and many `DEADLINE_EXCEEDED` errors right after starting", "Elasticsearch and Kibana are still starting and using most of the CPU", "Wait about 5 minutes and pass the warm-up gate before testing"],
      ["A run at 10 payments/s fails with deadline errors", "A 16 GB laptop runs out of CPU above about 3 to 5 payments/s with the full stack", "Keep local runs at about 3/s, or drop the `elk` profile"],
      ["On Windows, `docker exec ... /opt/kafka/...` fails with \"no such file or directory\"", "Git Bash rewrites the path into a Windows path", "Use PowerShell or WSL, or set `MSYS_NO_PATHCONV=1`. `scripts/dev.py` is not affected"],
    ],
    [3.2, 3.3, 3.8]
  ),
  callout("lesson", "Turn off sleep while testing, on any laptop. Two runs in this guide were spoiled by the computer sleeping."),

  // ------------------------------------------------------------ 4. Stack tour
  h1("4. Stack tour"),
  p("The figure below shows every component, how payments flow between them, and where each kind of telemetry is collected."),
  figure(`${DIAG}/diagram_stack.png`, "The local stack. Payments flow along the top row. Prometheus scrapes metrics from the ESS, the Hub, the Kafka broker and kafka-exporter; Grafana draws them. Logs go from container output through Filebeat to Elasticsearch, and Kibana searches them."),
  h2("4.1 Components"),
  table(
    ["Component", "Container", "Port (localhost)", "What it does", "What it gives you"],
    [
      ["Payment Hub", "`hub`", "8443 gRPC edge\n8080 status API\n9464 metrics", "Receives payments, parses, screens, routes, settles, dispatches them and matches the network ACK. All 11 stages in one process (`solo`)", "Hub metrics at `/metrics`; one JSON log line per event"],
      ["External Systems Simulator (ESS)", "`hub-ess`", "9101 control", "Plays the outside world: delivers payments in (as FIN and SnF), receives payments out, answers compliance screening, sends ACKs. Fault profiles let you make it slow, fail or go down", "ESS metrics, including the simulator's own end-to-end latency"],
      ["Kafka broker", "`hub-kafka`", "29092 clients\n9404 JMX metrics", "Stores every topic. Single broker, KRaft mode. Includes the Prometheus JMX exporter agent", "Broker throughput, request and transaction timings, log sizes, JVM memory and GC"],
      ["kafka-init", "`hub-kafka-init`", "–", "Creates all topics, then exits", "–"],
      ["PostgreSQL", "`hub-postgres`", "5432", "The system of record, written by the DB sink stage", "Tables `payment` (latest state) and `payment_audit` (every state change)"],
      ["Prometheus", "`hub-prometheus`", "9090", "Scrapes all metrics every 5 s and stores them for 24 hours", "PromQL queries, the targets page"],
      ["Grafana", "`hub-grafana`", "3000", "Dashboards, provisioned from files in the repository", "Three dashboards, annotations"],
      ["kafka-exporter", "`hub-kafka-exporter`", "(internal 9308)", "Asks the broker for each consumer group's lag", "`kafka_consumergroup_lag`"],
      ["Elasticsearch", "`hub-elasticsearch`", "9200", "Stores the logs, deleted after a day", "Index `logs-payments-*`"],
      ["Kibana", "`hub-kibana`", "5601", "Searches the logs", "One payment's journey by UETR"],
      ["Filebeat", "`hub-filebeat`", "–", "Reads container output and ships the JSON lines to Elasticsearch", "–"],
    ],
    [2.1, 1.9, 1.8, 3.6, 2.6], { caption: "Every component of the local stack." }
  ),
  h2("4.2 A payment's journey"),
  p("Following one MX (ISO 20022) payment through the Hub shows where each metric comes from. Each step writes to the next Kafka topic, inside a transaction, and publishes a state change to `hub.pay.status`."),
  table(
    ["Step", "Where", "State", "Timestamp"],
    [
      ["The ESS, playing SnF, calls `DeliverMx`. This is message **M1**", "Hub edge (gRPC)", "–", "T0 network delivery"],
      ["The edge writes the raw message to `hub.in.mx.raw` and replies ACCEPTED", "Edge", "RECEIVED", "T1 accepted"],
      ["Parse the XML into the canonical model", "mx-parser", "PARSED", "T2"],
      ["Call the compliance `Screen` service (the ESS answers)", "screening", "SCREENED (or HELD on a hit)", "T3"],
      ["Choose the network and route", "routing", "ROUTED", ""],
      ["Post to the ledger", "settlement", "SETTLED", ""],
      ["Call `SendMx` on SnF (the ESS receives message **M2**)", "dispatcher-mx", "DISPATCHED", "T4 handoff"],
      ["SnF sends the network ACK back through `NotifyAck`", "Edge, then ack-matcher", "COMPLETED", "T5 ACK, T6 completed"],
      ["Every state change is written to PostgreSQL", "db-sink", "–", ""],
    ],
    [4.6, 2.2, 2, 1.7]
  ),
  p("The ESS times M1 to M2 itself (the **simulator's** end-to-end latency). The Hub times T0 to T6 (the **Hub's** end-to-end latency). Comparing the two is useful: the simulator's view includes every gRPC hop and any queueing outside the Hub's own code."),
  callout("key", "MT (SWIFT FIN) payments follow the same steps with `DeliverFin`, `hub.in.fin.raw`, fin-parser, dispatcher-fin and `SendMt`. MT and MX only share the middle of the pipeline."),
];
