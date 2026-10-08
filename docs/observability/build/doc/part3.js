// Chapters 6-7: running a test, the triage method.
const { p, h1, h2, bullets, numbered, code, callout, figure, table } = require("./lib");

module.exports = (DIAG) => [
  h1("6. Running a performance test"),
  p("Every scenario in this guide followed the same procedure. Most of the runs that had to be thrown away skipped a step of it."),
  h2("6.1 Readiness: is the system really ready?"),
  p("\"All containers are running\" is not enough. `python scripts/dev.py ready` checks the first five of these for you; add `--wait 300` to keep trying while the stack starts. This table explains what it checks and why:"),
  table(
    ["Check", "How", "Why"],
    [
      ["Every container healthy", "`docker compose ps`", "Obvious, but not sufficient"],
      ["Every scrape target up", "Prometheus, Status, Targets (or http://localhost:9090/targets)", "Missing targets mean missing panels"],
      ["**All 11 consumer groups hold partitions**", "Prometheus query: `count(hub_kafka_assigned_partitions > 0)` must return **11**", "After the PC slept, most groups held 0 partitions while the Hub still reported healthy (scenario 1's first attempt)"],
      ["Lag is zero", "`sum(clamp_min(kafka_consumergroup_lag,0))` returns 0", "Leftover work distorts the baseline"],
      ["The stack has warmed up", "At least 5 minutes since start-up", "Elasticsearch and Kibana use most of the CPU while they start (scenario 2's aborted attempt)"],
      ["**Warm-up gate passes**", "Send 30 payments at 1/s: all must complete with p95 under 1 s", "Proves the baseline is healthy, not just running"],
    ],
    [3, 3.8, 3.6]
  ),
  code([
    "# Warm-up gate (one line; the same in any shell)",
    "docker compose exec -T ess python -m ess.cli batch -n 30 -r 1 --run-id warmup --wait 60s",
    "",
    "# Expect: settled 30  unsettled 0 ... p95 under 1000 ms ... batch ok",
  ]),
  p("To split a long command over several lines, end each line with a backslash in bash or zsh, and with a backtick in PowerShell."),
  h2("6.2 Generating traffic"),
  p("`ess batch` sends a fixed number of payments at a steady rate and reports how they settled. It is the simulator's sender, not a load generator: about 10 payments a second is its local ceiling, and this machine itself copes with about 3."),
  table(
    ["Option", "Meaning", "Used in the scenarios"],
    [
      ["`-n`", "How many payments", "540 to 2,700"],
      ["`-r`", "Payments per second", "3"],
      ["`-c`", "Deliveries in flight at once", "4"],
      ["`--mix`", "Weighted message types", "`pacs.008=60,MT103=30,pacs.009=10`"],
      ["`--hit-rate`", "Share of payments with a party on the sanctions watchlist", "0.05 in the demo runs"],
      ["`--run-id`", "A name for the run, stored with every payment", "e.g. `s2-db-slow-r3`"],
      ["`--wait`", "How long to wait for payments to finish before reporting", "30 s to 8 min"],
    ],
    [1.6, 4.4, 4]
  ),
  h2("6.3 Injecting a fault"),
  p("Two kinds of fault were used: changing how the simulated outside world behaves (an ESS profile), and starving a container of CPU. Both can be applied and removed while traffic runs."),
  table(
    ["Scenario", "Inject", "Remove"],
    [
      ["1 Slow compliance service", "`ess profile set compliance --accept-latency 300ms --p99 1500ms --kind lognormal`", "`ess profile set compliance --accept-latency 2ms --kind fixed`"],
      ["2 Slow database", "`docker update --cpus 0.02 hub-postgres`", "`docker update --cpus <N> hub-postgres`"],
      ["3 SnF outage", "`ess profile set snf --outage --for 60s`", "Ends by itself after 60 s"],
      ["4 Slow broker", "`docker update --cpus 0.5 hub-kafka`", "`docker update --cpus <N> hub-kafka`"],
      ["5 Held payments", "`ess profile set compliance --hit-rate 0.3 --decision-delay 60m`", "`ess profile set compliance --decision-delay 100ms`"],
    ],
    [1.8, 4.6, 3.8]
  ),
  p("`ess` commands run inside the simulator container: prefix them with `docker exec hub-ess python -m ess.cli`."),
  callout("warn", [
    "`profile set` replaces the NAK, hit, block and duplicate rates with whatever you pass (0 if you leave them out), but keeps the release rate if you pass 0. Check the current profile before changing it, so a fault changes only what you intend.",
    "`docker update --cpus 0` does **not** remove a CPU limit. Set it to the number of CPUs Docker has, shown as `<N>` above: `docker info --format \"{{.NCPU}}\"` prints it (12 on the laptop used here).",
  ]),
  h2("6.4 Marking the run on the dashboards"),
  p("Post an annotation at each phase change, so every panel shows when the fault went in and came out:"),
  code([
    "curl -s -X POST localhost:3000/api/annotations -H 'Content-Type: application/json' \\",
    "  -d '{\"dashboardUID\":\"hub-performance\",\"time\":'$(date +%s%3N)',",
    "       \"tags\":[\"my-run\"],\"text\":\"FAULT: compliance slow\"}'",
  ]),
  p("This is bash or zsh syntax (macOS, Linux, WSL or Git Bash). Repeat it with `hub-pipeline` and `hub-run-overview` to mark all three dashboards."),
  h2("6.5 A scenario as a script"),
  p("Each scenario was a small script: start the traffic in the background, then sleep, inject, sleep, remove, and annotate each step."),
  code([
    "docker exec hub-ess python -m ess.cli batch -n 900 -r 3 --run-id s4 --wait 120s &",
    "ann \"S4 start: baseline\";                 sleep 90",
    "docker update --cpus 0.5 hub-kafka;  ann \"S4 FAULT\";   sleep 120",
    "docker update --cpus $(docker info --format '{{.NCPU}}') hub-kafka; ann \"S4 RECOVER\"",
    "wait;                                  ann \"S4 end\"",
  ]),
  h2("6.6 Looking at the run"),
  p("To show exactly one run, put its start and end in the dashboard address as milliseconds since 1970, for example:"),
  code("http://localhost:3000/d/hub-performance?orgId=1&from=1791154290000&to=1791154770000"),
  p("Or use the time picker in the top right of Grafana and type the start and end times."),
  h2("6.7 After the run"),
  bullets([
    "Read the batch report: accepted, duplicates, errors, settled, unsettled, end-to-end percentiles.",
    "Count final states in the system of record:",
  ]),
  code([
    "docker exec hub-postgres psql -U hub -d payments -c \"select state, count(*) from payment where run_id='s4-broker-cpu' group by 1;\"",
  ]),
  bullets([
    "Check `hub_awaiting_ack{overdue=\"true\"}` on dashboard 1: anything above the pre-run value is a payment that will never complete on its own.",
    "Restore every profile and CPU limit you changed.",
  ]),
  callout("lesson", "Count outcomes from the Hub's own records, not from client errors. In scenario 4 the client counted 9 delivery errors, but 6 of those payments had been written durably: their reply simply arrived after the client's deadline."),

  h1("7. The triage method"),
  p("Every investigation in this guide used the same five questions, in the same order. The figure below summarises them."),
  figure(`${DIAG}/diagram_triage.png`, "The triage method. Read the dashboard top-down; confirm with one payment's journey."),
  table(
    ["Question", "Panels", "What you learn"],
    [
      ["1. Is the user affected?", "E2E Latency (Simulator); M1 sent / M2 received", "Whether there is a problem at all, how big, and since when"],
      ["2. Where is work piling up?", "Consumer Lag (Broker), per group", "Which stage is behind, or that nothing is (scenario 4)"],
      ["3. Which stage is slow?", "IO Wait Ratio; Process latency avg and max", "Which stage is saturated, and whether it is one stage or all of them"],
      ["4. Why?", "gRPC client calls and duration; commit vs process latency; JDBC; memory", "Whether the cause is a dependency, Kafka, the database or the process itself"],
      ["5. Confirm", "One payment's journey: Kibana (search `payment.uetr`), PostgreSQL `payment_audit`", "Exactly what happened, hop by hop, with timestamps"],
    ],
    [2.6, 4, 3.8]
  ),
  h2("7.1 Finding one payment's journey in Kibana"),
  numbered([
    "Open http://localhost:5601 and go to **Discover**.",
    "Choose the `logs-payments-*` data view (create it once if asked).",
    "Search for `payment.uetr : \"<the UETR>\"` and sort by time, oldest first.",
    "Read the events in order: `grpc.server.recv`, `kafka.produce`, `payment.state`, `grpc.client.send`, and so on. Each line names the service.",
  ]),
  p("In scenario 3, this is what showed that a payment marked FAILED had in fact been re-sent, ACKed and completed 59 seconds later."),
  callout("warn", "Elasticsearch deletes logs after one day, and Prometheus keeps metrics for 24 hours. Take screenshots and notes within a day of a run."),
];
