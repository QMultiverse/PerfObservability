// Chapter 5: dashboards and the meaning of every panel.
const { p, h1, h2, h3, bullets, callout, figure, table } = require("./lib");

// [id, title, what it shows, normal here, bad looks like, seen in]
const ROWS = [
  ["Simulator", "What the outside world experiences. Measured by the ESS, so it includes everything the Hub's own timers cannot see.", [
    [2, "E2E Latency (Simulator)", "p50, p95 and p99 time from the ESS sending a payment in (M1) to receiving the Hub's outbound message for it (M2), by format.", "p95 about 0.5–0.8 s at 3/s", "Climbs and stays up; flat at 30 s means \"30 s or more\" in older runs (top bucket)", "1, 3, 4"],
    [3, "M1 sent (Simulator), M2 received (Simulator)", "Payments delivered in, and outbound messages received out, per second. Also: M2 with no waiting M1 (a repeat), and how many M1 are still waiting.", "M1 and M2 lines on top of each other at the input rate; \"awaiting\" near 0", "A gap between the lines: the Hub is taking in more than it sends out. \"Awaiting\" rising: work is stuck inside", "1, 3, 5"],
    [4, "E2E Latency", "The Hub's own T0 to T6 percentiles, observed when the network ACK completes a payment.", "Close to the simulator's view, slightly lower", "Rises with the simulator's view; a big difference between the two points at queueing outside the Hub's code", "1"],
  ]],
  ["Processing & Lag", "Where work is piling up, and how fast each stage and the broker are moving records.", [
    [6, "Consumer Lag (Consumer)", "Lag reported by each stage when it reads a record, by group and topic.", "0–5", "Rising for one group. It freezes if the consumer stops reading, so check the broker view too", "1"],
    [7, "Consumer Lag (Broker)", "Lag from kafka-exporter: the broker's view of each group's committed position. Clamped at 0.", "0–5 for every group", "One group climbing while the others stay at 0 points at that stage (scenarios 1 and 2). Rising for every group at once points at Kafka or the whole process", "1, 2, 4"],
    [8, "Streams processed records per second", "Records each stage processed, by outcome: `ok`, `retry` (sent to a retry topic) or `dlq`.", "Only `ok`, several per second per stage", "`retry` appearing (a dependency failing); `dlq` appearing (a human must look)", "3"],
    [9, "Listener consumed records per second", "Every record each consumer group read, including the internal state topic.", "Steady, roughly proportional to the payment rate", "Drops to 0 for a group while lag rises: that group has stopped consuming", ""],
    [10, "Broker records in per sec", "Records appended to each topic per second (from the broker). One payment writes several records, so this is not payments per second.", "A stable stack of topics", "New topics appearing, for example `hub.out.mx.retry.30s`", "3"],
    [11, "Broker records in total", "Cumulative records per topic since the broker started.", "Climbing steadily", "Sizing only", ""],
    [12, "Broker bytes in per sec", "Bytes written to each topic per second. The raw MT/MX topics dominate because only they carry the full message.", "Tens of kB/s locally", "Sizing only", ""],
    [13, "Broker bytes out per sec", "Bytes read from each topic per second; multiplied by the number of groups reading it.", "Higher than bytes in", "Sizing only", ""],
  ]],
  ["Transactions and Commits", "The cost of Kafka transactions, from the Hub's side and the broker's side.", [
    [15, "Streams commit latency average", "Average time for a stage to commit one transaction (output records and consumer position together).", "7–15 ms", "Much higher than process latency: the stages spend their time committing, not working (scenario 4: about 150 ms)", "4"],
    [16, "Broker prepare to complete average", "The broker's average time for `WriteTxnMarkers` (writing commit markers to every partition) and `EndTxn` (the client's commit request).", "1–5 ms", "Rising together with commit latency confirms the broker is the slow side. This is a slow-moving average, so it understates spikes", "4"],
    [17, "Commit per second", "Committed and aborted transactions per stage.", "Roughly the record rate at low load (one record per batch)", "Commits falling while records stay flat: batches are growing because commits are slow (scenario 4). Any `aborted` series needs a look", "4"],
    [18, "Streams commit latency max", "Longest single commit per stage over the last 30–60 s.", "Under 0.3 s", "Seconds: one slow commit holds back every record in its batch", "1, 4"],
    [19, "Broker prepare to complete max", "The broker's maximum for the same two requests.", "Under 0.4 s", "Sustained high values", "4"],
    [20, "Broker errors", "Request errors by API and error code, plus failed produce and fetch requests.", "Occasional `CONCURRENT_TRANSACTIONS` or `COORDINATOR_LOAD_IN_PROGRESS` blips are normal", "Sustained error rates", ""],
  ]],
  ["Process", "How busy each stage is and where its time goes.", [
    [22, "Process latency avg", "Average time to process one record in each stage, not counting the commit. Includes outbound gRPC calls. A separate `post-commit` series shows work done after the commit (the DB sink's database write).", "1–20 ms; screening about 17 ms", "One stage jumping (scenario 1: screening 17 → 390 ms)", "1, 2, 4"],
    [23, "Rebalance", "Partition assignment changes per consumer group since the process started, and the partitions each group holds now.", "One assignment per group at start-up; partitions held never 0", "Steps up mid-run (a consumer left or crashed); **partitions held at 0** means a stage is doing nothing", "–"],
    [24, "Process Total", "Records processed since the process started, by stage and outcome.", "Climbing steadily", "A stage that stops climbing", ""],
    [25, "Process latency max", "Longest single-record processing time per stage (30–60 s window), plus the longest post-commit step.", "Under 0.5 s", "Spikes show individual slow records that an average hides", "1"],
    [26, "Poll time", "Average and p99 time a stage spends inside one Kafka poll.", "Close to the 50 ms poll timeout when idle", "Mostly a sanity check; high when idle is normal", ""],
    [27, "IO Wait Ratio", "Share of each stage thread's time spent waiting for records. Near 100 %: idle. Near 0 %: saturated.", "60–99 %", "One stage near 0 % is the bottleneck (scenarios 1 and 2). All stages falling together points at something shared (scenario 4)", "1, 2, 4"],
  ]],
  ["gRPC", "Every call the Hub makes and serves, as RED (rate, errors, duration).", [
    [29, "gRPC client calls (outbound)", "Calls made per second by the Hub and the ESS, by method and status. Each retry counts as a separate attempt; retries are also shown on their own.", "All `OK`", "`DEADLINE_EXCEEDED` or `UNAVAILABLE` on one method points at that dependency (scenario 1: `Screen`; scenario 3: `SendMx`)", "1, 3, 4"],
    [30, "gRPC client call duration (outbound)", "p50 and p99 duration of outbound calls by method. Deadlines: `Deliver*` 200 ms, `Send*` 500 ms, `Screen` 1 s.", "Tens of ms", "p99 reaching the method's deadline", "1"],
    [31, "gRPC server calls (inbound)", "Calls served per second by method and status (the ESS control API is excluded).", "All `OK`", "Errors on the edge methods", ""],
    [32, "gRPC server call duration (inbound)", "p50 and p99 time inside the server handler.", "p99 under about 0.2 s", "If clients see deadlines but this stays low, calls are queueing before the handler runs (an overloaded process)", "4"],
  ]],
  ["JDBC", "The DB sink's connection to PostgreSQL. The Hub is Python, so these are psycopg figures, named after their JDBC equivalents.", [
    [34, "JDBC idle connections", "The DB sink's one connection: idle between batches, active during a write.", "Flicks between idle and active", "Pinned **active**: the database cannot keep up (scenario 2)", "2"],
    [35, "JDBC Calls", "Statements and transaction calls per second, by operation (`upsert`, `audit_insert`, `commit`) and outcome.", "About 7 upserts per payment: 21 a second at 3 payments a second", "A ceiling below what the load needs, or `error` outcomes", "2"],
  ]],
  ["Disk Usage", "What is stored, and whether it keeps growing.", [
    [37, "Kafka disk usage", "Size of each topic's log on the broker.", "Grows with traffic, trimmed by retention", "Sizing only", ""],
    [38, "Kafka streams Disk usage per instance", "Estimated bytes held in each stage's in-memory state store (the Kafka Streams equivalent).", "Small and bounded", "Rising for as long as traffic runs: a leak (scenario 5)", "5"],
    [39, "Kafka stream number of keys per instance", "Keys (payments) held in each stage's state store.", "Bounded: payments in flight plus a window of finished ones", "Rising in a straight line with every payment (scenario 5: routing and settlement)", "5"],
  ]],
  ["Memory: Kafka broker (JVM)", "The broker is a Java process. Under its 1.5 GB container limit the JVM picks the Serial garbage collector, so the old generation is called \"Tenured Gen\".", [
    [41, "Used Memory - Old Gen", "Long-lived objects in the broker's heap.", "A sawtooth", "A floor that keeps rising", ""],
    [42, "Used Memory - Heap", "Total heap used, committed and maximum (1 GB).", "Well under the maximum", "Close to the maximum", ""],
    [43, "Used Memory - Non-Heap", "Metaspace, code cache and class space.", "Flat after start-up", "Growing", ""],
    [44, "Full GC Pauses", "Time per second in full collections, and their count.", "Rare", "Frequent: the broker pauses for every one", ""],
  ]],
  ["Memory: Hub and ESS (Python)", "Python has no old/young heap split; the resident memory (RSS) is what the container's memory limit is enforced against.", [
    [46, "Used Memory - Resident (RSS)", "Physical memory used by the Hub and ESS processes.", "Hub 120–200 MB, ESS 60–80 MB", "A steady upward slope under steady load: a leak (scenario 5: about 37 KB per payment)", "5"],
    [47, "Allocated blocks", "Memory blocks the Python interpreter has allocated.", "Flat under steady load", "Growing with RSS", "5"],
    [48, "Virtual memory", "Address space reserved by each process.", "Flat", "Rarely useful", ""],
    [49, "Full GC Pauses (generation 2)", "Time per second in Python's full garbage collection, and the p99 pause.", "Pauses under 100 ms", "Pauses longer than the 200 ms edge deadline (scenario 5: 210 ms)", "5"],
  ]],
];

module.exports = (SHOTS) => {
  const out = [
    h1("5. Dashboards and what every panel means"),
    p("Grafana has three dashboards in the **Payment Hub** folder. Dashboards 1 and 2 are summaries; dashboard 3, the performance deep-dive, has 40 panels in rows. This chapter goes through every panel."),
    h2("5.1 The three dashboards"),
    table(
      ["Dashboard", "Purpose", "Look here first for"],
      [
        ["1 · Run overview", "Is the run healthy? Payments per second in and out, achieved ratio, errors, held payments, end-to-end percentiles, retries, payments waiting for an ACK", "The headline numbers of a test"],
        ["2 · Pipeline stage breakdown", "Where time goes and where work accumulates: lag, payments in flight, latency per stage, transaction cost", "Which stage is slow"],
        ["3 · Performance deep-dive", "Every detail: simulator, processing, transactions, gRPC, database, disk, memory", "Root cause"],
      ],
      [2.4, 5.2, 2.6]
    ),
    figure(`${SHOTS}/dash01.png`, "Dashboard 1 · Run overview, over the scenario 5 soak. The bottom panel, \"Waiting for the network ACK\", was added after scenarios 1, 3 and 4 found payments stuck without one. The coloured number tiles show the value at the END of the time window, after the traffic had stopped, which is why \"Achieved ratio\" reads 0 % here; the graphs show the run itself.", 520),
    h2("5.2 Reading a panel"),
    bullets([
      "**Hover** over the ⓘ next to a panel title for its description.",
      "The **legend** table under each graph shows mean, max or last values for each line; click a line's name to show only that line.",
      "The **Stage** drop-down at the top filters the stage-level panels to one or more stages.",
      "Drag across a graph to **zoom** in on a time range; the whole dashboard follows.",
      "Vertical markers are **annotations** posted by the test scripts (start, fault, recovery, end).",
    ]),
    callout("key", "All screenshots in this chapter come from the scenario 5 soak (5 October, 00:46 to 01:04), so they show steady traffic and, in the Disk Usage and Memory rows, a real leak. Each scenario chapter shows the panels that mattered for that scenario."),
  ];
  let n = 2;
  for (const [row, intro, panels] of ROWS) {
    n += 1;
    out.push(h2(`5.${n} ${row}`));
    out.push(p(intro));
    for (const [id, title, what, normal, bad, seen] of panels) {
      out.push(h3(title));
      out.push(p(what));
      out.push(...table(["Normal on this stack", "What bad looks like", "Scenarios"], [[normal, bad, seen || "–"]], [3.5, 5, 1.5]));
      out.push(...figure(`${SHOTS}/ref_p${String(id).padStart(2, "0")}.png`, `${title}.`, 560));
    }
  }
  return out;
};
