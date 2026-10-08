# Known issues

Open problems in the Payment Hub, the External Systems Simulator (ESS), the test
suite and the local environment, as of 8 October 2026. Each was found by the
local fault-injection runs (`docs/performance-testing.md`, "Local fault-injection
runs") or while making the stack cross-platform. None is fixed yet.

Problems that were found and **fixed** are not listed here; see chapter 13.2 of
`docs/observability/Payment-Hub-Observability-Guide.docx` and the decisions table
in `CLAUDE.md`.

When an issue is fixed, remove it from this file in the same change.

## Summary

| # | Issue | Area | Severity | Needs |
| --- | --- | --- | --- | --- |
| 1 | State stores never drop finished payments | Hub | High | Design decision |
| 2 | Held payments and overdue ACKs never expire | Hub | High | Business rules |
| 3 | Screening amplifies a slow compliance service | Hub | Medium | Design decision |
| 4 | One database connection, two statements per state change | Hub | Medium | Code change |
| 5 | The "30 s" retry tier is slow and uneven | Hub | Medium | Investigation |
| 6 | The `format` header is lost when a record is retried | Hub | Low | Code change |
| 7 | The simulator drops the end-to-end sample after an edge retry | ESS | Low | Code change |
| 8 | The full test suite occasionally hangs | Tests | Medium | Investigation |
| 9 | Docker Desktop loses its network after the host sleeps | Environment | Low | Workaround known |
| 10 | Not yet verified: macOS, arm64 and the CI workflow | Environment | Low | A machine or a push |

Severity is for a production-shaped deployment, not for the local stack.

## Hub

### 1. State stores never drop finished payments

- **What happens:** the parsers, routing, settlement and the dispatchers keep an
  entry for every payment they have processed until the process restarts. Only
  screening and the ACK matcher drop finished payments (their `completed` set is
  capped at 100,000 by design).
- **Evidence:** scenario 5 (`s5-memory-soak`). The routing store gained exactly
  one key per payment, with or without the fault. Hub memory (RSS) rose from
  125 MB to 217 MB in 15 minutes at 3 payments a second, about 28 KB a payment
  from this leak.
- **Impact:** out of memory after about 90 minutes at 3 payments a second, and
  after about 10 seconds at the 1,500 a second design target. Each restart also
  rebalances every consumer group. Latency SLOs pass until the process dies.
- **Where:** `hub/common/state_store.py`, `hub/common/processor.py`.
- **Suggested fix:** evict finished payments in every stage (bounded LRU or a
  time to live), and store a small marker rather than a full copy of the
  envelope.
- **How to see it:** dashboard 3, "Kafka stream number of keys per instance"
  (`working` series) and "Used Memory - Resident (RSS)".

### 2. Held payments and overdue ACKs never expire

- **What happens:** a payment held for a compliance decision stays in the
  screening store until a decision arrives, however long that takes. A payment
  whose network ACK never arrives stays DISPATCHED; the Hub reports it but does
  nothing about it.
- **Evidence:** scenario 5: 584 held payments stayed held after the fault ended,
  about 10 KB each. Scenarios 1, 3 and 4 produced payments with no ACK before the
  ESS learned to redeliver.
- **Impact:** held payments and missing ACKs accumulate silently, in memory and
  as unfinished business.
- **Where:** `hub/screening/processor.py`; `ACK_OVERDUE_S` and the
  `TODO(business-rules)` in `hub/ack_matcher/processor.py`.
- **Suggested fix:** a business decision first: re-send, query the network,
  escalate to an operator, or time out. This is on the open-questions list in
  `CLAUDE.md`.
- **How to see it:** `hub_awaiting_ack{overdue="true"}` (dashboard 1, "Waiting
  for the network ACK"); the screening store's key count on dashboard 3.

### 3. Screening amplifies a slow compliance service

- **What happens:** screening calls `ComplianceScreening.Screen` one record at a
  time inside one Kafka transaction, so every record in a batch waits for all the
  calls before it.
- **Evidence:** scenario 1. A dependency slowed to a median of 300 ms raised
  end-to-end latency about 40 times, far more than the slowdown itself.
- **Impact:** a modest slowdown in one dependency becomes a large customer-facing
  delay.
- **Where:** `hub/screening/processor.py`, the transactional loop in
  `hub/common/processor.py`.
- **Suggested fix:** concurrent calls within a batch, and a cap on how long one
  transaction may stay open.
- **How to see it:** dashboard 3, "IO Wait Ratio" (screening near 0) and the
  gRPC client call duration for `Screen`.

### 4. One database connection, two statements per state change

- **What happens:** `cg-db-sink` writes to PostgreSQL over a single connection,
  with two statements for every state change.
- **Evidence:** scenario 2. With PostgreSQL throttled, the sink ran 64 seconds
  behind while customers saw nothing: a payment was *completed* long before it
  was *persisted*.
- **Impact:** no slack when the database slows; the system of record lags the
  status API.
- **Where:** `hub/db_sink/processor.py`.
- **Suggested fix:** batch the statements, and use a small connection pool.
- **How to see it:** dashboard 3, "Consumer Lag (Broker)" for `cg-db-sink`,
  "JDBC Calls" and "JDBC idle connections".

### 5. The "30 s" retry tier is slow and uneven

- **What happens:** a record on a `.retry.30s` topic is republished 30 to 60
  seconds later, not 30, and some records take several rounds.
- **Evidence:** scenario 3, and its rerun on 8 October (`s3-verify`): three
  minutes after a 60-second outage ended, 14 records were still waiting on
  `hub.out.mx.retry.30s` and eight payments were still FAILED. All completed
  about a minute later; the slowest payment took 329 seconds end to end. The
  5-minute tier was barely used, so the delay was in the 30-second tier. The
  cause of the repeated rounds has not been traced.
- **Impact:** recovery from a short outage takes several minutes for a few
  payments. Nothing is lost.
- **Where:** `hub/common/retry_consumer.py` (group `cg-retry`).
- **Suggested fix:** trace one slow payment first (it needs the `elk` profile),
  then decide; likely candidates are the drain interval and how a record is
  re-queued when its retry also fails.
- **How to see it:** dashboard 3, "Broker records in per sec" for the retry
  topics, and the lag of `cg-retry`.

### 6. The `format` header is lost when a record is retried

- **What happens:** a record republished from a retry topic no longer carries
  the `format` header (MT / MX) that architecture rule 3 requires on shared
  topics.
- **Impact:** metrics and panels that split by format under-count retried
  payments. Processing is not affected.
- **Where:** `hub/common/retry_consumer.py`.
- **Suggested fix:** copy the original headers when republishing, with a test.

## External Systems Simulator

### 7. The simulator drops the end-to-end sample after an edge retry

- **What happens:** when `DeliverFin` / `DeliverMx` misses its deadline, the
  sender retries and the Hub correctly answers DUPLICATE. The recorder then
  discards that payment's end-to-end measurement.
- **Evidence:** the `s3-verify` run reported 898 settled of 900, with two
  `DEADLINE_EXCEEDED` and two duplicates, while PostgreSQL held 900 COMPLETED.
- **Impact:** the simulator's report under-counts, and the slowest payments are
  the ones missing from its latency figures. The batch is also reported as
  "had failures" although every payment completed.
- **Where:** `ess/recorder.py` (`m1_done`), `ess/sender.py`.
- **Suggested fix:** treat DUPLICATE after a retry of the same send as accepted,
  and keep the first send time.

## Tests

### 8. The full test suite occasionally hangs

- **What happens:** a full `pytest` run sometimes never finishes: roughly one run
  in three to five on a 16 GB Windows laptop under memory pressure. The figure
  comes from a handful of runs.
- **Evidence:** two signatures: a thread join during pytest teardown, and an
  event loop waiting forever. Pinning `grpc.aio.init_grpc_aio()` was tried and
  did not help. It is not known whether the hang predates the recent changes: the
  comparison run against the old commit was cut short.
- **Impact:** a local or CI run may need to be started again. No test is known
  to give a wrong result.
- **Workaround:** stop the run and start it again.
- **Next step:** repeat the suite with free memory and a per-test timeout that
  dumps every thread's stack, on this code and on commit `4e215b2`.

## Environment

### 9. Docker Desktop loses its network after the host sleeps

- **What happens:** after the computer sleeps, DNS inside Docker Desktop's VM
  stops working, or the engine stops answering. Seen on Windows; Docker Desktop
  on macOS pauses its VM on sleep in the same way.
- **Impact:** image builds fail and containers may not start. The Hub itself now
  recovers from the frozen VM (a stalled stage rejoins its group after
  `HUB_REJOIN_AFTER_S`).
- **Workaround:** turn sleep off during test runs; otherwise restart Docker
  Desktop, then `python scripts/dev.py up`.
- **Not yet tested:** a fixed DNS server in Docker Desktop's Docker Engine
  settings; whether a VPN client on the host contributes.

### 10. Not yet verified: macOS, arm64 and the CI workflow

- **macOS:** the stack and `scripts/dev.py` are written to run there, but have
  not been run on a Mac.
- **arm64:** every image is published for `arm64`, but the stack has not been
  run on it; the emulated run was cut short.
- **CI:** `.github/workflows/ci.yml` has never run, because the repository has
  not been pushed since it was added.
- **Linux:** a fresh-clone install and check pass in a container.
