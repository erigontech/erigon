---
name: qa-test-results
description: Read and interpret the results, logs and artifacts of Erigon's QA workflows (the `qa-*.yml` GitHub Actions workflows - tip tracking, sync from scratch, snapshot download, clean exit, stage exec, RPC integration/performance, txpool). Use when asked why a QA test failed, what a QA run measured, how to read a QA test log, what "Deadline reached" / "total sync time below threshold" / "exec_steps_in_db exceeded threshold" mean, or when triaging a red `QA - ...` check on a PR or release branch.
---

# Reading QA test results

The `qa-*` workflows run on Erigon's **self-hosted QA runners** (bare-metal Hetzner
machines) and take hours. They are not ordinary unit tests: a red QA check almost
always means "Erigon misbehaved on real chain data", not "a flaky CI runner".

Each workflow follows the same shape:

1. build Erigon from the branch under test;
2. run a **Python driver** from the `erigon-qa` repo (`$ERIGON_QA_PATH =
   /home/qarunner/erigon-qa`) that starts Erigon, tails its log, polls its
   JSON-RPC and Prometheus endpoints, and applies pass/fail thresholds;
3. write `result-<chain>.json` and upload it plus the Erigon debug log, metric
   plots and an FD-leak report as artifacts;
4. push the measurements to MongoDB → Grafana
   (<https://monitoring.erigon.io/d/ddqiwbfvrgwlcd/erigonqa>).

## Triage order — cheapest signal first

Never start by dumping the whole run log. A tip-tracking step log is hours of
Erigon debug output (hundreds of MB). Work down this list and stop as soon as
you have the answer:

**1. `result-<chain>.json`** — the verdict in one small file.

```bash
gh run download <run-id> --repo erigontech/erigon -D /tmp/qa   # all artifacts
# or list first, artifact names vary per workflow:
gh api repos/erigontech/erigon/actions/runs/<run-id>/artifacts --jq '.artifacts[].name'
jq '{outcome, reason, measures: (.measures | keys)}' /tmp/qa/test-results*/result-*.json
```

```json
{ "outcome": "FAILURE", "reason": "in sync less than 75% of the time",
  "exit_code": 1, "measures": { "total-sync-time_%": 63, ... } }
```

`outcome` is one of `SUCCESS` / `FAILURE` / `Unexpected error` (`ERROR` in the
clean-exit and stage-exec drivers). See
[references/result-json.md](references/result-json.md) for what each field and
measure means, and how `outcome` maps to root cause.

**2. The `***` report lines** in the test log. Every line the driver considers
part of the final report is marked with three asterisks, so the whole verdict is
one grep away:

```bash
gh run view <run-id> --repo erigontech/erigon --log 2>/dev/null | grep -F '***'
# or, on a downloaded artifact / local run:
grep -F '***' test_execution.log
```

**3. The Erigon debug log** (`erigon-logs*` artifact, or `.../logs/erigon.log*`
inside `test-results-*`) — only once you know *which* phase failed and roughly
when. Grep around the failure timestamp for `[EROR]`, `panic`, `SIGSEGV`.

**4. The stack dump** at the bottom of the test log. On abort the driver sends
`SIGUSR1` to Erigon to dump every goroutine — the go-to evidence for a
suspected deadlock or a stuck stage.

## Reading the test log

The step log is an **annotated Erigon log**:

| Prefix | Meaning |
|--------|---------|
| `--> [<ts>] [INFO\|WARNING\|ERROR] ...` | driver commentary (Python logger) |
| `*** ...` | a report / milestone line — the summary at the end of the run |
| anything else | a raw Erigon log line, verbatim |

`--> ... [INFO] *** ...` is both: a driver line that belongs to the report.
The clean-exit and stage-exec drivers use a simpler `*** - <utc-ts> - <msg>`
form and prefix each Erigon line with `OK->` / `!!->`.

The report block sits at the very end of a completed run. A canonical passing
tip-tracking report:

```
--> [...] [INFO] *** Tip tracking completed
--> [...] [INFO] *** Total tracking time: 7200 secs
--> [...] [INFO] *** Total sync time: 7063 secs (98%)
--> [...] [INFO] Final value of metric 'exec_steps_in_db': 15.75
--> [...] [INFO] *** Tip tracking completed successfully
--> [...] [INFO] *** In-sync delay: 1979.9 secs
--> [...] [INFO] *** Snapshots download time: 6.1 mins
--> [...] [INFO] *** Data-dir size increment: 83.75 mb
--> [...] [INFO] *** Block height increment: 1276
--> [...] [INFO] *** Per-block size increment: 0.07 mb
--> [...] [INFO] *** Blocks per second: 0.18
```

Full field-by-field reference, including the metric block that follows it:
[references/tip-tracking-report.md](references/tip-tracking-report.md).

## The three ways a tip-tracking-family test fails

These cover `qa-tip-tracking*`, `qa-constrained-tip-tracking`,
`qa-sync-from-scratch*`, `qa-sync-with-externalcl` and
`qa-sync-test-bisection-tool` — they all run the same
`tip-tracking/run_and_check_tip_tracking.py` driver.

### a) Sync-time threshold — Erigon reached the tip but could not hold it

```
*** Total sync time: 4536 secs (63%)
[ERROR] *** Test failed: total sync time below threshold (75%), final value: 63%
```

The node must be within `2 × block_time` of wall-clock (24 s mainnet/sepolia/
hoodi, 10 s gnosis/chiado, 4 s bor-mainnet/amoy) for **≥ 75 %** of the tracking
window. Below that → `FAILURE`, `reason: "in sync less than 75% of the time"`.
It's a performance regression: something made block processing slower than the
chain produces blocks. Cross-check `exec_mgas_sec`, the
`block_consumer_delay_hist_bucket` percentages and the metric plots.

### b) `exec_steps_in_db` threshold — state is not being collated fast enough

```
Final value of metric 'exec_steps_in_db': 378.656
[ERROR] *** Test failed: exec_steps_in_db exceeded threshold (300), final value: 378.656
```

Undigested execution steps piling up in the DB. Only enforced for Erigon ≥ 3.5;
on 3.4.x the driver logs a warning and skips the check (the block-snapshot
collation cap there produces benign spikes).

### c) `Deadline reached` — the run timed out before reaching the tip

```
*** Deadline reached
[ERROR] *** Aborting test: Deadline reached
```

**Read this carefully — it is the most misread outcome.** It means Erigon never
got to the chain tip within `TOTAL_TIME_SECONDS`, and the driver could not
attribute it to any single check. It surfaces as `outcome: "Unexpected error"`,
`reason: "Deadline reached"`.

> This is **not** an infrastructure problem. It is an Erigon problem that needs
> investigation. Do not report it as "runner too slow" or "CI flake" without
> evidence.

Investigate: find where the log stops progressing, identify which stage was
running (snapshot download / execution / commitment), and read the goroutine
stack dump at the end for a stuck or deadlocked stage.

Aborts also happen for `Erigon process terminated unexpectedly`, `Erigon in
ERROR: ...` (any `[EROR]` line while the test is active), `SIGSEGV`, and
`SyncSentinel error` — all `outcome: "Unexpected error"` with the cause in
`reason`. [references/triage.md](references/triage.md) has the full decision
tree, including which failures are genuinely environmental.

## Other test families

Each has its own driver, thresholds and report vocabulary:

- **snap-download** — counts snapshots, download rate, per-phase completion;
  fails with `*** Snapshot download completed with failure: <why>`.
- **clean-exit** — sends Ctrl-C and measures exit time; fails on `panic`,
  `segmentation fault`, or not exiting within the threshold.
- **stage-exec** — runs `integration stage_exec` and scans for `[EROR]`,
  `catch panic`, `wrong receipt`, `SIGSEGV`, `EXCEPTION`.
- **RPC integration** — diff count against expected responses; verdict lives in
  `results/test_report.json` and `output.log`, plus a `summary.md` echoed into
  the job summary.
- **RPC performance / txpool** — latency percentiles at increasing QPS, then
  change-point detection; a run can fail because an *open change-point issue*
  exists, not because this run was slow.

Details and the per-workflow table (runner labels, driver script, timings,
artifact names, `--test_name` used in MongoDB):
[references/workflows.md](references/workflows.md).

## Rules of thumb

- **The result JSON is the verdict; the log is the explanation.** Quote `reason`
  before theorising.
- **Measures are as important as pass/fail.** A green run whose
  `per-block-size-increment_mb` doubled is a regression the thresholds missed —
  compare against Grafana history rather than judging one run in isolation.
- **A test that never produced `result-<chain>.json`** (no `test-results`
  artifact, `test_executed != true`) failed *before* the test ran — build,
  datadir restore, or runner problem. That, unlike `Deadline reached`, really is
  infrastructure.
- **Warnings are not failures**, but `Node in false sync condition`,
  `batch processing with #blocks=N` and `head updated with age=N` are the
  breadcrumbs that explain a sync-time failure.
- **Never propose muting or skipping a QA test** to get a check green; see the
  project's test-skip policy in `CLAUDE.md`.
