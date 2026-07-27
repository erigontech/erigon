# Triage decision tree

Start from a red `QA - ...` check. Work top-down; each step is cheaper than the
next.

## 1. Did the test actually run?

```bash
gh run view <run-id> --repo erigontech/erigon
gh api repos/erigontech/erigon/actions/runs/<run-id>/artifacts --jq '.artifacts[].name'
```

- **No `test-results*` artifact / test step never reached** → infrastructure.
  Which step failed: `make erigon`? the datadir rsync? `pause_production.py`?
  The job may also have hit `timeout-minutes` (conclusion `cancelled` /
  `timed_out`, no report block). Report as an infra/build problem, not an
  Erigon regression.
- **Job conclusion is `cancelled`** and a newer run exists → concurrency
  cancellation, ignore.
- Otherwise continue.

## 2. Read the verdict

```bash
gh run download <run-id> --repo erigontech/erigon -D /tmp/qa
jq -r '.outcome + " — " + .reason' /tmp/qa/test-results*/result-*.json
```

Branch on `outcome`:

### `FAILURE` — a threshold was breached

Erigon ran to the end but underperformed. `reason` names the threshold.

- `in sync less than 75% of the time` → go to §3.
- `exec_steps_in_db exceeded threshold of 300` → go to §4.
- Snapshot-download / clean-exit / stage-exec wording → §6.

### `Unexpected error` / `ERROR` — the run aborted

- `Deadline reached` → §5. **Erigon bug, not infrastructure.**
- `Erigon process terminated unexpectedly: <r>` → Erigon died. Find the last
  lines of `erigon.log`; look for OOM (`dmesg`-style kill, or the constrained
  cgroup), a panic, or a clean-but-unexpected shutdown.
- `Erigon in ERROR: Errored line: <line>` → a single `[EROR]` line aborted the
  test. **The line itself is the finding** — quote it. Check whether it is
  genuinely fatal or an over-eager abort on a benign error.
- `Erigon aborted: Segmentation fault: <line>` → crash; the goroutine dump is
  at the tail of the test log.
- `SyncSentinel error: <d>` → the driver could not reach `:8545`. Usually means
  Erigon's RPC never came up or stopped responding — check the Erigon log at
  that timestamp before calling it a test-harness fault.
- `Consensus client startup failure` (external-CL runs) → prysm/lighthouse
  checkpoint-sync endpoint problem; genuinely environmental, and the driver
  picks the endpoint at random from a list, so retrying may pass.
- `LoadTool error` (with-load runs) → Vegeta side; check the load-tool output.
- `Exception: ...` → a driver bug or a bad argument in the workflow YAML.

### `SUCCESS` but the check is red

The test passed and a *later* step failed — most often the RPC integration
suite that `qa-sync-from-scratch.yml` runs on the freshly synced datadir. Look
at `rpc-test-results-<chain>`, not at the sync report.

## 3. Sync-time failure — Erigon can't keep up

The node held the tip < 75 % of the tracking window. This is a performance
regression. Build the story from:

```bash
grep -F '***' <log>                       # the report
grep -cF '*** Out of sync' <log>          # once, or flapping?
jq '.measures | {"in-sync-intervals", "blocks-per-second",
                 "per-block-size-increment_mb"}' result-<chain>.json
```

- **One long out-of-sync interval** → the node fell behind and never recovered;
  find what happened at that timestamp (a merge/collation, a reorg, a stall).
- **Many short intervals** → chronically marginal throughput; look at
  `exec_mgas_sec` mean and the `block_consumer_delay_hist_bucket` percentages.
- Warnings that explain it: `Execution performances: batch processing with
  #blocks=N` (the node is catching up in batches, i.e. it was behind) and
  `Execution performances: head updated with age=N` (block accepted N s late).
- Open `metric-plots*` — the delay histogram and `exec_mgas_sec` plots localise
  the slowdown in time far faster than grepping.
- Compare `total-sync-time_%` and `exec_mgas_sec` against Grafana history: a
  63 % run after a run of 98 % on the previous commit is a bisectable
  regression; `qa-sync-test-bisection-tool.yml` exists for exactly that.

## 4. `exec_steps_in_db` failure

Undigested execution steps accumulating in the DB — collation/pruning is not
keeping pace with execution.

- Check the Erigon version: on **3.4.x the check is skipped** and only warns
  (`check skipped for erigon <= 3.4.x`). If you see the warning rather than the
  failure, it isn't the cause of the red run.
- `metrics-<chain>-plots_exec_steps_in_db_default_gauge.png` shows whether it
  grew steadily (a real backlog) or spiked bimodally (collation capped at the
  block-snapshot boundary).
- A run can breach both thresholds; only the sync-time one is reported as
  `reason` because it is checked first. Always look at
  `exec-steps-in-db-final-value` too.

## 5. `Deadline reached`

The node never reached the tip within `TOTAL_TIME_SECONDS` and the driver could
not attribute it to a specific check.

> Not an infrastructure problem. An Erigon problem that requires investigation.

Method:

1. **Find where progress stopped.** The log is chronological; locate the last
   line showing forward movement (block number, download progress, stage
   transition) and note the gap to the deadline.
2. **Identify the stage.** `*** Warm-up completed` present but `*** Chain tip
   reached` absent means it never got in sync. Which phase was it in —
   OtterSync snapshot download, execution history download, block execution,
   commitment?
   ```bash
   grep -nE 'OtterSync|Snapshot-Downloading|ExecutionHistoryDownload|\[EROR\]|\[WARN\]' <log> | tail -50
   ```
3. **Read the goroutine dump** at the tail of the test log (driver sends
   `SIGUSR1` on abort). A stage blocked on a channel/mutex across the whole dump
   is a deadlock; that's the finding.
4. **Check whether it's a throughput problem instead**: if the node was steadily
   executing but too slowly to close the gap in the allotted hours, the metric
   plots will show it — that's a performance regression, not a hang.
5. Compare `in-sync-delay_m` on the last green run of the same workflow/chain.
   Deadlines are sized with headroom, so a genuine timeout usually means a large
   slowdown, not a marginal one.

## 6. Other families

- **snap-download** — `*** Snapshot download completed with failure: <why>`:
  *not started* (downloader never began — check peers/torrent status),
  *not completed* (stalled — see `torrent-client-status.txt` and the per-phase
  `*** <key> snap download time: not completed` lines), *phases not completed*.
- **clean-exit** — `panic` / `segmentation fault` on shutdown, or "Process did
  not exit within timeout period"; `exit_time_secs` quantifies it. A slow exit
  is a real regression: users see it as a hung Ctrl-C.
- **stage-exec** — the matched pattern is the reason (`catch panic`,
  `wrong receipt`, `EXCEPTION`, …). Check whether **only the `parallel`
  matrix entries** failed → the parallel executor (`ERIGON_EXEC3_PARALLEL`), not
  execution in general.
- **RPC integration** — start from the job summary (`summary.md` is echoed
  there), then `results/test_report.json` for the failing methods and the diff
  files in the result dir.
- **RPC performance / txpool** — a red run may reflect an **open change-point
  issue** from an earlier regression rather than this run. Check the issue
  tracker state before attributing it to the commit under test.

## 7. What to report

State, in this order: which workflow/chain/run, `outcome` + `reason` verbatim,
the phase it failed in, the supporting numbers (measures or metric plots), and
whether history shows this as new. Say explicitly when the evidence supports
"infrastructure" — and don't say it for `Deadline reached`.

Never propose adding a skip, loosening a threshold, or re-running until green to
clear a QA failure; see the test-skip policy in `CLAUDE.md`.
