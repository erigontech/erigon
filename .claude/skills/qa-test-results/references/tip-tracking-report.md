# The tip-tracking report, line by line

Driver: `erigon-qa/test_system/qa-tests/tip-tracking/run_and_check_tip_tracking.py`.
Shared by `qa-tip-tracking*`, `qa-constrained-tip-tracking`,
`qa-sync-from-scratch*`, `qa-sync-with-externalcl`, `qa-sync-test-bisection-tool`
and the pre-test stabilisation steps.

## Test phases

The driver is a state machine; the log announces every transition.

| State | Entered when | Log line |
|-------|--------------|----------|
| `STARTING` | Erigon launched | — |
| `LOOKING_FOR_CHAIN_TIP` | after the warm-up timer (120 s; 2 s in mock mode) | `*** Warm-up completed` |
| `TRACKING_CHAIN_TIP` | SyncSentinel first reports in-sync | `*** Chain tip reached` + `*** In sync` |
| `ENDED` | tracking timer expires, or deadline hit while already tracking | `*** Tip tracking completed` |
| `ABORTED` | fatal event | `*** Aborting test: <reason>` |

Two timers bound the run:

- `TRACKING_TIME_SECONDS` — how long the node must hold the tip once reached
  (typically 7200 = 2 h). Expiry → normal completion + analysis.
- `TOTAL_TIME_SECONDS` — hard deadline for the whole run. Expiry while still
  `LOOKING_FOR_CHAIN_TIP` → **abort** (`Deadline reached`). Expiry while already
  `TRACKING_CHAIN_TIP` → the run completes normally with a shorter window.

`*** In sync` / `*** Out of sync` bracket each in-sync interval; the count of
these pairs tells you whether the node lost the tip once or flapped constantly.

## What "in sync" means

`SyncSentinel` polls `http://localhost:8545` every `block_time` seconds using
`EnhancedBlockTimestampPolicy`: in sync ⇔ `now - latest_block.timestamp <=
2 × block_time`.

| Chain | block_time | max delay |
|-------|-----------|-----------|
| mainnet, sepolia, holesky, hoodi | 12 s | 24 s |
| gnosis, chiado | 5 s | 10 s |
| bor-mainnet, amoy | 2 s | 4 s |

The policy also cross-checks `eth_syncing`. A mismatch logs
`Node in false sync condition` / `Node in false out-of-sync condition` — a
warning, not a failure, but worth reporting: it can indicate a bug in
`eth_syncing` itself.

## The report block

`*** Tip tracking completed` opens it. Then, in order:

| Line | Measure key | Meaning |
|------|-------------|---------|
| `*** Total tracking time: N secs` | `total-tracking-time_m` | wall clock from first in-sync to end |
| `*** Total sync time: N secs (P%)` | `total-sync-time_m`, `total-sync-time_%` | sum of in-sync intervals; **P is the first threshold checked** |
| `Final value of metric 'exec_steps_in_db': V` | `exec-steps-in-db-final-value` | undigested execution steps left in the DB |
| `*** Tip tracking completed successfully` | — | printed only when no threshold was breached |
| `*** Total execution history download time: N secs` | `total-execution-history-download-time_m` | only if history download was observed |
| `*** In-sync delay: N secs` | `in-sync-delay_m` | time from process start to first in-sync — **the headline number for sync-from-scratch** |
| `*** Snapshots download time: N mins` | `total_download-time_m` | from first `Snapshot-Downloading-Start` to last `-End` |
| `*** Data-dir size: N mb` | `sync-final-dir-size_mb` | |
| `*** Data-dir size increment: N mb` | `dir-size-increment_mb`, `dir-size-increment_%` | growth during tracking only |
| `*** Block height increment: N` | `block-height-increment` | `0` → warning `completed with warning: no block height increment` |
| `*** Per-block size increment: N mb` | `per-block-size-increment_mb` | **the classic regression detector** |
| `*** Blocks per second: N` | `blocks-per-second` | |
| `*** FD leak analysis report added to results` | `fd-leak-report` | see the `fd-leak-analysis-*.md` artifact |

`subdir-size-increments_mb` (per datadir sub-directory) is in the JSON only —
it is what localises a size regression to `chaindata`, `snapshots`, etc.

## The metric block

Scraped from Erigon's Prometheus endpoint (`:6061/debug/metrics/prometheus`)
every `block_time` seconds while tracking. Not thresholded (except
`exec_steps_in_db`) but decisive when explaining a sync-time failure.

```
Percentage of time 'block_consumer_delay_hist_bucket-post_execution' was under threshold 4s: 95.14%
Percentage of time 'block_consumer_delay_hist_bucket-pre_execution'  was under threshold 4s: 95.14%
Initial/Final/Rate of metric 'chain_tip_mgas_per_sec': ...
Metric 'exec_mgas_sec': min 6.96, max 77.16, mean 45.84
Metric 'exec_txns':     min 262.9, max 1230.7, mean 583.8
Initial/Final/Rate of metric 'exec_blocks': ...
```

- **`block_consumer_delay_hist_bucket`** — how long a block waits before/after
  execution. Thresholds are chain-dependent: `[4]` s for the 12 s chains,
  `[1, 2]` for gnosis/chiado, `[0.5, 1, 2]` for bor-mainnet/amoy. A drop here
  is the usual cause of a sync-time failure.
  A companion `...-threshold-Ns-warning` measure is set when a sampling window
  missed the bucket exactly at the threshold — treat those percentages as
  approximate.
- **`exec_mgas_sec` / `exec_txns`** — raw execution throughput (gauge: min/max/mean).
- **`exec_blocks`** — counter; its `rate` is blocks/s from the metrics side,
  a cross-check on `*** Blocks per second`.
- **`chain_tip_mgas_per_sec`** — `nan` is normal when the node was never
  processing at the tip.

Plots of all of these ship in the `metric-plots*` artifact
(`metrics-<chain>-plots_<metric>_*.png`) — usually faster to read than the numbers.

## Failure messages, verbatim

```
*** Test failed: total sync time below threshold (75%), final value: <P>%
      → outcome FAILURE, reason "in sync less than 75% of the time"

*** Test failed: exec_steps_in_db exceeded threshold (300), final value: <V>
      → outcome FAILURE, reason "exec_steps_in_db exceeded threshold of 300"

*** Aborting test: Deadline reached
      → outcome "Unexpected error", reason "Deadline reached"

*** Aborting test: Erigon process terminated unexpectedly: <reason>
*** Aborting test: Erigon in ERROR: Errored line: <the [EROR] line>
*** Aborting test: Erigon aborted: Segmentation fault: <line>
*** Aborting test: SyncSentinel error: <description>
*** Aborting test: LoadTool error: <description>
*** Aborting test: Consensus client startup failure: <description>
      → outcome "Unexpected error", reason as shown
```

Only the first two threshold breaches are reported: sync-time is checked first,
so an `exec_steps_in_db` breach in the same run is recorded as a measure but not
as the failure reason.

Errors logged by Erigon *after* the test has ended (`STOPPING` / `STOPPED` /
`ENDED`) are deliberately ignored — `Ignoring error: ...` in the log. Don't
report those as the cause.

On abort the driver calls `dump_coroutine_stacks()` → `SIGUSR1` to Erigon, so
the goroutine dump lands at the tail of the log.

## Node types

The workflow's last positional arg selects the prune mode, which changes what is
normal for datadir growth and download time:

| arg | erigon flag |
|-----|-------------|
| `archive_node` (and `standard_node` on Erigon3) | `--prune.mode=archive` |
| `full_node` | `--prune.mode=full` |
| `minimal_node` | `--prune.mode=minimal` |
