# The `qa-*` workflows

All live in `.github/workflows/`. All but `qa-test-report.yml` run on
self-hosted QA runners selected by label; `$ERIGON_QA_PATH` is
`/home/qarunner/erigon-qa` on every runner.

Runner label families: `tip-tracking` / `rpc-*` runners hold a **pre-built DB**
kept aligned with the chain tip; `long-running` runners start from a **blank
datadir**.

## Tip-tracking family

All drive `qa-tests/tip-tracking/run_and_check_tip_tracking.py` — same report
format, same thresholds ([tip-tracking-report.md](tip-tracking-report.md)).

| Workflow | Runner labels | DB | Times | `--test_name` |
|----------|---------------|-----|-------|---------------|
| `qa-tip-tracking.yml` — *QA - Tip tracking & migration* | `Ethereum, tip-tracking` | pre-built | track 2 h / total 10 h | `tip-tracking` |
| `qa-tip-tracking-gnosis.yml` | `Gnosis, tip-tracking` | pre-built | 2 h / 8 h | `tip-tracking` |
| `qa-constrained-tip-tracking.yml` — *with constraints* | `Ethereum, tip-tracking` | pre-built, run under `cgexec -g memory:constrained_res_32G` | 2 h / 10 h | `constrained-tip-tracking` |
| `qa-tip-tracking-with-load.yml` | `Ethereum, tip-tracking` (erigon) / `rpc-latest-geth` (geth) | pre-built | 2 h / 3 h | `tip-tracking-with-load-<client>` |
| `qa-sync-from-scratch.yml` — *archive node* | `long-running` | blank | 2 h / 5–23 h per chain | `sync-from-scratch` |
| `qa-sync-from-scratch-full-node.yml` | `long-running` | blank | 2 h / matrix | `sync-from-scratch-full-node` |
| `qa-sync-from-scratch-minimal-node.yml` | `long-running` | blank | 2 h / 12 h | `sync-from-scratch-minimal-node` |
| `qa-sync-with-externalcl.yml` | `long-running` | blank, external CL (prysm / lighthouse) | 1 h / 8 h | `sync-from-scratch-<client>-minimal-node` |
| `qa-sync-test-bisection-tool.yml` | `long-running` | blank | 2 min / 8 h | dispatch input |

Notes that change how you read a run:

- The report and Erigon output for the verdict live in the GitHub step
  **`Run Erigon, wait sync and check ability to maintain sync`**.
- The tip-tracking and Gnosis workflows run a **pre-test stabilisation step**
  first (`pre_test_step`, shown as
  **`Run previous Erigon version and wait for sync (stabilization step)`**,
  previous Erigon version, 120 s tracking). A failure there is *not* a failure of
  the branch under test — it means the reference datadir was not tracking the tip.
  Check which step failed before blaming the PR.
- `sync-from-scratch` (archive) also runs the **RPC integration suite** against
  the freshly synced datadir afterwards, for mainnet and gnosis. That step can
  fail on its own — check `rpc-test-results-<chain>`.
- `qa-constrained-tip-tracking` runs Erigon in a 32 GB memory cgroup and passes
  `statistics`, adding `proc_stat.log` to the `erigon-logs-<chain>` artifact.
  A sync-time failure here means "too slow under memory pressure", not
  "too slow" — compare against the unconstrained run before concluding.
- `qa-tip-tracking-with-load.yml` drives Vegeta at increasing QPS and produces
  the `sync_with_load` plot (sync state vs QPS). Read the plot, not the numbers.

## Other families

| Workflow | Driver | Fails when |
|----------|--------|-----------|
| `qa-snap-download.yml` | `qa-tests/snap-download/run_and_check_snap_download.py` | download doesn't start, doesn't complete, or a phase is left incomplete — `*** Snapshot download completed with failure: <why>`. Reports per-phase `*** <key> snap tot=…`, `*** <key> snap download time: N mins`, `*** Snapshots download time: N mins`. Also aborts on `*** Deadline reached` (8 h). |
| `qa-clean-exit-block-downloading.yml`, `qa-clean-exit-snapshot-downloading.yml` | `qa-tests/clean-exit/run_and_check_clean_exit.py` | Erigon panics/segfaults, or doesn't exit within the threshold after Ctrl-C. Log form is `*** - <utc> - <msg>`, Erigon lines prefixed `OK->` / `!!->`. Measure: `exit_time_secs`. |
| `qa-stage-exec.yml` | `qa-tests/stage-exec/run_and_check_stage_exec.py` | a scanned pattern appears: `[EROR] … catch panic`, `[EROR]`, `wrong receipt`, `SIGSEGV`, `panic`, `EXCEPTION`; or a non-zero exit. Matrix: `{resume-nonchaintip, from-0, chaintip} × {serial, parallel}` via `ERIGON_EXEC3_PARALLEL`. A parallel-only failure points at the parallel executor. |
| `qa-rpc-integration-tests*.yml` (+ `-gnosis`, `-latest`, `-remote`, `-polygon`, `-clients`) | `.github/workflows/scripts/run_rpc_tests_*.sh` | response diffs vs expected (or vs geth/nethermind for `-latest`). Verdict in `results/test_report.json`; `summary.md` is echoed into the job summary, so **the GitHub run summary alone often suffices**. Log format + per-test diffs: [rpc-tests.md](rpc-tests.md). |
| `qa-rpc-performance-tests.yml`, `qa-rpc-performance-comparison-tests.yml` | `qa-tests/rpc-tests/perf_hdr_analysis.py` + `change-points/change_point_analysis.py` | latency change-point detected → an issue is opened on the internal tracker; the job fails while **any change-point issue is open**. A red run therefore may reflect an older unresolved regression. Log format: [rpc-tests.md](rpc-tests.md). |
| `qa-rpc-test-bisection-tool.yml`, `qa-sync-test-bisection-tool.yml` | dispatch-only | bisect a regression across commits; inputs include `test_name`. |
| `qa-test-report.yml` | `.github/workflows/scripts/test_report/generate-test-report.ts` (`ubuntu-latest`) | never — it renders a pass/fail grid over a date range for the workflows in its `acceptedWorkflows` list. Use it for "was this already failing yesterday?". |

## Artifacts

Names vary per workflow (chain / node-type / matrix suffixes), so list before
downloading:

```bash
gh api repos/erigontech/erigon/actions/runs/<run-id>/artifacts --jq '.artifacts[].name'
gh run download <run-id> --repo erigontech/erigon -D /tmp/qa
```

Common shapes:

| Artifact | Contents |
|----------|----------|
| `test-results*` | `result-<chain>.json`; on the sync-from-scratch and snap-download workflows also `erigon_data/logs/` |
| `erigon-logs*` | the full Erigon **debug**-level log (`--log.dir.verbosity debug`, `trace` with an external CL); `proc_stat.log` on the constrained run |
| `metric-plots*` | `metrics-<chain>-plots_<metric>_*.png` and `_data.json` |
| `fd-leak-analysis*` | `fd-leak-analysis-<chain>.md`, sampled every 60 s against a baseline |
| `torrent-client-status*` | downloader torrent state at the end of the run |
| `rpc-test-results-<chain>` | the RPC suite's result dir (`results/test_report.json`, `output.log`, `summary.md`) |

## The job log

The annotated test log is the **step log**, not an artifact. It is huge. Prefer:

```bash
gh run view <run-id> --repo erigontech/erigon                       # step list + conclusions
gh run view --job <job-id> --repo erigontech/erigon --log | grep -F '***'
```

`gh run view --log-failed` is usually the wrong tool here: the failing step is
the whole multi-hour test step.

## Workflow-level failures that are not test failures

`test_executed=true` is written by the test step as soon as the driver runs.
If a job is red and **no** `test-results` artifact exists / the upload steps were
skipped, the run died earlier — `make erigon` failed, the reference datadir
rsync failed, `pause_production.py` misbehaved, or the job hit
`timeout-minutes`. Several workflows say so explicitly:

```
::error::Test not executed, workflow failed for infrastructure reasons
```

A job killed by `timeout-minutes` (not by the driver's own deadline) shows as
`cancelled`/`timed_out` with no report block at all — distinguish it from
`*** Deadline reached`, which the driver writes and which *is* an Erigon bug.
