# `result-<chain>.json`

Written by every QA driver at the end of the run, uploaded as the
`test-results*` artifact, and pushed to MongoDB by
`erigon-qa/test_system/qa-tests/uploads/upload_test_results.py`.

```json
{
  "outcome": "FAILURE",
  "reason": "in sync less than 75% of the time",
  "exit_code": 1,
  "measures": { "total-sync-time_%": 63.0, "...": "..." }
}
```

## `outcome` → what actually happened

| `outcome` | `exit_code` | Produced by | Interpretation |
|-----------|-------------|-------------|----------------|
| `SUCCESS` | 0 | all checks passed | still read the measures — thresholds are loose |
| `FAILURE` | 1 | an explicit threshold check failed | Erigon underperformed; `reason` says which threshold |
| `Unexpected error` | 2 | `_abort_test()` or an unhandled exception in the tip-tracking driver | the run never got to the checks — deadline, crash, `[EROR]` line, sentinel failure |
| `ERROR` | 1 | the clean-exit / stage-exec drivers' equivalent of the above | same idea, different driver |

`Unexpected error` with `reason: "Deadline reached"` is **an Erigon problem, not
an infrastructure problem** — see the main SKILL.md.

`reason` is free text and is the single most useful field. The workflows extract
it for the GitHub annotation via
`erigon-qa/test_system/qa-tests/tip-tracking/print_reason.py`, so the `::error::`
line in the job log often already contains it:

```
::error::Tip-tracking test encountered an error: in sync less than 75% of the time
```

To read it yourself:

```bash
jq -r '.outcome + " — " + .reason' result-mainnet.json
```

## `measures`

A flat dict; keys carry their unit as a suffix (`_m` minutes, `_mb` megabytes,
`_%` percent, `_s` seconds). Tip-tracking-family keys are documented in
[sync-tests.md](sync-tests.md). Notable ones:

| Key | Why you care |
|-----|--------------|
| `total-sync-time_%` | the 75 % threshold |
| `exec-steps-in-db-final-value` | the 300 threshold (enforced on ≥ 3.5 only) |
| `in-sync-delay_m` | time to reach the tip — the headline for sync-from-scratch |
| `per-block-size-increment_mb` | DB-growth regressions |
| `in-sync-intervals` | list of interval lengths; many short ones = flapping |
| `subdir-size-increments_mb` | localises a size regression to a datadir subdir |
| `metric-block_consumer_delay_hist_bucket-*-percentage-under-threshold-*` | explains a sync-time failure |
| `metric-*-threshold-*-warning` | that percentage is approximate |
| `fd-leak-report` | nested JSON; the readable version is the `fd-leak-analysis-*.md` artifact |
| `chain` | added after the run by the workflow |

`measures` is `[]` (an empty list, not a dict) when the driver died before any
measurement — a reliable marker of an early crash.

Other families:

- **clean-exit**: `exit_time_secs`.
- **stage-exec**: `timeout_seconds`, `actual_exit_code`, `chaintip_mode`,
  `rm_state_mode`.
- **RPC integration**: verdict is not here — it's in
  `<result-dir>/results/test_report.json` alongside `output.log` and `summary.md`.

## Historical comparison

One run in isolation says little. The same JSON feeds MongoDB → Grafana
(<https://monitoring.erigon.io/d/ddqiwbfvrgwlcd/erigonqa>), keyed by the
`--test_name` the workflow passes to `upload_test_results.py` (see
[workflows.md](workflows.md)), plus repo, commit, branch, chain, runner and
`db_version`. Compare a suspicious measure against that trend before calling it
a regression — the RPC-performance suite automates exactly this with
change-point detection.
