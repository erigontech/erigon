# Cross-client ranking comparisons

Use this workflow to combine a target client's local Benchmarkoor result with peer runs from the
[Benchmarkoor website](https://benchmarkoor.core.ethpandaops.io/) and produce tables like the
[Erigon PR #22409 performance table](https://github.com/erigontech/erigon/pull/22409#issue-4868249288).

## Contents

- Preserve provenance and secrets
- Select comparable runs
- Extract normalized fixture data
- Calculate rankings and summaries
- Present and qualify the results

## Preserve provenance and secrets

- Obtain an API key from the user or environment and keep it only in
  `BENCHMARKOOR_API_KEY`. Never put a key in this skill, a script, a command literal, a result file,
  a commit, or the final report. Disable shell tracing before authenticated requests.
- Record the suite page, suite hash, local result path and run ID, every selected peer run ID, and
  the retrieval time. Do not substitute a website run for a local target run when the user asks to
  rank a particular local build.
- Record each run's Benchmarkoor version, image and digest, client version, instance ID, command,
  resource limits, host CPU, datadir method, cache-drop policy, rollback strategy, State Actor
  manifest, and pass/fail counts. Fetch `config.json` through `/api/v1/files/*` when the run query
  does not expose these fields; follow the returned presigned URL when the response contains `url`.
- Treat matching suite hashes as fixture identity, not proof of experimental equivalence. Prefer a
  contemporaneous peer batch from one host with matching CPU, memory, frequency, cache, storage,
  and runner settings. Disclose every mismatch. In particular, label local OverlayFS versus remote
  Schelk or different-host comparisons as directional rather than controlled measurements.

## Select comparable runs

Use the indexed API rather than scraping rendered tables. Pass the API key as a bearer token from
the environment. The query endpoints use `column=operator.value`, support `order`, `limit`, and
`offset`, and return rows in `.data`:

```bash
API_BASE=https://benchmarkoor.core.ethpandaops.io/api/v1
SUITE_HASH=<suite-hash>
CLIENT=reth
set +x
curl -fsS --config - --get "$API_BASE/index/query/runs" \
  --data-urlencode "suite_hash=eq.$SUITE_HASH" \
  --data-urlencode "client=eq.$CLIENT" \
  --data-urlencode "status=eq.completed" \
  --data-urlencode "has_result=eq.true" \
  --data-urlencode "order=timestamp.desc" \
  --data-urlencode "limit=100" <<EOF
header = "Authorization: Bearer ${BENCHMARKOOR_API_KEY:?set BENCHMARKOOR_API_KEY}"
EOF
```

Inspect candidates instead of blindly taking the newest run. Require the exact suite hash, intended
full-suite instance such as `<client>-bal-full`, completed status, a result, zero failed tests, and
the expected fixture count. Select one consistent snapshot across peers and explain any exception.
Keep the selected metadata as an auditable manifest.

Fetch each selected run's fixture rows from `/index/query/test_stats` with filters for both
`suite_hash` and `run_id`. Select at least `client,run_id,test_name,test_gas_used,test_time_ns,
test_mgas_s`. Paginate with `limit` and `offset` until no rows remain; never assume the first page is
complete. Reject duplicate `test_name` rows within one run.

## Extract normalized fixture data

Extract the local target's successful test-step gas and payload-processing duration from
`result.json`:

```bash
jq -r '
  .tests | to_entries[] |
  select((.value.steps.test.aggregated.fail // 0) == 0) |
  [
    .key,
    .value.steps.test.aggregated.gas_used_total,
    .value.steps.test.aggregated.gas_used_time_total
  ] | @tsv
' "$RUN_DIR/result.json"
```

Use full `test_name` values as join keys. Shorten names only when rendering a table. Build the exact
intersection across the local target and every selected peer, and report each source count plus the
intersection count. Do not impute or silently drop missing or failed fixtures. Verify that gas is
positive and identical across clients for every joined fixture; investigate or explicitly exclude
any mismatch.

Calculate each unrounded fixture rate from raw fields:

```text
rate_mgas_s(client, fixture) = gas_used * 1000 / time_ns
```

Use `gas_used_time_total` locally and `test_time_ns` remotely. Do not use total step duration because
it includes non-payload work. Treat the API's `test_mgas_s` as a cross-check, not a separate metric.

## Calculate rankings and summaries

For every fixture in the common intersection:

- Sort unrounded rates descending. Set the target rank to one plus the number of clients with a
  strictly higher rate; exact ties share a rank. Never derive ranks from rounded display values.
- Set `gap to 1st` to `fastest peer rate / target rate` and `gap to 2nd` to
  `second-fastest peer rate / target rate`. Choose both from peer clients, excluding the target. A
  ratio below `1.0x` means the target is faster than that peer.
- Define the target's "worst fixtures" by descending `gap to 1st`, not by raw target MGas/s or rank
  alone. Use target rank descending and full fixture name ascending as deterministic tie-breakers.
- For a before/after target comparison, calculate `improvement = after / before`, calculate both
  ranks against the same fixed peer snapshot, and set `rank delta = rank_before - rank_after`, so a
  positive delta means improvement.

Calculate an overall or grouped throughput only over the same fixture set for every client:

```text
aggregate_mgas_s = sum(gas_used) * 1000 / sum(time_ns)
```

Never average per-fixture MGas/s values. Group by the full test module path before `::` when module
summaries are useful. Define a 100M subset from the raw gas value `100000000`, not from a name match.
Optionally report target-versus-peer wins, losses, and exact ties, plus the target's fixture-rank
distribution.

## Present and qualify the results

Start with a provenance and comparability block, then report:

- coverage counts and exclusions;
- weighted overall and, when requested, 100M or module summaries;
- a PR-style fixture table with target, peer rates, target rank, gaps to the two fastest peers, and
  optional baseline improvement and rank movement;
- the requested top-N worst fixtures sorted by `gap to 1st`;
- head-to-head and rank-distribution summaries when they help diagnose broad behavior.

Round only for display and retain raw values for calculations. State that cross-host or mixed
datadir-method rankings measure the observed environments as a whole and cannot isolate client-code
performance. Preserve the selected-run manifest and derived machine-readable data so later tables
use the same snapshot instead of silently fetching newer peers.
