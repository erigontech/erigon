# Reading RPC test logs

The **RPC test** category runs against `rpcdaemon` (Erigon's JSON-RPC component)
and splits into two sub-families with completely different log shapes and
pass/fail logic:

- **integration** — correctness: replay ~1000 requests and diff each response.
- **performance** — latency: hammer each method at increasing QPS and watch for
  regressions via change-point detection.

Both run on a **pre-built DB** kept aligned with the chain tip.

> If the RPC step log is empty or truncated, an **earlier workflow step failed**
> (build, datadir rsync, rpcdaemon startup). Check the preceding steps before
> reading anything into the missing report.

---

## Integration tests

Workflows: `qa-rpc-integration-tests*.yml` (`-gnosis`, `-latest`, `-remote`,
`-clients`, …). Driver: `.github/workflows/scripts/run_rpc_tests_*.sh`.

Two comparison modes:

- **historical data** — responses are diffed against a stored set of **expected
  responses** (`erigontech/rpc-tests`).
- **latest / tip data** (`-latest`, `-clients`) — responses are diffed against a
  **reference client** (geth / nethermind) running the same request.

### Log format

Each test is one line:

```
0187. http           ::debug_traceBlockByNumber/test_42.json                          OK
0169. http           ::debug_traceBlockByNumber/test_24.json                          failed: diff mismatch
```

- `OK` — response matched.
- `failed: diff mismatch` — the rpcdaemon response differs from the
  expected/reference one. **This is the finding**; the method + test file name
  point straight at the failing case.

Tests run in **batches**, and a batch stops at the first failure:

```
Latest batch 1/5 (50 tests)
Latest block number for localhost:8545, 157.180.55.78:8545: 25622906
...
Latest batch 1/5 had failures, stopping
```

Because tip data can momentarily be inconsistent, the whole suite is **retried
up to 5 times** (`Attempt 1`, `Attempt 2`, …); a mismatch that clears on retry
was a transient tip artefact, one that persists across all attempts is real.

The run ends with a summary block — the numbers that matter are the counts:

```
Test suite total tests:        1653
Number of skipped tests:       38
Number of selected tests:      218
Number of executed tests:      50
Number of success tests:       48
Number of failed tests:        2

JSON report generated: integration/mainnet/results/test_report.json
```

### Where the verdict lives

- **`summary.md`** is echoed into the GitHub **job summary** — often enough on
  its own to see which methods failed.
- **`results/test_report.json`** — the machine-readable verdict (the
  `result-<chain>.json` at the top level does *not* carry the RPC verdict).
- **`output.log`** — the full per-test log shown above.

All three ship in the `rpc-test-results-<chain>` artifact.

### Inspecting a `diff mismatch`

The artifact contains, **per failing test**, a folder with the **actual**
response, the **expected** response, and their **diff** — download it to see
exactly what changed:

```bash
gh api repos/erigontech/erigon/actions/runs/<run-id>/artifacts --jq '.artifacts[].name'
gh run download <run-id> --repo erigontech/erigon -D /tmp/qa
```

The same diffs are browsable in the Hive UI:
<http://rpctests.erigon.io/hive/main/index.html#summary-sort=name>.

---

## Performance tests

Workflows: `qa-rpc-performance-tests.yml`,
`qa-rpc-performance-comparison-tests.yml` (+ `-latest`). Driver drives **Vegeta**
at rising QPS per method, then runs HDR-percentile and change-point analysis.

### Log format

Per method:

```
Performance Test started
Test repetitions: 5 on sequence: 1:1,100:30,1000:20,10000:20,20000:20 for pattern: .../stress_test_eth_call_001_14M.tar

Test on port: http://localhost:8545
[1.1] erigon: executes test qps:     1 time:  1 -> success=100.00% lat=[max=415.19ms]
[2.1] erigon: executes test qps:   100 time: 30 -> success=100.00% lat=[max=723.16ms]
[2.2] erigon: executes test qps:   100 time: 30 -> success=100.00% lat=[max=  4.21ms]
...
Performance Test completed successfully.
```

- The **sequence** `1:1,100:30,1000:20,…` is a list of `qps:duration_seconds`
  stages: 1 QPS for 1 s, then 100 QPS for 30 s, then 1000 QPS for 20 s, …
- `[G.R]` — `G` is the QPS-stage index, `R` is the repetition within it
  (`repetitions: 5`).
- `success=100.00%` — share of requests that got a valid response at that load.
  **Below 100 % means requests failed/timed out** at that QPS — the first thing
  to look at.
- `lat=[max=…]` — worst-case latency for that run. The **first repetition of a
  stage is routinely much higher** (cold caches / warm-up); judge steady-state
  from the later repetitions, not `[x.1]`.

Then per method the driver writes an HDR percentile report and saves to MongoDB:

```
Analysis complete. Results saved to ./erigon-eth_call-latency_hdr_analysis.pdf
Save test result on DB
branch_name=main
commit_hash=bafdb4d5cfa5e91855ddbec4f1878c42d791dcbe
method=eth_call
db_version=218f7d0af9ab691960f3395f1e2e0c3c638045ab
outcome=success
result_file=erigon-eth_call-result.json
```

The `-latency_hdr_analysis.pdf` per method is the readable artifact for latency
distribution.

### Pass/fail is *not* the log

`Performance Test completed successfully` / `outcome=success` only means the run
executed — **it is not the pass/fail verdict**. The workflow fails based on
**change-point detection**: after the run, latencies are compared against
history and, if a regression is detected, an issue is opened on the internal
tracker. The job fails while **any change-point issue is open** — so a red run
can reflect an *older, still-open* regression, not this commit. Check the issue
tracker state before attributing a red perf run to the branch under test
(see [triage.md](triage.md) §6).
