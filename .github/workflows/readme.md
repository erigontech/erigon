# Erigon QA Test Suite

All `qa-*` workflows run on self-hosted runners and upload results to a centralised test database via
`upload_test_results.py`. Reference datadirs live under `/opt/erigon-versions/`, testbed data under
`/opt/erigon-testbed/`.

---

## Sync Tests

### 1. Snapshot Download (`qa-snap-download`)

**Goal:** Test the downloader's ability to download all snapshots and index them correctly.

**Database:** Blank DB

**Triggers:** Push to `release/3.*`, daily Mon–Sat 20:00 UTC, manual

**Duration:** Up to 8 hours (500-minute timeout)

**Measures:**

- Number of snapshots downloaded
- Max number of peers
- Max download rate
- Download time
- Indexing time

---

### 2. Sync from Scratch (`qa-sync-from-scratch`)

**Goal:** Test Erigon's ability, starting from a completely empty database, to reach the chain tip and maintain it for
at least 2 hours.

**Database:** Blank DB

**Triggers:** Push to `release/3.*`, scheduled Sunday 00:00 UTC, manual

**Chains (matrix):** Mainnet (23h), Gnosis (18h), Sepolia (10h), Hoodi (6h), Chiado (5h)

**Measures:**

- Percentage of time in sync
- Percentage of increment in datadir sub-folders
- FD leak analysis
- Metric plots (generated as artifacts)

---

### 3. Sync from Scratch – Minimal Node (`qa-sync-from-scratch-minimal-node`)

**Goal:** Same as sync from scratch but running Erigon in minimal-node mode to verify reduced-footprint operation from
genesis.

**Database:** Blank DB

**Triggers:** Push to `release/3.*`, daily 00:00 UTC, manual

**Chains (matrix):** Mainnet (18h tracking), Gnosis (8h tracking)

**Duration:** Up to 24 hours (1440-minute timeout); 2-hour stabilisation phase then 16+ hours tip tracking

**Measures:**

- Sync ability in minimal-node mode
- Stable tip tracking after genesis sync
- FD leak analysis and metric plots

---

### 4. Exec from Zero (`qa-exec-from-zero`)

**Goal:** Same as sync from scratch but with `--snap.skip-state-snapshot-download`, so only blocks come from BitTorrent
and Erigon executes from genesis. Checks that the node reaches the tip on its own execution, then that the state
snapshots it built have the same torrent infohashes as the officially published ones.

**Database:** Blank DB

**Triggers:** Scheduled Saturday 00:00 UTC, manual

**Chains (matrix):** Gnosis (4.5 days) and Mainnet (34 days) — the 6-hour job limit is GitHub-hosted only, so the
ceiling is the 35-day workflow-run limit. Sepolia, Hoodi and Chiado are commented at the moment.

**Measures:**

- Sync ability with no downloaded state
- Number of state snapshot data files (`.kv`, `.v`, `.ef`) matching / differing from the published hashes, per subdir
- FD leak analysis and metric plots

---

### 5. Sync with External CL (`qa-sync-with-externalcl`)

**Goal:** Verify that Erigon's Engine API integrates correctly with external consensus layer clients (Lighthouse and
Prysm).

**Database:** Blank DB

**Triggers:** Push to `release/3.*`, scheduled Sunday 08:00 UTC, manual

**Chains / CL matrix:**

- Mainnet + Lighthouse v8.1.0
- Gnosis + Lighthouse v8.1.0
- Mainnet + Prysm

**Duration:** 8 hours of tip tracking per combination

**Measures:**

- EL + CL sync stability over 8 hours
- JWT authentication handshake
- Chain-specific beacon log analysis
- FD leak analysis

---

## Tip Tracking Tests

### 6. Tip Tracking – Mainnet (`qa-tip-tracking`)

**Goal:** Measure the ability of Erigon to remain synchronised on the mainnet chain tip after a version upgrade, for a
continuous period of 10 hours.

**Database:** Pre-built DB (previous minor release used for stabilisation)

**Triggers:** Scheduled Sunday 01:00 UTC, push to `release/3.*`, manual

**Duration:** Up to ~22 hours (1300-minute timeout); 2-minute pre-sync with previous version then 10-hour tip-tracking
test

**Measures:**

- Percentage of time in sync
- Percentage of increment in datadir sub-folders
- Metric plots and FD leak analysis

---

### 7. Tip Tracking – Gnosis (`qa-tip-tracking-gnosis`)

**Goal:** Same as mainnet tip tracking but targeting the Gnosis chain, for a continuous period of 8 hours.

**Database:** Pre-built DB (previous minor release)

**Triggers:** Scheduled Sunday 01:00 UTC, push to `release/3.*`, manual

**Duration:** Up to 20 hours (1200-minute timeout); 8-hour tracking phase

**Measures:**

- Percentage of time in sync on Gnosis post-upgrade
- Metric plots

---

### 8. Tip Tracking with Load (`qa-tip-tracking-with-load`)

**Goal:** Measure tip-tracking stability while Erigon serves continuous RPC load, and optionally compare against Geth.

**Database:** Pre-built DB (shared workspace mirror)

**Triggers:** Scheduled Sunday 00:00 UTC, manual (with parameters: `execution_name`, `load_pattern`, `load_sequence`,
`skip_pprof`, `run_geth`)

**Duration:** Stabilisation phase + 3-hour load test

**Measures:**

- Percentage of time in sync under RPC load
- RPC latency distributions (Vegeta / HDR histograms)
- pprof CPU/memory profiles (optional)
- Erigon vs Geth comparative metrics (when `run_geth=true`)

**Notes:** Exit code 1 treated as a warning (non-blocking); full workspace cleaned before each run.

---

### 9. Constrained Tip Tracking (`qa-constrained-tip-tracking`)

**Goal:** Verify tip-tracking stability under strict memory constraints (cgroups, 32 GB limit).

**Database:** Pre-built DB (previous minor release for stabilisation)

**Triggers:** Scheduled Sunday 20:00 UTC, manual

**Duration:** 10 hours total; 2-minute stabilisation then 2-hour constrained tracking

**Measures:**

- Sync stability inside a cgroup-enforced 32 GB memory envelope
- Metric plots and FD leak analysis

---

## RPC Integration Tests

### 10. RPC Integration Tests – Mainnet (`qa-rpc-integration-tests`)

**Goal:** Test the proper functioning of the RPC APIs against a mainnet reference dataset.

**Database:** Pre-built DB

**Triggers:** Push to `main`/`release/**`, PRs, manual

**Test suite:** ~800 tests via cached `rpc-tests` binary (v1.121.0); covers `eth_`, `erigon_`, `ots_`, `trace_` APIs

**Measures:**

- Pass/fail count per API namespace
- Markdown test-result summary uploaded as artifact

**Note:** Tests are written and maintained by the Silkworm team.

---

### 11. RPC Integration Tests – Gnosis (`qa-rpc-integration-tests-gnosis`)

**Goal:** Same as mainnet RPC integration tests but against a Gnosis reference dataset.

**Database:** Pre-built DB (Gnosis)

**Triggers:** Push to `main`/`release/**`, PRs, manual

**Notes:** Reference datadir is selected dynamically based on the release branch version.

---

### 12. RPC Integration Tests – Polygon (`qa-rpc-integration-tests-polygon`)

**Goal:** Test RPC API correctness on the Bor/Polygon chain, including Bor-specific endpoints.

**Database:** Pre-built DB (Bor-mainnet)

**Triggers:** Manual, workflow_call, push to `main`/`release/3.*`, PRs

**APIs tested:** `bor`, `admin`, `debug`, `eth`, `parity`, `erigon`, `trace`, `web3`, `txpool`, `ots`, `net`

---

### 13. RPC Integration Tests – Remote (`qa-rpc-integration-tests-remote`)

**Goal:** Run RPC integration tests against a locally started Erigon node in archive + commitment-history mode (no
network/downloader).

**Database:** Pre-built DB

**Triggers:** PRs `ready_for_review`, manual

**Notes:** Starts both `erigon` and `rpcdaemon`; no peer discovery, no downloader; full commitment history enabled.

---

### 14. RPC Integration Tests – Latest (`qa-rpc-integration-tests-latest`)

**Goal:** Compare Erigon's RPC responses against a live remote reference node to catch regressions against the canonical
latest state.

**Database:** Pre-built DB (mirrored from reference instance)

**Triggers:** Workflow call, scheduled nightly 00:00 UTC, push to `release/3.*`, manual (with optional
`force_dump_response`)

**Duration:** 30-minute window

**Reference system:** Remote canonical node

**Measures:**

- JSON diff between Erigon and reference for each RPC method
- Optional response dump for debugging

---

## RPC Performance Tests

### 15. RPC Performance Tests (`qa-rpc-performance-tests`)

**Goal:** Benchmark RPCDaemon throughput and latency across 10 RPC methods; detect performance regressions over time.

**Database:** Pre-built DB

**Triggers:** Push to `release/3.*`, daily 03:00 UTC, manual (with `run_geth` option)

**Methods benchmarked:** `eth_call`, `eth_getLogs`, `eth_getBalance`, and 7 others

**Measures:**

- HDR latency histograms per method
- Change-point detection (warnings, non-blocking)
- Binary Vegeta reports saved as artifacts

**Notes:** RPC-tests repo v1.124.0; tests tagged as `erigon` or `geth`.

---

### 16. RPC Performance Comparison Tests (`qa-rpc-performance-comparison-tests`)

**Goal:** Compare Erigon RPC performance against Geth under identical coordinated load, using variable QPS patterns.

**Database:** Pre-built DB

**Triggers:** Daily 02:00 UTC, manual (with `run_geth` flag)

**Measures:**

- Latency at loads from 1 to 20,000 QPS
- HDR latency analysis and distributions
- 5 repetitions per test for statistical stability

**Notes:** Redis-based `coordinated_start.py` synchronises Erigon and Geth startup; Geth comparisons run on Sundays only
by default.

---

## TxPool Tests

### 17. TxPool Performance Test (`qa-txpool-performance-test`)

**Goal:** Measure TxPool throughput and latency in a realistic multi-node environment using Kurtosis + Assertoor.

**Database:** N/A (Docker-based ephemeral network)

**Triggers:** Scheduled Sunday 00:00 UTC, push to `release/3.*`, manual, workflow_call

**Tools:** Kurtosis v1.1.7, Assertoor, HDR histogram analysis, custom Python throughput plots

**Measures:**

- Transaction submission latency (HDR histograms)
- Throughput (transactions/second)
- Assertoor assertion results
- PNG plots uploaded as artifacts

---

## Clean-Exit Tests

### 18. Clean Exit – Block Downloading (`qa-clean-exit-block-downloading`)

**Goal:** Verify Erigon shuts down cleanly (no crash, no data corruption) when interrupted mid-block-download.

**Database:** Saved/restored chaindata (production DB)

**Triggers:** Push to `release/3.*`, daily Mon–Sat 08:00 UTC, manual

**Duration:** 10-minute run (600s), then Ctrl+C

**Measures:**

- Clean exit code and absence of panic/error output
- DB version tracking pre/post shutdown

---

### 19. Clean Exit – Snapshot Downloading (`qa-clean-exit-snapshot-downloading`)

**Goal:** Verify Erigon shuts down cleanly when interrupted during snapshot downloading from a blank state.

**Database:** Blank DB (fresh testbed dir each run)

**Triggers:** Push to `main`/`release/3.*`, PR `ready_for_review`, manual

**Duration:** 10-minute run (600s), then Ctrl+C

**Measures:**

- Clean exit code and absence of panic/error output

---

## Stage Exec Tests

### 20. Stage Exec Smoke Test (`qa-stage-exec`)

**Goal:** Smoke-test the `stage_exec` pipeline by replaying a short block range against a mirrored reference datadir.

**Database:** Mirrored reference datadir

**Triggers:** Manual only

**Duration:** 10-minute timeout (600s)

**Notes:** Reference datadir selected dynamically based on release branch; validates the staged sync execution stage
independently.

---

## Bisection Tools

### 21. RPC Test Bisection Tool (`qa-rpc-test-bisection-tool`)

**Goal:** Use `git bisect` to automatically identify the first commit that caused a specific RPC test to fail.

**Triggers:** Manual only

**Inputs:** `starting_commit`, `ending_commit`, `test_name`

**How it works:** Clones the full git history, generates a dynamic bisect script that runs the named RPC test, and
returns the first bad commit. Exit code 125 skips a commit (e.g. build failure).

---

### 22. Sync Test Bisection Tool (`qa-sync-test-bisection-tool`)

**Goal:** Use `git bisect` to automatically identify the first commit that broke chain synchronisation on a given
network.

**Triggers:** Manual only

**Inputs:** `starting_commit`, `ending_commit`, `chain` (default: mainnet)

**Duration:** Up to 5 days (7200-minute timeout); each commit under test runs an 8-hour sync check in minimal-node mode.

---

## Reporting

### 23. Test Report (`qa-test-report`)

**Goal:** Generate a consolidated markdown/JSON test-result report covering all QA runs within a specified date range.

**Triggers:** Manual only

**Inputs:** `start_date`, `end_date`

**Tools:** Node.js 20, TypeScript `generate-test-report.ts`, GitHub API (read-only token)

---

## Infrastructure Notes

| Concern                | Detail                                                                                      |
|------------------------|---------------------------------------------------------------------------------------------|
| **Runners**            | Self-hosted, labelled by capability: `Ethereum`, `Gnosis`, `Polygon`, `X64`, `long-running` |
| **Reference datadirs** | `/opt/erigon-versions/<version>/`                                                           |
| **Testbed datadirs**   | `/opt/erigon-testbed/`                                                                      |
| **Result upload**      | `upload_test_results.py` – records repo, commit, branch, chain, runner, db_version, outcome |
| **DB lifecycle**       | `pause_production.py` / `resume_production.py` wrap tests that need exclusive DB access     |
| **Artifacts**          | Logs, metric plots (PNG), FD-leak analysis, pprof profiles, HDR histogram reports           |
