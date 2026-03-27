# Integration Test for Concurrent Commitment Rebuild

## Overview
Add an integration test that validates the concurrent commitment rebuild pipeline end-to-end:
generate deterministic data → record baseline state root → rebuild sequentially (ground truth) → rebuild concurrently (primary target) → compare roots and log file sizes + timing.

This proves that `ERIGON_REBUILD_CONCURRENT_COMMITMENT=true` produces identical state roots to both the original computation and the sequential rebuild, catching regressions in the parallel trie implementation.

## Context (from discovery)
- **Files involved:**
  - `db/state/squeeze_test.go` — existing helpers (`makeAccountAddr`, `makeStorageKey`, `makeCodeValue`, `testDbAndAggregatorForLargeData`)
  - `db/state/squeeze.go` — `RebuildCommitmentFiles()` (line 820), `ERIGON_REBUILD_CONCURRENT_COMMITMENT` env var (line 935)
  - `execution/commitment/commitmentdb/commitment_context.go` — `concurrentTrieContextFactory`, `EnableParaTrieDB`
- **Related tests:**
  - `TestGenerateCommitmentRebuildData` — data generation pattern (squeeze_test.go:547)
  - `TestAggregator_RebuildCommitmentBasedOnFiles` — wipe + rebuild pattern (squeeze_test.go:266)
- **Env var mechanics:** `dbg.EnvBool("ERIGON_REBUILD_CONCURRENT_COMMITMENT", false)` checks `os.LookupEnv` directly, so `t.Setenv()` works

## Development Approach
- **Testing approach**: Regular (this IS the test)
- Complete each task fully before moving to the next
- Reuse existing helpers — do not duplicate `makeAccountAddr` etc.
- The test file is `db/state/squeeze_concurrent_rebuild_test.go` (package `state_test`)
- **Assertions:** Root hash equality is a hard failure. File sizes are logged, not asserted.
- **Sequential rebuild:** Runs first as ground truth. Root mismatch = hard failure (something is wrong with data generation or rebuild itself).
- **Concurrent rebuild:** Runs second. Root mismatch against baseline = hard failure (concurrent bug). Timing/size comparison against sequential is logged.

## Progress Tracking
- Mark completed items with `[x]` immediately when done
- Add newly discovered tasks with ➕ prefix
- Document issues/blockers with ⚠️ prefix

## Implementation Steps

### Task 1: Create test file with helper infrastructure

**Files:**
- Create: `db/state/squeeze_concurrent_rebuild_test.go`

- [x] Create file with package `state_test`, imports matching squeeze_test.go patterns
- [x] Add `type rebuildResult` struct to hold: `root []byte`, `duration time.Duration`, `fileSizes map[string]int64`
- [x] Add `envIntOr(key string, def uint64) uint64` helper that reads `os.Getenv` + `strconv.ParseUint` with default fallback (for TEST_ACCOUNTS, TEST_STEPS, etc.)
- [x] Add `collectCommitmentFiles(dirs datadir.Dirs) map[string]int64` helper that uses `dir.ListFiles(dirs.SnapDomain, ".kv")` filtered by `kv.CommitmentDomain.String()`, returning filename→size map
- [x] Add `wipeCommitment(t *testing.T, db kv.TemporalRwDB, agg *state.Aggregator, dirs datadir.Dirs)` helper that:
  - Opens RwTx, lists tables, clears commitment tables, commits
  - Lists commitment `.kv` files via `collectCommitmentFiles` and deletes them (plus `.kvi`, `.kvei`, `.bt` siblings)
  - Calls `agg.OpenFolder()` to rescan
- [x] Add `logComparison(t *testing.T, baseline, sequential, concurrent rebuildResult, originalSizes map[string]int64)` helper that logs a summary table via `t.Logf`

### Task 2: Implement Phase 1 — data generation

**Files:**
- Modify: `db/state/squeeze_concurrent_rebuild_test.go`

- [x] Add `TestConcurrentRebuildCommitment(t *testing.T)` with `testing.Short()` skip
- [x] Read env parameters: `TEST_ACCOUNTS` (default 10000), `TEST_STEPS` (default 5), `TEST_SLOTS_PER_ACCT` (default 2), `TEST_CODE_ACCOUNTS` (default 3000), `TEST_STEP_SIZE` (default 10), `TEST_DATADIR` (optional persistent dir)
- [x] Create aggregator via `testDbAndAggregatorForLargeData(t, stepSize, persistentDir)` — returns `db, agg, dirs`
- [x] Open `BeginTemporalRwTx`, create `SharedDomains`
- [x] Implement data generation loop (reuse `makeAccountAddr`, `makeStorageKey`, `makeCodeValue`):
  - Write accounts, storage, code in batches per txNum
  - At step boundaries: `ComputeCommitment` → record root, `Flush`, `rawdbv3.TxNums.Append`
- [x] After loop: close SharedDomains, commit tx
- [x] Call `agg.BuildFiles(totalTxs)` to create snapshot files
- [x] Record `baselineRoot` (last root from generation) and `originalSizes` via `collectCommitmentFiles`
- [x] Extract root from files via `ac.DebugGetLatestFromFiles` + `commitment.HexTrieExtractStateRoot` to double-check baseline matches
- [x] Log generation stats: accounts, storage keys, code accounts, steps, total txs, time

### Task 3: Implement Phase 2 — sequential rebuild (ground truth)

**Files:**
- Modify: `db/state/squeeze_concurrent_rebuild_test.go`

- [x] Close aggregator, reopen via `testAgg` + `temporal.New` (following pattern from `TestAggregator_RebuildCommitmentBasedOnFiles`)
- [x] Call `wipeCommitment(t, db, agg, dirs)` to delete all commitment state
- [x] Record `start := time.Now()`
- [x] Call `state.RebuildCommitmentFiles(ctx, db, &rawdbv3.TxNums, log.New(), true)` — sequential (env var not set)
- [x] Record sequential `rebuildResult`: root, duration, file sizes
- [x] `require.Equal(t, baselineRoot, sequentialResult.root)` — hard failure if sequential doesn't match baseline
- [x] Log sequential rebuild stats

### Task 4: Implement Phase 3 — concurrent rebuild (primary target)

**Files:**
- Modify: `db/state/squeeze_concurrent_rebuild_test.go`

- [x] Close aggregator, reopen again (fresh state for concurrent run)
- [x] Call `wipeCommitment(t, db, agg, dirs)` to delete all commitment state
- [x] `t.Setenv("ERIGON_REBUILD_CONCURRENT_COMMITMENT", "true")` to enable concurrent mode
- [x] Record `start := time.Now()`
- [x] Call `state.RebuildCommitmentFiles(ctx, db, &rawdbv3.TxNums, log.New(), true)`
- [x] Record concurrent `rebuildResult`: root, duration, file sizes
- [x] `require.Equal(t, baselineRoot, concurrentResult.root)` — hard failure if concurrent doesn't match
- [x] Log concurrent rebuild stats

### Task 5: Implement Phase 4 — comparison report

**Files:**
- Modify: `db/state/squeeze_concurrent_rebuild_test.go`

- [x] Call `logComparison` to print summary table:
  ```
  === Rebuild Comparison ===
  Sequential: root=<hex> time=<dur> files=<count> totalSize=<bytes>
  Concurrent: root=<hex> time=<dur> files=<count> totalSize=<bytes>
  Root match: sequential=<bool> concurrent=<bool>
  Speedup: <ratio>x
  Size delta: <bytes> (<percent>%)
  ```
- [x] Log per-file size comparison (original vs sequential vs concurrent)

### Task 6: Smoke test and lint

- [x] Run `go build ./db/state/...` to verify compilation
- [x] Run `go test -run TestConcurrentRebuildCommitment -count=1 -v ./db/state/...` with small params (default env)
- [x] Run `make lint` and fix any issues
- [x] Verify test passes with `ERIGON_REBUILD_CONCURRENT_COMMITMENT` properly toggled between phases

### Task 7: Verify acceptance criteria

- [ ] Verify: test generates data with env-parameterized sizes
- [ ] Verify: sequential rebuild produces root matching baseline (asserted)
- [ ] Verify: concurrent rebuild produces root matching baseline (asserted)
- [ ] Verify: file sizes are logged (not asserted)
- [ ] Verify: timing is logged for both paths
- [ ] Verify: test runs in under 2 minutes with default (medium) params
- [ ] Run full test suite: `go test -short ./db/state/...`

## Technical Details

### Data Generation Parameters
| Env Var | Default | Large (TEST_LARGE=true) |
|---------|---------|------------------------|
| TEST_ACCOUNTS | 10000 | 3000000 |
| TEST_STEPS | 5 | 59 |
| TEST_SLOTS_PER_ACCT | 2 | 2 |
| TEST_CODE_ACCOUNTS | 3000 | 1000000 |
| TEST_STEP_SIZE | 10 | 100 |

### Wipe Procedure
1. Open RwTx → `ListTables()` → clear tables containing "commitment" → commit
2. `dir.ListFiles(dirs.SnapDomain, ".kv")` → filter by "commitment" → `dir.RemoveFile` each + siblings (`.kvi`, `.kvei`, `.bt`)
3. `agg.OpenFolder()` to rescan file state

### Env Var Toggle
- Sequential: no env var set (default `false`)
- Concurrent: `t.Setenv("ERIGON_REBUILD_CONCURRENT_COMMITMENT", "true")` before rebuild call
- `t.Setenv` auto-restores on test cleanup

### Aggregator Lifecycle
Between sequential and concurrent phases, the aggregator must be closed and reopened to ensure clean file state. Pattern:
```go
agg.Close()
agg = testAgg(t, db, agg.Dirs(), stepSize, log.New())
db, err = temporal.New(db, agg)
```

## Post-Completion

**Manual verification:**
- Run with `TEST_LARGE=true` on a machine with 32GB+ RAM to validate at scale
- Run with `TEST_DATADIR=/path/to/dir` to create a persistent dataset for `erigon integration commitment rebuild` testing
- Compare timing across different `GOMAXPROCS` values to verify concurrent speedup
