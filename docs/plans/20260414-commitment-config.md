# Consolidate Commitment Config into TrieConfig Struct

## Overview
- Consolidate ~27 scattered setter methods, constructor parameters, package-level constants,
  and struct fields in `execution/commitment/` into a single `TrieConfig` struct
- Solves: config threaded through 3+ layers via individual setter calls makes it hard to reason
  about what's configured where and easy to miss settings
- New `config.go` with `TrieConfig` + `DefaultTrieConfig()`, wired into constructors and callers
- Issue: #20553 | Branch: `awskii/commitment-config`

## Context (from discovery)
- **Files/components involved**: `hex_patricia_hashed.go`, `commitment.go`,
  `hex_concurrent_patricia_hashed.go`, `commitmentdb/commitment_context.go`, `metrics.go`,
  `warmuper.go`, `warmup_cache.go`, `verify.go`
- **External callers**: `db/state/execctx/domain_shared.go`, `execution/stagedsync/exec3.go`,
  `db/state/squeeze.go`, `rpc/rpchelper/commitment.go`, `rpc/jsonrpc/eth_simulation.go`,
  `rpc/jsonrpc/receipts/receipts_generator.go`, `db/integrity/commitment_integrity.go`
- **Test files with constructor calls**: ~50+ call sites across `hex_patricia_hashed_test.go`,
  `trie_trace_test.go`, `verify_test.go`, `hex_concurrent_patricia_hashed_test.go`,
  `trie_reader_test.go`, fuzz tests, bench tests, `db/test/aggregator_ext_test.go`
- **Config layers**: SharedDomainsCommitmentContext -> Trie interface -> HexPatriciaHashed -> BranchEncoder
- **Existing pattern**: `WarmupConfig` struct already exists for warmup settings (passed per-Process call)

## Decisions (resolved 2026-04-14)

1. **`EnableWarmupCache` in `Trie` interface** -> Do NOT change. Being reworked in separate task.
2. **`SetDeferBranchUpdates` on SharedDomainsCommitmentContext** -> Keep setter as shim that
   overrides config value. Default: deferred=true for exec, false for RPC callers.
3. **`CsvMetricsFilePrefix`** -> Add to TrieConfig. `NewMetrics()` reads field first, falls back
   to env var if empty.
4. **`memoizationOff`** -> Export as `MemoizationOff bool` in TrieConfig.

## Development Approach
- **Testing approach**: Regular (code first, then tests)
- Complete each task fully before moving to the next
- Make small, focused changes
- **CRITICAL: every task MUST include new/updated tests** for code changes in that task
- **CRITICAL: all tests must pass before starting next task**
- **CRITICAL: update this plan file when scope changes during implementation**
- Maintain backward compatibility via deprecated shims where callers modify config post-construction

## Testing Strategy
- **Unit tests**: required for every task
- Key test commands:
  - `go test ./execution/commitment/... -count=1`
  - `go test ./db/state/... -count=1 -short`
  - `go test ./execution/stagedsync/... -count=1 -short`
- Build verification: `make erigon integration`
- Lint: `make lint` (run repeatedly until clean)

## Progress Tracking
- Mark completed items with `[x]` immediately when done
- Add newly discovered tasks with + prefix
- Document issues/blockers with ! prefix
- Update plan if implementation deviates from original scope

## Solution Overview

### What goes into TrieConfig (static, set-once config)
- `DeferBranchUpdates` (bool, default true) - branch update collection mode
- `LeaveDeferredForCaller` (bool, default false) - deferred update handling
- `MaxDeferredUpdates` (int, default 50,000) - flush threshold
- `EnableWarmupCache` (bool, default false) - warmup cache toggle
- `CsvMetricsFilePrefix` (string, empty=disabled) - CSV metrics output
- `MemoizationOff` (bool, default false) - disable memoized hashes

### What stays as setter methods (runtime/per-call config)
- `SetTrace` / `SetTraceDomain` - toggled per-block in exec3
- `SetCapture` - debug tool, set ad-hoc
- `SetCollapseTracer` - runtime callback for witness generation
- `SetStateReader` / history readers - per-transaction context
- `EnableParaTrieDB` - takes a DB reference, runtime dependency
- `EnableWarmupCache` on Trie interface - unchanged (separate rework)
- `SetDeferBranchUpdates` on SDC - kept as shim overriding config

## Technical Details

### TrieConfig struct
```go
type TrieConfig struct {
    DeferBranchUpdates     bool   // default: true (via DefaultTrieConfig)
    LeaveDeferredForCaller bool   // default: false
    MaxDeferredUpdates     int    // 0 = use default 50,000
    EnableWarmupCache      bool   // default: false
    CsvMetricsFilePrefix   string // empty = check env var
    MemoizationOff         bool   // default: false
}

func DefaultTrieConfig() TrieConfig {
    return TrieConfig{DeferBranchUpdates: true}
}
```

### Constructor chain
```
NewSharedDomainsCommitmentContext(sd, mode, variant, tmpDir, cfg TrieConfig)
  -> InitializeTrieAndUpdates(variant, mode, tmpDir, cfg)
       -> NewHexPatriciaHashed(accountKeyLen, ctx, cfg)
            -> newHexPatriciaHashed() + apply cfg fields
       -> NewConcurrentPatriciaHashed(root, nil) // root carries cfg
```

### Config propagation for sub-tries
`SpawnSubTrie` inherits parent config with `DeferBranchUpdates` forced to false (sub-tries fold
directly, deferred updates would never be applied).

## What Goes Where
- **Implementation Steps**: All code changes, test updates, lint fixes
- **Post-Completion**: CI verification, manual sync testing

## Implementation Steps

### Task 1: Create TrieConfig struct and DefaultTrieConfig

**Files:**
- Create: `execution/commitment/config.go`
- Create: `execution/commitment/config_test.go`

- [ ] Create `execution/commitment/config.go` with `TrieConfig` struct containing 6 fields: `DeferBranchUpdates`, `LeaveDeferredForCaller`, `MaxDeferredUpdates`, `EnableWarmupCache`, `CsvMetricsFilePrefix`, `MemoizationOff`
- [ ] Add `DefaultTrieConfig()` returning production defaults (`DeferBranchUpdates: true`)
- [ ] Add `func (c TrieConfig) maxDeferredUpdatesOrDefault() int` helper (returns 50,000 when 0)
- [ ] Write tests: `TestDefaultTrieConfig` verifies defaults match production values
- [ ] Write tests: `TestTrieConfig_maxDeferredUpdatesOrDefault` verifies zero->50000 fallback and custom value
- [ ] Run `go test ./execution/commitment/... -count=1 -run TestDefaultTrieConfig` - must pass

### Task 2: Wire TrieConfig into HexPatriciaHashed constructor

**Files:**
- Modify: `execution/commitment/hex_patricia_hashed.go`

- [ ] Add `cfg TrieConfig` field to `HexPatriciaHashed` struct (after `collapseTracer`)
- [ ] Change `NewHexPatriciaHashed(accountKeyLen int16, ctx PatriciaContext)` to `NewHexPatriciaHashed(accountKeyLen int16, ctx PatriciaContext, cfg TrieConfig)`
- [ ] In `NewHexPatriciaHashed`: apply `cfg.DeferBranchUpdates` via `branchEncoder.SetDeferUpdates(cfg.DeferBranchUpdates)` (replacing hardcoded `true` in `newHexPatriciaHashed`)
- [ ] Apply `cfg.LeaveDeferredForCaller` -> `hph.leaveDeferredForCaller`
- [ ] Apply `cfg.EnableWarmupCache` -> `hph.enableWarmupCache`
- [ ] Apply `cfg.MemoizationOff` -> `hph.memoizationOff`
- [ ] Apply `cfg.CsvMetricsFilePrefix` -> call `hph.metrics.EnableCsvMetrics(cfg.CsvMetricsFilePrefix)` when non-empty
- [ ] Store `cfg` as `hph.cfg = cfg`
- [ ] In `SpawnSubTrie`: pass parent's `hph.cfg` with `DeferBranchUpdates` forced to `false`
- [ ] In `resetForReuse`: reset config fields from stored `hph.cfg` (not hardcoded values)
- [ ] Update all internal callers in `commitment.go`: `InitializeTrieAndUpdates` lines 155, 170 - add `cfg TrieConfig` param and pass through
- [ ] Update internal caller in `verify.go:120` - pass `DefaultTrieConfig()`
- [ ] Verify build: `go build ./execution/commitment/...`

### Task 3: Update all test files calling NewHexPatriciaHashed

**Files:**
- Modify: `execution/commitment/hex_patricia_hashed_test.go`
- Modify: `execution/commitment/hex_patricia_hashed_fuzz_test.go`
- Modify: `execution/commitment/hex_patricia_hashed_bench_test.go`
- Modify: `execution/commitment/hex_concurrent_patricia_hashed_test.go`
- Modify: `execution/commitment/commitment_bench_test.go`
- Modify: `execution/commitment/trie_trace_test.go`
- Modify: `execution/commitment/trie_reader_test.go`
- Modify: `execution/commitment/verify_test.go`
- Modify: `execution/commitment/config_test.go`
- Modify: `db/test/aggregator_ext_test.go`

- [ ] Add `DefaultTrieConfig()` as third arg to all `NewHexPatriciaHashed` calls in `hex_patricia_hashed_test.go` (~30 call sites)
- [ ] Add `DefaultTrieConfig()` as third arg in `hex_patricia_hashed_fuzz_test.go` (~4 call sites)
- [ ] Add `DefaultTrieConfig()` as third arg in `hex_patricia_hashed_bench_test.go` (~1 call site)
- [ ] Add `DefaultTrieConfig()` as third arg in `hex_concurrent_patricia_hashed_test.go` (~2 call sites)
- [ ] Add `DefaultTrieConfig()` as third arg in `commitment_bench_test.go` (~1 call site)
- [ ] Add `DefaultTrieConfig()` as third arg in `trie_trace_test.go` (~18 call sites)
- [ ] Add `DefaultTrieConfig()` as third arg in `trie_reader_test.go` (~2 call sites)
- [ ] Add `DefaultTrieConfig()` as third arg in `verify_test.go` (~4 call sites)
- [ ] Add `commitment.DefaultTrieConfig()` as third arg in `db/test/aggregator_ext_test.go:82`
- [ ] Add config propagation test to `config_test.go`: create HPH with custom config, verify fields applied
- [ ] Run `go test ./execution/commitment/... -count=1` - must pass
- [ ] Run `go test ./db/test/... -count=1 -run TestAggregator` - must pass

### Task 4: Wire TrieConfig into ConcurrentPatriciaHashed

**Files:**
- Modify: `execution/commitment/hex_concurrent_patricia_hashed.go`

- [ ] Verify `NewConcurrentPatriciaHashed` receives config via `root *HexPatriciaHashed` (which now carries `cfg`)
- [ ] Verify `SpawnSubTrie` in HPH propagates config to sub-tries (done in Task 2)
- [ ] Add config propagation for `EnableWarmupCache`: when `ConcurrentPatriciaHashed.EnableWarmupCache(b)` is called, also update stored config
- [ ] Add config propagation for `EnableCsvMetrics`: when called, propagate to root and mounts
- [ ] Add test in `config_test.go`: create ConcurrentPatriciaHashed, verify config propagates to sub-tries via SpawnSubTrie
- [ ] Run `go test ./execution/commitment/... -count=1` - must pass

### Task 5: Wire TrieConfig into InitializeTrieAndUpdates and NewSharedDomainsCommitmentContext

**Files:**
- Modify: `execution/commitment/commitment.go`
- Modify: `execution/commitment/commitmentdb/commitment_context.go`

- [ ] Update `InitializeTrieAndUpdates` signature: add `cfg TrieConfig` parameter
- [ ] Pass `cfg` to `NewHexPatriciaHashed` calls in both `VariantConcurrentHexPatricia` and default branches
- [ ] Update `NewSharedDomainsCommitmentContext` signature: add `cfg commitment.TrieConfig` parameter
- [ ] Pass `cfg` through to `InitializeTrieAndUpdates`
- [ ] Keep `SetDeferBranchUpdates` setter on SharedDomainsCommitmentContext as shim (overrides config value at runtime)
- [ ] Verify build: `go build ./execution/commitment/...`

### Task 6: Wire TrieConfig into NewMetrics for CsvMetricsFilePrefix

**Files:**
- Modify: `execution/commitment/metrics.go`

- [ ] Add `csvPrefix string` parameter to `NewMetrics` (or have it accept from TrieConfig)
- [ ] In `NewMetrics`: if `csvPrefix` is non-empty, use it; else fall back to env var `ERIGON_COMMITMENT_CSV_METRICS_FILE_PATH_PREFIX`
- [ ] Update `newHexPatriciaHashed` to pass `cfg.CsvMetricsFilePrefix` to `NewMetrics`
- [ ] Run `go test ./execution/commitment/... -count=1` - must pass

### Task 7: Make maxDeferredUpdates configurable

**Files:**
- Modify: `execution/commitment/commitment.go`
- Modify: `execution/commitment/hex_patricia_hashed.go`

- [ ] Remove `const maxDeferredUpdates = 50_000` from `commitment.go`
- [ ] Add `maxDeferredUpdates int` field to `BranchEncoder` struct
- [ ] Set `maxDeferredUpdates` from `cfg.maxDeferredUpdatesOrDefault()` during HPH construction (pass to branchEncoder)
- [ ] Update the flush check in `BranchEncoder` to use the field instead of the const
- [ ] Add test: verify custom `MaxDeferredUpdates` value is respected
- [ ] Run `go test ./execution/commitment/... -count=1` - must pass

### Task 8: Update external callers

**Files:**
- Modify: `db/state/execctx/domain_shared.go`
- Modify: `execution/stagedsync/exec3.go`
- Modify: `db/state/squeeze.go`
- Modify: `rpc/rpchelper/commitment.go`
- Modify: `rpc/jsonrpc/eth_simulation.go`
- Modify: `rpc/jsonrpc/receipts/receipts_generator.go`
- Modify: `db/integrity/commitment_integrity.go`

- [ ] `domain_shared.go:139`: pass `commitment.DefaultTrieConfig()` to `NewSharedDomainsCommitmentContext`
- [ ] `exec3.go`: verify `SetTrace` calls remain (runtime toggle, not config) - no changes needed beyond constructor arg
- [ ] `squeeze.go`: pass `commitment.DefaultTrieConfig()` at commitment context creation sites
- [ ] `rpc/rpchelper/commitment.go`: verify `SetDeferBranchUpdates(false)` still works as shim (no change needed - shim kept in Task 5)
- [ ] `rpc/jsonrpc/eth_simulation.go`: verify `SetDeferCommitmentUpdates` still works (SDC-level, not trie-level - no change)
- [ ] `rpc/jsonrpc/receipts/receipts_generator.go`: verify `SetDeferCommitmentUpdates` and `SetHistoryStateReader` still work (no change)
- [ ] `db/integrity/commitment_integrity.go`: verify `SetTrace` and `SetLimitedHistoryStateReader` still work (no change)
- [ ] Build verification: `make erigon integration`
- [ ] Run `go test ./db/state/... -count=1 -short`

### Task 9: Verify acceptance criteria

- [ ] Verify all config fields from TrieConfig are applied in HexPatriciaHashed
- [ ] Verify config propagates through ConcurrentPatriciaHashed to sub-tries
- [ ] Verify SetDeferBranchUpdates shim on SDC still works for RPC callers
- [ ] Verify CsvMetricsFilePrefix falls back to env var when empty
- [ ] Verify MemoizationOff is respected in computeCellHash
- [ ] Run full commitment test suite: `go test ./execution/commitment/... -count=1`
- [ ] Run affected package tests: `go test ./db/state/... -count=1 -short && go test ./db/test/... -count=1 -short`
- [ ] Run lint: `make lint` (repeat until clean)
- [ ] Build: `make erigon integration`

### Task 10: [Final] Cleanup and move plan

- [ ] Update CLAUDE.md if new patterns discovered
- [ ] Move this plan to `docs/plans/completed/`

## Post-Completion

**Manual verification:**
- Run `erigon` with `--datadir=dev --chain=dev` briefly to confirm startup
- Verify CSV metrics still work with `ERIGON_COMMITMENT_CSV_METRICS_FILE_PATH_PREFIX` env var

**CI verification:**
- All existing CI checks must pass (no new failures)
- The lint check is the most likely to catch issues due to interface changes
