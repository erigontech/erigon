# TrieConfig Deep Refactor — Implementation Plan

## Context

PR #20553 consolidated scattered config into TrieConfig. This refactor completes the job:
TrieConfig becomes the **single source of truth** for all trie construction and behavior.
No post-construction config setters. TrieVariant moves into TrieConfig. Callers pass explicit
config at construction time.

**Branch**: `awskii/commitment-config` (on top of commit `6edda37a5c`)

---

## Step 1: Expand TrieConfig struct (additive, non-breaking)

**File**: `execution/commitment/config.go`

Add three fields:
```go
type TrieConfig struct {
    Variant            TrieVariant // NEW — selects trie implementation
    DeferBranchUpdates     bool
    LeaveDeferredForCaller bool
    MaxDeferredUpdates     int
    EnableWarmupCache      bool
    EnableTrieWarmup       bool       // NEW — parallel MDBX page-cache warmup
    CsvMetricsFilePrefix   string
    MemoizationOff         bool
    SubtrieConfig          *TrieConfig // NEW — config for ConcurrentPH sub-tries; nil = derive
}
```

Update `DefaultTrieConfig()`:
```go
func DefaultTrieConfig() TrieConfig {
    return TrieConfig{
        Variant:            VariantHexPatriciaTrie,
        DeferBranchUpdates: true,
        EnableTrieWarmup:   true,
        EnableWarmupCache:  true,
    }
}
```

---

## Step 2: Update internal constructors to consume new fields

### 2a: `InitializeTrieAndUpdates` — remove `tv` param
**File**: `execution/commitment/commitment.go:152`

```go
// OLD: func InitializeTrieAndUpdates(tv TrieVariant, mode Mode, tmpdir string, cfg TrieConfig)
// NEW: func InitializeTrieAndUpdates(mode Mode, tmpdir string, cfg TrieConfig)
//      switch on cfg.Variant instead of tv
```

### 2b: `SpawnSubTrie` — use SubtrieConfig
**File**: `execution/commitment/hex_patricia_hashed.go:126-135`

```go
func (hph *HexPatriciaHashed) SpawnSubTrie(ctx PatriciaContext, forNibble int) *HexPatriciaHashed {
    var subCfg TrieConfig
    if hph.cfg.SubtrieConfig != nil {
        subCfg = *hph.cfg.SubtrieConfig
    } else {
        subCfg = hph.cfg
        subCfg.DeferBranchUpdates = false
    }
    subCfg.SubtrieConfig = nil // no further nesting
    subTrie := NewHexPatriciaHashed(hph.accountKeyLen, ctx, subCfg)
    subTrie.mountTo(hph, forNibble)
    return subTrie
}
```

### 2c: `NewSharedDomainsCommitmentContext` — remove `trieVariant` param, read `trieWarmup` from cfg
**File**: `execution/commitment/commitmentdb/commitment_context.go:170`

```go
// OLD: func NewSharedDomainsCommitmentContext(sd sd, mode commitment.Mode, trieVariant commitment.TrieVariant, tmpDir string, cfg commitment.TrieConfig)
// NEW: func NewSharedDomainsCommitmentContext(sd sd, mode commitment.Mode, tmpDir string, cfg commitment.TrieConfig)
//      set ctx.trieWarmup = cfg.EnableTrieWarmup
//      call InitializeTrieAndUpdates(mode, tmpDir, cfg) — no trieVariant arg
```

---

## Step 3: Change `NewSharedDomains` signature

**File**: `db/state/execctx/domain_shared.go`

```go
// NEW single constructor — replaces both NewSharedDomains and NewSharedDomainsWithTrieVariant
func NewSharedDomains(ctx context.Context, tx kv.TemporalTx, logger log.Logger, cfg commitment.TrieConfig) (*SharedDomains, error) {
    sd := &SharedDomains{...}
    sd.mem = tx.Debug().NewMemBatch(&sd.metrics)
    sd.sdCtx = commitmentdb.NewSharedDomainsCommitmentContext(sd, commitment.ModeDirect, tx.Debug().Dirs().Tmp, cfg)
    // ...
}
```

Delete `NewSharedDomainsWithTrieVariant` entirely.

---

## Step 4: Update all callers (by category)

### 4a: exec3 / stageloop callers — DefaultTrieConfig()
**Files**:
- `execution/stagedsync/stageloop/stageloop.go:154, 249`
- `execution/stagedsync/stage_headers.go:268`
- `execution/stagedsync/stage_custom_trace.go:230`

Pattern:
```go
// OLD: doms, err := execctx.NewSharedDomains(ctx, tx, logger)
// NEW:
cfg := commitment.DefaultTrieConfig()
if statecfg.ExperimentalConcurrentCommitment {
    cfg.Variant = commitment.VariantConcurrentHexPatricia
}
doms, err := execctx.NewSharedDomains(ctx, tx, logger, cfg)
```

Remove post-construction setter calls in `exec3.go:200-202`:
```go
// DELETE these lines:
// doms.EnableTrieWarmup(true)
// doms.EnableWarmupCache(true)
// (EnableParaTrieDB stays — excluded from refactor)
```

### 4b: RPC callers — DeferBranchUpdates=false
**Files** (8 call sites):
- `rpc/jsonrpc/eth_call.go:463, 714`
- `rpc/jsonrpc/receipts/receipts_generator.go:300, 507`
- `rpc/jsonrpc/debug_execution_witness.go:709, 905`
- `rpc/jsonrpc/eth_simulation.go:158`
- `rpc/rpchelper/commitment.go:89`

Pattern:
```go
// OLD:
domains, err := execctx.NewSharedDomains(ctx, tx, log.New())
sdCtx.SetDeferBranchUpdates(false)

// NEW:
cfg := commitment.TrieConfig{Variant: commitment.VariantHexPatriciaTrie, DeferBranchUpdates: false}
domains, err := execctx.NewSharedDomains(ctx, tx, log.New(), cfg)
// DELETE SetDeferBranchUpdates call
```

### 4c: Builder — DeferBranchUpdates=false
**Files**:
- `execution/builder/builder.go:148`
- `execution/builder/exec.go:117, 132`

Same pattern as RPC. Delete `SetDeferBranchUpdates(false)` call.

### 4d: Integrity checker
**File**: `db/integrity/commitment_integrity.go:205, 870, 937, 1003`

Site at 870 calls `SetDeferBranchUpdates(false)`. Pass config with `DeferBranchUpdates: false`.
Other sites use default — pass `DefaultTrieConfig()`.

### 4e: Squeeze / RebuildCommitment
**File**: `db/state/squeeze.go:331, 479, 586, 971`

```go
// OLD (line 971):
domains, err := execctx.NewSharedDomainsWithTrieVariant(ctx, rwTx, log.New(), trieVariant)
domains.EnableTrieWarmup(true)
domains.EnableWarmupCache(useWarmupCache)

// NEW:
cfg := commitment.TrieConfig{
    Variant:           trieVariant,
    EnableTrieWarmup:  true,
    EnableWarmupCache: useWarmupCache,
}
domains, err := execctx.NewSharedDomains(ctx, rwTx, log.New(), cfg)
```

Other squeeze sites (331, 479, 586): pass `DefaultTrieConfig()`, remove warmup setter calls.

### 4f: Backtester
**File**: `execution/commitment/backtester/backtester.go:203, 210, 220`

```go
// OLD:
statecfg.ExperimentalConcurrentCommitment = true
sd, _ := execctx.NewSharedDomains(...)
sd.EnableTrieWarmup(true)
sd.GetCommitmentCtx().EnableCsvMetrics(deriveBlockMetricsFilePrefix(...))

// NEW:
cfg := commitment.DefaultTrieConfig()
cfg.Variant = commitment.VariantConcurrentHexPatricia
cfg.CsvMetricsFilePrefix = deriveBlockMetricsFilePrefix(...)
sd, _ := execctx.NewSharedDomains(..., cfg)
```

### 4g: execmodule callers
**Files**:
- `execution/execmodule/exec_module.go:471`
- `execution/execmodule/set_head.go:100`
- `execution/execmodule/forkchoice.go:206`
- `execution/execmodule/inserters.go:50`

Pass `DefaultTrieConfig()` (+ concurrent variant check where applicable).

### 4h: cmd/ callers
**Files**:
- `cmd/evm/runner.go:194`
- `cmd/evm/internal/t8ntool/transition.go:314, 656`
- `cmd/integration/commands/commitment.go:216, 614`
- `cmd/integration/commands/state_stages.go:167, 379`
- `cmd/integration/commands/stages.go:386, 725, 743`
- `cmd/integration/commands/state_domains.go:537`

Pass `DefaultTrieConfig()` (+ concurrent variant check where integration commands use flags).

### 4i: Test callers (~50+ sites)
**Files**: various `*_test.go` across `db/test/`, `db/state/`, `execution/`, `txnprovider/`

Mechanical: append `, commitment.DefaultTrieConfig()` to every `NewSharedDomains(ctx, tx, logger)` call.

Fix `execution/commitment/hex_patricia_hashed_test.go:843`:
```go
// OLD: trieDeferred.SetDeferBranchUpdates(true)
// NEW: construct with TrieConfig{DeferBranchUpdates: true, ...}
```

---

## Step 5: Remove now-unused setters

### 5a: SharedDomainsCommitmentContext — delete setters
**File**: `execution/commitment/commitmentdb/commitment_context.go`

Delete these methods:
- `SetDeferBranchUpdates(bool)` (line 78-82)
- `EnableTrieWarmup(bool)` (line 70-72)
- `EnableWarmupCache(bool)` (line 153-156) — the one on SharedDomainsCommitmentContext only; HPH's own `EnableWarmupCache` stays for internal use in ComputeCommitment recording toggle
- `EnableCsvMetrics(string)` (line 166-168)

### 5b: SharedDomains — delete wrapper methods
**File**: `db/state/execctx/domain_shared.go`

Delete these wrapper methods:
- `EnableTrieWarmup(bool)` (line 761-763)
- `EnableWarmupCache(bool)` (line 769-771)

Keep: `EnableParaTrieDB` (excluded), `SetDeferCommitmentUpdates` (runtime), `ClearWarmupCache` (operational), `SetTrace` (runtime).

### 5c: HexPatriciaHashed — delete SetDeferBranchUpdates
**File**: `execution/commitment/hex_patricia_hashed.go:2969-2973`

Delete `SetDeferBranchUpdates(defer_ bool)` method.

---

## Step 6: Unexport BranchEncoder.SetDeferUpdates

**File**: `execution/commitment/commitment.go:350-362`

Rename `SetDeferUpdates` → `setDeferUpdates`. Update the single caller in `applyConfig` (hex_patricia_hashed.go:155).

---

## Verification

1. `make erigon integration` — builds main + integration binaries
2. `make lint` — run repeatedly until clean (non-deterministic)
3. `make test-short` — quick unit tests
4. Grep for removed methods to confirm no stale references:
   ```
   grep -r 'SetDeferBranchUpdates\|NewSharedDomainsWithTrieVariant' --include='*.go'
   grep -r '\.EnableTrieWarmup\|\.EnableWarmupCache\|\.EnableCsvMetrics' --include='*.go'
   ```
   (should only find internal/retained usages)

---

## Files Modified (summary)

| File | Changes |
|------|---------|
| `execution/commitment/config.go` | Add Variant, EnableTrieWarmup, SubtrieConfig fields; update DefaultTrieConfig |
| `execution/commitment/commitment.go` | InitializeTrieAndUpdates loses tv param; unexport setDeferUpdates |
| `execution/commitment/hex_patricia_hashed.go` | SpawnSubTrie uses SubtrieConfig; delete SetDeferBranchUpdates |
| `execution/commitment/commitmentdb/commitment_context.go` | Constructor loses trieVariant param; delete 4 setter methods |
| `db/state/execctx/domain_shared.go` | NewSharedDomains takes cfg; delete WithTrieVariant + wrapper setters |
| `execution/stagedsync/exec3.go` | Remove warmup setter calls |
| `execution/builder/builder.go`, `exec.go` | Pass explicit config, remove SetDeferBranchUpdates |
| `rpc/jsonrpc/eth_call.go` | Pass config with DeferBranchUpdates=false |
| `rpc/jsonrpc/receipts/receipts_generator.go` | Same |
| `rpc/jsonrpc/debug_execution_witness.go` | Same |
| `rpc/jsonrpc/eth_simulation.go` | Same |
| `rpc/rpchelper/commitment.go` | Same |
| `db/integrity/commitment_integrity.go` | Pass explicit config |
| `db/state/squeeze.go` | Pass config with variant + warmup |
| `execution/commitment/backtester/backtester.go` | Pass config with CsvMetricsFilePrefix |
| `execution/execmodule/*.go` | Pass DefaultTrieConfig() |
| `cmd/evm/*.go`, `cmd/integration/commands/*.go` | Pass DefaultTrieConfig() |
| ~50 test files | Append `commitment.DefaultTrieConfig()` arg |
