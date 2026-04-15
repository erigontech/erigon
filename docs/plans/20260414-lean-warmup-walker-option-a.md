# Lean Warmup Walker (Option A) for PR #20535

## Overview

PR `awskii/caching-patricia-context-warmup` (commit `b694f6f36d`) replaces the old `WarmupCache` with a read-through `CachingPatriciaContext` and drives warmup workers through `TrieReader.LookupWithVisitor`. Live verification on `arb-dev1` (Sepolia tip, 21 PR-branch samples vs 30 baseline samples on `86f3540fed`) shows:

| stat   | baseline | PR     | delta        |
| ------ | -------- | ------ | ------------ |
| min    | 28 ms    | 32 ms  | +14%         |
| median | 87 ms    | 107 ms | **+23%**     |
| p95    | ~135 ms  | 178 ms | +32%         |
| max    | 150 ms   | 203 ms | **+35%**     |

Variance widening (range 122 → 171 ms) with near-unchanged min points to per-op cost, not a dropped cache: `LookupWithVisitor` does `parseCellAt` + `fillFromFields` + `deriveHashedKeys` (keccak256) + extension match at every level, vs the old bespoke `warmupKey` which only did bitmap arithmetic + `extractBranchCellAddresses`. Warmup workers fall behind on heavy blocks → cache misses → Process pays full DB+parse cost itself.

**Goal:** keep the PR's clean `CachingPatriciaContext` (read-through cache, `LoadOrStore`/`Set` semantics, per-cycle `Reset`). Port the OLD bespoke `warmupKey` depth walk from `main:execution/commitment/warmuper.go` into the `Warmuper` worker loop, but route its reads through `view.Branch()`/`view.Account()`/`view.Storage()` on the wrapped context so entries land in the shared cache. This gets the architectural win (transparent cache) without re-introducing per-descent keccak cost.

## Context (from discovery)

- Files involved:
  - `execution/commitment/warmuper.go` — rewrite worker loop; re-introduce `startDepth`/`MaxDepth`; drop `TrieReader` dependency.
  - `execution/commitment/warmuper_test.go` — update tests for the new walk (depth-bounded, cache-populated).
  - `execution/commitment/commitment.go` — `HashSort` re-plumbs `startDepth` via common-prefix tracking, like old `main`.
  - `execution/commitment/trie_reader.go` — `LookupWithVisitor` / `BranchVisitor` become dead code after the port. Evaluate removal. `TrieReader` itself may be kept as lean primitive.
  - `execution/commitment/trie_reader_test.go` — remove tests for dead visitor path.
  - `execution/commitment/hex_patricia_hashed.go` — contains `skipCellFields` (line 560). **`extractBranchCellAddresses` was removed when the PR switched to `TrieReader`** and must be re-introduced from `origin/main:execution/commitment/hex_patricia_hashed.go:591` (not present on this branch). See Task 2.
  - `docs/plans/completed/20260413-caching-patricia-context-trie-reader-warmup.md` — append a follow-up note pointing to this plan.

- Patterns observed:
  - `maphash.Map` is the cache primitive. `LoadOrStore` protects against stale worker overwrites when Process has already run `Set` via `PutBranch`.
  - Warmup workers run N-way parallel via `errgroup`, pulling keys from a buffered channel.
  - Old `HashSort` in `main` tracked previous hashed key and computed common-prefix length to pass `startDepth` — avoids re-walking already-warm prefixes.

- Only-non-test caller of `LookupWithVisitor` is `warmuper.go:139`. `TrieReader` has no external (non-test) consumers on the PR branch.

## Development Approach

- **Testing approach:** Regular (port code first, update tests after).
- Complete each task fully before moving to the next.
- Make small, focused changes — one commit per task.
- **CRITICAL: every task MUST include new/updated tests** for code changes in that task.
- **CRITICAL: all tests must pass before starting next task** — no exceptions.
- **CRITICAL: update this plan file when scope changes during implementation.**
- Run `make lint` after code changes (non-deterministic — run until clean).
- Maintain backward compatibility of the `Warmuper` public API where sensible; the `WarmKey(hashedKey, startDepth)` signature change is intentional.

## Testing Strategy

- **Unit tests:** required for every task — `warmuper_test.go` covers new walk; commitment tests cover HashSort startDepth plumbing.
- **Integration:** `go test ./execution/commitment/...` must pass after each task.
- **End-to-end:** Sepolia tip run on `arb-dev1` to verify execution-time regression is within ≤5% of baseline median 87 ms. This happens in the final verify task.

## Progress Tracking

- Mark completed items with `[x]` immediately when done.
- Add newly discovered tasks with ➕ prefix.
- Document blockers with ⚠️ prefix.
- Update plan if implementation deviates from scope.

## Solution Overview

Two-part change:

1. **Warmuper walk rewrite.** Replace `TrieReader.LookupWithVisitor` in worker loop with a bespoke depth-walk (ported from `main`): read `view.Branch(prefix)`, use `extractBranchCellAddresses` for the `nextNibble` slot to prefetch plain-key reads via `view.Account`/`view.Storage`, then use `skipCellFields` + varint extension parse to advance `depth` and loop. The walk is bounded by `MaxDepth` (default 128) and starts at `startDepth` supplied by the caller. All reads go through the **cachedView**, so results are stored in the shared `CachingPatriciaContext` exactly like today — just far cheaper per key.

2. **HashSort re-plumbs startDepth.** Track previous `hashedKey`, compute common-prefix nibble count, pass as `startDepth` to `WarmKey` so a worker never re-walks prefixes already warmed by a neighboring key. Matches the old `main` behavior.

Result: warmup is lean (no keccak, single-slot cell decode) and transparent (all writes land in the same `CachingPatriciaContext` that Process reads). Architectural win preserved, CPU cost restored.

## Technical Details

### `WarmupConfig` additions
```go
type WarmupConfig struct {
    Enabled    bool
    CtxFactory TrieContextFactory
    NumWorkers int
    MaxDepth   int      // NEW — default 128 if zero; caps walk depth per key
    LogPrefix  string
    // AccountKeyLen is no longer required (TrieReader path removed)
}
```

### `warmupWorkItem`
```go
type warmupWorkItem struct {
    hashedKey  []byte
    startDepth int
}
```

### `WarmKey` signature
```go
func (w *Warmuper) WarmKey(hashedKey []byte, startDepth int)
```

### Worker loop (pseudo-code)
```go
view := w.cache.Wrap(trieCtx)
for item := range w.work {
    w.warmupKey(view, item.hashedKey, item.startDepth)
    w.keysProcessed.Add(1)
}

func (w *Warmuper) warmupKey(view PatriciaContext, hashedKey []byte, startDepth int) {
    for depth := startDepth; depth < len(hashedKey) && depth < w.maxDepth; {
        prefix := nibbles.HexToCompact(hashedKey[:depth])
        branchData, _, err := view.Branch(prefix) // populates cache via LoadOrStore
        if err != nil || len(branchData) < 4 { return }

        nextNibble := int(hashedKey[depth])
        cellAccounts, cellStorages := extractBranchCellAddresses(branchData, nextNibble)
        for _, a := range cellAccounts  { _, _ = view.Account(a) }
        for _, s := range cellStorages { _, _ = view.Storage(s) }

        bitmap := binary.BigEndian.Uint16(branchData[2:4])
        childBit := uint16(1) << nextNibble
        if bitmap & childBit == 0 { return }

        // Advance past siblings + read fieldBits of target cell
        pos := 4
        for n := 0; n < nextNibble; n++ {
            if bitmap & (1 << n) != 0 {
                fieldBits := branchData[pos]; pos = skipCellFields(branchData, pos+1, fieldBits)
            }
        }
        fieldBits := branchData[pos]; pos++

        // Extension handling — advance depth by extLen if present, else by 1
        if fieldBits & 1 != 0 {
            if extLen, n := binary.Uvarint(branchData[pos:]); n > 0 && extLen > 0 {
                depth += int(extLen)
                continue
            }
        }
        depth++
    }
}
```

### HashSort startDepth plumbing
```go
var prevHK []byte
...
commonNibbles := commonPrefixNibbles(prevHK, hk) // 0 if prevHK nil
if warmuper != nil { warmuper.WarmKey(hk, commonNibbles) }
prevHK = hk // kept alive in arena
```

### Cache hit-rate at Info level
Promote `Process()`'s existing `log.Debug("commitment cache stats", ...)` to `log.Info(...)` gated by a per-cycle threshold (emit only if block produced >=N commitment updates, to avoid log spam on empty cycles).

## What Goes Where

- **Implementation Steps** (`[ ]` checkboxes): code, tests, docs updates in this repo.
- **Post-Completion** (no checkboxes): Sepolia tip measurement run, PR body update.

## Implementation Steps

### Task 1: Add `startDepth` and `MaxDepth` plumbing; restore `warmupWorkItem`

**Files:**
- Modify: `execution/commitment/warmuper.go`

- [x] add `MaxDepth int` field to `WarmupConfig`; default to 128 in `NewWarmuper` when zero
- [x] define `warmupWorkItem { hashedKey []byte; startDepth int }` and change `work` channel type
- [x] change `WarmKey(hashedKey []byte)` → `WarmKey(hashedKey []byte, startDepth int)`; keep the copy-into-arena behavior
- [x] leave worker loop temporarily consuming `item.hashedKey` only (no behavior change yet — TrieReader path stays until Task 2)
- [x] update `warmuper_test.go` call sites for new `WarmKey` signature (minimal edit — keep tests green)
- [x] run `go test ./execution/commitment/...` — must pass before next task

### Task 2: Rewrite worker loop with bespoke depth-walk

**Files:**
- Modify: `execution/commitment/warmuper.go`
- Modify: `execution/commitment/hex_patricia_hashed.go`

- [x] re-introduce `extractBranchCellAddresses` helper in `hex_patricia_hashed.go` by porting the definition from `origin/main:execution/commitment/hex_patricia_hashed.go` (starting ~line 587). It was removed from this branch when the PR switched to `TrieReader`; we need it again for the bespoke walk. Place it near `skipCellFields`. Include its unit tests if main has any.
- [x] add private `warmupKey(view PatriciaContext, hashedKey []byte, startDepth int)` method implementing the bespoke walk (see Technical Details); reuse `extractBranchCellAddresses` / `skipCellFields` from `hex_patricia_hashed.go`, import `nibbles` and `encoding/binary`
- [x] replace worker-goroutine body: remove `NewTrieReader` / `visitor` / `LookupWithVisitor`; call `w.warmupKey(view, item.hashedKey, item.startDepth)` instead
- [x] drop `AccountKeyLen` field from `WarmupConfig` and `Warmuper` (and caller in `Process()` setup); remove `accountKeyLen` parameter uses
- [x] update `warmuper_test.go`: add tests asserting the walk populates `CachingPatriciaContext` branches/accounts/storage for representative trees; test depth bound via `MaxDepth`; test early termination when bitmap lacks child bit
- [x] test error path: underlying `Branch()` returns error — walk returns without crashing
- [x] run `go test ./execution/commitment/...` — must pass before next task

### Task 3: Plumb `startDepth` through `HashSort`

**Files:**
- Modify: `execution/commitment/commitment.go`

- [ ] add helper `commonPrefixNibbles(a, b []byte) int` (internal, file-local)
- [ ] in `HashSort` both `ModeDirect` and `ModeUpdate` branches: track `prevHK []byte`; compute `startDepth := commonPrefixNibbles(prevHK, hk)`; pass to `warmuper.WarmKey(hk, startDepth)`; update `prevHK = hk` after enqueue
- [ ] write unit test for `commonPrefixNibbles` covering: nil prev, identical, diverging-at-0, diverging-midway, differing lengths
- [ ] write/update integration test confirming `HashSort` drives `Warmuper.WarmKey` with non-zero `startDepth` when consecutive keys share a prefix (use a fake warmuper or verify via mock)
- [ ] run `go test ./execution/commitment/...` — must pass before next task

### Task 4: Remove dead `LookupWithVisitor` / `BranchVisitor`

**Files:**
- Modify: `execution/commitment/trie_reader.go`
- Modify: `execution/commitment/trie_reader_test.go`

- [ ] confirm no non-test callers exist: `git grep -n "LookupWithVisitor\|BranchVisitor"` has only `trie_reader.go` itself (plus this file's tests) after Task 2
- [ ] delete `BranchVisitor` type and `LookupWithVisitor` method; make `Lookup` free-standing (inline what it needs from the old `LookupWithVisitor` body, since it was just a `LookupWithVisitor(..., nil)` wrapper)
- [ ] if `TrieReader` has no remaining callers after this, mark `TrieReader` and `parseCellAt` as deletion candidates — remove if nothing else depends on them; if there are still test-only callers (Lookup), keep them for now
- [ ] drop tests for the visitor path in `trie_reader_test.go`; keep `Lookup` tests if `Lookup` survives
- [ ] run `go test ./execution/commitment/...` — must pass before next task

### Task 5: Promote cache-stats emit to Info level

**Files:**
- Modify: `execution/commitment/hex_patricia_hashed.go`

- [ ] in `Process()` after `warmuper.DrainPending()`, add `log.Info` with warmup cache stats (hit/miss counts, hit-rate percentage, keys processed, warmup duration) — gated on `warmuper != nil` and `KeysProcessed > 0` to avoid spam on empty cycles
- [ ] reset cache hit counters in `Metrics.Reset()` so hit% is accurate per cycle, not cumulative
- [ ] format uses structured log fields with hit-rate percentage (hit%, cb, ca, cs counters)
- [ ] add `TestWarmuper_StatsPopulatedAfterCycle` in `warmuper_test.go` asserting `Stats()` is populated after a warmup cycle
- [ ] run `go test ./execution/commitment/...` — must pass before next task

### Task 6: Verify acceptance criteria

- [ ] `make lint` — run repeatedly until clean (linter is non-deterministic)
- [ ] `make erigon integration` — both binaries build
- [ ] `go test ./execution/commitment/... ./db/... ./execution/stagedsync/...` — full local pass
- [ ] rebuild on `arb-dev1`; restart Sepolia-tip run with same command as prior test; collect ≥20 `head validated` samples
- [ ] compute median/p95/max execution time from `head validated` lines; record in this plan file under "Results"
- [ ] verify median within ≤5% of baseline 87 ms (target: ≤91 ms median) and p95 within ≤10% (target: ≤150 ms)
- [ ] capture one `commitment cache stats` Info line; verify hit-rate on heavy block is ≥ PR-branch observed rate; record both numbers here
- [ ] update PR body test-plan to reflect lean-walk re-introduction

### Task 7: Final — update docs and move plan

**Files:**
- Modify: `docs/plans/completed/20260413-caching-patricia-context-trie-reader-warmup.md`
- Move: `docs/plans/20260414-lean-warmup-walker-option-a.md` → `docs/plans/completed/`

- [ ] append to the `20260413-...md` plan a "Follow-up" section referencing this plan's filename and the observed 23–35% regression it addresses
- [ ] move this plan to `docs/plans/completed/`
- [ ] commit: `docs: move lean warmup walker plan to completed`

## Post-Completion

**Manual verification (Sepolia tip, `arb-dev1`):**
- 20+ `head validated` sample collection is scripted via the existing tmux session-0 setup.
- Compare PR-branch medians pre-fix and post-fix to the `86f3540fed` baseline recorded in this plan.
- If regression remains >5% after this plan, the follow-up knobs live outside Option A: `NumWorkers` tuning, channel buffer size, or revisiting the warmup-vs-process race pacing.

**PR body update:**
- Add a "Perf verification" section to PR #20535 with the before/after numbers.
- Note that `TrieReader.LookupWithVisitor` was removed in favor of the bespoke walk.
