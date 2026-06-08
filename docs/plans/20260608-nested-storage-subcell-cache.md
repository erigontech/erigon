# Nested storage sub-cell cache (incremental whale re-fold)

## Overview

Make a big-storage account's re-fold **incremental**: cache its 16 depth-65
storage-child cells and, when a re-fold is triggered, re-fold only the storage
nibbles whose slots changed, reuse the cached cells for the rest, and re-aggregate
the account leaf — instead of re-folding the account's entire storage every time.

Today both streaming fold paths re-fold the **whole** top-nibble split (including a
whale account's full storage) on every re-fold. A prototype
(`nested_storage_prototype_test.go`, committed `f72a94a197`) proved the incremental
form is correct (root identical) and **~13.4×** faster on a 750k whale when a block
touches one storage nibble (656 ms → 49 ms).

**Scope reality (read first):** this is a **bulk-commitment** win — it helps when
>10k storage slots of one account are touched in a single commitment (initial
state build, snapshot squeeze, a mass-storage tx). It does **not** change
steady-state live throughput: on the live mainnet frontier the max keys/commit is
~8.9k across *all* accounts, the deep fan-out never triggers, and commitment is
~10 ms/block. So this must be measured on a **bulk path**, not the live frontier.

Builds on: streaming committer (`docs/plans/completed/20260607-streaming-commitment.md`),
the doubling gate (`e91672aef9`), the prototype (`f72a94a197`).

## Context (from discovery)

Worktree/branch: `/Users/awskii/org/wrk/erigon-prepare-fold` @ `awskii/parallel_prepare_fold`.

Files/components:
- `execution/commitment/streaming_commitment.go` — `StreamingCommitter`, `splitState`,
  `TouchKey`, `foldSplitBg`→`foldKeys` (background single-stream, gate-bounded),
  `foldDirtySplits`→`foldPresentSplits`→`foldSplit`→`sc.pph.dfsSubtreeDeep`
  (Process deep fan-out), `stitchSplitCells`, `shouldEagerFold`/`SetEagerFold`,
  `Reset`, the `overlayContext` (self-flush detection via `flushed`).
- `execution/commitment/parallel_mount.go` — `concurrentStorageRoot` (per-nibble
  `foldChildAt` + assemble → storageRoot; **re-folds all 16 nibbles fresh**),
  `setAccountStorageRoot` (inject storageRoot into account leaf via `cell.hash`),
  `dfsSubtreeDeep` (deep trigger: `plainKey!=nil && len(path)==64 &&
  OnesCount16(bitmap)>=2 && subtreeCount>deepStorageThreshold=10000`).
- `execution/commitment/deep_storage_concurrent_bench_test.go` — `foldChildAt`,
  `whaleByNibble`, `storKV` (reused by the prototype).
- `execution/commitment/nested_storage_prototype_test.go` — the proven incremental
  mechanism (`foldAllChildren`, `assembleAccountFromChildren`, `extraSlotsInNibble`)
  + `TestNestedStorage_IncrementalParity` + `Benchmark_NestedStorageRefold`.
- `execution/commitment/hex_patricia_hashed.go` — `computeCellHash` (account uses
  `cell.hash` as storageRoot, :1239), `mountTo`, `foldMounted`.
- `execution/commitment/wide_nested_parallel_test.go` — `requireIncrementalEquiv`
  (+ its `streaming` arm), `branchDiff`, `sparseBatch2`.
- Bulk paths for measurement: `db/state/squeeze.go` (rebuild/squeeze,
  `PickTrieVariant`), `cmd/integration` `stage_exec`.

Related patterns / dependencies:
- `setAccountStorageRoot` already injects a storageRoot into an account leaf; the
  cache supplies that storageRoot incrementally instead of a fresh full fold.
- The per-split doubling gate (`shouldEagerFold`) is the template for the per-nibble
  gate inside the cache.
- The prev-invariant (nothing flushed mid-block → stable on-disk pre-image) is what
  makes reusing a clean nibble's cached cell sound — same reasoning as the gate.

## Development Approach

- **Testing approach: TDD.** Every behavior change starts with a failing
  root+branch parity test, then the cache logic to make it green.
- Reuse the proven engine (`setAccountStorageRoot`/`foldMounted`/`mountTo`/
  `computeCellHash`/the stitch); new code is the cache + dirty routing +
  integration only. **Do NOT reinvent hashing/unfolding.**
- **ISOLATION (load-bearing): the cache is STREAMING-ONLY. Do NOT modify
  `parallel_mount.go`'s `concurrentStorageRoot` / `dfsSubtreeDeep` (the ModeParallel
  path) in a way that changes its behavior.** Keep `ModeParallel` a clean, unchanged
  baseline so the whole nested-cache strategy is droppable and `ModeParallel` vs
  `ModeStreaming` stays a fair comparison. The streaming cache-aware deep fold lives
  in streaming code; it may *call* shared primitives read-only, and may use a
  streaming-local per-nibble fold (duplicating the small `foldChildAt` logic) rather
  than refactoring the parallel path. If a behavior-preserving extraction of the
  per-nibble fold is taken instead, it must leave ModeParallel byte-identical
  (verified by the no-regression suite).
- Small, focused tasks; each fully green (incl. `-race` for concurrency tasks)
  before the next.
- Keep `default`, `ERIGON_CMT_MOUNT=1`, `ERIGON_CMT_DEEP=1`, and streaming paths
  green throughout (no regression).
- Gate behind the existing streaming variant; default behavior unchanged.

## Testing Strategy

- **Oracle (every task): streaming root == sequential root AND every stored branch
  matches** — `requireBranchParity` / `branchDiff` / the `streaming` arm of
  `requireIncrementalEquiv` / `TestStreaming_DeepBranchParity` /
  `TestNestedStorage_IncrementalParity`.
- **Net-new bespoke tests** (the rest inherited via the `streaming` arm matrix):
  incremental-block over a cached whale (mass-write then sparse re-touch);
  only-dirty-nibbles-refolded (fold-count assertion); cached-nibble collapse/delete
  invalidation; per-block Reset clears caches; concurrency (`-race`) of TouchKey +
  background folds over a cached account.
- **Bulk benchmark** (measurement task): the whale re-fold drop toward 13× via the
  streaming committer on a mass-write corpus (and/or squeeze/`stage_exec`). The live
  frontier is **not** a valid measurement (Post-Completion note).

## Progress Tracking

- `[x]` when done; `➕` new tasks; `⚠️` blockers. Keep in sync.

## Solution Overview

**Big-account storage becomes a cache-backed unit, removed from the top-nibble
split's key stream in BOTH fold paths.** A top-nibble split fold processes the
account *leaf* with an injected storageRoot (from the cache), skipping the
account's storage keys; the cache re-folds only dirty storage nibbles.

- **`accountStorageCache`** (new): `prefix accHash[:64]`, `children [16]cell`,
  per-nibble `{dirty, keyCount, lastFoldedSize}`, `present uint16`, `storageRoot
  cell`, `mu`. Owned by the `StreamingCommitter` in `map[string]*accountStorageCache`.
- **Promotion:** an account is cached once its touched storage exceeds the threshold
  (tracked per-account at `TouchKey`).
- **`TouchKey`:** a storage key of a cached account marks only its storage-nibble
  (`KeyToHexNibbleHash(key)[64]`) dirty + `keyCount++`; a per-nibble doubling gate
  decides eager re-fold (mirrors `shouldEagerFold`).
- **Incremental fold:** a cache-aware `concurrentStorageRoot` re-folds only dirty
  nibbles (via `foldChildAt`), reuses cached clean cells, aggregates → storageRoot,
  clears dirty. `dfsSubtreeDeep` takes the storageRoot from the cache instead of a
  fresh full fold; `setAccountStorageRoot` injects it.
- **Both paths unified:** `foldKeys` (background) and `foldSplit` (Process) must both
  treat a cached account's storage as cache-only (skip its storage keys, inject from
  cache). Cleanest is to route big-account storage out of the split key stream so
  neither path re-streams it.
- **Process:** final aggregation from each cache → injected leaves → stitch → root.

Key decisions / rationale:
- **Reuse-clean is sound only mid-block** (prev-invariant: nothing flushed, stable
  pre-image). On a **self-flush/collapse** of a cached nibble (delete-driven),
  invalidate that nibble's cell and re-fold — never reuse a stale cell. The
  `overlayContext.flushed` flag already signals this.
- **Correctness == the prototype**, which is byte-identical to sequential; the cache
  changes *when/how much* is re-folded, not the result. Process always re-aggregates
  the final state, so a dropped/invalidated cell only costs work, never correctness.

## Technical Details

- Cache key = `string(accHash[:64])`. Per-nibble gate = `keyCount >= floor &&
  keyCount >= 2*lastFoldedSize` (reuse `defaultEagerFold`/`SetEagerFold`).
- A cached nibble's cell is the depth-65 child cell (post-trim) `foldChildAt`
  produces; aggregation is the existing `assembleAccountFromChildren` logic moved
  into a reusable (non-test) helper.
- Skipping big-account storage from the split stream: either filter it in
  `collectSplitKeys`/`foldKeys` and in `dfsSubtreeDeep`'s recursion, or maintain the
  account's storage in a side structure keyed off the prefix trie. Decide in Task 4.
- Concurrency: `accountStorageCache.mu` + per-nibble CAS on `keyCount` vs fold-start
  snapshot (mirror the split gate's gen handling). The cache's own per-nibble fan-out
  must use a separate errgroup from the split-level one (no shared `SetLimit`).

## What Goes Where
- **Implementation Steps** (`[ ]`): cache, routing, integration, tests, bulk bench.
- **Post-Completion** (no checkbox): live-node validation must use a bulk path
  (squeeze/rebuild); the steady-state frontier won't exercise it.

## Implementation Steps

### Task 1: Streaming-local cache + cache-aware storage-root fold helper

**Files:**
- Create: `execution/commitment/streaming_storage_cache.go` — `accountStorageCache`
  type + a streaming-local per-nibble fold + assembler, reusing shared primitives
  (`foldMounted`/`mountTo`/`setAccountStorageRoot`/`computeCellHash`); does NOT touch
  `concurrentStorageRoot`/`dfsSubtreeDeep`.
- Create: `execution/commitment/streaming_storage_cache_test.go`

- [x] define `accountStorageCache{prefix, children [16]cell, present uint16, perNibble [16]struct{dirty bool; keyCount,lastFoldedSize uint64}, mu}`
- [x] streaming-local per-nibble fold (production copy of `foldChildAt`) + assembler (production copy of `assembleAccountFromChildren`) — `parallel_mount.go` unchanged
- [x] `foldStorageRootCached(cache, accNib, groups)`: re-fold only dirty nibbles, reuse cached clean cells, aggregate → storageRoot cell; clear dirty + set lastFoldedSize
- [x] write parity test: cached re-fold (after dirtying one nibble + adding slots) account root **== the sequential `HexPatriciaHashed.Process` account root** (not the promoted test helpers — use the real engine as oracle, like `TestDeepConcurrent_WhaleParity`)
- [x] write fold-count test: only dirty nibbles' per-nibble fold runs (instrument a counter)
- [x] run tests — must pass before next task

### Task 2: Extract a streaming-local deep walk (cache-FREE) — isolate from the parallel path

**Files:**
- Modify: `execution/commitment/streaming_commitment.go` (`foldSplit` calls a new
  streaming-local deep walk instead of `sc.pph.dfsSubtreeDeep`)
- Modify: `execution/commitment/streaming_commitment_test.go`

This is the isolation step: today `foldSplit` calls `sc.pph.dfsSubtreeDeep` (shared
parallel code). Duplicate that walk into streaming code, cache-free, and prove it's
byte-identical BEFORE any cache logic — so a later failure localizes to the cache,
not the extraction. `parallel_mount.go` stays untouched (ModeParallel unaffected by
construction; the meaningful guard is streaming-root-unchanged across this swap).

- [ ] add `sc.dfsDeepLocal(...)` mirroring `dfsSubtreeDeep` (big-account detect, `concurrentStorageRoot`-equivalent via the Task-1 per-nibble fold + assembler, `setAccountStorageRoot` inject, skip storage children) — no cache yet
- [ ] `foldSplit` calls `sc.dfsDeepLocal` instead of `sc.pph.dfsSubtreeDeep`
- [ ] gate: `TestStreaming_DeepBranchParity` + the `streaming` arm of `requireIncrementalEquiv` stay green (streaming root + branches byte-identical across the extraction)
- [ ] confirm `ModeParallel` (`ERIGON_CMT_MOUNT=1 ERIGON_CMT_DEEP=1`) still green (it cannot regress — `parallel_mount.go` untouched — but verify)
- [ ] run tests — must pass before next task

### Task 3: Per-account cache + TouchKey routing + promotion + lifecycle + seam

**Files:**
- Modify: `execution/commitment/streaming_commitment.go`
- Modify: `execution/commitment/streaming_commitment_test.go`

- [ ] add `caches map[string]*accountStorageCache` (key `string(accHash[:64])`) + `nestedCacheOn bool` (default on) + `SetNestedCache(bool)` runtime seam (for the apples-to-apples bench) to `StreamingCommitter`
- [ ] promote an account to a cache on the **same effective condition the deep walk uses** (touched storage > `deepStorageThreshold` AND its storage spans ≥2 first-storage-nibbles) so the cache and `dfsDeepLocal` agree on which accounts are "big"
- [ ] `TouchKey`: a cached account's storage key marks `cache.perNibble[hk[64]]` dirty + keyCount++; it must **not** also bump the owning top-nibble split's keyCount/gen (avoid the split gate fighting the per-nibble gate / double counting). Non-cached accounts unchanged.
- [ ] per-nibble doubling gate (reuse `defaultEagerFold`/`SetEagerFold`); count cap on cached accounts with **fall-back-to-full-fold** over cap (no LRU); `Reset` clears caches
- [ ] write tests: storage touches route to the right nibble; promotion fires on the effective condition incl. the **10k-slots-in-one-nibble** case (promotes by count but single-nibble → must match the deep walk's behavior); cached storage touch does not re-trigger the split eager fold; Reset clears; over-cap falls back
- [ ] run tests — must pass before next task

### Task 4: Wire the cache into the Process deep walk (`dfsDeepLocal`)

**Files:**
- Modify: `execution/commitment/streaming_commitment.go` / `streaming_storage_cache.go`
- Modify: `execution/commitment/streaming_commitment_test.go`

Note: the existing `streaming`-arm matrix does NOT exercise the cache (its corpora —
`genRandomAccountsStorage(256)`, `genAccountsWithNestedStorage(4)` — never cross
`deepStorageThreshold`), so it only proves *no regression for non-cached accounts*.
The cache itself is covered solely by the new cached-whale tests below.

- [ ] `dfsDeepLocal`: for a cached big account take the storageRoot from `foldStorageRootCached` (incremental) instead of a fresh full fold; inject via `setAccountStorageRoot`
- [ ] write cached-whale Process parity: big-account corpus (>10k storage) via the committer → root + branches == sequential
- [ ] write incremental-block parity: batch-1 mass-writes a whale, batch-2 touches a sparse subset of its storage → root + branches == sequential AND only the touched nibbles re-folded (fold-count)
- [ ] run tests — must pass before next task

### Task 5: Background path (`foldKeys`) shares the cache — single source of truth

**Files:**
- Modify: `execution/commitment/streaming_commitment.go`
- Modify: `execution/commitment/streaming_commitment_test.go`

The background `foldKeys` folds big storage sequentially while `dfsDeepLocal` folds
it incrementally; both must yield byte-identical cells, and the two-level gen logic
must compose without torn reads. (Defer the "route storage out of the split stream"
refactor — that's a perf refinement, not correctness; minimal correct version: both
paths read the cache as the single storageRoot source.)

- [ ] make the background fold consult the cache for a cached account's storageRoot (don't re-stream its storage); the cache is the single source of truth for both paths
- [ ] define the ordering of the account-cache per-nibble gen/dirty vs the split-level gen/CAS (`foldSplitBg`) so a touch landing between snapshot and CAS cannot install a stale cell
- [ ] write fold-mechanism-parity test: a nibble's background-folded cached cell == its Process-folded cell (byte-identical)
- [ ] write `-race` test: scheduler + multi-goroutine TouchKey over a cached whale → root + branches == sequential
- [ ] run tests (`-race`, `-count=5`) — must pass before next task

### Task 6: Collapse/delete invalidation on the Process deep path

**Files:**
- Modify: `execution/commitment/streaming_commitment.go` / `streaming_storage_cache.go`
- Modify: `execution/commitment/streaming_commitment_test.go`

The Process path (`foldSplit`/`dfsDeepLocal`) uses the REAL ctx, NOT `overlayContext`,
so the existing `flushed` signal does not cover it. Run the streaming-local per-nibble
fold against a streaming-local `overlayContext` so a self-flush (delete-driven
collapse) is detectable — then invalidate that nibble's cached cell, re-fold it, and
re-aggregate; ensure a collapse that changes account structure does not let stale
sibling cells be reused. **Do NOT edit `parallel_mount.go`.**

- [ ] per-nibble fold runs against a streaming-local overlay; on `flushed`, invalidate + re-fold that nibble (never reuse a stale cell); re-aggregate the account
- [ ] write **cached-whale** deletes test: build a >10k-storage whale (batch 1), batch 2 zeroes storage in one nibble (collapse) → root + branches == sequential (the existing `*StorageIncrementalDeletes` corpus is below threshold and does NOT exercise a cached nibble — this corpus is net-new)
- [ ] run tests — must pass before next task

### Task 7: Bulk benchmark (the win is bulk-only)

**Files:**
- Create: `execution/commitment/nested_cache_bench_test.go`

- [ ] benchmark on a mass-write whale (one commitment touching many storage nibbles, then an incremental block touching one): **ModeParallel (untouched baseline) vs ModeStreaming gate-only vs ModeStreaming + cache** — re-fold time + allocs, so the strategy stays directly comparable (and droppable) against ModeParallel
- [ ] report whale re-fold time vs the prototype's full-fold (~656 ms) and incremental (~49 ms) targets; label clearly that the metric is the **whale re-fold**, not steady-state commitment
- [ ] document results inline (Progress Tracking)

### Task 8: Verify acceptance criteria
- [ ] streaming root + branches == sequential across all corpora (mixed, big-account, whale, random, nested-storage, deletes), caching on
- [ ] no regression: `go test -run TestVerifyParallel -count=1` on `default`, `ERIGON_CMT_MOUNT=1`, `ERIGON_CMT_MOUNT=1 ERIGON_CMT_DEEP=1`, and the full `Streaming` suite
- [ ] `-race` clean on the cache concurrency tests
- [ ] `make lint` clean (repeat; non-deterministic); `make erigon integration` builds

### Task 9: [Final] Docs
- [ ] update `/Users/awskii/org/wrk/HANDOFF-parallel-storage-fold.md` with the cache design + bulk results
- [ ] move this plan to `docs/plans/completed/`

## Post-Completion
*Manual / external — no checkboxes.*

**Bulk measurement (the only valid live measurement):**
- Run a snapshot squeeze / commitment-rebuild (or `integration stage_exec` over a
  mass-write range) on `/Users/awskii/dev` with `--experimental.streaming-commitment`;
  compare commitment wall-clock for whale-heavy batches cache-on vs cache-off, and
  grep `Wrong trie root`. The steady-state frontier will NOT show a difference (no
  whale re-fold there) — do not use it to judge this feature.

**Risk note:**
- Cache reuse is correctness-safe only under the prev-invariant (nothing flushed
  mid-block); the collapse-invalidation (Task 5) is load-bearing. The Process drain
  always re-aggregates the final state, so any cache miss/eviction degrades to the
  proven full fold, never to a wrong root.
