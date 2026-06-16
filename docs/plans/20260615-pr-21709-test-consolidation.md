# PR #21709 — Commitment Test Suite Consolidation

## Overview

PR #21709 (`execution/commitment: streaming commitment trie`) is a draft/
checkpoint that landed ~5,500 lines of exploratory tests across 21 new test
files. This plan consolidates that suite: extract one shared in-package test
toolkit, regroup files by theme, and prune/merge redundant cases — **without
changing any production code and without losing coverage of any distinct
engine path**.

This is a **test-only** refactor. No file outside `*_test.go` is modified.

Target outcome (grounded in a per-file read + an adversarial prune-hunt over
all 123 tests):

- Test funcs: **123 → ~99** (~24 fewer top-level funcs).
- Files: **21 → 9** (`deep_storage_concurrent_test.go` and
  `streaming_vs_direct_bench_test.go` are deleted outright).
- Lines: **~1,000–1,100 net removed (~20–22%)**, the bulk being duplicated
  runner/constructor/corpus boilerplate folded into the toolkit and four
  whale-corpus builders collapsed into one.

The reduction comes from three sources, in increasing risk:
1. **Dedup** — ~13 root-runner helpers and ~17 corpus builders collapse into
   a shared toolkit. Zero behavior change.
2. **Merges (14)** — fold near-duplicate tests into grouped/table-driven
   functions, preserving every assertion verbatim as a subtest.
3. **Prunes (15)** — delete pure redundant root-parity / measurement-only
   tests, each with a named strict-superset survivor.

Decision on record (from the brainstorm): owner chose **Approach B** (theme
grouping + one shared toolkit file, keep each test a named function) and
**aggressive pruning, accepting some real redundant-coverage loss**.

## Context

### Package and scope

All target test files are `package commitment` under
`execution/commitment/`, except the two flag tests under
`db/state/execctx/` (`package execctx_test`). Tests poke unexported package
internals (`HexPatriciaHashed.grid/fold/computeCellHash`,
`StreamingCommitter.splits/DeepLocalFolds/RefoldCount`,
`BranchData.decodeCells`, `prefixNode.subtreeCount`, `MockState.cm`), so the
toolkit and all theme files MUST stay in-package — nothing moves to an
external `_test` package, and no symbol is exported solely for tests.

Because everything is one package, a relocated helper is visible from every
`*_test.go` regardless of file. The only compile hazard is **delete-before-
relocate**: a file may be deleted only after every symbol it defines and that
is still referenced elsewhere has been moved. **Rule for the whole plan: a
helper must be defined exactly once in the package at every commit boundary —
relocate first, delete the old home second, in the same task.**

**Transitive closure.** Before deleting any bench file, move not just the
named benchmarks/tests but EVERY bench-only helper transitively reachable from
the surviving content (corpus builders, CPU-burn rigs, sink vars, per-account
fold helpers). The named-symbol lists below are the known cases; the executor
must also `grep` each to-be-deleted file for any remaining
defined-and-still-referenced symbol and relocate it. A symbol whose only
caller is itself pruned (e.g. `streamingCycle`) is deleted, not moved.

### Exact PR-21709 scope (the only files this plan may touch)

Confirmed via `git diff --diff-filter=A` vs merge-base: **21 added test
files**. Do NOT modify any other test file — `commitment_test.go`,
`patricia_state_mock_test.go`, `verify_test.go`, `hex_patricia_hashed_test.go`,
`commitment_bench_test.go`, etc. are pre-existing and out of scope (reuse only).

commitment (19): `additive_updates_test.go`, `clustered_ab_bench_test.go`,
`deep_integration_test.go`, `deep_storage_concurrent_bench_test.go`,
`deep_storage_concurrent_test.go`, `parallel_patricia_hashed_bench_test.go`,
`parallel_patricia_hashed_edge_test.go`, `parallel_patricia_hashed_test.go`,
`parallel_update_test.go`, `prefix_trie_test.go`, `prepare_on_touch_test.go`,
`storage_fold_scaling_bench_test.go`, `streaming_commitment_bench_test.go`,
`streaming_commitment_test.go`, `streaming_deep_fold_test.go`,
`streaming_multidepth_parity_test.go`, `streaming_split_fold_test.go`,
`streaming_vs_direct_bench_test.go`, `wide_nested_parallel_test.go`.
execctx (2): `parallel_commitment_flag_test.go`,
`streaming_commitment_flag_test.go`.

### Cross-file helper homes (bench files own helpers used by non-bench tests)

These definitions currently live in **bench** files but are consumed by
**non-bench** theme tests, so the bench consolidation is NOT a leaf operation —
each must be relocated (per the rule above) before its home file is touched:

- `buildMixedCorpus`, `build100KAccountsCorpus`, `build500KStorageHeavyCorpus`,
  `build1MWhaleCorpus`, `benchWorkerCounts`, `runDirectBench`, `runParallelBench`
  → defined in `parallel_patricia_hashed_bench_test.go`.
  `buildMixedCorpus` is used by `streaming_commitment_test.go`,
  `streaming_split_fold_test.go`, `streaming_multidepth_parity_test.go`,
  `prepare_on_touch_test.go`, AND `streaming_commitment_bench_test.go` — it
  moves to the toolkit (Task 1), so it is visible package-wide regardless.
  `build1MWhaleCorpus` is used by `prepare_on_touch_test.go` and
  `streaming_vs_direct_bench_test.go`.
- `whaleByNibble`, `storKV` (a type), `foldChildAt`, `concurrentAccountRoot`
  → defined in `deep_storage_concurrent_bench_test.go`.
  `whaleByNibble` is used by `streaming_deep_fold_test.go`,
  `streaming_multidepth_parity_test.go`, `streaming_split_fold_test.go`.
  `concurrentAccountRoot` is called by BOTH `TestDeepConcurrent_WhaleParity`
  (→ `deep_storage_test.go`) and `Benchmark_DeepStorageWhale` (→ bench file),
  so it must land in `deep_storage_test.go` (visible to both).
- `streamingCycle`, `runStreamingOverlapBench`, `streamingBenchCorpora`,
  `burnCPU`, `benchCPUSink`, `buildStreamingWhaleCorpus`
  → defined in `streaming_commitment_bench_test.go` (alongside
  `Benchmark_StreamingOverlap` and `TestStreaming_Metrics`). All move to the
  new bench file EXCEPT `streamingCycle` (pruned with `TestStreaming_Metrics`).
  `buildStreamingWhaleCorpus` is one of the 4 builders folded into
  `buildWhaleCorpus` (Task 2) — its definition is removed there, so by Task 3
  only `burnCPU`/`benchCPUSink`/`runStreamingOverlapBench`/`streamingBenchCorpora`
  remain to move.

### Existing infrastructure to REUSE (do not reinvent)

- `MockState` (`patricia_state_mock_test.go`): `NewMockState`,
  `applyPlainUpdates`, `SetConcurrentCommitment`, branch map `cm`.
- `UpdateBuilder` / `NewUpdateBuilder` (`.Balance/.Storage/.Build`),
  `WrapKeyUpdates`, `mockTrieCtxFactory`, `DefaultTrieConfig`,
  `WarmupConfig`, `KeyToHexNibbleHash`.
- `findAddressForNibble` + `nibbleAddressCache` (`hex_concurrent_patricia_hashed_test.go`)
  — the single brute-force address engine. Do NOT add a second one.

### Two gating constraints (violating these silently drops coverage)

1. **Branch-parity wiring before the collapse merge.**
   `TestStreaming_StorageCollapseAcrossSplit` may be merged into
   `TestStreaming_MultiDepthCollapseParity` **only if** `requireBranchParity`
   (byte-level `MockState.cm` compare + count equality) is wired into the
   target first. Otherwise the partial-collapse path loses its only
   byte-level branch check on a deletion corpus — a dropped-deletion /
   clobbered-sibling that still hashes to the correct root would pass
   silently. If the wiring cannot be done cleanly, **downgrade: KEEP
   `TestStreaming_StorageCollapseAcrossSplit` standalone and do NOT prune
   `TestStreaming_RefoldAfterCollapse`.**
2. **Ordering: merges before prunes.**
   `TestStreaming_RefoldAfterCollapse` is pruned naming
   `TestStreaming_StorageCollapseAcrossSplit` as the surviving lazy-collapse
   owner — but that owner is itself being merged. Sequence so the lazy
   `modeStreaming` collapse path always has a live named owner before any
   deletion (Task 5 merges, Task 6 prunes).

### Mandatory dead-helper cleanup (or the package will not compile)

- Delete `streamingCycle` (in `streaming_commitment_bench_test.go`) together
  with `TestStreaming_Metrics` (same file) — `streamingCycle` has no other
  caller once that test is gone.
- Delete `foldSubWorkerTo` + `buildDenseWhale` (both in
  `deep_storage_concurrent_test.go`) together with that file. `buildDenseWhale`
  is collapsed into `buildWhaleCorpus` first (Task 2); `foldSubWorkerTo` is
  local to the pruned `TestDeepConcurrent_DenseStorageParity`.
- Before deleting `streaming_vs_direct_bench_test.go`, confirm
  `runDirectBench` / `runParallelBench` / `runStreamingOverlapBench` /
  `streamingBenchCorpora` are defined in (and still referenced from) the merged
  `parallel_streaming_bench_test.go`, and `build1MWhaleCorpus`'s callers
  (`prepare_on_touch_test.go`) point at `buildWhaleCorpus`.

## Target file layout (21 → 9)

| Target file | Merges from | Holds |
|---|---|---|
| `parallel_testkit_test.go` *(new)* | duplicated runners/builders/asserts from 6 files | toolkit only, no tests |
| `parallel_patricia_hashed_test.go` | `parallel_patricia_hashed_edge_test.go`, `wide_nested_parallel_test.go` | object-api, DFS/fanout, delete/bloatnet/wide-nested parity |
| `streaming_commitment_test.go` | `streaming_multidepth_parity_test.go`, `streaming_split_fold_test.go` | all streaming parity + white-box helpers |
| `deep_storage_test.go` *(new)* | `deep_integration_test.go`, `deep_storage_concurrent_test.go`*, `streaming_deep_fold_test.go` | whale/cell-fold white-box tests |
| `prefix_trie_test.go` | `parallel_update_test.go`, 1 test from `additive_updates_test.go` | data-structure unit tests |
| `additive_updates_test.go` *(slimmed)* | — | `TestAdditiveTouch` (parallel+streaming subtests) |
| `parallel_streaming_bench_test.go` *(new)* | the 6 bench files per the **Bench consolidation** table (`streaming_vs_direct_bench_test.go`* fully deleted) | surviving `Benchmark_*` + shared rigs |
| `prepare_on_touch_test.go` | — (kept standalone; uniquely internals-coupled) | `TestPrepareOnTouch_Parity`, `Benchmark_PrepareOnTouch` |
| `db/state/execctx/commitment_flag_test.go` | `parallel_commitment_flag_test.go`, `streaming_commitment_flag_test.go` | flag-precedence + root-equivalence; **carry over `TestPickTrieVariant_StreamingFlag` verbatim** |

`*` = source file deleted after its surviving tests move out.

## Shared toolkit API (`parallel_testkit_test.go`)

- **Constructors** (centralize `length.Addr` + `DefaultTrieConfig()` + reset/release):
  - `newSeqTrie(t, ms) *HexPatriciaHashed`
  - `newParTrie(t, ms, workers) *ParallelPatriciaHashed` (workers ≤0 → NumCPU; caller defers Release)
  - `newStreamCommitter(t, ms, workers, scheduler) *StreamingCommitter` (ms must already `SetConcurrentCommitment(true)`)
- `processRoot(t, trie, ut) []byte` — the `Process → require.NoError → common.Copy(root)` idiom (inlined ~15×).
- `engineRoot(t, mode engineMode, workers, keys, upds) ([]byte, *MockState)` — single-batch dispatch over `engineMode` (extends existing `runMode` with `modeStreamingUpdates` / `modeStreamingPublic`). Replaces `sequentialRoot`, `streamingRoot`, `parallelRoot`, `streamingViaUpdatesRoot`, `streamingViaPublicProcessRoot`.
- `incrementalRoot(t, mode, workers, k1, u1, k2, u2) ([]byte, *MockState)` — two-batch form (one MockState across batches). Replaces `runIncremental`.
- `requireRootParity(t, keys, upds, workers) []byte` — direct-vs-parallel driver (replaces `assertEquivalentRoot` / `assertEquivalentRootWorkers`).
- `requireAllEnginesParity(t, k1, u1, k2, u2, workers)` — parallel/streaming/scheduled each == sequential, dumps `branchDiff` on mismatch (replaces `requireIncrementalEquiv`).
- Branch helpers (relocated verbatim, read `MockState.cm`, stay in-package): `requireBranchParity`, `branchDiff`, `snapshotBranches`, `requireBranchesUnchanged`.
- Nibble sugar (relocated): `nibbleAddr`, `addrHex`, `slotHashBytes`, `nibs`. `findAddressForNibble` stays the one brute-force engine. Do NOT merge `wide_nested`'s `findHashForNibbles`/`findAddrForNibbles`/`findSlotForNibbles` (multi-nibble prefix craft) — co-locate them.

**Stay local (NOT in the shared toolkit)** — they read narrow unexported state:
`waitSchedulerIdle`/`foldedSplitCount`/`splitCount`/`requireResetClean`
(streaming theme file); `computeCellHashAt`/`foldChildAt`/`whaleByNibble`/
`storKV` (deep-storage theme file).

## Corpus consolidation

- **`buildWhaleCorpus(opts whaleOpts)`** collapses `buildBigAccountCorpus`,
  `buildStreamingWhaleCorpus`, `build1MWhaleCorpus`, `buildDenseWhale`.
  `whaleOpts{bigSlots, smallAccountsBefore/After, smallSlots, extraWhales[],
  tailSingleSlotAccounts, seed}`. **Defaults must reproduce the existing
  seeds (771 / 919273) so produced roots are unchanged.**
- Keep canonical, verbatim: `buildMixedCorpus(seed, nKeys)`,
  `whaleByNibble(slots)` (returns the `[16][]storKV` partition the deep-fold
  tests need), `whaleCollapseCorpus`/`whaleFullCollapseCorpus`,
  `buildClusteredStorageCorpus`, `build100KAccountsCorpus`,
  `build500KStorageHeavyCorpus`, `buildMultiDepthCorpus`, the
  `wide_nested` gen* builders + `sparseBatch2` (need keccak-prefix brute
  force; not collapsible).

## Bench consolidation (the 6 NEW bench files → 1)

All six PR-added bench files collapse into `parallel_streaming_bench_test.go`,
EXCEPT items that are not benchmarks or that belong to a theme file. Per-file
disposition:

| Source bench file | Benchmark(s) → bench file | Non-bench content → elsewhere |
|---|---|---|
| `clustered_ab_bench_test.go` | `Benchmark_Commitment_Clustered` (+ `buildClusteredStorageCorpus`) | — |
| `parallel_patricia_hashed_bench_test.go` | `Benchmark_Commitment_{SmallCounts,1MWhales,DirectVsParallel}` + rigs `runDirectBench`/`runParallelBench`/`benchWorkerCounts` | `buildMixedCorpus` + `build100KAccountsCorpus`/`build500KStorageHeavyCorpus` move to toolkit (Task 1); `build1MWhaleCorpus` folds into `buildWhaleCorpus` (Task 2) |
| `storage_fold_scaling_bench_test.go` | `Benchmark_StorageConcurrency` (+ `storageGroup`/`groupRun` rig) | — |
| `streaming_commitment_bench_test.go` | `Benchmark_StreamingOverlap` + rigs `runStreamingOverlapBench`/`streamingBenchCorpora`/`burnCPU`/`benchCPUSink` | `TestStreaming_Metrics` + `streamingCycle` PRUNED (Task 6, not moved); `buildStreamingWhaleCorpus` folded into `buildWhaleCorpus` (Task 2) |
| `deep_storage_concurrent_bench_test.go` | `Benchmark_DeepStorageWhale` | `TestDeepConcurrent_WhaleParity` (a TEST) → `deep_storage_test.go`; `whaleByNibble`/`storKV`/`foldChildAt`/`concurrentAccountRoot` → `deep_storage_test.go` (Task 3) |
| `streaming_vs_direct_bench_test.go` | both benches PRUNED (Task 6) → **file deleted** | — |

Survivor benches named in the prune list resolve as: `Benchmark_Commitment_1MWhales`
(from `parallel_patricia_hashed_bench_test.go`) and `Benchmark_StreamingOverlap`
(from `streaming_commitment_bench_test.go`) both land in
`parallel_streaming_bench_test.go`. `Benchmark_PrepareOnTouch` stays in
`prepare_on_touch_test.go` (coupled to `preparedSplits`).

## Merge list (14 — fold, assertions preserved verbatim)

1. `TestParallelFanout_{TwoNibbles,FourNibbles,AsymmetricWorkload,LopsidedBuckets}` → table-driven `TestParallelFanout` (4 rows).
2. 9 plumbing Skeleton tests (`RootTrie`, `Variant`, `SetNumWorkers`, `ResetContextPropagates`, `SetTrieContextFactory`, `SetTraceFlags`, `EnableWarmupCache`, `CaptureRoundTrip`, `EnableCsvMetricsNoPanic`) → `TestParallelPatriciaHashedSkeletonPlumbing`, one `t.Run` per method. **Keep**: the `require.Same` template identity, the `SetNumWorkers(0/-3)→NumCPU` clamp, the capture truncation round-trip, the `ResetContext` `assert.Same`.
3. `…SkeletonReleaseNilSafe` + `…SkeletonRootHashAfterRelease` → `TestParallelPatriciaHashedSkeletonRelease`. **Keep** the released `RootHash()→(nil,no error)` assertion.
4. `TestParallelUpdateConstruction` + `TestParallelUpdateClose` → `TestParallelUpdateLifecycle`. **Keep** `trie.root != nil`.
5. `TestParallelUpdateAppendDeferredEmpty` → fold into `…AppendDeferredSequential`. **Keep** the opening `assert.Empty` guard.
6. `TestPrefixTrieDivergenceAtEndOfExtension` → fold into `…DivergenceInsideExtension`.
7. `TestPrefixTrieMixedPrefixCountsPropagate` → fold into `…TwoInsertsDivergeAtRoot`. **Keep** the 4/3/7 asymmetric `subtreeCount` case.
8. `TestPrefixTriePopcount` → fold into `…ChildIndex`. **Keep** the all-16-bits popcount case.
9. `TestStreaming_RetouchAfterFold` → fold into `…StorageMidAccountFold`. **Keep** the Process-time `reusable()`-skip sequence.
10. `TestStreaming_MultiBlockResetWithScheduler` + `…MultiBlockNoResetAccumulation` → `TestStreaming_MultiBlockReuse` (reset / no-reset subtests).
11. `TestStreaming_StorageCollapseAcrossSplit` → fold into `…MultiDepthCollapseParity` **(GATED — see constraint 1)**.
12. `TestDeepIntegration_Parity` → fold into `…BranchParity` **by adding the {1,4,8} worker loop around the BranchParity run** (else the multi-worker ordering sweep is lost).
13. `TestUpdatesModeParallel_AdditiveTouchDirect` + `TestStreaming_AdditiveTouchKey` → `TestAdditiveTouch` (parallel/streaming subtests). **Keep** `require.Equal(len(keys), ut.Size())` in the parallel subtest.
14. `TestPrefixTrieInsertDuplicateMerges` relocates from `additive_updates_test.go` to `prefix_trie_test.go` (theme move).

## Prune list (15 — outright delete, named survivor)

| Test(s) | Survivor |
|---|---|
| `TestParallelProcessSkeleton_EmptyUpdates` | `TestParallelEmptyUpdates` (edge file; identical body) |
| `TestParallelProcessSkeleton_SingleAccount` | `TestParallelSingleTouchedKey` |
| `TestParallelProcessSkeleton_SingleNibbleBucket` | `TestParallelProcessSkeleton_DenseSingleNibbleBucket` (superset) |
| `TestStreaming_Metrics` (+ `streamingCycle`) | `TestStreaming_StorageMidAccountFold` (owns `require.Positive(RefoldCount())`) |
| `TestStreaming_RefoldAfterCollapse` | `TestStreaming_StorageCollapseAcrossSplit` body (now inside MultiDepthCollapseParity) — **GATED, see constraint 2** |
| `TestStreaming_DeepLocalWalkUsed` | `TestStreaming_StorageInteriorSplits` (superset: 20k whale, {1,4,8}w) |
| `TestDeepConcurrent_DenseStorageParity` (+ `foldSubWorkerTo`,`buildDenseWhale`; delete file) | `TestDeepFold_StorageRootParity` (production deep-fold path) |
| `TestParallelMixedAccountStorage` | `TestParallelBloatnetShape` (superset; note: `-short`-gated) |
| `TestVerifyParallel_StorageWideNestedIncremental` | `TestVerifyParallel_RandomStorageIncremental` (worker sweep superset) |
| `TestSharedDomains_ParallelFlagOff_UsesSequentialTrie`, `…ParallelFlagOn_UsesParallelTrie` | `TestSharedDomains_ParallelFlag_RootEquivalence` (asserts both flag states inline) |
| `TestSharedDomains_StreamingFlagOn_UsesStreamingTrie` | `TestSharedDomains_StreamingFlag_RootEquivalence` |
| `Benchmark_RegularVsStreaming` | `Benchmark_StreamingOverlap` + `Benchmark_Commitment_SmallCounts` |
| `Benchmark_TrieVariants_1MWhale` (then delete emptied file) | `Benchmark_Commitment_1MWhales` + `Benchmark_StreamingOverlap` |

## Path-coverage guarantee (must hold after every task)

Every non-negotiable engine path keeps ≥1 named owner:
split · partial-collapse · full-collapse · multi-block-reset ·
scheduler-concurrent · deep-fold · public-Process · additive-touch ·
random-order · wide-nested · data-structure · object-api · flag-precedence.
(The synthesis produced the explicit owner list; reproduce it as a checklist
in Task 7 and diff before/after.)

## Development Approach

- **Test-only.** No non-`*_test.go` file changes. If a task seems to need a
  production change, stop — it's out of scope.
- One task per commit. Each task must build and its directly-affected tests
  must pass before commit. Commit prefix: `execution/commitment: …`.
- Per-task validation uses targeted `go build` + `go test -run`, not the full
  `make erigon integration`.
- `make lint` is non-deterministic (repo CLAUDE.md) — run it repeatedly until
  clean in the final task.
- Reviewer diffs **assertion-by-assertion** on merges, not just "the func
  still exists."
- Five merges rest on medium/low-confidence verdicts
  (`StorageCollapseAcrossSplit` low; `RetouchAfterFold`, `MultiBlockReuse`,
  `DeepConcurrent_DenseStorageParity`, `AdditiveTouch` medium) — give the new
  owner funcs (`MultiBlockReuse`, `AdditiveTouch`, `ParallelFanout`) a careful
  second read so no assertion is silently weakened in the port.

## Implementation Steps

### Task 1: Add shared toolkit, rewire existing runners to delegate

**Files:** Create `execution/commitment/parallel_testkit_test.go`; modify the
runner-defining files to delegate to it.

Add constructors (`newSeqTrie`/`newParTrie`/`newStreamCommitter`/`processRoot`),
`engineRoot`/`incrementalRoot` over an `engineMode` enum (extend the existing
`runMode`), `requireRootParity`/`requireAllEnginesParity`, and relocate the
branch-parity + nibble-sugar helpers. Replace the bodies of the old runner
helpers (`sequentialRoot`, `streamingRoot`, `parallelRoot`,
`assertEquivalentRoot[Workers]`, `runIncremental`, `requireIncrementalEquiv`)
with thin delegations (do not delete their call sites yet).

- [x] `go build ./execution/commitment/...` compiles (no duplicate symbols).
- [x] `go test ./execution/commitment/... -run 'Parity|Equiv|Streaming|Parallel|Deep' -count=1` green.
- [x] No new exported symbols added for tests.

### Task 2: Consolidate the 4 whale builders into buildWhaleCorpus

**Files:** `parallel_testkit_test.go` + the 4 builder homes:
`buildBigAccountCorpus` in `deep_integration_test.go`,
`buildStreamingWhaleCorpus` in `streaming_commitment_bench_test.go`,
`build1MWhaleCorpus` in `parallel_patricia_hashed_bench_test.go`,
`buildDenseWhale` in `deep_storage_concurrent_test.go`.

Add `buildWhaleCorpus(whaleOpts)`. Add a TEMPORARY scratch test asserting its
output is **byte-identical** (keys and updates, in order) to
`buildBigAccountCorpus(15000)`, `buildStreamingWhaleCorpus()`,
`build1MWhaleCorpus(b)`, and `buildDenseWhale(...)` at their original seeds.
Only once green, replace call sites, delete the 4 originals, and delete the
scratch test.

- [x] Scratch byte-identity test green for all 4 shapes.
- [x] `go test ./execution/commitment/... -run 'Parity|Equiv|Deep|Streaming|Whale' -count=1` green (roots unchanged).
- [x] Scratch test and 4 old builders removed.

### Task 3: Theme regrouping + bench consolidation (relocate, no logic change)

**Files:** per the target-file-layout table and the **Bench consolidation**
table. Pure relocation — no test func is deleted in this task.

Non-bench theme moves: move test funcs into their theme files and delete each
emptied source file (`parallel_patricia_hashed_edge_test.go`,
`wide_nested_parallel_test.go`, `streaming_multidepth_parity_test.go`,
`streaming_split_fold_test.go`, `deep_integration_test.go`, `parallel_update_test.go`,
and `TestPrefixTrieInsertDuplicateMerges` out of `additive_updates_test.go`).

Bench moves (apply the **Bench consolidation** table; Task 1 already moved
`buildMixedCorpus`/`build100K…`/`build500K…` to the toolkit and Task 2 folded
`build1MWhaleCorpus` into `buildWhaleCorpus`, so those are gone from
`parallel_patricia_hashed_bench_test.go` by now):
- Create `parallel_streaming_bench_test.go`; move every surviving `Benchmark_*`
  + the rigs (`runDirectBench`/`runParallelBench`/`benchWorkerCounts`/
  `runStreamingOverlapBench`/`streamingBenchCorpora`/`burnCPU`/`benchCPUSink`)
  into it. Apply the transitive-closure rule: `grep` each deleted bench file
  for any remaining defined-and-referenced symbol and move it too.
- Move `whaleByNibble`/`storKV`/`foldChildAt`/`concurrentAccountRoot` AND
  `TestDeepConcurrent_WhaleParity` into `deep_storage_test.go` **before**
  deleting `deep_storage_concurrent_bench_test.go` (relocate-before-delete rule).
- Move `TestStreaming_Metrics` + `streamingCycle` into the new bench file (they
  are pruned later in Task 6, not here), then delete the emptied
  `streaming_commitment_bench_test.go`, `clustered_ab_bench_test.go`,
  `storage_fold_scaling_bench_test.go`.
  Do NOT delete `streaming_vs_direct_bench_test.go` yet — its benches are pruned
  in Task 6.

execctx merge: merge the two flag files into
`db/state/execctx/commitment_flag_test.go` with a unified
`withCommitmentFlag(t, variant)` toggle (keep `t.Cleanup` flag restore; no
`t.Parallel`). **Carry over all six funcs**, including
`TestPickTrieVariant_StreamingFlag` (not in any merge/prune list).

- [x] `go vet ./execution/commitment/... ./db/state/execctx/...` compiles, no duplicate or undefined symbols.
- [x] `go test ./execution/commitment/... ./db/state/execctx/... -count=1 -short` green.
- [x] Every relocated func accounted for (no test func lost — this task only relocates); the 6 execctx funcs all present.

### Task 4: Object-api / unit merges (assertion-preserving)

**Files:** `parallel_patricia_hashed_test.go`, `prefix_trie_test.go`.

Apply merges 1–8 (Fanout table, Skeleton Plumbing/Release, parallel-update
lifecycle/append, prefix-trie divergence/counts/popcount). Carry every listed
"Keep" assertion verbatim as a subtest.

- [x] `go test ./execution/commitment/... -run 'Fanout|Skeleton|ParallelUpdate|PrefixTrie' -count=1` green.
- [x] Diff confirms each "Keep" assertion present in the new owner.

### Task 5: Streaming / deep merges (incl. the gated branch-parity wiring)

**Files:** `streaming_commitment_test.go` (already holds the multidepth /
split-fold tests after Task 3), `deep_storage_test.go`, `additive_updates_test.go`.

Apply merges 9–13. For #11: FIRST wire `requireBranchParity` into
`TestStreaming_MultiDepthCollapseParity`, THEN fold
`StorageCollapseAcrossSplit` in. If wiring is not clean, **downgrade**: keep
`StorageCollapseAcrossSplit` standalone and skip the corresponding prune in
Task 6. For #12: add the `{1,4,8}` worker loop into `…BranchParity`.

- [x] `go test ./execution/commitment/... -run 'Streaming|MultiDepth|Collapse|Additive|DeepIntegration' -count=1` green.
- [x] `-race` green on `TestStreaming_SchedulerConcurrentParity`, `TestStreaming_StorageMidAccountFold`, `TestStreaming_MultiBlockReuse`.
- [x] Byte-level `requireBranchParity` runs in the collapse owner (merge taken, not downgrade — wired into `TestStreaming_MultiDepthCollapseParity`).

### Task 6: Prunes + dead-helper + file deletions

**Files:** the prune-list files; delete `deep_storage_concurrent_test.go` and
`streaming_vs_direct_bench_test.go`.

Apply the 15 prunes only after Task 5's owners are live. Delete
`streamingCycle`, `foldSubWorkerTo`, `buildDenseWhale`. Confirm the bench
runners survive in `parallel_streaming_bench_test.go` before deleting
`streaming_vs_direct_bench_test.go`.

- [ ] `go build ./execution/commitment/... ./db/state/execctx/...` compiles (no orphaned helpers).
- [ ] Each pruned test's distinct path still has a live named owner (cross-check the path-coverage checklist).
- [ ] `go test ./... -count=1 -short` green for both packages.

### Task 7: Final verification

- [ ] Path-coverage checklist diff: every non-negotiable path present pre- is present post-.
- [ ] `go test ./execution/commitment/... -race -count=1` green (full, not `-short`).
- [ ] `make lint` green (repeat until stable — non-deterministic per repo CLAUDE.md).
- [ ] `make test-short` green.
- [ ] `make erigon integration` builds (repo pre-commit convention; expected trivially green since no non-test code changed).
- [ ] Replace the plan's estimates with ACTUAL before/after file + test + line counts in the commit body.
