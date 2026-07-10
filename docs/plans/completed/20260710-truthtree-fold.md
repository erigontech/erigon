# TruthTree fold — direct buffer-reuse recursion replacing mount+replay

## Overview

The frontier commitment engine keeps two representations and pays to bridge them: a lightweight touched-key `prefixTrie` built during `TouchKey`, then at `Process` each leaf fold task copies every touched key's hashed nibbles into `[]touchedKey` (`collectSubtreeKeys`/`keyArena`), mounts a transient `HexPatricia` row-grid, and **replays** each key via `followAndUpdate` before `foldMounted`. That double-materialization is both an allocation cost and the reason disk reconciliation is bolted onto the fold.

This replaces the **leaf task's fold body** with a **direct buffer-reuse recursion** over the leaf's `prefixTrie` subtree: recurse the touched subtree, reconcile untouched on-disk siblings by reading their branch records in place, hash bottom-up with reused scratch, emit the same deferred branch updates, and return the same mount-wall-relative cell the parent stitches. The frontier DAG + shared worker pool + merge/finale stitch stay unchanged; only *how a leaf computes its cell* changes, behind `--experimental.truthtree-fold`, until root+branch parity + a replay arm gate the flip.

**What the prototype actually validated (narrow):** `wt/truthtree-proto` is an **all-fresh, storage-plane-only, root-only** spike — it folds a fresh whale's storage subtree to a hash, wraps the account as one leaf, reads no on-disk siblings and emits no branch records. It proves the *hashing math and the buffer-reuse win* (750k whale: 50 ms/47 MB parallel, 418 ms/44 MB serial, vs current fold 84 ms/331 MB). It does **not** validate the three load-bearing production behaviors, which are **net-new** and gated by the differential harness, not the proto:
1. **On-disk-sibling reconciliation** — a touched leaf subtree may have untouched on-disk children that must survive into the folded branch (today `followAndUpdate`/`seedBaseAtPrefix` unfold them). The direct recursion sees only touched keys and must read the branch record to fold blinded siblings in. This is the crux.
2. **Deferred branch-update emission** — `Process` must *write* branch records; the proto emits none. The recursion must emit `DeferredBranchUpdate` per branch prefix byte-identical to `foldMounted`.
3. **Account-plane + inline depth-64 seam** — the proto never recurses an account branch or crosses the seam inline (it wraps the account as a single depth-0 leaf).

**Optimization order:** correctness > fail-closed > performance > simplicity.

## Context (from discovery)

- **Base / rollback:** branch `awskii/truthtree-fold` off tag **`truthtree-rollback-base` (= `46ffcc1b8c`, `awskii/frontier-fresh-whale-fix`)** — frontier engine + storage-only-seam fix + fresh-whale parallelization + coverage test + alloc win. In-place evolution of the frontier engine, NOT off raw origin/main. Rollback: `git reset --hard truthtree-rollback-base`.
- **Reference (hashing math + buffer reuse only):** `wt/truthtree-proto/execution/commitment/truthtree_proto_test.go`. Its `srV3Lean` starts at `hashBranch(node, 64)` (storage subtree), returns `common.Hash` **by value** (stack-friendly), reuses one leaf-hash scratch buffer, and directly calls `computeCellHash`/`computeCellHashLen`/`setFromUpdate` (leaf/extension hashing runs *transitively* inside `computeCellHash`; only ~15 lines of branch keccak — a 17-slot RLP list via `EncodeListPrefixToBuf` — are hand-rolled). Match this `common.Hash`-by-value shape; do NOT introduce an `out *[]byte` sink (re-opens the escape question the proto already answered). Depth accounting: storage leaves hash at their parent-branch-slot depth, sub-branches return `{ext,hash}` the parent applies extension hashing to, the depth-64 storage branch hashes first-nibble children at depth 65.
- **Files** (`execution/commitment/`):
  - `fold_pool.go` — `foldLeafTask`, `foldFreshStorage`, `foldMergeTask`/`foldStorageSeam` (per-task fold bodies); `mergeChildrenAtPrefix`, `stripCellToMountWall` (mount-wall cell — KEEP; the recursion must produce the same shape), `dispatchFrontier`/`run`/`newFoldPool` (dispatch — KEEP).
  - `fold_dag.go` — `deriveFoldFrontier`/`derive`/`foldTask` (DAG — KEEP; leaf/merge split stays).
  - `streaming_deep_fold.go` — `seedBaseAtPrefix`, `seedOrDemote`, `aggregateMountedStorageRoot`, `storageRootFromSingleChild`, `collectSubtreeKeys`, `stripCellToMountWall` companions (seed/stitch primitives; the recursion replaces the *leaf replay*, and only subsumes merge/collapse once Task 7 proves it). NOTE: there is **no** `seedEmptyWall` in this tree — the fresh path uses `seedBaseAtPrefix` tolerating `errStorageBaseNotBranch`.
  - `hex_patricia_hashed.go` — `computeCellHash`, `computeCellHashLen`, `setFromUpdate`, `cell`, and the branch-record encoder used by `foldMounted` (REUSE; the deferred-emission task must call the same encoder).
  - `config.go` / `TrieConfig` — the flag field lives here; both `newFoldPool` sites (`parallel_patricia_hashed.go`, `streaming_commitment.go`) and `parallel_mount.go`/`streaming_commitment.go` `dispatchFrontier` callers thread it.
  - `prefix_trie.go` — `prefixNode`, `dfsSubtree`.
  - Tests/bench: `parallel_testkit_test.go` (`requireBranchParity`, `runEngineBatches`, `processModeBatchState`), `deepfold_regression_test.go` (`buildMixedCorpus`, `TestDeepFold_LeafSurvivorCollapse`, incremental block1→block2), `deepfold_subset_regression_test.go` (`TestDeepFold_FreshWhaleParallelStorage_Parity`), `streaming_commitment_test.go`, `parallel_streaming_bench_test.go` (`Benchmark_DeepStorageWhale`).
- **Scope boundary:** Phase 1 = the direct fold under the **parallel regime only**. The `foldPool` also runs under the streaming engine, so the flag MUST gate streaming **inert** in Phase 1 (streaming keeps the current fold). The new flag is only meaningful when `--experimental.parallel-commitment` is already set. Streaming unfold-on-touch + convergence is a **separate later plan**.

## Development Approach

- **Testing approach: TDD (red-first)** — repo CLAUDE.md mandate. Net-new behaviors (on-disk siblings, deferred emission, account plane) get a **red** differential test with an on-disk-state MockState fixture *before* the code; the proto's fresh oracle does not cover them.
- **Hard gate every step:** flag-on == flag-off == sequential `HexPatricia`, root AND stored-branch byte parity, after every batch, N≥3 batches, encode/restore round-trip (`requireBranchParity`/`runEngineBatches`).
- **Cornerstone invariant — buffer reuse:** the fold returns `common.Hash`/small cell by value with per-goroutine reused scratch; no per-node heap `cell`. The **alloc-ceiling bench** (B/op vs the ~47 MB proto figure, never regressing toward the 575 MB naive figure) is the definition-of-done gate; a `-gcflags=-m` check is a manual spot-check only (compiler-version-brittle), not a CI assertion.
- `go test ./execution/commitment/ -count=1` after each task; `make lint` before every push. Fold-scoped `BeginTemporalRo` pin (#21945); step-boundary invariant #22092 untouched.

## Testing Strategy

- **Unit (fresh, proto-covered):** the fold primitive vs the sequential oracle on a fresh storage subtree.
- **Differential (net-new, harness-covered):** flag-on vs flag-off vs sequential, root + branch byte parity, on corpora that exercise: untouched on-disk siblings inside a touched subtree, single-survivor collapse, delete-to-empty, mixed account-plane (`buildMixedCorpus`), incremental block1→block2 batches, encode/restore. These need on-disk-state fixtures (block 1 seeds disk, block 2 folds through `foldNode`).
- **Concurrency:** `-race`; alloc-ceiling regression bench.

## Solution Overview

The frontier **DAG + pool + merge/finale stitch are unchanged.** A leaf task today returns a mount-wall-relative cell (`foldMounted` → `stripCellToMountWall`) plus deferred branch updates. `foldNode` must return the **same** cell shape and the **same** deferred updates — only computed by direct recursion instead of replay:

- **branch node** — recurse touched children into reused scratch; **read `Branch(prefix)` and fold in untouched on-disk children as blinded-hash cells** (the reconciliation the proto lacks); hash the branch (reused buffer); **emit its `DeferredBranchUpdate`** via the same encoder `foldMounted` uses; return `{ext,hash}` up.
- **terminal node** — `setFromUpdate` + leaf hash at the correct depth.
- **depth-64** — cross the plane inline: fold storage subtree → root, hash the account leaf over (fields + storage root).
- **leaf-task output** — apply the mount-wall strip so the parent stitches it verbatim (invariant M).

`mergeChildrenAtPrefix`/`stripCellToMountWall`/`aggregateMountedStorageRoot`/`storageRootFromSingleChild` are NOT assumed subsumed up front — they stay until Task 7 proves the recursion reproduces their seed+stitch+collapse behavior byte-for-byte on the seeded/collapse corpora.

**Migration:** flag `--experimental.truthtree-fold` selects `foldNode` inside `foldLeafTask` for the parallel regime; streaming is gated inert. Parity chains + a replay arm gate the flip; subsumed code is deleted only after the flip.

## Technical Details

- **Fold signature:** `foldNode(node *prefixNode, prefix []byte, depth int, tctx) (childCell, error)` returning a small by-value cell (ext tail + hash), matching the proto's non-escaping shape; per-goroutine scratch. NO `out *[]byte`.
- **On-disk reconciliation:** at each recursed branch, `Branch(HexToCompact(prefix))` supplies the on-disk child bitmap+hashes; children present on disk but absent from the touched trie are folded in as blinded cells; a missing branch (fresh) means no siblings, exactly the proto case. This is where `seedBaseAtPrefix`'s job moves into the recursion.
- **Deferred emission:** each branch node emits a `DeferredBranchUpdate` for its prefix using the encoder `foldMounted`/`base.fold()` uses today, collected into the task's deferred slice, applied post-Process. Byte-parity via `requireBranchParity`.
- **Task→parent hand-off (pinned):** leaf tasks return a **mount-wall-relative cell**; merge tasks + finale stitch unchanged. `stripCellToMountWall` survives.
- **Fail-closed:** any fold/read error drops all deferred, returns nothing.

## What Goes Where

- **Implementation Steps** (`[ ]`): fold primitive, reconciliation, deferred emission, account plane, flag wiring, benches, deletion, docs.
- **Post-Completion** (no checkboxes): mainnet/sepolia replay arm before the flag-default flip.

## Implementation Steps

### Task 1: Fresh storage-plane direct fold (the proto port)

**Files:**
- Create: `execution/commitment/truthtree_fold.go`
- Create: `execution/commitment/truthtree_fold_test.go`

- [x] port `srV3Lean` into production `foldNode` for a **fresh** storage subtree — `common.Hash`/small-cell by value, reused leaf scratch, reuse `computeCellHash`/`computeCellHashLen`/`setFromUpdate`, hand-roll only the branch keccak
- [x] unit tests vs the sequential oracle for a fresh storage subtree (5k/50k slots), root byte-equal
- [x] error-path: empty subtree; malformed node
- [x] run tests — must pass before next task

### Task 2: Buffer-reuse alloc-ceiling gate

**Files:**
- Modify: `execution/commitment/truthtree_fold_test.go`

- [x] alloc-ceiling bench on the 750k fresh whale storage fold asserting B/op stays near ~47 MB and never regresses toward 575 MB (`Benchmark_TruthtreeFold_FreshWhaleAlloc` + `TestTruthtreeFold_AllocCeiling` gate, ceiling 96 MB)
- [x] one concise buffer-reuse invariant comment at `foldNode`; manual `-gcflags=-m` spot-check noted (not a CI assertion)
- [x] run bench — record the alloc number; tests pass before next task — measured 44,618,517 B/op (42.6 MB), 411 ms/op, 4,525,621 allocs/op (Apple M5 Max, serial), matching the proto's ~44 MB serial figure

### Task 3: On-disk-sibling reconciliation (net-new crux)

**Files:**
- Modify: `execution/commitment/truthtree_fold.go`
- Modify: `execution/commitment/truthtree_fold_test.go`

- [x] RED first: on-disk-state MockState fixture — block 1 seeds a storage subtree on disk, block 2 touches a scattered subset; `TestTruthtreeFold_OnDiskSiblingReconciliation` pins that the fresh hand-rolled fold drops the untouched on-disk siblings and diverges (RED baseline, `require.NotEqual`), and the reconciling fold reproduces the sequential root. Confirmed RED by neutering the on-disk seed → root mismatch on all three corpora
- [x] implement: `foldReconciledStorageRoot` seeds row 0 from `Branch(accPrefix)` and replays the touched keys so `followAndUpdate`'s unfold reads each deeper on-disk branch and fold keeps its untouched children blinded; a missing branch (`errStorageBaseNotBranch`) = fresh, empty wall, no siblings. Discovery: reconciliation rides the proven seed+replay+fold, not a hand-rolled on-disk descent (that reimplements the engine); the hand-rolled `foldNode` stays the fresh no-read fast path
- [x] fixtures + parity for single-survivor collapse (`TestTruthtreeFold_SingleSurvivorCollapse`, delete-all-but-one-nibble → depth-64 extension-node root) and delete-to-empty (`TestTruthtreeFold_DeleteToEmpty`, delete every slot → empty-storage root + storage-root-branch delete)
- [x] differential branch byte-parity vs sequential on all three — `requireReconciledFoldParity` applies the fold's in-place deletes + returned deferred updates over the block-1 disk and `requireBranchParity` asserts the branch store equals the sequential oracle (single-account whale ⇒ every stored branch is a storage branch)
- [x] run tests — `go test ./execution/commitment/ -count=1` green, `-race` clean, `make lint` 0 issues

### Task 4: Deferred branch-update emission

**Files:**
- Modify: `execution/commitment/truthtree_fold.go`
- Modify: `execution/commitment/truthtree_fold_test.go`

- [x] each branch node emits a `DeferredBranchUpdate` for its prefix via the same encoder `foldMounted` uses (`foldCtx.emitBranchUpdate` calls `branchEncoder.EncodeBranch` + `getDeferredUpdate`); the fresh direct recursion threads the hex-nibble prefix and collects into `fc.deferred`, surfaced by `foldFreshStorageRootDeferred`. The pure `foldFreshStorageRoot` stays emission-free so the Task 2 alloc ceiling holds (verified 42.6 MB/op unchanged — `cellData` proved non-escaping on the no-emit path)
- [x] branch byte-parity oracle: `TestTruthtreeFold_FreshDeferredEmission` runs the sequential fresh-whale engine (`seqFreshBranchOracle`), applies the fold's deferred updates to an empty branch store, and `requireBranchParity` asserts prefix-for-prefix byte equality (single-account whale ⇒ every stored branch is a storage branch); slots 5k/50k
- [x] fail-closed: `foldFreshStorageRootDeferred` drops (and pool-returns) every collected update on any fold error; `TestTruthtreeFold_FreshDeferredFailClosed` folds one emitting sub-branch then a malformed sibling and asserts `err != nil` with `deferred == nil`
- [x] run tests — `go test ./execution/commitment/ -count=1` green, `-race` clean, `make lint` 0 issues

### Task 5: Account-plane fold + inline depth-64 seam

**Files:**
- Modify: `execution/commitment/truthtree_fold.go`
- Modify: `execution/commitment/truthtree_fold_test.go`

- [x] recurse account branches (mixed depths) and cross the depth-64 seam inline — `foldNode` now branches on `branchDepth >= 64` (storage plane) vs account plane; a child with `plainKey != nil && bitmap != 0` is the seam. Three seam representations matching the sequential engine's stored cell: single storage slot held inline as `storageAddr` (`childSeamAccountInline`), single storage first-nibble over a sub-branch held as storage-extension + sub-branch hash (`childSeamAccountExt`), and a ≥2-nibble storage branch held as the branch-root hash (`childSeamAccount`). `foldFreshAccountRoot`/`foldFreshAccountRootDeferred` fold a whole fresh account trie → state root + branch records
- [x] apply the mount-wall strip so a leaf-task subtree returns the parent-stitchable cell (invariant M) — `foldFreshAccountSubtreeCellDeferred`/`foldSubtreeCell` fold a top-nibble account subtree, wrap its branch hash in a full-prefix cell, and call `stripCellToMountWall`; `TestTruthtreeFold_AccountPlaneMountWall` stitches those cells into the finale root wall and reproduces the oracle root + branch store
- [x] tests vs oracle: `buildMixedCorpus` account-plane subtree, depth-{2,64,65} byte parity — `TestTruthtreeFold_AccountPlaneFresh` (2k/20k keys, direct whole-trie fold) + `TestTruthtreeFold_AccountPlaneMountWall` (mount-wall stitched); `requireSpansAllPlanes` asserts the corpus exercises account branches (depth 2), storage-root seams (depth 64), and interior storage branches (depth 65). Discovery: the account-with-storage cell has three distinct on-disk encodings (inline slot / storage-extension / branch-root hash); the root hash matched from the start, but stored branch-record bytes only reached parity once all three were reproduced
- [x] run tests — `go test ./execution/commitment/ -count=1` green, `-race` clean, `make lint` 0 issues, `make erigon integration` builds

### Task 6: Flag plumbing + wire into the leaf task (parallel regime only)

**Files:**
- Modify: `execution/commitment/config.go` (`TrieConfig` flag field)
- Modify: `execution/commitment/parallel_patricia_hashed.go` + `streaming_commitment.go` (both `newFoldPool` sites copy the field)
- Modify: `execution/commitment/parallel_mount.go` (dispatchFrontier caller)
- Modify: `execution/commitment/fold_pool.go` (`foldLeafTask` branch on flag)
- Modify: `cmd/utils/flags.go` (`--experimental.truthtree-fold`, guarded by `--experimental.parallel-commitment`)

- [x] add the flag to `TrieConfig` (default off) and thread through both `newFoldPool` sites + `parallel_mount.go` — `TrieConfig.TruthtreeFold`; the parallel `newFoldPool` (`parallel_patricia_hashed.go`) copies `p.cfg.TruthtreeFold` into `foldPool.truthtreeFold`, which flows to `processMounted`'s `dispatchFrontier` via the pool (no change needed in `parallel_mount.go` — the pool carries the flag). CLI: `--experimental.truthtree-fold` (`cmd/utils/flags.go` + `node/cli/default_flags.go` + `cmd/integration/commands/flags.go`) → `ethconfig.Config.ExperimentalTruthtreeFold` → `statecfg.ExperimentalTruthtreeFold` → `NewSharedDomains` sets `trieCfg.TruthtreeFold` only when the parallel variant is selected
- [x] gate streaming **inert**: flag only selects `foldNode` in the parallel regime; streaming keeps the current fold in Phase 1 — streaming's `newFoldPool` deliberately leaves `truthtreeFold` false, and `domain_shared.go` only sets `TruthtreeFold` when `Variant == VariantParallelHexPatricia`, so streaming/sequential keep mount+replay even with the flag on
- [x] `foldLeafTask` branches on the flag: `foldNode` vs current replay — `directLeafEligible` routes a provably-fresh (empty mounted slot ⇒ no on-disk siblings) pure account-plane branch leaf through the direct recursion (`foldFreshAccountSubtreeCellDeferred`); everything else stays on replay. Discovery: the ModeParallel prefix trie carries nil terminator updates (value = re-read from ctx), so a fail-closed `resolveSubtreeUpdates` pre-pass materializes them from the worker's ctx before the pure fold; a resolved empty/deleted leaf falls back to replay (which drops it) — the fresh direct fold would wrongly hash it
- [x] parity chains green flag-on AND flag-off (root + branch, N≥3 batches, encode/restore) on whale + mixed + incremental corpora — `TestTruthtreeFold_LeafFlagParity` runs `balancedBatches` (account plane) + `megaWhaleBatches` (whale + incremental re-touch/delete) through sequential vs flag-off vs flag-on parallel, root + stored-branch byte parity after every batch across the encode/restore restart, and asserts the direct path fired; `TestTruthtreeFold_FreshDeleteFallback` covers the set-then-delete fallback guard
- [x] run tests — `go test ./execution/commitment/ -count=1` green, `-race` clean, `make lint` 0 issues, `make erigon integration` builds, flag registered in `erigon --help`

### Task 7: Prove subsumption, then route merge/fresh-whale through the recursion

**Files:**
- Modify: `execution/commitment/fold_pool.go`
- Modify: `execution/commitment/streaming_deep_fold.go`

- [x] route the fresh-whale storage leaf through `foldNode` where it reproduces `foldFreshStorage` byte-for-byte — `foldWhaleLeaf`'s fresh branch now calls `foldFreshWhaleStorage`, which flag-on folds a real (`OnesCount16(bitmap) >= 2`) storage-root branch through `foldFreshStorageRootDeferred` (the proven direct recursion) instead of the mount+replay fan-out. The nil-update storage leaves are materialized via `resolveSubtreeUpdates` first (same pre-pass the account leaf uses); a resolved delete falls back to `foldFreshStorage` (which drops it). Discovery: `foldNode`'s depth-64 keccak builds a 1-slot branch for a single-first-nibble storage top — but the sequential engine collapses that to an extension via `storageRootFromSingleChild`, so the `>=2` guard keeps single-nibble tops on `foldFreshStorage`. `mergeChildrenAtPrefix`/`aggregateMountedStorageRoot`/`storageRootFromSingleChild`/`stripCellToMountWall` stay — they serve the seedable on-disk-sibling merge/seam/reconciliation paths the fresh `foldNode` cannot reproduce. The parallel topology stays on `foldFreshStorage`; the serial direct routing lands the buffer-reuse alloc win, and the parallel-direct topology is Task 9's perf gate ("do not tune topology + fold together")
- [x] `TestDeepFold_FreshWhaleParallelStorage_Parity` corpus + the mega-whale re-touch chain green flag-on — `TestTruthtreeFold_WhaleStorageFlagParity` runs the fresh-whale-parallel corpus (`buildSubsetTouchedWhale(nibs 3,7) + buildMixedCorpus`) flag-on vs flag-off vs sequential (root + stored-branch byte parity) via `requireFreshWhaleFlagParity`, then `requireFlagLeafParity(megaWhaleBatches(20_000))` for the N>=3 chain, asserting the direct whale path fired (`directWhaleStorageFolds`). `TestTruthtreeFold_FreshWhaleStorageDeleteFallback` pins the delete-fallback guard fired. The flag-off `TestDeepFold_FreshWhaleParallelStorage_Parity` still holds (unchanged path)
- [x] flag-on == flag-off == sequential on every corpus — full `execution/commitment` suite green (15.9s) flag-off; `TestTruthtreeFold_LeafFlagParity` (now also routing the mega-whale storage direct flag-on) + the two new whale tests assert flag-on == flag-off == sequential, root + branch, per batch across the encode/restore restart
- [x] run tests — `go test ./execution/commitment/ -count=1` green, `-race` clean (whale + flag-on + mega-whale), `make lint` 0 issues, `make erigon integration` builds

### Task 8: Error, cancel, and pin discipline

**Files:**
- Modify: `execution/commitment/truthtree_fold_test.go`

- [x] failable MockState on the recursion's `Branch`/read paths — fail-closed (drop deferred, write nothing). `TestTruthtreeFold_ReconciledReadFailsClosed` injects a `failState` Branch fault on the reconciling fold (`foldReconciledStorageRoot`) at the depth-64 seed and at a deeper on-disk-sibling unfold; both surface `errInjected`, return nil deferred + zero storage root, and leave the branch store byte-unchanged (`requireBranchesUnchanged`). `TestTruthtreeFold_ResolveSubtreeReadFailsClosed` pins the nil-update pre-pass fails closed on the first read error without crossing planes. The two fresh deferred paths were already covered by Task 4's `TestTruthtreeFold_FreshDeferredFailClosed`. Reused the existing `failState`/`injector` harness; factored `makeFoldPoolFactory` to pool the fold onto a fault-injecting ctx
- [x] ctx-cancel mid-fold: clean unwind; each goroutine's reads stay in its own `BeginTemporalRo` pin. `TestTruthtreeFold_ReconciledContextCancel` runs the reconciling fold on an already-cancelled context → `context.Canceled`, nil deferred, store untouched, then a fresh live-context fold reproduces the sequential root + branches (`requireReconciledFoldParity`). `TestTruthtreeFold_ReconciledPinScope` runs the fold through a `pinState` ctx that flags any read after its own cleanup and asserts root+branch parity with zero post-cleanup reads — the fold confines its `newDeferredStorageWorker` reads to its own pin scope
- [x] `go test -race ./execution/commitment/` clean flag-on — race-clean on the flag-on parity chains (`LeafFlagParity`, `WhaleStorageFlagParity`, `FreshDeleteFallback`, `FreshWhaleStorageDeleteFallback`) plus the new Reconciled read/cancel/pin tests
- [x] run tests — `go test ./execution/commitment/ -count=1` green (15.6s), `-race` clean flag-on, `make lint` 0 issues

### Task 9: Performance gate

**Files:**
- Modify: `execution/commitment/parallel_streaming_bench_test.go`

- [x] `Benchmark_TruthtreeFold_Gate` (whale + mixed + incremental-seeded corpus, flag-on vs flag-off, `numWorkers=NumCPU`, alloc + time) added to `parallel_streaming_bench_test.go`; `runParallelBenchCfg` (config-parameterized `runParallelBench`) + `runIncrementalBenchCfg` (seed batch1 untimed → measure batch2). Numbers below (Apple M5 Max, 18 workers, `-benchtime=5x -count=4`)
- [x] acceptance evaluated: **flag-on does NOT beat flag-off on the whale — it is 4.56× slower** (623.8 vs 136.8 ms/op); mixed regresses ~14% (4.10 vs 3.59 ms/op); incremental is wall-clock parity. This is the topology trade-off Task 7 flagged: flag-on routes the fresh-whale storage leaf through the **serial** `foldFreshStorageRootDeferred`, replacing `foldFreshStorage`'s per-nibble parallel fan-out, so the 750k storage subtree folds single-threaded. The fold-body buffer-reuse win (Task 2 isolated: ~44 MB) is real but swamped at the whole-Process level by ~300 MB of shared setup allocation, so B/op is ~flat (344.6 vs 350.5 MB, +1.7%). Alloc-ceiling holds engine-wide: whole-Process B/op stays ~345 MB regardless of flag (never regressing toward the 575 MB naive figure), and `TestTruthtreeFold_AllocCeiling` stays green (~44 MB fold body)
- [x] whale wall-clock regresses → **stopped, recorded, did NOT tune topology + fold together** (per the plan rule). The parallel-direct fold topology (fan the direct recursion out per-nibble instead of serial) is the follow-up; flag stays default-off — the gate result gates the flip: the direct fold is not ready to flip default-on under the parallel regime until the storage fold is parallelized
- [x] ran benches — recorded below; `make lint` 0 issues

**Results** (Apple M5 Max, `numWorkers=NumCPU=18`, `-benchtime=5x -count=4`, averaged):

| corpus | arm | ns/op | B/op | allocs/op |
|---|---|---|---|---|
| whale750k | flag-off | 136.8M | 344.6M | 5.87M |
| whale750k | flag-on | 623.8M (**4.56× slower**) | 350.5M (+1.7%) | 6.66M (+13%) |
| mixed20k | flag-off | 3.59M | 16.6M | 171k |
| mixed20k | flag-on | 4.10M (+14%) | 14.4M (noisy) | 179k (+4.7%) |
| incremental-whale120k | flag-off | 28.5M | 115M | 1.233M |
| incremental-whale120k | flag-on | 28.0M (parity) | 133M (+15%) | 1.241M (+0.6%) |

Conclusion: under the parallel regime the direct fold serializes the whale storage fold and loses to the per-nibble fan-out on wall-clock; it is an alloc-neutral-to-slightly-worse change at whole-Process granularity (the buffer-reuse win lives at the fold-body level, gated separately by Task 2). Keep the flag default-off; the parallel-direct storage topology is required before a default flip. Incremental (seedable/replay path) is inert under the flag, as designed.

### Task 10: Delete the subsumed fold path (post-flip)

**Files:**
- Modify: `execution/commitment/fold_pool.go`, `streaming_deep_fold.go`, `fold_dag.go`

- [x] BLOCKED — not automatable, intentionally not executed: this is a post-flip cleanup gated on the Post-Completion replay arm (manual mainnet/sepolia replay-validation, cannot run in-session) AND on flipping the default. Task 9's perf gate already evaluated the flip and deferred it — flag-on is 4.56× slower on the 750k whale because the direct fold serializes the storage subtree, so the flag stays default-off until a parallel-direct storage topology lands. Deleting the subsumed replay path now would delete the faster path, regress performance, and break the plan's flip gating (correctness > performance ordering). Left for a follow-up after the replay arm passes and the perf regression is resolved.
- [x] record net line delta (skipped - depends on the deletion above, which is intentionally not executed)
- [x] run tests (skipped - no code change this iteration; the suite remains green from Task 9)

### Task 11: Verify acceptance criteria

- [x] all Overview goals verified against code: direct fold (`foldNode`), on-disk reconciliation (`foldReconciledStorageRoot` + `TestTruthtreeFold_OnDiskSiblingReconciliation`), deferred emission (`foldFreshStorageRootDeferred`/`emitBranchUpdate` + `TestTruthtreeFold_FreshDeferredEmission`), account-plane seam (`foldFreshAccountSubtreeCellDeferred` + `TestTruthtreeFold_AccountPlaneFresh`/`_AccountPlaneMountWall`), dispatch reused (`directLeafEligible` gates the leaf task on `fp.truthtreeFold`, DAG/pool/stitch unchanged), buffer-reuse invariant (`foldCtx` reused-scratch + `TestTruthtreeFold_AllocCeiling`)
- [x] edge cases covered by tests: single-survivor collapse / extension-topped root (`TestTruthtreeFold_SingleSurvivorCollapse`, delete-all-but-one-nibble → depth-64 extension-node root), delete-to-empty (`TestTruthtreeFold_DeleteToEmpty`), encode/restore restart (`TestTruthtreeFold_LeafFlagParity` folds across the encode/restore round-trip), whale + mixed + incremental (`TestTruthtreeFold_WhaleStorageFlagParity` fresh-whale-parallel corpus + `megaWhaleBatches` incremental re-touch/delete, `buildMixedCorpus` account plane)
- [x] `make test-short` (repo-wide: 239 packages ok, 0 FAIL) + commitment package `-count=1` (15.8s) AND `-race` clean (124.9s, full package); `make lint` 0 issues. Chose the `make test-short` + package `-count=1 -race` arm over `GOGC=80 make test-all` — the change is flag-gated (default off) and fully contained in `execution/commitment`, so the package race run is the load-bearing gate and test-short covers the flag-plumbing packages
- [x] alloc-ceiling bench green: `TestTruthtreeFold_AllocCeiling` 42.6 MB/op (under the 96 MB ceiling, near the ~47 MB proto figure); `Benchmark_TruthtreeFold_FreshWhaleAlloc` 44.6 MB/op / 4.53M allocs, matching the recorded Task 2 figure and never regressing toward 575 MB

### Task 12: [Final] Update documentation

- [x] package doc comments if fold-facing names changed — added `execution/commitment/doc.go` with a package overview orienting readers to the two leaf-fold regimes (mount+replay vs the direct buffer-reuse `foldNode` recursion in `truthtree_fold.go`) and the `--experimental.truthtree-fold` flag (`TrieConfig.TruthtreeFold`, parallel regime only, default off, both paths byte-identical). The fold-facing symbols themselves already carry doc comments from Tasks 1–7. Per the repo comment policy (no git-tag/task references or scope narration in source), the `truthtree-rollback-base` rollback tag (`46ffcc1b8c`) and the Phase 2 (streaming unfold-on-touch) follow-up are recorded in the commit message / this plan, not in code
- [x] move this plan to `docs/plans/completed/`

## Post-Completion

**Manual verification:**
- Mainnet/sepolia replay-validation arm (root + branches every block) with the flag on, before flipping the default.
- High-core (≥32) bench on a real whale block.

**External decisions:**
- Flag-default flip only after the replay arm passes.
- Phase 2: streaming unfold-on-touch + streaming/parallel convergence — separate plan.

## Known risks

- The proto validated only the fresh/storage/root-only math; **on-disk-sibling reconciliation, deferred emission, and the account-plane seam are net-new** (Tasks 3–5), gated by the differential harness with on-disk-state fixtures — the real risk surface.
- The `foldPool` serves both engines; the flag MUST gate streaming inert in Phase 1 or it silently changes the streaming root path.
- Buffer-reuse is load-bearing (575 MB naive regression); the Task 2 alloc-ceiling bench must stay green through every later task.
- Deep change to a consensus-path fold — mitigated by flag-gating, flag-on==flag-off==sequential parity, the replay arm, and rollback tag `truthtree-rollback-base`.
