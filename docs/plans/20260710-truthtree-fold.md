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

- [ ] only after Tasks 3–5 pass: route merge/seam/fresh-whale leaves through `foldNode` where it reproduces `mergeChildrenAtPrefix`/`aggregateMountedStorageRoot`/`storageRootFromSingleChild`/`foldFreshStorage` byte-for-byte; keep any hand-off (`stripCellToMountWall`) the DAG still needs
- [ ] `TestDeepFold_FreshWhaleParallelStorage_Parity` + full deep-fold suite green flag-on
- [ ] flag-on == flag-off == sequential on every corpus
- [ ] run tests — must pass before next task

### Task 8: Error, cancel, and pin discipline

**Files:**
- Modify: `execution/commitment/truthtree_fold_test.go`

- [ ] failable MockState on the recursion's `Branch`/read paths — fail-closed (drop deferred, write nothing)
- [ ] ctx-cancel mid-fold: clean unwind; each goroutine's reads stay in its own `BeginTemporalRo` pin
- [ ] `go test -race ./execution/commitment/` clean flag-on
- [ ] run tests — must pass before next task

### Task 9: Performance gate

**Files:**
- Modify: `execution/commitment/parallel_streaming_bench_test.go`

- [ ] `Benchmark_DeepStorageWhale` + a mixed + an incremental (seeded) corpus, flag-on vs flag-off, `numWorkers=NumCPU`, alloc + time; record numbers here
- [ ] acceptance: flag-on beats flag-off on the whale, no regression on mixed/incremental; alloc-ceiling holds engine-wide
- [ ] if any corpus regresses: stop, record, do not tune topology + fold together
- [ ] run benches — record; `make lint` clean

### Task 10: Delete the subsumed fold path (post-flip)

**Files:**
- Modify: `execution/commitment/fold_pool.go`, `streaming_deep_fold.go`, `fold_dag.go`

- [ ] after parity + the replay arm (Post-Completion): flip default, delete the replaced replay path + any merge/collapse code Task 7 subsumed; remove the flag
- [ ] record net line delta (target: negative); full suite + branch-parity chains green
- [ ] run tests — must pass before next task

### Task 11: Verify acceptance criteria

- [ ] all Overview goals: direct fold, on-disk reconciliation, deferred emission, account-plane seam, dispatch reused, buffer-reuse invariant
- [ ] edge cases: single-survivor collapse, extension-topped, delete-to-empty, encode/restore, whale + mixed + incremental
- [ ] `GOGC=80 make test-all` (or `make test-short` + package `-count=1 -race`); `make lint` clean
- [ ] alloc-ceiling bench green

### Task 12: [Final] Update documentation

- [ ] package doc comments if fold-facing names changed; note the `truthtree-rollback-base` tag + the Phase 2 (streaming unfold-on-touch) follow-up
- [ ] move this plan to `docs/plans/completed/`

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
