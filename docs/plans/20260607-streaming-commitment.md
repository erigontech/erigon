# Streaming Commitment (prepare-on-touch, Process folds)

## Overview

Overlap commitment fold work with block execution. Today the parallel trie
accumulates touched keys into a prefix trie during execution and does ALL the
fold work at commitment time (`Process`). This plan adds a **streaming** mode:
as keys are touched, the owning split-point subtree is (re-)folded in the
background by pooled workers, so by commitment time `Process` collapses to a
merge of already-folded split cells.

Reuses the proven engine wholesale — `followAndUpdate` / `unfold` / `foldMounted`
/ `mountTo` / `computeCellHash` / `concurrentStorageRoot` / the prefix trie are
**not modified**. The only new code is orchestration: a `StreamingCommitter` that
owns per-split state, schedules background re-folds, replaces per-split deferred
branch updates, and merges at `Process`.

Headline win: hide the per-split fold (and its DB-read `unfold` latency) under
execution. Builds directly on the deep storage fan-out already proven this
session (whale 3.94×, root+branch parity == sequential).

Background: `/Users/awskii/org/wrk/HANDOFF-parallel-storage-fold.md`.

## Context (from discovery)

Target worktree/branch: `/Users/awskii/org/wrk/erigon-prepare-fold` @
`awskii/parallel_prepare_fold` (all session work uncommitted).

Files/components involved:
- `execution/commitment/parallel_mount.go` — `processMounted`, `dfsSubtreeDeep`,
  `concurrentStorageRoot`, `setAccountStorageRoot`, the stitch, env gates
  `cmtTiming`/`cmtDeep`. The streaming committer lives here or a sibling file.
- `execution/commitment/parallel_patricia_hashed.go` — `ParallelPatriciaHashed`,
  `Process` (mount gate at the `ERIGON_CMT_MOUNT` check), worker pool, deferred
  apply (`applyDeferredUpdates`, `TakeDeferredUpdates`, `appendDeferred`).
- `execution/commitment/commitment.go` — `Updates`, mode enum (`ModeDirect`,
  `ModeParallel`, `ModeUpdate`), `TouchPlainKey` / `TouchPlainKeyDirect`
  (ModeParallel branch at ~:1746 inserts into the prefix trie carrying the
  value), `TrieVariant` consts, trie factory (~:162).
- `execution/commitment/parallel_update.go` / `prefix_trie.go` — the prefix trie
  (`Insert` order-independent; `prefixNode{ext, children, plainKey, update,
  subtreeCount, bitmap}`; `update==nil` means "re-read from ctx"); `prepareDFS`
  with the `nodeDepth>=64` storage-split ban.
- `execution/commitment/hex_patricia_hashed.go` — `followAndUpdate` (monotonic),
  `foldMounted`, `computeCellHash` (account uses `cell.hash` as storageRoot,
  :1239), `mountTo`.
- `execution/stagedsync/calc_state.go` / `committer.go` — produce updates via
  `FlushToUpdates` → `TouchPlainKeyDirect`.
- **Single funnel**: `SharedDomainsCommitmentContext.TouchKey`
  (`commitmentdb/commitment_context.go:246`) → `updates.TouchPlainKey(...)`, the
  SAME `Updates` entry the MockState/`WrapKeyUpdates` tests use. So `ModeStreaming`
  on `Updates` covers production (`sdctx.TouchKey`) and tests uniformly — no
  separate hook. The **inline-touch** path (`SharedDomains.SetDisableInlineTouchKey`,
  `db/state/execctx/domain_shared.go:626`) is what makes touches arrive *during*
  execution (vs batched at commit) — that inline arrival is the overlap source the
  streaming committer exploits.
- Wiring for selection: `cmd/utils/flags.go` (`ExperimentalParallelCommitmentFlag`),
  `node/ethconfig/config.go`, `node/eth/backend.go` (~:309), `node/cli/default_flags.go`,
  `db/state/statecfg/state_schema.go` (`ExperimentalParallelCommitment`),
  `db/state/execctx/domain_shared.go` (~:175 variant selection),
  `cmd/integration/commands/flags.go`, `db/state/squeeze.go`.
- Correctness oracle: `execution/commitment/wide_nested_parallel_test.go`
  (`branchDiff`, `requireIncrementalEquiv`) and `deep_integration_test.go`
  (`TestDeepIntegration_BranchParity`).

Related patterns:
- Deferred branch updates already exist (`setDeferUpdates`+`LeaveDeferredForCaller`
  → nothing written mid-fold; flushed at Process). Streaming holds them per-split
  and replaces.
- The stitch in `processMounted` (place split cells in base row, fold) is the
  Process merge to reuse.

Dependencies/constraints proven this session (MUST respect):
1. A shared hph cannot absorb out-of-order keys (monotonic `followAndUpdate`;
   128-row grid = one path). Never keep a persistent per-split hph mutated by
   touches — re-fold statelessly from the prefix-trie key set instead.
2. An account leaf can't fold until its storageRoot is ready (bottom-up:
   storage → storageRoot → account leaf).
3. Prefix trie `Insert` is order-independent — the correct persistent structure.

## Development Approach

- **Testing approach: TDD** (repo mandates Red→Green→Refactor; we have a hard
  parity oracle so Red→Green is natural). For each behavior change, write the
  failing parity/branch test first, then the orchestration to make it green.
- Complete each task fully (impl + tests green) before the next.
- Small, focused changes; reuse the engine, never modify `followAndUpdate`/
  `unfold`/`foldMounted`/`computeCellHash`/`mountTo`/`concurrentStorageRoot`/
  the prefix trie.
- **Every task includes new/updated tests** (success + error/edge), all passing
  before moving on.
- Keep `default` and `ERIGON_CMT_MOUNT=1` paths green throughout (no regression).
- Gate everything behind the new mode/flag; default behavior unchanged.
- **Shippable-unit framing**: Tasks 1-3 (lazy fold-at-Process, no background) are
  a correctness-complete, gateable unit — they make streaming *correct*. Tasks 4+
  add the background overlap, whose only real win is on a live node (Post-Completion).
  Land 1-3 first; treat 4+ as the optimization layer.
- **Dependency note**: this plan reuses the **uncommitted** deep-fold work in this
  worktree (`concurrentStorageRoot`/`dfsSubtreeDeep`/`setAccountStorageRoot` in
  `parallel_mount.go`). A clean checkout would not compile — implementation assumes
  that work is present (committed or staged) first.

## Testing Strategy

- **Unit/parity tests** (required every task): the oracle is **streaming root ==
  sequential root AND every stored branch matches** (template
  `TestDeepIntegration_BranchParity` + `branchDiff`). Use MockState corpora:
  `buildMixedCorpus`, `buildBigAccountCorpus`, `genRandomAccountsStorage`,
  `genAccountsWithNestedStorage`, the whale.
- **Concurrency tests**: interleave touches with background folds; a
  re-touch-after-fold scenario; run under `-race`.
- **Multi-block/incremental**: block 1 batch-commit, block 2 streaming → parity
  (exercises existing-DB unfold in the streaming fold path).
- **Benchmarks** (measurement tasks): simulated-execution overlap + re-fold-count
  instrument. (Wall-clock overlap on a real node is Post-Completion — MockState
  has no execution to overlap with.)
- No e2e/UI tests in this repo area.

## Progress Tracking

- Mark `[x]` immediately when done. `➕` for newly discovered tasks, `⚠️` for
  blockers. Keep this file in sync; update scope notes inline.

## Solution Overview

`StreamingCommitter` (new, orchestration only) wraps the existing pieces:

- **Owns**: the prefix trie (split structure), the hph worker pool, a base trie
  (the upper/shared trie), and per-split-point state
  `split{prefix, cell, dirty, gen, deferred []*DeferredBranchUpdate, mu}`.
- **TouchKey(hk, pk, upd)**: `prefixTrie.Insert(hk, pk, upd)` (order-independent,
  carries `upd` or nil=read-from-ctx) → locate owning split → mark dirty
  (`gen++`) → optionally enqueue for background fold.
- **Background fold worker** (during execution): pull a dirty split, snapshot its
  `gen`; grab a pooled hph; `mountTo(base)`; `followAndUpdate` the split's keys in
  **sorted order via prefix-trie in-order walk** (uses carried `upd`, else
  ctx-read); `foldMounted` → cell (NEVER fold to root); a big-storage account uses
  `concurrentStorageRoot`. Capture the fold's deferred branch updates. CAS:
  store `{cell, deferred}` only if `gen` unchanged; if it bumped, leave dirty.
- **Branch updates**: deferred, held **per split, replaced** on each re-fold.
  Each `DeferredBranchUpdate` carries `prev` = `ctx.Branch(prefix)` captured at
  fold time and the final apply does `Merge(prev, update)` (commitment.go:605,
  :678). **Replace-per-split is sound ONLY because `prev` is invariant across
  re-folds** — nothing is flushed mid-block, so every re-fold reads the same
  on-disk pre-image. This invariant is the one novel correctness claim and is
  trivially true from-scratch (`prev` empty) — it MUST be tested against a
  **non-empty DB** (Task 3).
- **Process()**: fold remaining dirty splits, then merge split cells bottom-up
  via the existing stitch → root. Flush = apply split sets, **then** the merge
  set. Split-internal prefixes and the merge's upper prefixes are *almost*
  disjoint but the split-boundary prefix can collide with the merge's bottom row;
  the flush MUST use a duplicate-prefix-flush guard (as `CollectDeferredUpdate`
  does, commitment.go:644) so no prefix is written twice and last-writer-wins is
  correct. `ApplyDeferredBranchUpdates` (commitment.go:462) does NOT dedup across
  the slice — the flush contract must (Task 3).

Selection: new `ModeStreaming` (in `Updates`) + `--experimental.streaming-commitment`
→ `VariantStreamingHexPatricia`, layered on the mount path.

Key design decisions & rationale:
- **Stateless re-fold from keys** (not a mutated persistent hph): sidesteps the
  monotonic/out-of-order panic; reuses the proven engine verbatim.
- **Defer all cells to fold**: a leaf's hash depends on its depth, which a later
  sibling can change — so nothing is hashed at touch.
- **Replace-per-split deferred**: keeps commitment history to the final changed
  prefixes regardless of re-fold count.
- **Correctness == the mount/deep fold** (already byte-identical to sequential);
  streaming only changes *when* and *how often* splits fold, not the result.

## Technical Details

- `ModeStreaming`: `TouchPlainKey` (val→read later from ctx) and
  `TouchPlainKeyDirect` (carry `*Update`) both `prefixTrie.Insert(hashedKey,
  internKey, updateOrNil)` and notify the committer of the dirtied split. Mirrors
  the existing ModeParallel branches; `update==nil` already means ctx-read in the
  fold (`followAndUpdate(hk, pk, nil)`).
- Split identity = the prefix-trie split-point prefix (top-nibble subtree today;
  reuse `prepareDFS`-style split detection, but the streaming committer maps a
  touched key → its split by walking the prefix trie from the root branch).
- `gen` is a per-split counter; CAS = compare `gen` at fold start vs store time.
- Worker pool: reuse `p.workerPool` (sync.Pool, concurrent-safe). Background
  scheduler is a bounded goroutine pool; the deep fan-out's nested errgroup must
  use a separate group (no shared SetLimit) to avoid starvation/deadlock.
- Re-fold reads the same base store because deferred updates are never applied
  mid-block; `ResetContext` per fold worker from the trie ctx factory.
- Per-block lifecycle: `Reset()` clears per-split state, prefix trie, base trie
  between blocks; `Release()` returns pooled hphs.

## What Goes Where

- **Implementation Steps** (`[ ]`): all code + tests + benchmarks in this repo.
- **Post-Completion** (no checkboxes): live-node wall-clock measurement on
  `/Users/awskii/dev`; deep-path gap closure beyond streaming (it shares the
  oracle but is broader); any consuming-config changes.

## Implementation Steps

### Task 1: StreamingCommitter — promote the existing prototype to a lazy fold-at-Process path

The lazy path is ~80% already written: `prepare_on_touch_test.go`'s `preparedSplits`
already does route-by-nibble `touch()` → `followAndUpdate` and `process()` =
`foldMounted` each split + the exact stitch + parity vs sequential
(`TestPrepareOnTouch_Parity`). Promote it; the only real change vs the prototype is
driving folds off the **persistent prefix trie** (stateless re-fold from keys, per
constraint 1) instead of 16 persistent hphs.

**Files:**
- Create: `execution/commitment/streaming_commitment.go`
- Create: `execution/commitment/streaming_commitment_test.go`
- Modify: `execution/commitment/wide_nested_parallel_test.go` (add a `streaming` arm to `runIncremental`/`requireIncrementalEquiv`)

- [x] define `StreamingCommitter` owning prefix trie ref, base trie, worker pool ref, trieCtxFactory, and `map[splitKey]*splitState{cell,dirty,gen,deferred,mu}` (the `dirty/gen/mu` fields are used by the Task-4 scheduler; struct is shared) — base trie is built per-`Process` in the lazy path (a persistent base buys nothing until the Task-4 scheduler)
- [x] `TouchKey(hk,pk,upd)` = prefix-trie `Insert` + locate/create split + mark dirty (gen++); no folding yet
- [x] `Process()` = fold every dirty split (stateless `mountTo`+`followAndUpdate`+`foldMounted`, reuse) → merge via the existing stitch → root
- [x] `Reset()`/`Release()` lifecycle
- [x] **add a `streaming` arm to `runIncremental`/`requireIncrementalEquiv`** (wide_nested_parallel_test.go) next to the `parallel` arm — the committer then inherits the WHOLE existing incremental + deletes + worker-count + branch-parity matrix for free (`TestVerifyParallel_*Incremental`, `*StorageIncrementalDeletes`). Streaming arm feeds `nil` updates (ctx-read at fold), matching how the proven barrier arm (`WrapKeyUpdates` ModeParallel) inserts `nil` and re-reads the full account from ctx — a carried *partial* update would drop the DB codeHash. Carried-update support is Task 6.
- [x] write the one net-new Task-1 test: feed `buildMixedCorpus` via `TouchKey` in **randomized (execution) order** (NOT sorted — order-independence is the premise; the in-order walk re-sorts at fold) → root+branches == sequential (`TestStreaming_RandomOrderParity`, workers 1/4/8)
- [x] run tests (incl. `-race`) — must pass before next task

### Task 2: Per-split stateless re-fold + deferred capture (reuse deep fan-out)

**Files:**
- Modify: `execution/commitment/streaming_commitment.go`
- Modify: `execution/commitment/streaming_commitment_test.go`

- [x] `foldSplit(s *splitState)`: pooled hph, `mountTo(base)`, in-order-walk `followAndUpdate` of s's prefix-trie keys (carried `upd` or nil→ctx), `foldMounted` → cell; big-storage account → `concurrentStorageRoot` (reused via held `*ParallelPatriciaHashed.dfsSubtreeDeep`)
- [x] merged split cell **trims correctly** (leaf vs hash-only sub-branch — the proven `stitchSplitCells`) and big-storage account's storageRoot/CodeHash assembly is correct (CodeHash carried by the account `Update`; `setAccountStorageRoot` injects only the storageRoot) — verified by `TestStreaming_DeepBranchParity`
- [x] capture the fold's deferred branch updates into `s.deferred` (replace prior)
- [x] never fold to root (`foldMounted` returns the split cell at depth 1; never `fold()`-to-root in `foldSplit`)
- [x] write **branch-parity** test for an account-leaf split + a big-storage account split — exercises the trim/CodeHash correctness (reuse the `TestDeepIntegration_BranchParity` corpus) → `TestStreaming_DeepBranchParity` (workers 1/4/8)
- [x] existing-DB-unfold, deletes-across-64, storage-only/account-only are **inherited from the Task-1 `streaming` arm matrix** (`*Incremental`, `*StorageIncrementalDeletes`) — confirmed passing with `foldSplit` wired in (incl. `-race`)
- [x] run tests — must pass before next task

### Task 3: Replace-per-split deferred + Process merge/flush

**Files:**
- Modify: `execution/commitment/streaming_commitment.go`
- Modify: `execution/commitment/streaming_commitment_test.go`

- [x] `Process()` flush = apply split sets, THEN the merge set, through a **duplicate-prefix-flush guard** (`applyDeferredGuarded`, mirrors `CollectDeferredUpdate` commitment.go:644) so a colliding prefix re-reads the just-written value as `prev` and the merger accumulates instead of clobbering — last-writer-wins is cumulative. Bare `ApplyDeferredBranchUpdates` (commitment.go:462) no longer used by the committer's apply path.
- [x] mid-block folds defer only — the committer applies solely at `Process`. ⚠️ **Discovered exception**: the engine's `readBranchAndCheckForFlushing` (hex_patricia_hashed.go:1718) self-flushes a *pending* prefix to ctx when it re-reads it mid-fold, which a **delete-driven branch collapse** triggers. So the strict "nothing written mid-block" invariant holds only in the **collapse-free** regime; re-folding a *collapsed* split is unsound (the second fold reads the mutated branch as `prev` and double-applies — empirically corrupts the root). The lazy path folds each split **once** at Process, so it stays correct; the re-fold safety (isolate/gate self-flush) is a **Task-4** scheduler concern. Documented on `foldDirtySplits`.
- [x] **non-empty-`prev`** test (the novel claim): `TestStreaming_NonEmptyPrevRefold` — block-1 streaming commit, then block-2 re-fold (`foldDirtySplits`) N=4× before Process; after each re-fold the store equals the post-block-1 snapshot (deferred-only), final root+branches == sequential. Collapse-free corpus, the regime where the invariant holds.
- [x] **split∪merge collision** test: `TestStreaming_SplitMergeCollisionDedup` — two updates for one prefix over a non-empty pre-image, each supplying a different half of the full child set. Guard preserves both halves; bare apply silently clobbers the first (branch merger treats `branch2.afterMap` as authoritative). Asserts per-nibble cell source, not just afterMap.
- [x] re-fold-after-collapse: `TestStreaming_RefoldAfterCollapse` — focused assertion that a streaming **single fold at Process** over a delete/collapse batch on non-empty `prev` stays root+branch parity-clean (workers 1/4). Multi-re-fold of a collapsed split intentionally NOT asserted (Task-4, per the discovered exception above).
- [x] run tests — pass (incl. `-race`); `make lint` clean

### Task 4: Background fold scheduler + dirty/gen CAS (concurrency)

**Files:**
- Modify: `execution/commitment/streaming_commitment.go`
- Modify: `execution/commitment/streaming_commitment_test.go`

- [ ] bounded background goroutine pool; `TouchKey` enqueues dirtied splits
- [ ] `foldSplit` stores `{cell,deferred}` only if `gen` unchanged since fold start; else leave dirty (CAS)
- [ ] **storageRoot cross-dependency** (constraint 2): a storage-slot touch must bump the OWNING ACCOUNT split's `gen` (the account leaf embeds storageRoot), even mid big-storage fan-out. Define how a storage touch maps to its account split and bumps it.
- [ ] per-split `mu`; ensure deep fan-out (`concurrentStorageRoot`) uses a separate errgroup (no shared SetLimit) — no deadlock/starvation
- [ ] ➕ (from Task 3) re-fold safety vs engine self-flush: `readBranchAndCheckForFlushing` writes a pending prefix to ctx mid-fold on collapse re-read, so re-folding a *collapsed* split double-applies and corrupts. The scheduler MUST isolate fold writes (per-fold overlay ctx discarded unless committed) or skip re-folding splits that self-flushed. Repro: `foldDirtySplits` N× over a `sparseBatch2(..., deletes=true)` corpus diverges from sequential.
- [ ] add a **re-fold counter** (per split + total) to quantify wasted work (the only instrument we keep from the old Task 5)
- [ ] `Process()` drains: waits for in-flight, folds remaining dirty, then merges
- [ ] write concurrency test (`-race`): interleave `TouchKey` from multiple goroutines with background folds → root+branches == sequential
- [ ] write re-touch-after-fold test (account-granular): fold a split, touch it again → re-fold → parity
- [ ] write **storage-mid-account-fold** test: touch an account, start its fold, touch one of its storage slots mid-fold → storageRoot changes → assert re-fold happens and parity holds
- [ ] run tests (`-race`, `-count=10`) — must pass before next task

### Task 5: Single fold-trigger policy (defer alternatives to measurement)

**Files:**
- Modify: `execution/commitment/streaming_commitment.go`
- Modify: `execution/commitment/streaming_commitment_test.go`

Cut from the original plan: the three-way pluggable policy framework was YAGNI
before any realistic measurement. Ship ONE policy now; the re-fold counter lives
in Task 4. Add a second policy ONLY if Task 9 / the live run shows re-fold waste.

- [ ] implement one fold trigger: `foldEager` (fold-on-dirty) for hot splits, fall through to fold-at-Process for splits never scheduled
- [ ] write test: policy yields root+branches == sequential (correctness invariant)
- [ ] run tests — must pass before next task

### Task 6: ModeStreaming in Updates (provide Update OR read from ctx)

Prefer a **`streaming bool` flag on `Updates` alongside `ModeParallel`** over a new
`Mode` enum value: streaming wants ModeParallel's Insert/intern/deferred machinery
verbatim; a new enum forces byte-identical copies of every `Touch*`/`Init`/`Reset`/
`canDoConcurrent` switch branch (commitment.go switches). Only add a distinct enum
if the flag turns out to fork behavior in more than one switch.

**Files:**
- Modify: `execution/commitment/commitment.go`
- Modify: `execution/commitment/commitment_test.go` (or nearest)

- [ ] add `streaming` flag on `Updates` (or a `ModeStreaming` enum only if a flag proves insufficient); route Touch* to the ModeParallel `Insert(hashedKey, internKey, value)` path
- [ ] lifetime: **mirror the existing ModeParallel branch verbatim** — `u := new(Update); *u = *update` + `internKey(keyBytes)` already copy both the value and the key into the stable arena (commitment.go:1744-1746). No new design decision; just reuse.
- [ ] support nil update (ctx-read at fold) AND carried update, exactly as the prefix trie already does (`update==nil` → re-read from ctx)
- [ ] wire `Updates` to notify the `StreamingCommitter` of dirtied splits on insert (single funnel — both `sdctx.TouchKey` and `WrapKeyUpdates`/MockState reach it)
- [ ] confirm inline-touch (`SetDisableInlineTouchKey(false)`) routes touches through this path during execution; note the batched-vs-inline implication for overlap
- [ ] write one lifetime-regression test (mutate caller's `*Update`/key buffer after Touch returns → root unaffected)
- [ ] write test: carried-update parity AND nil/ctx-read parity, driven through the same `Updates.TouchPlainKey` funnel MockState uses (faithful to `sdctx.TouchKey`)
- [ ] run tests — must pass before next task

### Task 7: `--experimental.streaming-commitment` flag + variant wiring

**Files:**
- Modify: `cmd/utils/flags.go`, `node/ethconfig/config.go`, `node/eth/backend.go`,
  `node/cli/default_flags.go`, `db/state/statecfg/state_schema.go`,
  `db/state/execctx/domain_shared.go`, `cmd/integration/commands/flags.go`
- Modify: `execution/commitment/commitment.go` (TrieVariant + factory)
- Modify: `execution/commitment/parallel_patricia_hashed.go` (route to streaming)
- Create: tests near the variant selection

- [ ] add `ExperimentalStreamingCommitmentFlag` + `statecfg.ExperimentalStreamingCommitment`
- [ ] add `VariantStreamingHexPatricia` (or parallel variant + streaming mode); select in `domain_shared.go`
- [ ] route `ParallelPatriciaHashed.Process` to the streaming committer when the mode/flag is set (keep `ERIGON_CMT_MOUNT`/`ERIGON_CMT_DEEP` layering)
- [ ] write test: flag → correct variant; integration parity via the public `Process` path (big-storage corpus, workers 1/4/8)
- [ ] run tests — must pass before next task

### Task 8: New-split-mid-block + multi-block lifecycle

**Files:**
- Modify: `execution/commitment/streaming_commitment.go`
- Modify: `execution/commitment/streaming_commitment_test.go`

- [ ] handle a touch that creates a split point that didn't exist mid-stream (new top-nibble/subtree); this is the genuinely scheduler-specific case (a fold may already be in flight when the split set changes)
- [ ] verify `Reset()` between blocks leaves no stale split state/branches
- [ ] write test: corpus where new splits appear after earlier ones folded (with the Task-4 scheduler running) → parity
- [ ] multi-block end-to-end + deletes are **inherited from the Task-1 `streaming` arm** matrix (`*Incremental`, `*StorageIncrementalDeletes`); confirm they pass with the scheduler enabled — no bespoke copies
- [ ] run tests (`-race`) — must pass before next task

### Task 9: Measurement — simulated-execution overlap + re-fold metric

**Files:**
- Create: `execution/commitment/streaming_commitment_bench_test.go`

- [ ] simulated-execution benchmark: interleave a tunable CPU cost per touch with background folds; compare total wall-clock vs touch-all-then-`Process`. **Label it a mechanism sanity-check only** (re-fold count + Process-time reduction) — NOT a perf claim; the synthetic-CPU number must not be cited as the headline (the real number is the live-node run, Post-Completion).
- [ ] report re-fold count and Process-only time for the whale + mixed corpora
- [ ] document results inline in the plan (Progress Tracking)

### Task 10: Verify acceptance criteria
- [ ] streaming root+branches == sequential across all corpora (mixed, big-account, whale, random, nested-storage)
- [ ] no regression: `go test ./execution/commitment/ -run TestVerifyParallel -count=1` on `default`, `ERIGON_CMT_MOUNT=1`, and `ERIGON_CMT_MOUNT=1 ERIGON_CMT_DEEP=1` (streaming reuses `concurrentStorageRoot`)
- [ ] `-race` clean on the streaming concurrency tests
- [ ] `make lint` clean (run repeatedly; non-deterministic)
- [ ] `make erigon` builds

### Task 11: [Final] Docs
- [ ] update `/Users/awskii/org/wrk/HANDOFF-parallel-storage-fold.md` with the streaming results
- [ ] update repo `CLAUDE.md`/agents docs if new patterns warrant
- [ ] move this plan to `docs/plans/completed/`

## Post-Completion
*Manual / external — no checkboxes.*

**Manual verification:**
- Live-node wall-clock: run on `/Users/awskii/dev` mainnet with
  `--experimental.streaming-commitment` (+ mount/deep gates), compare
  `[4/6 Execution]` stage wall-clock streaming vs `--experimental.parallel-commitment`
  vs sequential; grep `Wrong trie root | parallel done | panic`. MockState
  benchmarks cannot show the overlap win — this is the real measurement.
- `-race` under sustained load on a live node.

**Broader correctness (shared oracle, beyond streaming scope):**
- Deep-path existing-DB incremental + deletes hardening (tracked from the prior
  handoff) — streaming relies on it; Task 8 covers the streaming-specific cases,
  but a full deep-path sweep over real mainnet blocks is a live exercise.
