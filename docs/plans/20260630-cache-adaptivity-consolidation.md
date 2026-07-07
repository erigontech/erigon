# Cache adaptivity consolidation

Status: design — to be executed on `mh/trunk-pin-on-cachestack` **after #21386 merges**, gated on perf numbers.

## Context

Three "adaptive cache" mechanisms grew up independently and now overlap. We need to consolidate
them into one coherent model when the StateCache-LRU PR (#21386) lands in `main`. This note is the
reconciliation #21386's merge (and a follow-up here) should encode.

The trigger: merging current `main` into the cache branch surfaced that `main` carries
**#21468 "auto-nuke StateCache on low hit_rate"** (cherry-pick of #21435) on the same files
#21386 rewrites. They are competing designs, not a textual conflict.

## The three mechanisms today

1. **#21386 StateCache LRU** (`execution/cache/generic_cache.go`, branch): `freelru.ShardedLRU`
   sized once — `capacityEntries = byteBudget / avgBytesPerEntry(256B)`, clamped `[1024, 1<<22]`,
   **pre-allocated at construction** (the "single alloc": no per-entry GC churn — the perf win).
   `Mode` = `ModeEvictLRU` (continuous cold-entry eviction by entry count) or `ModeNoOp`. The byte
   budget is **fixed for the cache's lifetime**; freelru has no in-place resize, so lowering the
   footprint at runtime requires rebuilding at a smaller capacity.

2. **#21468 auto-nuke** (`main`, on the old `maphash.Map` StateCache): the only downward lever a
   non-LRU cache had. When full: freeze (refuse new keys) if hit-rate is healthy, or **nuke the
   whole map** if `total ≥ 1000 && hitRate < 10%`. This *is* the "downward adjustment" — but it is
   an artifact of a non-adaptive cache (can't evict cold-to-admit-hot, so it nukes-or-freezes), and
   it keeps the cache small (64MB) so nuke/refill stays cheap.

3. **Adaptive trunk-pin residency** (`execution/commitment/adaptive_pin.go` + `db/state/execctx/
   domain_shared.go`, branch): promote / extend / **demote** of contract storage-trunk *pins* in
   the **BranchCache** (commitment branches). Orthogonal to state-cache *size*, but it is the
   precedent for "a controller drives a cache's residency."

## The tension

Auto-nuke was downward-adjustment *for a non-adaptive cache*. The #21386 LRU is adaptive
(continuous eviction) but **fixed-allocation**, so it cannot shrink its memory footprint at
runtime — it can only evict within a fixed budget. So with the LRU in place:
- auto-nuke is **redundant** (LRU already evicts cold entries continuously), and
- auto-nuke is **harmful** (a transient low hit-rate would nuke the hot set the LRU maintains), and
- the LRU gives us no *memory-pressure* downsizing on its own.

## Proposed consolidated model

1. **One state-cache abstraction** = the freelru LRU with `Mode`. Continuous LRU eviction replaces
   auto-nuke's coarse freeze/nuke. Drop the nuke action (`nukeMinSamples` / `nukeHitRateThreshold`
   / the nuke-when-full branch).
2. **Keep #21468's telemetry** — the hit-rate / usage_pct / capacity logging (`PrintStatsAndReset`).
   It drives budget/Mode decisions and the perf A/B; it is independent of the nuke action.
3. **Downward adjustment becomes explicit, not a hit-rate nuke.** If runtime memory-pressure
   downsizing is needed, add `Resize(newBudget)` that rebuilds the `ShardedLRU` at a smaller
   capacity and re-admits the MRU entries — one explicit, observable path. Default behavior: a
   conservative fixed budget + LRU eviction (residency self-limits to the working set, so a larger
   budget mostly buys hit-rate, not waste).
4. **Budgets must be conscious of overall memory.** The per-cache byte budgets (account, storage,
   commitment, code, addr, **plus** the BranchCache and any other resident structures) are not
   independent — their **sum** must fit a global cache envelope sized as a fraction of the node's
   available RAM. Allocate per-cache budgets *from* that envelope (and have `Resize`/Mode react to
   it) rather than hard-coding each in isolation. This is why #21386's investigation knobs
   (account 100MB / storage 150MB / code 100MB, with a noted "permanent default" of 1GB / 512MB)
   and `main`'s 64MB can't just be diffed value-by-value: the right defaults depend on the total
   envelope and on the LRU vs nuke regime.
5. **adaptive_pin stays separate** (BranchCache pin residency), but the state-cache budget
   controller, if we add one, should sit alongside it as a second policy under the same
   "controller drives cache residency" pattern.

## Slot values and branch pins are one residency decision

The state slot cache (StateCache **storage**, holding `addr→slot→value`) and account-level branch
pinning (BranchCache trunk-pin via `adaptive_pin`, holding the storage-trie **skeleton/branches**
for a contract) are two halves of the same per-contract working set:

- `adaptive_pin` promotes a hot contract → pins its storage **branches** (the trie structure) so
  commitment doesn't re-unfold them.
- But the slot **values** for that same contract still ride the global StateCache LRU. Under
  eviction pressure they can be demoted independently — so a "pinned" contract can still force
  slot-value re-reads on the execution side, defeating much of the pin's benefit and skewing the
  perf numbers.

**So promote/demote must pin both: the slots (branches, BranchCache) and the slot values
(StateCache storage).** When the controller promotes a contract it should also protect that
contract's slot-value entries from LRU eviction; when it demotes, it releases both.

Implications:
- The single-alloc freelru LRU has no native "pinned entry" concept. Protecting a promoted
  contract's slots means a **pinned tier alongside the LRU tail** — the same shape as the
  BranchCache's fixed-array trunk tiers + LRU tail (`748098a933`). Pinning is bounded (only the
  promoted hot set), so it stays within the envelope.
- Pinned slot values draw from the **same global memory envelope** as everything else; the
  envelope accounting must include the pinned-slot region, and demote must free it.
- Endgame (separate, larger refactor — see the commitment roadmap): fold the per-contract slot
  **values** and branch **skeleton** into one account-keyed structure so a single promote/extend/
  demote governs both, eliminating the parallel bookkeeping between StateCache and BranchCache.

## The code cache is the third per-account cache

The same per-contract residency logic extends to **code**. This branch already has the in-memory
`CodeCache` (`execution/cache/code_cache.go`): `addr→codeHash` LRU, `codeHash→code`, a size-only
`codeHash→len` layer, all under the same `(txNum, epoch)` invalidation as the state cache. So a
hot contract has three coupled resident pieces: its **branches** (BranchCache skeleton), its
**slot values** (StateCache storage), and its **code** (CodeCache). Promote/demote should govern
all three; all three draw from the **same global memory envelope**; all three honor `(txNum,
epoch)` invalidation regardless of pinning.

Two code-specific dimensions to fold in:
- **Cold tier (persistent code cache).** Code differs from slots in that an in-memory miss costs a
  btree lookup + multi-pass segment **decompression**, not just an IO. The separate
  persistent-code-cache design (codehash-keyed otter hot tier + an MDBX `TblCodeCache` backing,
  populated on the write path, evicted via prune) turns "evicted from hot ⇒ re-decompress" into
  "evicted from hot ⇒ cheap MDBX read." That cold tier is **not on this branch** (designed only)
  and sits *below* the hot LRU — orthogonal to LRU eviction but part of the same code-residency story.
- **Provenance to reconcile.** Related code-cache work is spread: the in-mem layers are here;
  `origin/mh/sd-code-cache` carries the CodeDomain ethHash bypass, the `codeSizeCache` for
  EXTCODESIZE/EXTCODEHASH, `stateObject.code` populate, BAL→`cache.StateCache` prewarm, and
  surgical BranchCache unwind-invalidation; the MDBX backing is the persistent-code-cache design.
  These should be reconciled onto this branch as part of the consolidation rather than living as
  parallel forks.

## Review findings to fold in (#22120)

`#22120` is the code review of #21386 (the StateCache LRU we merged). All of its findings live in
code this branch now carries and extends, so they belong in this consolidation, not filed away
separately. Mapping (severity = reviewer's):

- **Finding 1 (med-high) — code content layers freeze-when-full, never evict.** `code_cache.go`'s
  `hashToCode` / `codeHashToCode` / `codeSizeByCodeHash` are plain `maphash.Map`; `putAccounted`
  *refuses* past the cap instead of evicting, the addr LRUs have no `OnEvict` (orphaned content),
  and there is no live `Clear()` caller. Over a sync the code caches fill permanently and refuse
  newly-seen contracts — the opposite of the warm-up goal, and it **biases our perf A/B** (cold code
  keeps paying the decompression stack). This is exactly the "code cache is the third per-account
  cache" section above: give the content layers the same LRU/eviction the account/storage
  `GenericCache`s have, under the shared envelope.
- **Finding 2 (med) — Account cache is entry-capped at ~384 MB, not the configured 1 GB.** The
  `1<<22` entry clamp in `generic_cache.go` binds before the byte budget, so residency settles at
  ≈ 4.19M × 96 B. This is the "budgets must be conscious of overall memory" point made concrete: the
  budget we would tune in the sweep is illusory until the clamp is reconciled with the envelope.
- **Finding 3 (med) — `putAccounted` over-cap back-out uses a raw `m.Delete`,** drifting the byte
  counter under concurrency (double-subtract or wedge-high). This is our parallel-mode shared-cache
  concurrency concern; route the back-out through the accounted `LoadAndDelete`.
- **Finding 4 (low, assert-gated) — false `stateCache divergence` panic** during an in-flight unwind
  when the cached `cStep < maxStep`; only fires under `ASSERT_STATE_CACHE`, but disrupts CI/asserts.
- **Findings 5–8 (cleanup)** — dead `DetachBranchCache` / `ClearBranchCache` / `ProbeReadLayers`
  (wire as defense-in-depth or delete); redundant `keccak` on every cold code read + warmup (the
  codeHash is already in the decoded account); duplicated `maxStep` gate + full `DeserialiseV3` on
  the fast path (fold into one `Get` helper); and CLAUDE.md comment-policy violations concentrated
  in `domain_shared.go` (issue/PR refs, incident anecdotes, client name-drops — trim to one sentence,
  move history to the commit/PR). We took `domain_shared.go` main-whole, so we now own finding 8.
- **Enforcement A/B** — the `(epoch, floor)` lazy-unwind model is correct-by-convention in two
  load-bearing spots (every epoch-bumping unwind fenced against in-flight warmup; every
  fork-validation epoch-bumps before reading a shared branch). Convert to code-enforced: the
  drain-free getter tracked in **#22116** (getter captures epoch at read-start, a late warmup Put
  can't launder a dead-fork value) is the preferred fix and removes the assumption rather than
  asserting it. Ties to [[arch-view-poisoning-dependent-tx-2026-06-13]].

Sequencing note: findings 1 and 2 change what the perf numbers mean, so decide them (fix, or
consciously accept and annotate) *before* reading the A/B — the rest can land as consolidation lands.

Decision (2026-07-01): run a **baseline** A/B on the merged branch as-is first (finding-1/2 bias
annotated), then fix and re-run to measure the delta. Scope for this PR = findings **1, 2, 3, 8**
(perf + parallel-mode concurrency + the comment-policy we took main-whole in `domain_shared.go`);
findings 4/5/6/7 and enforcement A/B stay with upstream #22120 (Imp2-tracked) and the consolidation
follow-up.

## Execution (after #21386 lands)

0. Fold in the #22120 findings above — findings 1 and 2 gate the perf A/B interpretation.
1. Merge `main`; for `execution/cache/*` keep #21386's LRU, fold auto-nuke down to telemetry +
   (optionally) the explicit `Resize` path; reconcile the 3 trunk-pin files
   (`branch_cache.go`, `domain_shared.go`, `branch_cache_test.go`).
2. Settle the default budgets against an overall-memory envelope (account + storage + commitment +
   code + addr + BranchCache as one sum).
3. Reconcile the spread code-cache work onto this branch (the `mh/sd-code-cache` pieces; the MDBX
   cold tier as a follow-up), so code residency sits under the same controller/envelope.
4. Couple the per-contract pinning across all three caches (branches + slot values + code) — start
   with the coupled-structures step (controller pins all three), not the full account-keyed merge.
5. Validate with perf numbers — the `--experimental.concurrent-commitment` on/off A/B plus a
   cache-budget sweep (benchmarkoor). Land the budgets/Mode/pinning the numbers support.

## Open questions

- Can `freelru.ShardedLRU` rebuild-resize cheaply enough to be a runtime memory-pressure response,
  or is a conservative fixed budget sufficient and `Resize` only a startup/config knob?
- What global envelope fraction (and how to read "available RAM" portably) drives the per-cache
  split — fixed config, or derived from a `--cache.total` style budget?
- Pinned-slot tier: do we keep StateCache and BranchCache as two structures coupled by the
  controller (promote pins both, demote frees both), or go straight to the unified account-keyed
  structure? The coupled-two-structures path is the smaller step to perf-test first.
- Does pinning slot values change correctness/unwind semantics? Pinned entries must still honor
  the `(txNum, epoch)` invalidation — pinning protects from *capacity* eviction, never from
  unwind/invalidation.

## Follow-up: rewire / shelve the adaptive pin controller (recorded 2026-07-01)

After the #21386 merge, `domain_shared.go` was taken from main (`sd.branchCache` model) and the
branch's adaptive pin controller was **re-wired in** so it is not dead code: field +
`NewAdaptivePinController` construction + `Bind` in `EnableParaTrieDB` + an `OnBlockComplete` hook in
`Commit` that runs **before `tx.Commit()`** using the in-flight tx (main commits then applies the
cache stash, so a post-commit read would need a fresh tx — the view-poisoning concern).

Task (choose one, perf-driven):
1. **Shelve for the core-pinning perf binary.** Remove `adaptive_pin.go`, `preload*.go`,
   `trunk_pin_metrics.go` and the four `domain_shared.go` injection sites; keep the `accountTrunk`
   upper-trie pinning (driven by normal Put). Re-add from git history when adaptive residency is
   perf-justified. This matches the earlier "core-pinning binary for the benchmark" decision.
2. **Proper post-commit placement.** Move `OnBlockComplete` to after `tx.Commit()` with a dedicated
   short-lived read tx (or an aggregator RoTx), so pins reflect committed state and never a
   rolled-back batch — resolving the in-flight-tx compromise above.

Until this runs, the controller is wired via the in-flight-tx compromise (functional; pins from a
rolled-back Commit are evicted by the coherence floor).
