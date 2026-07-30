# Continuous Soak — Known Rare Issues

**Owner:** whoever picks up the soak next
**Purpose:** track rare failure modes surfaced by the continuous
unwind soak. Reproducing + root-causing + resolving each entry is an
**exit criterion** for the soak (i.e. we don't declare the soak
"green" until every known issue here is closed or explicitly
downgraded with rationale).

Format for each entry:

- **Signature** — the shortest log fragment that identifies the
  occurrence (grep-able)
- **First seen** — datestamp + cycle log path + binary commit
- **Reproduction** — deterministic repro (unit test / script) if one
  exists; "in-soak only" otherwise
- **Root cause** — the code path or invariant break, once known
- **Fix** — commit hash(es); blank until landed
- **Diagnostic hook** — the log line / metric that fires on next
  occurrence (helps monitor scripts detect the pattern)

Add a new entry at the top of the OPEN list on every soak-surfaced
issue. Move an entry to CLOSED once (a) a deterministic reproducer
exists and (b) the fix has been in soak for ≥3 consecutive cycles
without recurrence.

---

## OPEN

### 3. Receipt boundary-step regen aborts when target < receipt inverted-index earliest txN

- **Signature:** `regen receipt boundary-step file domain/v3.0-receipt.<from>-<to>.kv: AsOfLookup(receipt, key, <txN>): seekInFiles(invIndex=receipt,txNum=<txN+1>) but data before txNum=<horizon> not available`
- **First seen:** 2026-07-30T15:59:30 UTC
  - Cycle log: `/tmp/continuous-soak/soak.cycle0001-20260730T140449.log`
  - Erigon log: `/tmp/continuous-soak/erigon.cycle0001-20260730T140449.log` (per-cycle log — F3 diag change from this batch)
  - Datadir: `/erigon/tmp/erigon-hoodi-continuous-soak.cycle0001-20260730T140449`
  - Binary commit: `228accbd71` (full fix-batch: doubled-path Seed + RoSnapshots.delete both parts + Bug #3 + F4 + F3 diag + Bug #6 diag + Bug #4)
  - Iter 4 mode_b, depth 169350, target 3149167 (txN 111871647)
  - Receipt IX earliest available txN: 112500000 (step 288 boundary)
  - Gap: 628k txs, spans step 286 → 288
  - Iters 1–3 all clean (F4 asymmetric-tx didn't trip; no seedLeftoverBlocks; iter-1 mode_b had a transient `inv_extras=2` on a 2300s deep unwind but recovered)
- **Reproduction:** in-soak only, but likely deterministic on any deep mode-B target that lands below the receipt IX earliest txN. Preserved datadir may reproduce via `debug_setHead` with target = 3149167 on the same binary.
- **Root cause (candidate, not yet code-traced):** Provider.Unwind's boundary-step regen for the receipt domain calls AsOfLookup on the receipt inverted-index at a txN below the IX's earliest available data. Under `--prune.mode=minimal` the receipt IX is pruned to the last ~100k blocks; if the target's txN falls below that horizon, AsOfLookup returns "not available" and regen aborts. Regen expects the IX to cover the whole step range being regenerated, which is incompatible with the pruning contract.
- **Fix:** landed 2026-07-30 (uncommitted at time of writing; commit to follow). `overrideActionForIXHorizon` applied after `classifyStateFileForUnwind` in `regenerateBoundaryStepFiles`. Per-domain probe of `tx.Debug().HistoryStartFrom(domain)` vs `lastTxNum+1`: when IX doesn't cover the target, receipt-domain regen actions become `actionRemove` (forward-exec restores every value; receipt keys are re-written on every txN, so retire produces a fresh boundary .kv naturally). Non-receipt history-tracked domains (accounts/storage/code) return an error — silent removal there would lose state for keys last written pre-target and never touched since. Commitment passes through (regen uses an encoded anchor, not per-key AsOf).
- **Follow-up (wipe-side latent):** `wipe_writable_shadow.go`'s `collectKeysChangedInRange` walks `HistoryKeyTxNumRange` and silently returns empty when the range is below the IX horizon. Downstream `applyReplay` then no-ops, so MDBX rows at step == stepContaining written pre-unwind at txN > lastTxNum can survive unchanged. Post-fix forward-exec DomainPut may collide with those stale DupSort rows for receipt. Not addressed by this fix; may surface as forward-exec receipt divergence in the next soak cycle and would need a targeted wipe-side change if so.
- **Diagnostic hook:** the error message names the IX name + requested txN + horizon — enough to identify the pattern on next occurrence.

### 2. `seedLeftoverBlocks` header/tx range mismatch on mode-B unwind

### 2. `seedLeftoverBlocks` header/tx range mismatch on mode-B unwind

- **Signature:** `storage.Provider.Unwind: snapshot-trim: seedLeftoverBlocks([X, Y]): seedLeftoverBlocks: tx range [X, Z) does not match headers [A, B)`
- **First seen:** 2026-07-29T10:53:31 UTC
  - Cycle log: `/tmp/continuous-soak/soak.cycle0001-20260729T075825.log`
  - Erigon log: `/tmp/erigon-hoodi.log`
  - Binary commit: `7938098e91` (built with the txpool panic+diagnostic fix)
  - Ran 8 iterations clean (mode_a + mode_a2 + mode_b × 8); iter 9 mode_b failed
  - Iter 9 mode_b: pre_head=3310510, target=3157217, depth=153293 (deep regime)
  - Actual concrete signature:
    ```
    tx range [3157000, 3158000) does not match headers [3150000, 3160000)
    ```
  - Headers file is a merged 10k-block chunk `[3150000, 3160000)`; tx
    file is an unmerged 1k-block chunk `[3157000, 3158000)`. The
    strict range-parity check at
    [provider_unwind_snapshot_rebuild.go:348-350](../../node/components/storage/provider_unwind_snapshot_rebuild.go#L348-L350)
    refuses to seed leftover blocks when the three ranges (headers,
    bodies, tx) aren't identical.
- **Reproduction:** in-soak only; no isolated reproducer. The
  triggering condition is a mode-B unwind whose target lands in a
  block window where headers has already been merged into a wider
  file but tx hasn't caught up.
- **Root cause:** race between retire/merge for headers (which merge
  into 10k chunks) and tx (which lag or merge on a different
  cadence). seedLeftoverBlocks assumes symmetry. Either:
    - the retire/merge coordination needs to make them symmetric
      before unwind can trim, or
    - seedLeftoverBlocks needs to handle asymmetric ranges (pick
      the narrowest containing tx chunk and iterate headers'
      corresponding sub-range).
- **Fix:** `3e8e06feb8` (2026-07-30) — seedLeftoverBlocks relaxed
  from strict tx-range equality to coverage-based: tx file must span
  the write range [fromBlock, toBlockInclusive+1) at minimum. Walk
  skips tx-getter advance for blocks outside the tx file's range.
  Two RED→GREEN tests pin the asymmetric case + guard the coverage
  gate. Awaiting ≥3 soak cycles clean before moving to CLOSED.
- **Diagnostic hook:** the error message names all three ranges —
  enough state to identify which files disagreed. Additional context
  (merge history, retire cadence) needs a log-side capture that
  isn't there yet.

### 1. `txpool.OnNewBlock` orphan senderID in `p.queued.best.ms`

- **Signature:** `panic: txpool.OnNewBlock queued.best.ms senderID=N not in senderID2Addr`
  (pre-fix: `panic: must not happen` in `senders.go:218`)
- **First seen:** 2026-07-28T22:03:16 UTC
  - Cycle log: `/tmp/continuous-soak/soak.cycle0001-20260728T075759.log`
  - Erigon log: `/tmp/erigon-hoodi.log`
  - Binary commit: `482ed374a1`
  - Ran 40 clean iterations (mode_a + mode_a2 + mode_b × 40) over ~13h
  - Panic fired between iter 40 mode_b completion and iter 41 mode_a
    start, inside `Fetch.handleStateChangesRequest` → `OnNewBlock` →
    `addTxnsOnNewBlock` → `queuedSenders` loop → `senders.info`
- **Reproduction:**
  - Unit test: `TestOnNewBlock_QueuedOrphanSenderIDPanicsWithDiagnostic`
    (`txnprovider/txpool/pool_test.go`) — constructs the divergence
    manually (add nonce-gap tx → force-evict senderID from
    `senderID2Addr`) and asserts panic. NOT a natural-path reproducer.
  - Natural path: in-soak only. No isolated reproducer yet.
- **Root cause:** the pool holds a metaTxn in `p.queued.best.ms` whose
  `SenderID` has been evicted from `senders.senderID2Addr`. The only
  path that shrinks `senderID2Addr` is `flushLocked` (line 2571-2576)
  gated on `!p.all.hasTxns(id)` — so the mt must have been removed
  from `p.all` without also being removed from `p.queued`. Every
  `discardLocked` call site pairs `removeFromSubPool` right before it
  and heap `Swap`/`Push`/`Pop` correctly maintain `bestIndex`/
  `worstIndex`. Divergence path is subtler — suspect
  `promote`/`demote`, a nested `heap.Fix` under `Updated`, or a
  `replaceOrInsert` edge case. **Confirmed only via diagnostic on
  next occurrence.**
- **Fix:** partial — `7799117ac3` (surface as loud panic + rich
  diagnostic dump instead of `"must not happen"` with no context).
  Full fix pending root cause.
- **Diagnostic hook:** the log line
  `[EROR] [txpool] queued.best.ms senderID unregistered — capturing diagnostic`
  fires immediately before the panic. Includes: `senderID`,
  `queued.best.len`, `all.count(senderID)`, `senderID2Addr.len`,
  `byHash.len`, `deletedTxns.len`, plus per-orphan-mt fields
  (`bestIndex`, `currentSubPool`, `in_p.all`, `in_p.byHash`). Soak
  drivers can grep for the "capturing diagnostic" marker to know an
  occurrence fired.

---

## CLOSED

*(empty — exit criteria met when every OPEN entry moves here)*

---

## How the soak treats these

- The soak driver stops on first failure by default. A panic from any
  OPEN issue therefore halts the current cycle with the datadir + log
  preserved for post-mortem.
- Set `KEEP_GOING=true` on the soak driver only for statistical runs
  where the goal is counting occurrences, not fixing. Never set it as
  the default because a real regression would be silently accumulated.
- When a NEW rare issue surfaces, add it here in the same commit as
  the diagnostic instrumentation you add for it. Don't skip the doc
  entry — future maintainers need to know it's known.
