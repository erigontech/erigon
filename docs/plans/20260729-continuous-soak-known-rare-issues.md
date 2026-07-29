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
- **Fix:** none landed. This is the SECOND rare issue surfaced by
  the soak this week; needs its own investigation session.
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
