# Lock-free snapshot-file reclamation via generation-chained bundle refcounts

## Context

PRs #20462 / #20490 made `Aggregator.BeginFilesRo()` lock-free: a reader does a single `a.visible.Load()` of an
immutable `aggregatorVisible` bundle, then pins each file with `refcntIncrement` (`FilesItem.refcount atomic.Int32`).
Physical file deletion is gated by a *second* atomic, `FilesItem.canDelete atomic.Bool`.

Two atomics gating one destructive action (`closeFilesAndRemove`) is the bug in #21384: `deleteMergeFile` does
`canDelete.Store(true)` then `refcount.Load()==0`, while `refcntDecrement` (on `RoTx.Close`) does
`refcount.Add(-1)==0 && canDelete.Load()`. Both can observe `(refcount==0 && canDelete==true)` and both call
`closeFilesAndRemove()` → double-free (TOCTOU). #21384 patches the symptom with
`sync.Once`.

The real problem is not "two atomics" — it is that the *destroy transition* has two owners, and that a per-file refcount
taken **after** the snapshot pointer is loaded cannot protect the `Load()`→`increment` window by itself (today only an
implicit timing gap saves it). Merging the atomics into one word does not fix the concept and does not scale to a third
gating condition.

**Goal:** replace per-file `refcount`/`canDelete` with MVCC reclamation gated by an oldest-reader watermark — exactly
MDBX's freelist model (a page freed at txnid
`T` is reusable once the oldest live reader's txnid `> T`), realized in Go by reference-counting the published bundle
object instead of individual files. This deletes both atomics from `FilesItem`, removes the whole double-free class
(reclamation gets a single owner), closes the `Load`→pin window, and makes
`Begin` cheaper (one atomic add instead of dozens of per-file adds).

Scope decisions (confirmed with user):

- **Reclaim inline on head-drain.**
- **Ignore the forkable subsystem entirely** (`forkable*.go`, `proto_forkable.go`,
  `snap_repo.go`) — not used.

## Design

### Bundle becomes a generation node

`aggregatorVisible` (db/state/aggregator.go:1763) gains:

- `gen uint64` — monotonically increasing generation, assigned at publish.
- `refcnt atomic.Int32` — number of live readers pinning this bundle.
- `retired []*FilesItem` — files removed from `dirtyFiles` during this bundle's tenure as *current* (i.e. the files this
  generation is the last to reference).
- `next *aggregatorVisible` — link forming an **oldest → newest** chain.

`a.visible atomic.Pointer[aggregatorVisible]` still points at the newest (current) bundle;
`a.oldestVisible *aggregatorVisible` (guarded by
`dirtyFilesLock`) is the chain head used by the reclaimer.

### Begin: load + validate-after-pin (closes the Load→pin window)

```go
func (a *Aggregator) BeginFilesRo() *AggregatorRoTx {
    var v *aggregatorVisible
    for {
        v = a.visible.Load()
        v.refcnt.Add(1)
        if a.visible.Load() == v { // still current → safe to use
            break
        }
        v.refcnt.Add(-1) // superseded mid-pin → drop and retry
    }
    // ... build AggregatorRoTx from v (no per-file refcntIncrement anymore) ...
}
```

Invariant this buys us: **a bundle accrues refs only while it is current; once superseded its refcount only
decreases** → monotonic drain → a well-defined watermark. A reader only ever *uses* a bundle it observed as current, and
a current bundle's files are never in any `retired` set.

### Close: one decrement, reclaim oldest-first

`AggregatorRoTx.Close` does `v.refcnt.Add(-1)`; if it hits 0 it calls
`a.tryReclaim()`. Per-file `refcntDecrement` is deleted.

```go
func (a *Aggregator) tryReclaim() {
    a.dirtyFilesLock.Lock()
    defer a.dirtyFilesLock.Unlock()
    cur := a.visible.Load()
    for h := a.oldestVisible; h != cur && h.refcnt.Load() == 0; h = h.next {
        for _, f := range h.retired {
            f.closeFilesAndRemove() // single owner, under lock → no double-free
        }
        h.retired = nil
        a.oldestVisible = h.next
    }
}
```

Reclaim only from the **oldest end**: when the head's refcount is 0 everything older is already gone, so the head's
`retired` files have no other live holder. (Deleting a file the moment the *immediately-previous* bundle drains would be
wrong if an even-older bundle is still held by a slow RPC reader — the file is present in every bundle up to the one
that dropped it.)

### Publish: attach retired files to the outgoing bundle, bump generation

`recalcVisibleFiles` (db/state/aggregator.go:1777) is the single publish point. Callers that remove files from
`dirtyFiles` (merge cleanup, prune, dedup) collect those `*FilesItem` and hand them to the publish:

```go
func (a *Aggregator) recalcVisibleFiles(retired []*FilesItem) {
old := a.visible.Load()
old.retired = retired // last referenced under `old`'s generation
next := &aggregatorVisible{ gen: old.gen + 1, /* ...built from dirtyFiles... */}
old.next = next
a.visible.Store(next)
a.tryReclaim() // outgoing bundle may already be drained
}
```

Tagging retired files with the *outgoing* (current-at-removal) generation is always safe: a file already shadowed before
its `dirtyFiles` removal is only *more* protected (deleted slightly later), never less.

### Two classes of deletion — only one routes through the new path

- **Type B (retire a file that WAS visible)** → route through the new mechanism (remove from `dirtyFiles`, pass to
  `recalcVisibleFiles`, delete on head-drain):
    - `deleteMergeFile` / `cleanAfterMerge` (db/state/dirty_files.go:355, db/state/merge.go:922-963)
    - dedup retirement loop (db/state/deduplicate.go:208,216)
    - squeeze old-file replacement (db/state/squeeze.go:290)
    - prune / retention: old History+II files leave the visible set the same way — via `dirtyFiles` removal +
      `recalcVisibleFiles` (see db/state/aggregator.go:1451
      `prune`, db/state/history.go:1070, db/state/inverted_index.go).
- **Type A (delete a just-built file that was NEVER published, error rollback)**
  → keep calling `closeFilesAndRemove()` directly; no reader can hold it:
    - merge error defers (db/state/merge.go:401,404,407,600,737,768)
    - dedup error defers (db/state/deduplicate.go:67,237)

### Fields / call sites removed

- `FilesItem.refcount`, `FilesItem.canDelete` (db/state/dirty_files.go:127,131).
- `visibleFiles.refcntIncrement` / `refcntDecrement` (db/state/dirty_files.go:748-773).
- `canDelete` guard in `checkForVisibility` (db/state/dirty_files.go:693) — moot once retirement == removal from
  `dirtyFiles` (recalc iterates `dirtyFiles`, so a removed file is excluded automatically; subset shadowing already
  drops overlaps).
- `sync.Once` from #21384 if merged — superseded.
- Update debug dumps that read these atomics (db/state/aggregator_debug.go:168,208,245).

## ASCII file lifecycle (for code comments)

```
                         build / merge produces file
                                     │
                                     ▼
        ┌───────────────────────────────────────────────────────┐
        │  LIVE: in DirtyFiles and VISIBLE in the current bundle  │
        │  readers Begin() here → pin the bundle (validate-pin)   │
        └───────────────────────────┬───────────────────────────┘
                                     │  removed from DirtyFiles because:
                                     │    • superseded by a merged file        (Merge)
                                     │    • below retention horizon            (Prune, e.g. --prune.mode=minimal)
                                     │    • replaced by dedup / squeeze output
                                     ▼
        attach file to the OUTGOING bundle's `retired` set (generation G),
        publish new bundle (generation G+1)  →  file no longer visible to NEW readers
                                     │
                                     │  readers holding bundles of gen ≤ G
                                     │  may still be using the file
                                     ▼
        ┌───────────────────────────────────────────────────────┐
        │  RETIRED: wait until every bundle of gen ≤ G has drained │
        └───────────────────────────┬───────────────────────────┘
                                     │  bundle gen G is the chain head AND its
                                     │  refcnt hit 0 (all older gens already reclaimed)
                                     ▼
                       closeFilesAndRemove()   ← single owner, under dirtyFilesLock
                       (close fd + delete .kv/.bt/.kvi/.ef/.efi/.torrent from disk)
```

```
 oldest (chain head)                                    newest (a.visible.Load())
        │                                                        │
        ▼                                                        ▼
   [gen G-1] ─next─► [gen G] ─next─► [gen G+1] ─next─► [gen G+2]
    rc=0             rc=0            rc=3              rc=5   ← readers pin here
    retired={..}     retired={f1,f2} retired={}        retired={}    (validate-after-pin)
        │
        │ reclaim from the OLDEST end only:
        │   head.refcnt==0 && head!=current  ⇒  delete head.retired; head=head.next; repeat
        ▼
```

## Key invariants (assert in code where cheap)

1. Files attached to `retired` are never in the current bundle's visible set.
2. A bundle's `refcnt` only increases while it is current; only decreases after.
3. `closeFilesAndRemove()` for a given `FilesItem` is reached from exactly one site (the reclaimer, under
   `dirtyFilesLock`) — no `sync.Once`/CAS needed.
4. `frozen` files are never retired (unchanged from today).

## Files to modify

- `db/state/aggregator.go` — bundle fields, `BeginFilesRo` (validate-pin),
  `tryReclaim`, `recalcVisibleFiles(retired)`, `AggregatorRoTx.Close`,
  `cleanAfterMerge` plumbing of retired set.
- `db/state/dirty_files.go` — remove `refcount`/`canDelete` from `FilesItem`, remove `refcntIncrement`/
  `refcntDecrement`, `deleteMergeFile`→return retired files instead of deleting, drop `canDelete` guard in
  `checkForVisibility`.
- `db/state/merge.go` — `cleanAfterMerge`/`garbage` return retired files upward.
- `db/state/domain.go`, `history.go`, `inverted_index.go` — `beginFilesRo` no longer increments per-file refcount; sub-
  `RoTx.Close` no longer decrements; prune returns retired files.
- `db/state/deduplicate.go`, `squeeze.go` — route Type-B retirements through publish.
- `db/state/aggregator_debug.go` — drop refcount/canDelete reads.

## Verification

1. **Reproduce the original race first (TDD / red):** `go test -race -count=10
   -run TestHistoryVerification_SimpleBlocks ./execution/verify/...` on current
   `main` (without #21384's `sync.Once`) — confirm it red's on double-free. This is the regression the redesign must
   turn green.
2. **Unit + race:** `make test-short`, then `make test-race` on `db/state/...`
   and `execution/...`. Add a focused concurrency test: many `BeginFilesRo`/`Close`
   goroutines racing a merge+cleanAfterMerge loop and a prune loop, under `-race`.
3. **No-leak check:** after a merge/prune storm with readers, assert every retired file is eventually deleted from disk
   (no orphaned `.kv/.ef/...`) and the bundle chain collapses to a single node once readers drain.
4. **End-to-end:** build (`make erigon integration`), run the
   `erigon-exec-from-0` / a short sync on a chain with `--prune.mode=minimal`, confirm old History/II files are removed
   and no "file not found" on readers.
5. **Throughput:** benchmark `Begin`/`Close` (the original #20462 goal) — expect the single bundle add to beat per-file
   increments; capture before/after.
6. `make lint` until clean before pushing.
