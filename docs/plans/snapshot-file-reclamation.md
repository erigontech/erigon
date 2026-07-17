# Snapshot file reclamation — how the refcount works today

How the state aggregator decides when an on-disk snapshot file (`.kv/.v/.ef` +
accessors) can be physically deleted, while readers may still be using it. This is
the mechanism merged by PR #21397; the design rationale is in
[20260525-lockfree-file-reclamation-spec.md](20260525-lockfree-file-reclamation-spec.md).
`FilesItem.frozen` was removed (#22126); `FilesItem.canDelete`/`refcount` are still
live, but for the SnapshotRepo mark-and-sweep path, not this one — the aggregator
reclaims via `aggregatorVisible` generations and never consults them.

## The model: reference-count the *visible generation*, not each file

Readers never pin individual files. Instead the aggregator publishes an immutable
`aggregatorVisible` bundle (the set of visible files for one consistent moment),
and a reader pins the **whole bundle** with a single atomic. This is MDBX's
freelist idea: a file removed at "generation G" is deletable once the oldest live
reader has moved past G.

`aggregatorVisible` (`db/state/aggregator.go`):
- `d/dh/dhii/iis` — the visible files for every domain / history / inverted index.
- `refcnt atomic.Int32` — number of live readers pinning this bundle.
- `retired []*FilesItem` — files removed from `dirtyFiles` *while this bundle was
  current* (i.e. this is the last generation that still references them).
- `next *aggregatorVisible` — oldest→newest linked list.

`a.visible atomic.Pointer[aggregatorVisible]` points at the newest (current)
bundle; `a.oldestVisible` is the chain head the reclaimer walks (guarded by
`dirtyFilesLock`).

## Open a reader — pin the current bundle (hazard-pointer style)

`BeginFilesRo` → `acquireVisibleFiles`:

```go
for {
    v = a.visible.Load()
    v.refcnt.Add(1)
    if a.visible.Load() == v { break } // still current → safe
    a.releaseVisibleFiles(v)           // superseded mid-pin → drop and retry
}
```

Load-then-increment isn't atomic, so it re-checks that the bundle is still current
after incrementing. A superseded bundle can still be incremented transiently — the
loop mis-pins one whenever a publish lands between the load and the add — so the
refcount is not monotonically falling once superseded. What the re-check buys is
that such a mis-pin is always released before the bundle is used: a reader only
ever *uses* a bundle it observed as current, and a current bundle's files are never
in any `retired` set. The drain watermark holds because a mis-pin is dropped, not
because the count cannot rise.

## Retire a file — remove from `dirtyFiles`, publish a new generation

A file leaves the visible set when it's removed from `dirtyFiles`. Producers:
- **merge cleanup** — `cleanAfterMerge` (superseded small files),
- **retention** — `RetireOldHistoryFiles` (aged history/index below the cutoff),
- **dedup / squeeze** replacements.

Each collects the removed `*FilesItem`s, tells the downloader to stop seeding
them with `a.onFilesDelete(names)` — the producer does this itself, ahead of any
unlink — and then calls the single publish point `recalcVisibleFiles(retired)`
(under `dirtyFilesLock`), which:
1. builds a fresh bundle from the current `dirtyFiles`,
2. attaches `retired` to the **outgoing** (current-at-removal) bundle,
3. `a.visible.Store(next)` — new readers no longer see the retired files,
4. opportunistically reclaims.

Physical deletion is deferred: readers still holding the outgoing (or older)
generation keep its `refcnt > 0`.

## Delete for real — last reader out, oldest-first

`AggregatorRoTx.Close` → `releaseVisibleFiles`:

```go
if v.refcnt.Add(-1) == 0 { a.reclaimRetired() }
```

`reclaimRetiredLocked` walks the chain from `oldestVisible` while
`refcnt == 0 && head != current`, collects each drained bundle's `retired` files,
advances `oldestVisible`, and hands the list back for its caller to remove —
`reclaimRetired` drops `dirtyFilesLock` first, the writer path already holds it:

```go
for h := a.oldestVisible; h != cur && h.refcnt.Load() == 0; h = h.next {
    toDelete = append(toDelete, h.retired...)
    h.retired = nil
    a.oldestVisible = h.next
}
```

Reclaiming only from the oldest end is what makes it safe: if the oldest bundle's
refcount is 0, everything older is already gone, so its `retired` files have no
other live holder — even if a *newer* bundle is still pinned by a slow reader, that
reader's bundle no longer contains the retired file. The actual unlink is
`FilesItem.closeFilesAndRemove` (`os.Remove` of the data file + accessors +
`.torrent`), now unconditional since `frozen` was removed.

## ASCII

```
 oldest (a.oldestVisible)                         newest (a.visible.Load())
      │                                                   │
      ▼                                                   ▼
 [gen G-1] ─next─► [gen G] ─next─► [gen G+1] ─next─► [gen G+2]
  rc=0             rc=0            rc=3              rc=5   ← readers pin here
  retired={}       retired={f1,f2} retired={}        retired={}
      │
      │ reclaim oldest-first while rc==0 && head!=current:
      │   delete head.retired; oldestVisible = head.next; repeat
      ▼
   closeFilesAndRemove(f1,f2)   ← single owner; under dirtyFilesLock on the writer
                                  publish path, after unlocking on reader-close
```

## Invariants

1. Files in a bundle's `retired` set are never in the current bundle's visible set.
2. A superseded bundle can still be mis-pinned, but never used: every mis-pin is
   released before the bundle is read from.
3. A `FilesItem` retired from `dirtyFiles` through this path reaches
   `closeFilesAndRemove` only from the reclaimer — no double-free, no per-file
   `canDelete`/`refcount` needed. Files that were never published (merge temp
   output) are removed by their producer and never enter a `retired` set.
