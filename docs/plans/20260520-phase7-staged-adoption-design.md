# Phase 7b/7c — Staged Canonical Adoption: design pass

**Status**: design draft, 2026-05-20. Phase 7a (verdict split) is committed
(`1ab26ac5c7`). 7b (staged download) and 7c (atomic cutover) are deliberately
held for this dedicated design because 7c modifies snapshot files underneath a
running execution client — the single riskiest change in the chain.toml + UCAN
flow (`docs/plans/20260520-chaintoml-ucan-flow-spec.md`).

## What 7a already gives us

`snapshotsync.CheckOwnAdvertisement(adv, genesis, canonicals)` returns:

- `*AdvertisementSelfCheckError` — a **divergence**: own hash conflicts with the
  pinned genesis v0. Fatal; the publisher halts.
- `*MinorityVerdict{Adopt []AdvertisementMismatch}` — a **minority**: own hash
  conflicts only with a quorum-promoted entry. Non-fatal; this is the input to
  staged adoption.

Today the backend self-check closure runs in legacy mode (static preverified
treated as genesis, so every mismatch is a divergence). 7b's first task is to
wire the closure to the live `CanonicalView` so the minority verdict can
actually fire.

## Hard problems surfaced by the code survey

1. **No quiesce primitive.** `RoSnapshots.OpenFolder()` rescans the directory
   and swaps the segment set via `closeWhatNotInList` + `openSegments`. There is
   no API to pause reads of a range while files change.
2. **rename(2)-over-mmap is actually safe for *existing* readers.** On Linux a
   `rename(2)` over an open/mmap'd segment leaves the old inode alive for every
   existing fd/mmap; new opens see the new file. So existing held views finish
   on old bytes — they do **not** see torn state. The real hazard is a *new*
   reader opening a half-swapped batch (segment A new, its paired .idx old, or
   sibling segment B still old).
3. **.seg + .idx + .vi are a set.** A single logical file is several on-disk
   artefacts. The cutover must swap all artefacts of all batch members before
   any new reader is allowed to open the range.
4. **Inventory is the source of truth.** `Inventory` (RWMutex-guarded) holds
   `FileEntry.TorrentHash` and `LifecycleState`. `ReplaceWithMerge` is the
   existing atomic multi-file metadata swap and the closest precedent;
   `AdvanceTo` does single-file state transitions; `refcount`/`pendingDeletes`
   already defer eviction of files under an active held view.

## Proposed design

### Adoption policy — operator choice

Whether a detected minority triggers an automatic cutover is an **operator
decision**, not a fixed behaviour. An operator running a node that favours
continuous, uninterrupted operation wants to choose *when* the disruptive
cutover happens (it reopens segments and republishes); an operator who wants
the node self-healing wants it automatic. A `--snapshot.adoption-policy` flag
selects how far the automatic pipeline runs:

| Policy  | On minority detection | Operator action needed |
|---------|----------------------|------------------------|
| `auto` (default) | stage + validate + cutover, all automatic | none |
| `stage` | stage + validate, then warn — files ready, no cutover | run `adopt` to cut over |
| `warn`  | warn only — nothing fetched, nothing changed | run `adopt` for stage + cutover |

The policy is just "how far down warn → stage → cutover the node goes on its
own." Default is `auto` (self-healing), but `stage` and `warn` hand the
disruptive step to the operator.

### Operator command — `erigon snapshots adopt`

Modelled on `erigon snapshots reset` (`cmd/utils/app/reset-datadir.go`):
datadir-scoped, with a `--dry-run` that prints the canonical delta (the files
that would be fetched / swapped) without acting. `adopt` completes whatever the
policy left undone — under `warn` it stages, validates and cuts over; under
`stage` it cuts over the already-staged batch; under `auto` it is a no-op (or a
manual re-trigger). It runs against a stopped node, or coordinates with a
running one through the same inventory lock the automatic path uses.

### Grace window (gates the stage step)

The minority verdict says "a different hash reached quorum". Before acting, the
publisher must give its own hash a fair chance — multi-canonical means both
forms can be canonical. The grace window lives in the **adoption trigger**, not
in `CheckOwnAdvertisement` (which stays a pure classifier):

- The trigger records, per `(name, ownHash)`, the wall-clock time the conflict
  was first observed.
- Adoption fires only once `now - firstSeen > graceWindow` AND the verdict still
  reports that `(name, ownHash)` as minority AND the canonical hash is still
  quorum-backed. Default window ~24h (same order as `DefaultCanonicalGCWindow`);
  a `--snapshot.adoption-grace` override is likely.
- Until the window elapses the publisher keeps advertising its own hash — it may
  still reach quorum and make adoption unnecessary.

### 7b — staged download (isolated, no live-file risk)

Runs automatically under policy `auto` and `stage`; under `warn` it runs only
when the operator invokes `adopt`.

1. **Delta**: from the minority verdict, the set of `(name, canonicalHash)` the
   node must obtain — canonical entries it holds at a non-canonical hash, plus
   canonical entries it is missing entirely.
2. **Staging dir**: `<snapDir>/.staging-<canonical-version>/`. The canonical
   version is `CanonicalView.Version()`. Never write into the live snapshot dir.
3. **Fetch** — *the architecture fork* (code survey, 2026-05-21). The torrent
   client (`db/downloader`) downloads every torrent flat into one configured
   dir (`cfg.Dirs.Snap`); `torrentsByName` is keyed by file name. A canonical
   file has the **same name** as the live minority file it replaces, so the
   canonical torrent cannot be added to the live client without colliding. And
   the canonical bytes are quorum-peer-seeded, not CDN-served, so a plain
   webseed HTTP GET is not enough. Two viable approaches:
   - **(A) Scoped staging client** — the adoption handler spins up a second,
     short-lived `Downloader`/torrent client rooted at the staging dir, fetches
     the canonical `(name, info-hash)` set there, then `rename`s into `snapDir`
     at cutover. Clean isolation; a crashed adoption just leaves a junk dir.
   - **(B) Custom storage path-mapping** — reuse the live client with an
     anacrolix/torrent storage impl that maps the canonical fetch into the
     staging dir. Less process overhead; reaches into torrent-library internals.
4. **Validate**: every staged file runs the full storage validator chain
   (the `StepValidator`s). Any failure aborts the **whole batch** — partial
   adoption would leave the state domain internally inconsistent.
5. 7b ends here. The staging dir is built and validated; nothing live is touched.
   This is a safe, independently testable checkpoint.

### 7c — atomic cutover (the risky part)

Runs automatically only under policy `auto`; under `stage`/`warn` it runs when
the operator invokes `adopt`. Performed under one `Inventory` write lock, after
the whole batch validates:

1. **The reader barrier already exists** (code survey, 2026-05-21 — good news).
   `Aggregator.commitGate` (a `sync.RWMutex`, accessor `CommitGate()` /
   `LockCollation()`) is the lock merges already take to replace files on disk:
   background readers hold `RLock` for their `db.View()`, the commit+prune+merge
   path holds `Lock`. An adoption cutover takes the same `commitGate.Lock()` —
   that both excludes a concurrent merge AND waits for in-flight readers to
   drain. `RoSnapshots.View()` then loads the segment set through an
   `atomic.Pointer` (`visible`), and `OpenFolder()` rebuilds it from a disk
   rescan under `dirtyLock`, atomically `Store`-ing the new pointer — so once
   the renames are done and `OpenFolder` runs, a new `View()` sees old-complete
   or new-complete, never half. Existing held views finish on old inodes
   (problem 2). No new barrier primitive is needed.
2. **rename(2)** each staged artefact (.seg, .idx, .vi, .kvi …) of every batch
   member over its live counterpart. Each rename is atomic; the batch is ordered
   so no name is half-present.
3. **Rewrite inventory**: `FileEntry.TorrentHash` ← canonical hash; reset
   `LifecycleState` to `LifecycleDownloaded` so the validator chain re-runs and
   re-advertises only after re-validation. Emit a single `ChangeSet`.
4. **Invalidate**: drop the superseded torrents (`Downloader.DropTorrentByName`),
   evict stale `RollingV2Publisher` generations (`evictInvalidLocked` already
   handles this once names change), drop the affected per-peer manifest cache
   and any segment caches.
5. **Reopen + republish**: `OpenFolder()` so new views pick up the swapped set;
   `RollingV2Publisher.Publish()` a fresh generation advertising only canonical
   hashes — its self-check now passes.
6. Remove the staging dir.

In-flight consumers of the superseded files: their torrents are dropped in
step 4; their download of the non-canonical hash fails (correct — those bytes
never reached canonical) and the downloader retries the canonical info-hash from
another peer.

## Open questions for the implementation session

- **Where the adoption trigger lives.** A new component, or a method on the
  storage provider? It needs the minority verdict, the `CanonicalView`, the
  downloader, and the `Inventory` — that argues for the storage component. The
  same code path must be reachable both from the automatic policy pipeline and
  from the `adopt` operator command.
- **`adopt` against a running node.** `reset` runs against a stopped datadir.
  `adopt`'s cutover step needs the live `Inventory` lock + reader barrier, so it
  either (a) requires the node stopped (simplest, matches `reset`), or (b)
  signals the running node to perform the cutover. Decide which; (a) is the
  safer first cut.
- **Staged-fetch mechanism** — approach (A) scoped staging client vs (B) custom
  storage path-mapping, in 7b step 3. (A) is the cleaner first cut.
- **Deep validation of staged files.** `AllFilesPresent` is path-parameterised
  and works on a staging dir, but `CommitmentDomainValidator` queries the live
  DB/BlockReader, not arbitrary paths — staged commitment files cannot get the
  full validator chain until after they are in place. Likely: cheap checks
  (presence, size, info-hash match) pre-cutover; full `StepValidator` chain
  post-cutover with rollback-on-fail.
- **Caplin / beacon files** — are they in scope for adoption, or blocks/state
  domains only?
- **Crash mid-cutover.** Staging dir + a small intent journal so a crashed
  cutover is either rolled back or completed on restart. `ReplaceWithMerge`'s
  deferred-eviction model is the precedent to study.
- **Interaction with the merge pipeline** — a merge running concurrently with a
  cutover both mutate inventory + files; they must be mutually exclusive.

## Sequencing

7b is safe to land on its own (staging dir, no live mutation). 7c should land
only after the cutover path is reviewed in isolation — it is the one change in
this whole effort that can corrupt a running node's state reads.

## Follow-up modes (out of scope for 7b/7c)

The flat `auto`/`stage`/`warn` policy ships first. Two graduated refinements are
deferred follow-up work (user, 2026-05-21):

- **Age-graduated adoption confidence.** A flat policy treats every minority the
  same. Snapshot merges happen at *known* step/range boundaries, so a node can
  predict when a file is *expected* to be superseded:
  - A hash change for a file at or near an imminent merge boundary is routine —
    adopt readily.
  - A hash change for an old, well-established file far from any merge boundary
    is suspicious — it smells closer to corruption or an attack than a routine
    minority. The node should be *more resistant* to auto-adopting it (raise the
    grace window, or downgrade to warn-only, or require a higher quorum).
  So adoption confidence becomes a function of (file age, distance to the next
  expected merge), not one global policy.
- **Download-the-merge vs recompute.** When the canonical change is a large
  file merge, re-downloading the finished merged file may be cheaper than
  computing the merge locally on the node — make that a per-size choice
  (small merges: compute locally from held constituents; large merges: fetch
  the merged file). This is a fetch-planning optimisation layered on staged
  download.
