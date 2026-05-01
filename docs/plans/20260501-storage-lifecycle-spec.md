# Storage-owned snapshot import lifecycle — spec

## Why this branch grew

The storage-views spec (`20260430-storage-views-spec.md`) called for read
handles to gain wait-or-pending behavior on read-miss, with the held-view
discipline on Inventory as foundation. Items #1 and #2 from
`app-integration-review-items.md` answered the data model. But item #2's
"unified external view" question, when chased through the real
choreography of file import, surfaced a deeper structural issue:

  - Today's stage loop calls `BlockRetire.BuildMissedIndicesIfNeed`
    from `execution/stagedsync/stage_snapshots.go:343`. The stage loop
    decides "now is the time to build indices" — a storage-domain
    decision living in the exec component.
  - `RetireBlocksInBackground` and `BuildMissedAccessorsInBackground`
    are similarly stage-triggered.
  - Wait-or-pending built on top of that signal would bake the
    boundary violation into the read path.

Stream 2 (move index-build into storage) is therefore a prerequisite
for Stream 1 (wait-or-pending), not a follow-on. This branch is the
boundary fix. Wait-or-pending falls out as a natural consequence once
storage owns the full file-import lifecycle.

This spec **supersedes** `20260501-readhandle-integration.md`. The
read-handle integration shape (Awaiter interface, `SetAwaiter` on
RoSnapshots/Aggregator, `MarkLocal`-from-OnFilesChange) is still
accurate but lands at the END of this branch, after the lifecycle
relocation. The earlier doc is kept for reference.

## Today's boundary violation

```
   ┌──────────────────────┐
   │  execution/stagedsync│
   │  (exec component)    │
   └──────────┬───────────┘
              │ buildOrDeferE2Indices (stage_snapshots.go:337)
              │   → blockRetire.BuildMissedIndicesIfNeed
              │       → snapshots().BuildMissedIndices
              │           → recsplit, .idx generation
              │           → recalcVisibleFiles
              │           → onFilesChange callback
              ▼
   ┌──────────────────────┐
   │  db/state            │
   │  db/snapshotsync     │  ← storage internals; index-build code,
   │  (storage internals) │    file lifecycle, recalcVisibleFiles
   └──────────────────────┘
```

The stage loop's role: pick the moment to invoke index-build, then
proceed with downstream stages. The storage internals are reactive — they
do whatever stagedsync tells them. From storage's perspective, files
become visible "whenever the stage loop happens to call BuildMissedIndices".

This was tolerable when downloading-while-operating wasn't a regime; the
node downloaded, then operated, in clean sequence. With snapshot-flow
backfill, files arrive continuously during operation. Stagedsync isn't
the right place to decide when to integrate them.

## The lifecycle storage owns

Storage component takes ownership of the full file-import pipeline:

```
   File state machine (per FileEntry):

   Declared  ──────►  Downloading  ──────►  Downloaded
                                                │
                                                │ (primary file on disk;
                                                │  dependent index files
                                                │  not yet built)
                                                ▼
                                            Indexing
                                                │
                                                │ (build .idx / .kvi /
                                                │  .ef / .efi as required
                                                │  by the file's kind)
                                                ▼
                                            Indexed
                                                │
                                                │ (all dependent files
                                                │  present; structural
                                                │  validators may run)
                                                ▼
                                            Validating
                                                │
                                                ▼
                                           Advertisable  (= Ready for read)
```

Each transition is driven by storage component, not by stagedsync.
Triggers:

  - **Declared → Downloading**: peer manifest seen, download requested.
    Already snapshot-flow's job.
  - **Downloading → Downloaded**: downloader signals primary file
    landed. Already wired.
  - **Downloaded → Indexing → Indexed**: storage fires index-build for
    the file's dependencies. **This is the relocation work.**
  - **Indexed → Validating → Advertisable**: validator chain runs.
    Producer-side validation framework is the home; item #3's
    extension-point model applies (each validator is its own
    component, registered into the chain).
  - **Advertisable**: visible to read handles; included in V2 manifest;
    seedable via downloader.

Read handles' "Ready" gate is `Advertisable=true`. `Local=true` alone
is not sufficient — a primary on disk without its index isn't yet
readable by Aggregator/RoSnapshots. The two flags collapse from the
read perspective: Advertisable implies the full lifecycle has
completed.

## File-level dependency model

`FileEntry` grows explicit knowledge of what dependent files it
requires for the lifecycle to advance. Two design options:

**Option A — explicit dependency list on FileEntry.**

```go
type FileEntry struct {
    // ... existing fields ...

    // Dependencies — names of files that must also be Local before this
    // entry can advance to Indexed. Empty for files that have no
    // dependents (idx files themselves, salt, meta).
    Dependencies []string
}
```

Each primary file's Dependencies are populated at AddFile time, derived
from the file's kind:
  - `.seg` (block primary) → `.idx` (recsplit accessor)
  - `.kv` (domain primary) → `.kvi` (b-tree accessor)
  - `.v` (history primary) → `.ef` (existence-filter), `.efi` (its accessor)
  - others (salt, meta, caplin) → no dependencies

**Option B — derive dependencies via a DependencyResolver.**

```go
type DependencyResolver interface {
    Dependencies(*FileEntry) []string
}
```

Resolver registered on Inventory at construction. Lookup happens at
state-transition time, no per-entry storage cost.

**Decision: Option A.** Dependencies are file-name-shaped and stable
(purely a function of kind + name); storing them on the entry makes the
inventory self-contained and serialisable for V2 manifest emission.
Option B's flexibility isn't load-bearing today; if a future kind
needs computed dependencies, it can be added then without breaking
existing serialisation.

A new state field captures the lifecycle position:

```go
type LifecycleState int

const (
    LifecycleDeclared    LifecycleState = iota
    LifecycleDownloading
    LifecycleDownloaded   // primary local, deps may not be
    LifecycleIndexing
    LifecycleIndexed      // primary + deps all local
    LifecycleValidating
    LifecycleAdvertisable // == Ready (internal + external)
)
```

`Local` stays as a derived "on disk" accessor (`state >= LifecycleDownloaded`)
— the downloader and seeding paths need to know what bytes have been
fetched, regardless of whether they're indexed/validated yet.
`Advertisable` becomes the **single Ready gate** for both internal reads
and external publication: `state == LifecycleAdvertisable`. A file that
hasn't passed structural validation isn't served internally OR
externally — validation is the same gate, we just hadn't been using it
that way internally.

This collapses what were two flags into one signal. `WaitForReady`
becomes "wait for Advertisable" by default; the existing
`requireAdvertisable=false` parameter in the current API stops being
useful for production code (kept as a tooling back-door for
operator-side debugging that wants pre-validation reads).

`Seeding` stays as a separate orthogonal flag — operator policy on
whether to actively serve the file via BitTorrent, independent of
readiness.

## Per-stage orchestration

The storage component runs a small state-machine driver. Triggers
arrive via the event bus or direct callbacks (downloader signals,
file-system watchers, post-build callbacks). Each trigger advances
files through one transition; the driver loops idempotently.

Concrete stage handlers (each a method or small package on storage):

  - **importDownloaded(name)**: called when downloader reports primary
    file complete. Advances `LifecycleDownloading → LifecycleDownloaded`.
    Schedules indexing if dependencies are missing.

  - **runIndexing(name)**: builds the dependent files (`.idx`, `.kvi`,
    `.ef`, etc.) using existing build code from `db/recsplit`,
    `db/state`. Calls into the same code stagedsync calls today; the
    *trigger* moves, not the build implementation. Advances
    `LifecycleDownloaded → LifecycleIndexing → LifecycleIndexed`.

  - **runValidation(name)**: invokes the producer-side validator chain
    (item #3's extension point). Validators are pluggable per the
    extension-point design; each validator that fails halts the file's
    promotion. Advances `LifecycleIndexed → LifecycleValidating →
    LifecycleAdvertisable` (or back to a quarantined state on failure).

  - **promote(name)**: `Advertisable=true`. Emits ChangeSet so read
    handles' WaitForReady wakes and includes the file in the V2
    manifest's next emission.

The driver's clock: event-bus triggered, with a periodic sweep for
idempotency (catch any file that landed via a code path that didn't fire
its event correctly). Periodic sweep is what `RetireBlocksInBackground`
and `BuildMissedAccessorsInBackground` were doing in stage form; their
content moves but their purpose remains.

## Validation-as-extension at the validate stage

Item #3's "validation as extension point" lives here naturally. The
`runValidation(name)` step iterates a pluggable chain of
`BatchValidator` components. Per the working decision in
`app-integration-review-items.md`:

  - `db/integrity` provides validator types (no bridge, no enum dispatch).
  - Extensions add validators by writing self-contained components that
    implement `validation.BatchValidator`.
  - The chain's composition lives in storage component config; operator
    can disable specific validators if needed.
  - `EXTENDING.md` + an example extension package land alongside the
    relocation.

This is also why item #3's working decision flagged "may not be the
first move on the branch — items #1 and #2 may dictate validator
dependency shapes". The shape now is: validators run inside the
storage-owned lifecycle, gated on `LifecycleIndexed`, advancing to
`LifecycleAdvertisable` on pass.

## Wait-or-pending fallout

With the lifecycle owned by storage, wait-or-pending becomes a
straightforward observation:

  - Read handle reads file F.
  - Miss in visible set.
  - Read handle calls `inventory.WaitForReady(ctx, F, requireAdvertisable=true)`.
  - WaitForReady blocks on inventory ChangeSet until F's
    `LifecycleState == LifecycleAdvertisable`, or ctx expires
    (`views.ErrPending`), or F is undeclared (`ErrNotFound`).
  - On nil return, read handle re-loads atomic visible pointer (which
    was updated by `recalcVisibleFiles` AT THE SAME MOMENT
    `LifecycleAdvertisable` was set), retries.

The Awaiter interface, `SetAwaiter` on RoSnapshots/Aggregator, and
`MarkAdvertisable` driven from `runValidation` completion are all the
same wires the previous micro-doc described. The difference: the signal
they consume comes from a coherent storage-owned lifecycle, not from a
stage-driven trigger.

## Migration: stage loop relocations

Concrete code movements (not exhaustive — pinned during implementation):

| Currently in `execution/stagedsync` | Moves to |
|---|---|
| `stage_snapshots.go::buildOrDeferE2Indices` | storage component's lifecycle driver |
| `stage_snapshots.go::*BuildMissedIndices*` calls | `runIndexing` step |
| `stage_snapshots.go::*BuildMissedAccessors*` calls | `runIndexing` step |
| `RetireBlocksInBackground` invocation from `SnapshotsPrune` | storage component's periodic sweep |
| `BuildMissedAccessorsInBackground` invocation from `SnapshotsPrune` | storage component's periodic sweep |
| `cfg.notifier.Events` plumbing for index-build progress | inventory ChangeSet emission |

The stages keep their *consumer* role: read via BlockReader and
Aggregator (now wait-or-pending capable). They no longer drive the
storage lifecycle.

`stage_snapshots.go` itself does not disappear — it still owns the
download-trigger / progress-tracking shape on the stage side. What
moves out is the index-build orchestration.

## Sequencing on this branch

Existing commits stay. New work proceeds:

1. **This spec** — supersedes `20260501-readhandle-integration.md`.
2. **FileEntry lifecycle state + dependencies** — additive; existing
   `Local`/`Advertisable` derived from `LifecycleState`. Inventory's
   public API gains `LifecycleState(name)` accessor; mutation methods
   shift to advance state rather than set flags directly.
3. **Storage component lifecycle driver skeleton** — handlers for each
   transition, event-bus subscription, periodic sweep loop. Initially
   no-op when the inputs aren't wired (existing OnFilesChange path
   continues to drive things until step 6 cuts over).
4. **runIndexing implementation** — `BuildMissedIndices` / accessor
   builders called from the driver instead of from the stage. Stage
   calls remain in place behind a config gate so the cutover is
   reversible.
5. **runValidation implementation** — wire validator chain + extension
   point. Item #3's `EXTENDING.md` + example package land here.
6. **Cutover: stage calls disabled in config**, storage drives.
   Integration tests verify equivalence across a synthetic full-import
   cycle.
7. **Awaiter interface + SetAwaiter** — read handles take the optional
   wrapper. `views.Awaiter` lands as the single-method interface.
8. **RoSnapshots wait-or-pending** — read methods consult Awaiter on
   miss, retry via atomic visible.
9. **Aggregator wait-or-pending** — same shape.
10. **Production wiring** — storage Provider calls `SetAwaiter` and
    drives the lifecycle. CLI tools and tests unchanged.
11. **Stage code cleanup** — once cutover is stable, remove the old
    stage-driven calls (one commit, low-risk now that storage owns it).
    The `LifecycleDrivenByStorage` feature flag survives this step as
    a kill-switch; it is removed only after the storage-driven path
    has accumulated production hours across multiple releases.

## Out of scope for this branch

  - **Extension indexing.** Future work could let the storage
    component manage *additional* indices — secondary indices à la a
    standard database, custom validator-driven indices, or
    consumer-supplied indices. The lifecycle's pluggability admits
    this naturally (additional dependent file types, additional
    indexing handlers in `runIndexing`), but no concrete extension
    indices are designed in this branch. Captured here as forward
    context only — does NOT drive current decisions.
  - **Cross-file consistency validators** (commitment chain across
    `.kv` generations, history/kv alignment). Stage 2 of the
    validation framework. Slot in via the extension point once
    `runValidation` is in place.
  - **Operator policy knobs** for backfill prioritisation, indexing
    parallelism limits, validator selection. Sensible defaults this
    branch; tunables when real deployment surfaces real needs.
  - **Forward-availability projection** (planner-facing API per the
    storage-views spec §3). Lands after the lifecycle is owned by
    storage.

## Open questions — flagged for review

  - **Periodic sweep cadence.** Today's `RetireBlocksInBackground`
    runs once per stage cycle. After relocation, the sweep is internal
    to storage; cadence is independent. Default 60s? Configurable?
    Pinned during implementation.

  - **Reversibility of the cutover (step 6).** Decided: feature flag
    `Snapshot.LifecycleDrivenByStorage` (bool, defaults to false at
    merge), flipped to true once the storage driver is exercised in
    integration tests and at least one canary deployment. The flag
    **survives the bedding-in period** — step 11 (legacy removal) does
    NOT delete the flag itself; the flag stays as a kill-switch
    through subsequent releases until the storage-driven path has
    accumulated enough production hours that we're confident the
    fallback is no longer worth carrying.

    Risk: a bug in the storage driver leaves a node with neither side
    driving. Mitigation: integration test the cutover explicitly; the
    feature flag is the operator-side rollback if a soak reveals
    issues.

  - **Boundary with downloader.** Downloader currently reports file
    completion; storage takes it from there. If validation fails and
    we re-download, who decides? Probably storage signals the
    downloader, but the contract needs pinning during step 3.

## References

  - `docs/plans/20260430-snapshot-flow-objective.md` — prior PR's
    objective; foundation this builds on.
  - `docs/plans/20260430-storage-views-spec.md` — read-API contract
    (still valid; sequencing tightens around this lifecycle).
  - `docs/plans/20260501-readhandle-integration.md` — superseded by
    this doc; kept for reference.
  - `app-integration-review-items.md` (memory) — items #1, #2, #3.
  - `feature-pluggable-validation-phase.md` (memory) — validation
    component shape; this spec's `runValidation` is its concrete
    home.
  - `inventory-visible-set-views.md` (memory) — held-view contract.
  - `existing-integrity-checks.md` (memory) — what `db/integrity`
    already provides; integration target for the validator chain.
  - `execution/stagedsync/stage_snapshots.go:337-350` — current
    boundary violation site (`buildOrDeferE2Indices`).
  - `db/snapshotsync/freezeblocks/block_snapshots.go:526` —
    `BuildMissedIndicesIfNeed`, the relocation target.
  - `node/components/storage/provider.go:107` — Provider's existing
    Initialize wiring; the new lifecycle driver is added here.
