# Storage views contract — spec for app integration

## Goal

The snapshot-flow PR (#20933) ships the inventory + manifest + producer-gate
scaffolding. Application integration is the next branch. Three open
architectural questions were captured at restart in
`app-integration-review-items.md`:

1. Publishable view stability vs local-file mutation.
2. Unified external view across `db/state.Aggregator`, `freezeblocks.RoSnapshots`,
   and `node/components/storage/snapshot.Inventory`.
3. Bridge direction — validation as a local plugin rather than a bridge target.

This spec answers the storage-side half (#1 and #2). #3 is captured separately
in the same memory file; its execution is independent and lands in parallel.

The decisions here are **the contract** the next branch's code will land
against. The scenario matrix is the contract's enforcement mechanism.

## Existing landscape

Empirical comparison of the three view models lives in the conversation
research pass; the load-bearing facts are:

- **`db/state.Aggregator`** (db/state/aggregator.go) provides scoped read
  views via `BeginFilesRo() *AggregatorRoTx`. Atomic-pointer-swap on
  `aggregatorVisible`; held views see the snapshot they captured. Eviction
  via snapshot isolation — old views keep their old visibility. Files in
  the visible set are assumed fully readable.
- **`db/snapshotsync.RoSnapshots`** (db/snapshotsync/snapshots.go) provides
  scoped read views via `VisibleSegments.BeginRo() *RoTx`. Atomic-pointer-swap
  on `snapshotVisible`. Eviction via segment refcount — segment stays open
  until last reader closes. Files in `visible` are frozen and indexed.
- **`node/components/storage/snapshot.Inventory`** (node/components/storage/snapshot/inventory.go)
  is a live mutable metadata registry. No `BeginRo`, no scoped view, no
  `Close`. Tracks `Local` (on-disk) and `Trust` (verified) flags
  per-`FileEntry`. Performs no I/O — declared state only.

Aggregator and RoSnapshots are **read handles**. Inventory is a **metadata
registry**. The two categories are not interchangeable; unifying them under
one interface conflates "what's declared and trusted" with "what I can
operationally read right now". They should stay structurally distinct.

What's missing today: when local-file mutation happens during operation
(downloads landing, merges firing, post-merge eviction, mid-validation),
the read handles have no notion of "temporarily unavailable" — they treat
their visible set as ready by construction. In the snapshot-flow regime
where backfill happens while the node is operating, consumers reading
files not yet local hit a hard error. The right outcome is a soft-fail
that consumers can wait on or surface as retryable.

## Contract — four parts

### 1. Transition contract

Inventory is the source of truth for the publishable manifest and for
`Local` / `Trust` / `Advertisable` state. It mutates in response to
downloads landing, validations completing, merges firing, and post-merge
eviction.

The contract is MVCC-shaped:

- **Held views are immutable snapshots.** A consumer obtains a held view via
  `Inventory.View() *InventoryView` (new). The view captures the current
  set of `FileEntry` references at that moment. Subsequent mutations to
  Inventory do not affect the held view.
- **Mutations are atomic transitions.** AddFile, RemoveFile, ReplaceWithMerge,
  MarkAdvertisable, etc. complete atomically — held views see either the
  pre-state or post-state, never a partial. Internally implemented as
  copy-on-write of the underlying entry slice + atomic-pointer-swap on the
  view-source pointer.
- **Eviction defers until held views release.** When a file is removed
  (post-merge constituent eviction is the canonical case), the file's
  on-disk bytes do NOT get deleted while any held view still references
  it. A held view's references count toward an internal refcount that
  gates the actual unlink. This matches `RoSnapshots`'s segment-refcount
  pattern; we reuse the mechanism rather than invent a parallel one.
- **ChangeSet emission.** Each mutation emits a `ChangeSet` describing the
  transition: files added, files removed, files replaced (merge), trust
  changes, advertisable changes. Emitted via the framework event bus
  (`flow.InventoryChanged{ChangeSet}` — new event). Consumers that need to
  react (read handles updating their expected set, monitoring,
  operator-facing logs) subscribe.
- **Held view lifetime.** Explicit `(*InventoryView).Close()`. Multi-call
  safe. Refcount-decrement on close releases any deferred eviction. Leak
  detector wraps construction (same pattern Aggregator uses for
  AggregatorRoTx).

Reference comparator for the eviction-after-held mechanism:
`db/snapshotsync/snapshots.go:498–539` (BeginRo + Close + DirtySegment
refcount). The Inventory implementation tracks file-level refcount on
`FileEntry`; close decrements; mutations check refcount before unlinking.

### 2. Read-handle behavior

`db/state.Aggregator` and `db/snapshotsync.RoSnapshots` gain an `Inventory`
dependency at view construction. The visible set captured at
`BeginFilesRo` / `BeginRo` time is paired with an *expected* set sourced
from Inventory: files that are declared, trusted, but not yet `Local`.

The consumer-facing read API does NOT change shape. `HeaderByNumber`,
`RangeAsOf`, etc. keep their existing signatures. The behavior changes:

- **Today:** read of a not-yet-local block → hard error.
- **New:** read of a declared-but-not-local block (in the view's expected
  set) → block until the file lands, bounded by `ctx`. On context
  cancellation or deadline exceeded → `errors.Is(err, ErrPending)`.

Detection mechanism inside the read handle:

1. Read attempts the local file as today.
2. On miss, check the view's expected set (captured at construction).
3. If expected: wait on a "file landed" signal sourced from Inventory's
   ChangeSet (filtered to the specific file). Wait is bounded by `ctx`.
   On signal: retry the local read; return Ready or move to step 4.
4. If not expected, or wait elapsed: return `ErrPending` (expected) or
   the existing not-found error (not declared = Missing).

Consumer-side discipline: pass a meaningful `ctx` deadline. Most call
sites already do this. Code that legitimately can't wait sets a short
deadline (or `context.WithTimeout(ctx, 0)`) and handles `ErrPending`
explicitly. Code that's happy to wait passes the request-scoped context
through.

`ErrPending` is exported from a neutral package
(likely `node/components/storage/views`) so consumers can check
`errors.Is(err, views.ErrPending)` without import cycles.

### 3. Forward-availability projection

A separate, optional API for *planners* — components that need to know
what's ready, pending, or missing without performing a read. Use cases:

- Peer manifest exchange — advertise files with their availability flag.
- RPC range-available endpoints — answer "is block range [N, M] queryable
  now?" without iterating reads.
- Monitoring / operator dashboards — surface backfill progress.
- Eth/68 GetBlockHeaders responder — short-circuit replies for ranges
  marked Pending rather than blocking the protocol.

Shape (illustrative; final names settle in code):

```go
type AvailabilityState int

const (
    Ready   AvailabilityState = iota // local + advertisable
    Pending                           // declared, not yet local (or mid-validation)
    Missing                           // not declared
)

type ViewProjection interface {
    // Snapshot the projection at the moment of call.
    Availability(scope ScopeKey) AvailabilityState
    // Range projection — returns a sparse map keyed by file or sub-range.
    AvailabilityRange(scope ScopeRange) AvailabilityMap
}
```

Implementations live next to the read handles; shape is shared.
Projections are point-in-time; they do NOT block. They are NOT a
substitute for reads — they describe; reads consume.

### 4. Test strategy

The contract above is enforced by tests that drive operational scenarios
through the consumer API. The consumer API is fixed; the variables are
setup, ctx, and observed effect (latency band, error type, value
correctness).

#### Harness shape

A new package alongside the existing event-flow harness:
`node/components/integration/storage/harness/` (proposed; final location
flexible). Provides:

- **Inventory driver.** Construct an Inventory; mutate it on a controlled
  schedule (`AddFile`, `MarkLocal`, `MarkAdvertisable`, `Remove`,
  `ReplaceWithMerge`).
- **Wired read handles.** Construct Aggregator + RoSnapshots paired with
  the test Inventory. Optionally back them with a fake on-disk layer for
  scenarios that exercise eviction or refcount mechanics.
- **Controllable clock.** Tests set wall-clock-independent timing:
  "at t=100ms, mark file X local" + "read with ctx deadline 200ms should
  return Ready within (90, 200)ms". Determinism is non-negotiable —
  flaky timing tests poison the suite.
- **Observation API.** `(value, latency, err)` triples per read. Latency
  bands assert "returned promptly" or "returned at or after the
  scheduled mutation".
- **Held-view assertions.** `view.RefCount(file)`,
  `inventory.PendingDeletes()` — exposed for test inspection so we can
  verify eviction-after-held-view actually defers.

The harness reuses the simulation-first methodology that worked for the
event-flow scenarios (per `feedback-harness-testing-methodology.md`):
deterministic scenarios first; real-network surprises encoded back as
deterministic scenarios.

#### Scenario matrix

| #  | Setup | Read | Mutation | Expected outcome |
|----|-------|------|----------|------------------|
| 1  | File F local. | `HeaderByNumber(F)` | none | Ready, latency ≈ 0. |
| 2  | F declared, not local. | `HeaderByNumber(F)` with ctx deadline 200ms. | At t=100ms, mark F local. | Ready, latency ∈ (95, 200)ms. |
| 3  | F declared, not local. | `HeaderByNumber(F)` with ctx deadline 50ms. | none | `ErrPending`, latency ≈ 50ms. |
| 4  | F not declared. | `HeaderByNumber(F)` with any ctx. | none | not-found error (Missing); no wait. |
| 5  | View V held; merge fires replacing constituents C1, C2 with merged M. | `HeaderByNumber(b)` via V where b ∈ C1. | At t=100ms, ReplaceWithMerge. | Ready from C1 (the held view sees pre-merge state). New view opened post-merge sees M. |
| 6  | View V held; constituent C1 evicted post-merge. | Read via V. | At t=100ms, Remove(C1). | Ready (eviction deferred); on `V.Close()`, C1 actually unlinked. Inventory.PendingDeletes empty after close. |
| 7  | F1 (Phase 0 latest) local; F2 (Phase 1 backfill) declared, not local. | Read F1, read F2 in parallel. | none | F1 Ready immediately; F2 Pending. |
| 8  | All Phase 0 and Phase 1 files local after backfill complete. | Read across full range. | none | All Ready. |
| 9  | F local, not yet advertisable (mid-validation). | Read F. | At t=100ms, MarkAdvertisable(F). | Pending until mark; Ready after. (Confirms reads gate on advertisable, not just local.) |
| 10 | F was local, corruption detected, marked re-download. | Read F. | At t=100ms, mark F not-local; at t=200ms, mark F local again. | Pending during gap; Ready after re-download lands. |

Each scenario maps to one test. The matrix expands as we encode
real-network surprises (per the methodology feedback). The matrix above is
the floor, not the ceiling.

#### What the tests do NOT assert

- **Specific latency floors.** The harness asserts bands, not point
  values. CI variance must not flake the suite.
- **Specific file paths or on-disk layout.** The contract is API-level;
  the on-disk layer is a fake when not relevant.
- **Implementation details of refcount or atomic pointer.** The contract
  is the externally-observable behavior. Refactoring the mechanism stays
  green.

## Sequencing

Each step a separate commit on `feat/snapshot-flow-app-integration`:

1. **This spec** — first commit, no code.
2. **Test harness** — `node/components/integration/storage/harness/`.
   Inventory driver, controllable clock, observation API. Stub implementations
   for read handles so the harness validates against the spec without
   waiting on real implementations.
3. **Item #1 — Inventory held-view discipline.** `(*InventoryView)`,
   `View()`, `Close()`, refcount, deferred eviction, ChangeSet emission.
   Scenarios 5, 6 pin it.
4. **Aggregator + RoSnapshots wait-or-pending.** Inventory dependency at
   view construction; expected-set capture; wait-on-ChangeSet at read
   time; `ErrPending` typed sentinel. Scenarios 1–4, 7, 8, 9, 10.
5. **Forward-availability projection.** Additive, planner-facing.
6. **Item #3 — validation extension point** (independent; lands in
   parallel from any of the above per the working decision in the
   memory file).

After the spec lands, the test harness goes in next so subsequent code
lands against passing scenarios.

## Out of scope for this branch

- **Lazy evaluation.** The read API is shaped to allow it (wait-or-pending
  is the affordance lazy needs), but no read handle defers file-open in
  this branch. A future change can introduce it without API impact.
- **Cross-file consistency validators** (commitment chain across .kv
  generations, history/kv alignment). Stage 2 of the validation
  framework, deferred per `feature-pluggable-validation-phase.md`.
- **Operator policy knobs.** ctx is the only mechanism for "how long
  willing to wait"; configurable defaults per consumer class come later
  if needed.

## References

- `app-integration-review-items.md` (memory) — the three review items
  this spec answers.
- `inventory-visible-set-views.md` (memory) — the held-view contract
  framing.
- `feedback-harness-testing-methodology.md` (memory) — simulate-first.
- `feedback-external-reference-tests.md` (memory) — open-loop test
  construction.
- `db/state/aggregator.go:2135–2155, 2361–2376, 1599–1616` — Aggregator
  view construction, close, atomic transition.
- `db/snapshotsync/snapshots.go:498–539, 244–258, 823–883` — RoSnapshots
  BeginRo, segment refcount, atomic transition.
- `node/components/storage/snapshot/inventory.go:51–98, 143–355` —
  current Inventory shape (pre-contract).
- `docs/plans/20260430-snapshot-flow-objective.md` — the prior PR's
  design doc; this spec builds directly on it.
