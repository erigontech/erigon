# Read-handle wait-or-pending integration — design micro-doc

## Context

The storage-views spec (`docs/plans/20260430-storage-views-spec.md`)
called for `db/state.Aggregator` and `db/snapshotsync.RoSnapshots` to
gain wait-or-pending behavior on read-miss. The contract is now in
place — `snapshot.Inventory` carries the held-view discipline and
`WaitForReady` is the building block. This doc decides HOW to wire that
behavior into the two read handles without leaking the Inventory
dependency into client code.

Three things to settle before code:

  1. The wrapper interface read handles depend on.
  2. Where the wait fires inside a read, including MVCC implications
     for held views.
  3. Staging: which read handle first, and why.

## 1. The wrapper interface

Read handles depend on a small interface, not the concrete
`*snapshot.Inventory`. The interface declares only what the read handle
actually needs:

```go
// Awaiter — the surface a read handle uses to wait for a declared-but-
// not-local file to become Ready. Satisfied by *snapshot.Inventory and
// any test/stub that needs to substitute its semantics.
type Awaiter interface {
    WaitForReady(ctx context.Context, name string, requireAdvertisable bool) error
}
```

Lives in a neutral location: either `node/components/storage/views`
(extending the existing typed-error package) or a sibling
`node/components/storage/awaiter`. Decision below — keep it in `views`
to avoid adding a single-type package; the `views` package's purpose is
"cross-cutting types shared by storage read paths" and Awaiter fits.

`*snapshot.Inventory` satisfies Awaiter structurally; nothing in the
inventory package changes.

Read handles take `Awaiter` as a constructor parameter, optional/nil-
safe:

  - Nil Awaiter → today's behavior. Hard fail on read-miss with
    whatever the existing not-found path returns. Existing call sites
    that don't yet pass an Awaiter compile and behave unchanged.
  - Non-nil Awaiter → on read-miss, the read consults the Awaiter
    before returning the not-found error.

This makes the integration a non-breaking change. Production wiring
(nodebuilder, storage component) passes the inventory's Awaiter
explicitly; everything else proceeds in steps.

## 2. Where the wait fires

The read handle's external API does not change shape. Consumers call
`HeaderByNumber(ctx, n)` / `RangeAsOf(ctx, ...)` and receive
`(value, error)`. Inventory does not appear in their import graph.

Internal flow at read-miss:

```
HeaderByNumber(ctx, n):
  1. seg := visible.find(containing n)         // existing logic
  2. if seg != nil { return readFromSeg(seg) } // existing happy path
  3. if awaiter == nil { return ErrNotFound }  // pre-integration behavior
  4. expected := inferExpectedFileFor(n)       // file-name from n + naming convention
  5. err := awaiter.WaitForReady(ctx, expected, false)
  6. switch err:
       - nil           → reload visible, retry from step 1; if still miss, ErrPending
       - ErrNotFound   → return the existing not-found error (file genuinely missing)
       - ErrPending    → return ErrPending (ctx expired)
```

Two subtleties:

### File-name inference (step 4)

The read targets a block number, txnum, key, or step range — not a file
name. The wait targets a file name. Each read-handle has the naming
convention internally (RoSnapshots knows `headers.{from}-{to}.seg`,
Aggregator knows `accounts.{fromStep}-{toStep}.kv` etc.). Step 4 uses
that internal mapping. No client awareness of file names.

If the requested key falls in a range NO file is declared for (no
expected file at all in the inventory's manifest), step 4 returns the
empty string and we skip directly to ErrNotFound — there is no point
waiting for a file that nothing has promised to deliver.

### MVCC for held views (step 6, retry)

A held `BeginFilesRo` / `BeginRo` captures `visible` at view-construction
time. If a read misses in that captured slice, two interpretations:

  - **Strict MVCC.** The tx is its snapshot. Reads see only what was
    captured at construction. WaitForReady would be useless inside a
    held tx because the captured slice never changes; the retry would
    miss again.
  - **Loose-on-miss MVCC.** The held tx is the consistency floor for
    the *visible* read path. On miss, the tx may reach beyond its
    snapshot via the inventory's expected set, wait, and resolve via
    the *current* atomic visible pointer.

Decision: **loose-on-miss**. The consumer passing a ctx with deadline
is opting in to "I want this data, willing to wait". A strict-MVCC tx
that returns ErrNotFound for files declared since open would be a
foot-gun. Producer-gate semantics still apply (advertisable filter
ensures consumers can't see unvalidated content); inventory's
held-view refcount keeps captured files readable through merge/eviction
even if they leave the visible set.

Concretely:

  - The held tx's CAPTURED visible slice is unchanged across the read.
    Files captured stay readable (refcount discipline).
  - On miss, the read consults the LIVE atomic visible pointer after
    WaitForReady returns nil. This is one atomic load, not a re-capture
    of the entire tx scope.
  - If a read needs strict MVCC (some snapshot-equality test, perhaps),
    the consumer passes a context with timeout 0 (`context.WithTimeout(ctx, 0)`)
    so WaitForReady returns ErrPending instantly without consulting
    live state. This is the explicit opt-out.

## 3. Staging

**RoSnapshots first.** Smaller surface — block-level reads
(`HeaderByNumber`, `BodyForBlock`, `BodyWithTransactions`,
`BlockBy*`). Naming convention is already exposed
(`headers.{from}-{to}.seg`). Three read paths, all in `BlockReader`.

**Aggregator second.** Larger surface — domain reads (`GetLatest`,
`GetAsOf`, `RangeAsOf`, `IteratePrefix`), plus inverted-index reads.
Different naming conventions per kind (`.kv` / `.v` / `.ef`). More
constructors that propagate the dep into. Worth landing only after the
RoSnapshots integration validates the pattern.

**Stage 1 (RoSnapshots, separate commit):**

  - `Awaiter` interface lands in `views` package.
  - `RoSnapshots` constructor accepts optional `Awaiter`.
  - `BlockReader.HeaderByNumber` is the reference application: detects
    miss, infers expected file name, delegates to Awaiter, retries via
    current visible. Other BlockReader read methods follow the same
    pattern in the same commit.
  - Existing tests run unchanged (Awaiter=nil path).
  - New scenario: integration test that constructs RoSnapshots with the
    test Awaiter (an in-process `*snapshot.Inventory` driving file
    landings on a Schedule), validates wait-or-pending observable
    through the public `BlockReader` API. Reuses the existing scenario
    harness.

**Stage 2 (production wiring, separate commit):**

  - nodebuilder / storage component passes the inventory to RoSnapshots
    construction. Awaiter is no longer nil in production.
  - Whatever consumer reads existed and were hitting the hard-fail path
    on Phase-1 backfill now wait-or-pending. Behavior change is
    observable end-to-end.

**Stage 3 (Aggregator):**

  - Apply the same shape to Aggregator's read methods. Larger
    constructor delta because Aggregator has more entry points and is
    used from more places. Likely splits into sub-commits per read
    method class (latest / asof / range / iterate).

## What is NOT in scope for this integration

  - **Lazy file-open.** Files becoming local after WaitForReady are
    expected to be picked up by the existing recalcVisibleFiles
    lifecycle path (downloader/storage component fires a recalc on
    file-landed events). Reads do not trigger recalc directly — they
    just re-load the atomic visible pointer. If recalc hasn't run yet,
    the read returns ErrPending and the consumer retries, by which
    point recalc will have caught up.

  - **Forward-availability projection.** Step 5 of the spec's
    sequencing. Lands as its own commit after Aggregator integration.

  - **Cross-file consistency validators** (Stage 2 of the validation
    framework). Independent path; tracked separately in
    `feature-pluggable-validation-phase.md`.

  - **Operator policy knobs** (timeouts per consumer class, retry
    backoff). ctx is the only mechanism for now; configurable defaults
    arrive only if real consumer behavior demands it.

## Open question — flagged for review

The "loose-on-miss MVCC" decision means a held `BeginRo` can return
data from files declared *after* the tx was opened. This is a
deliberate softening of strict MVCC semantics, justified by "the
consumer passed ctx, they're willing to wait". Consumers expecting
strict MVCC (e.g. some test that asserts "reads see exactly the state
at Open time") may be surprised.

If review pushes back: the alternative is to make WaitForReady INSIDE a
held tx return ErrPending immediately (no actual wait), and require
consumers to drop and re-open the tx to access newer files. Smaller
behavioral change but more consumer churn. Not yet decided; flagged for
the integration commit's PR description.
