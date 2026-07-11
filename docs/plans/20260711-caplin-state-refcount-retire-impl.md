# Plan: extract refcounted-generation retirement core; adopt in caplin state

Context: `docs/plans/20260711-caplin-state-snapshot-tiering-and-prune.md` (handoff).
This is the **prerequisite (B)** for safe live state-snapshot merge/removal. Today
`CaplinStateSnapshots.RemoveOverlaps` unlinks files directly (documented offline-only)
because state views are not refcounted; the live Beacon-API/historical reader can race
`closeAndRemoveFiles`. EL block snapshots already solve this with refcounted visible
generations. Extract that core and adopt it in state.

**Ground truth = the current EL code** (`db/snapshotsync/snapshots.go`,
`BaseRoSnapshots`: `snapshotVisible` :549-555, `recalcVisibleFiles` :809-871,
`acquireVisible`/`releaseVisible` :876-893, `reclaimRetiredLocked`/`reclaimRetired`
:895-913). Block-path behavior must be **byte-identical** after the refactor — existing
tests are the oracle: `snapshots_race_test.go`, `snapshots_test.go`, `block_snapshots_test.go`.

Branch: `awskii/caplin-state-refcount-retire` off `origin/main`. TDD; cover-before-move.
`make lint` (repeat until clean), `make erigon`, package tests. This touches
consensus-critical, hot-path block-snapshot infra — preserve semantics exactly; do not
"improve" the algorithm.

## Design: one key/data-agnostic core; both sides enum-indexed

The extracted core is the **single machinery both consumers use**, and it must be
**key/data-agnostic**. Both sides key visibility by an **enum → `[]VisibleSegments`**: EL by
`snaptype.Enum`, CL by a new `CaplinStateType` enum (Task 4 converts CL off its current
`map[string]`). So both payloads are the same shape — `[]VisibleSegments` indexed by the
consumer's own enum — which is what makes the shared machinery clean.

Keying, payload iteration, watermark computation, and `recalc` logic live **entirely in each
consumer's payload type `P` and its own recalc**. The core (`visibleGenerations[P any]`)
treats `P` as opaque: it never reads, indexes, ranges, or type-asserts the payload. It
touches only `refcnt`, `retired`, `next`, `oldest`, and the shared `DirtySegment` type. If
the core needs anything payload-specific, the abstraction has leaked — push it back into `P`.

## Load-bearing invariants (from EL; violating any is a bug)

- **I1 — retirement source.** A segment is retired **only** when it is removed from the
  `dirty` tree (RemoveOverlaps, detach, stale-file cleanup). Ordinary visibility
  replacement (a larger file hiding a smaller one in the visible set) does **not** retire
  or unlink the hidden file — it stays in `dirty`. `recalcVisibleFiles` publishes a new
  visibility payload with `retired = nil`; only dirty-removal paths pass `retired`.
- **I2 — eager reclaim, two call sites, exact EL timing.** The recalc/publish path reclaims
  (and, for unlink-retirement, physically unlinks) the retired files of already-drained older
  generations **inline under the dirty lock** (EL snapshots.go:870). The release path (a
  reader dropping the last pin on a superseded generation) reclaims **off-lock** (EL
  reclaimRetired :908-913). Both must be preserved as-is. So a dirty-removal with no live
  reader unlinks **by return**; deferral happens only for generations a live reader pins.
- **I2b — retire is unlink, close is not.** Only permanent-removal paths (RemoveOverlaps,
  future merge cleanup) put files on `retired` → `closeAndRemoveFiles` (unlink). Shutdown
  (`Close`) and re-open stale-cleanup (`closeWhatNotInList`) must **close fds only, never
  unlink**. Never route `Close` through the unlink path.
- **I3 — drain gate.** A retired file is unlinked only after every generation that
  referenced it has `refcnt == 0`. A reader pinning an older generation keeps its files
  alive until it releases.
- **I4 — watermark consistency.** `segmentsMax`/`idxMax` are computed from the *candidate*
  payload before publish and live inside the generation; `BlocksAvailable()` reads both
  from the same current generation.

---

### Task 1: characterization tests for the generation lifecycle (cover before move)

Pin current EL behavior so the extraction can't regress it. Extend
`snapshots_race_test.go` / `snapshots_test.go` where gaps exist:

- [x] reader `acquireVisible`s a generation; a concurrent dirty-removal + recalc retires
      files → NOT unlinked while pinned; unlinked after release (I3).
      (`TestStackedGenerationsReclaimInOrder` + existing `TestRemoveOverlapsDefersUnlinkWhileViewOpen`.)
- [x] **no-reader path**: a dirty-removal + publish with no live reader unlinks the retired
      files **by the time the call returns** (I2). (Existing `TestRemoveOverlaps` — asserts 45→15
      files on disk by return; identified and kept.)
- [x] hazard-pointer retry: `acquireVisible` under concurrent recalc never returns a stale
      generation. (`TestReadPinnedSegmentSurvivesConcurrentRetire`, `-race`.)
- [x] stacked generations reclaim oldest→current only as each drains.
      (`TestStackedGenerationsReclaimInOrder`.)
- [x] **I1 visibility-only recompute**: open small segments, add an indexed covering segment,
      run recalc/OpenFolder with **no dirty removal** → the hidden small files stay on disk
      and in `dirty` (they are NOT retired/unlinked). This is the test that catches a Task-3
      regression treating hidden-but-dirty segments as retired.
      (`TestVisibilityOnlyRecomputeKeepsHiddenDirty`.)

Temporarily weaken the drain guard to confirm the tests bite, then restore. No production
change beyond making behavior observable.
Acceptance: tests green vs unchanged production code; `make erigon` builds.

---

### Task 2: extract the generic refcounted-generation core

New file `db/snapshotsync/visible_generations.go`, payload-opaque:

- [x] `type generation[P any] struct { payload P; refcnt atomic.Int32; retired []*DirtySegment; next *generation[P] }`
- [x] `type visibleGenerations[P any] struct { lock *sync.RWMutex; current atomic.Pointer[generation[P]]; oldest *generation[P] }`
- [x] `acquire() *generation[P]` — load→`refcnt.Add(1)`→re-check current→retry (hazard pointer).
- [x] `release(*generation[P])` — `refcnt.Add(-1)`; if 0 → `reclaim()`.
- [x] **`publish(newPayload P, retired []*DirtySegment)`** — caller holds `lock`. Build `next`;
      set `old.retired = retired`, `old.next = next`; store `current = next`; then
      `closeAndRemoveSegments(reclaimLocked())` **inline, under the lock** — byte-identical to
      EL recalc tail (snapshots.go:862-870). Publish is publish-**and**-eager-reclaim, never
      store-only. (I2: recalc/publish closes under lock; the release path closes off-lock.)
- [x] `reclaimLocked() []*DirtySegment` (walk `oldest`→current, collect retired of drained
      gens, advance `oldest`); `reclaim()` (lock, reclaimLocked, unlock, `closeAndRemoveSegments`
      **off-lock** — the release-path timing, EL :908-913).
- [x] `DirtySegment` and `closeAndRemoveSegments` stay shared/unchanged.

Acceptance: builds; a focused unit test of `visibleGenerations[int]` proves: publish with a
drained old gen deletes immediately; publish while a reader pins defers until release; hazard retry.

---

### Task 3: rewire BaseRoSnapshots onto the core (oracle stays green)

Behavior-preserving. `P = blockVisible{ segments []VisibleSegments; segmentsMax uint64 }`.

- [x] Replace `snapshotVisible` + `visible` + `oldestVisible` + `acquireVisible`/`releaseVisible`
      + `reclaimRetired*` with embedded `visibleGenerations[blockVisible]` sharing `dirtyLock`.
- [x] `recalcVisibleFiles(alignMin, retired)` builds `blockVisible` as today and calls
      `publish(payload, retired)` — which closes the drained files inline under `dirtyLock`,
      byte-identical to the current recalc tail (I2). Preserve I1 (recalc's `retired` still
      comes only from its callers' dirty-removal, unchanged — a visibility-only recalc passes
      `retired = nil`).
- [x] All readers route through the core's current payload / acquire.
- [x] **Hot-path allocation gate**: add a benchmark (or `-gcflags=-m` escape check) for
      `View`/`ViewType`/`ViewSingleFile` proving **no new heap allocs / interface boxing** vs
      pre-refactor (generic must monomorphize to concrete types).
      (`TestViewHotPathNoExtraAllocs` + `BenchmarkViewHotPath`: View=1+nTypes, ViewType/ViewSingleFile=2 allocs, unchanged.)

Acceptance: **all existing `db/snapshotsync` tests green** (the oracle) under `-race`; the
alloc benchmark shows no regression; `make lint` clean; no behavior change.

---

### Task 4: introduce CaplinStateType enum; convert state snapshots off string keys

Give CL state types a first-class enum (like `snaptype.Enum`) so visibility is
`[]VisibleSegments` indexed by enum, not `map[string]`. Pure representation refactor, no
behavior change — existing caplin-state tests are the oracle.

- [x] Define `CaplinStateType` enum over the current 33 state types (the map had 33, not 34),
      with `String()` and a `ParseCaplinStateType(name string)`. **On-disk compat is load-bearing**:
      `String()` returns the *exact* current type-name strings (`kv.BlockRoot`="BlockRoot",
      `kv.StateRoot`, `kv.ValidatorBalance`, … — the names embedded in `v1.1-<from>-<to>-<Name>.seg`),
      backed by the `kv.*` constants so the name table can't drift from file/DB naming. A mismatch
      means `OpenFolder` won't find existing files. Round-trip test asserts
      `ParseCaplinStateType(t.String()) == t` for all types and that names equal `kv.*` constants
      via an independent oracle map (`caplin_state_type_test.go`).
- [x] Convert the string-keyed collections to enum-indexed: `snapshotTypes.KeyValueGetters`
      (`map[string]` → `map[CaplinStateType]`), `dirty` (`map[string]*btree` → `map[CaplinStateType]`),
      `visible` sync.Map + `CaplinStateView.roTxs` (enum-keyed),
      `Get`/`VisibleSegment`/`VisibleSegments`/`coveredRangesForType` signatures (string →
      `CaplinStateType`). Filenames/DB access stay by name via `String()`/`ParseCaplinStateType`
      at the `OpenList`/`NewCaplinSchema` boundaries. (No `TypeNames` symbol existed.)
- [x] **Reader bridge stays exact**: `state_accessors.GetValFnTxAndSnapshot` maps its `table`
      string → enum via `ParseCaplinStateType` (unknown → fall through to DB, byte-identical);
      antiquary uses `CaplinStateEvents`. `DumpCaplinState` builds the plan's string-labelled
      coverage via `String()`; `planStateDump`/`dumpCaplinState` stay label-based so their tests
      are unchanged.
- [x] Audited every caller of the converted signatures (antiquary collector, historical reader,
      capcli, `NewCaplinSchema`, publishable-check, `snapshots_cmd`) — all compile and behave
      unchanged; `db/snapshotsync` + `cl/antiquary` + `cl/persistence/state` tests green,
      `make erigon` builds, `make lint` clean.

Acceptance: all `db/snapshotsync` + `cl/antiquary` + `cl/persistence/state` tests green;
`make erigon` builds; no snapshot re-dump on an existing datadir (names unchanged).

---

### Task 5: adopt the core in CaplinStateSnapshots (+ consolidate the lock)

`P = caplinStateVisible{ segments []VisibleSegments; segmentsMax, idxMax uint64 }` — enum-indexed
`segments`, same shape as `blockVisible`.

- [x] **Lock consolidation (do first).** State has `dirtyLock` (declared, effectively
      unused), `visibleLock`, `dirtySegmentsLock` — real dirty mutations use
      `dirtySegmentsLock`. Consolidate onto ONE `RWMutex` that guards dirty-tree mutation
      **and** generation publish/reclaim; wire `visibleGenerations.lock` to it. Remove the
      dead locks. (Kept `dirtyLock` as the single lock, mirroring EL; removed
      `dirtySegmentsLock`/`visibleSegmentsLock`/`visibleLock`; `gens.init(&s.dirtyLock,…)`.)
- [x] Replace the `visible` recalc-in-place (`recalcVisibleFiles` :424-468) with
      `visibleGenerations[caplinStateVisible]`. Recalc builds the new enum-indexed `segments`,
      computes `segmentsMax` and `idxMax` **from the candidate `segments`** via a new
      `idxAvailabilityFrom([]VisibleSegments)` (do NOT call the old `idxAvailability` that reads
      the published generation — I4), and `publish`es with **`retired = nil`** (I1).
      (`idxMax` derives from the candidate; `segmentsMax` snapshots the OpenList-computed value
      into the payload — behavior-preserving, so `BlocksAvailable` stays byte-identical.)
- [x] `BlocksAvailable()` = `min(segmentsMax, idxMax)` read from the **current generation
      payload** (I4).
- [x] `CaplinStateView` **acquires** the generation on open and **releases** on Close (real
      pin); `VisibleSegment`/`Get` read the pinned generation's payload — not the shared set.
      (`TestCaplinStateViewPinsGeneration` pins the contract; confirmed it bites without the acquire.)

Acceptance: `db/snapshotsync` caplin-state tests green under `-race`; reads unchanged;
`make erigon` builds.

---

### Task 6: make state dirty-removal live-safe (unlink vs close split)

Two distinct drain-gated actions (I2b): **unlink-retirement** vs **fd-close-only**. Do not
conflate them.

- [ ] **`RemoveOverlaps` (:474) — unlink-retirement.** Drop covered segments from dirty and
      `publish(newPayload, retiredCovered)` (publish unlinks the drained set inline under the
      lock, I2). Contract: with no pre-existing reader pinning the retired generation, files
      are unlinked **by return** (take a temp View pin then release, mirroring EL, to force
      the drain); defer only while a live reader pins. Drop the "offline maintenance only" doc.
- [ ] **`Close` — shutdown, close fds only, NEVER unlink.** `Close` must not call the unlink
      path or `closeAndRemoveSegments`; it publishes an empty generation and closes fds after
      readers drain. Removing files here would delete the whole state snapshot set on normal
      shutdown.
- [ ] **`closeWhatNotInList` in `OpenList` — detach-only, single publish.** Make it
      detach-only (remove stale from dirty, return the stale set). `OpenList` holds the unified
      lock through: stale-detach → open/insert new files → build candidate payload → **one
      final `publish`** with the stale set as close-only (fd-close on drain, **no unlink**).
      Never publish mid-`OpenList` (avoids a transient generation with stale gone but new not
      yet visible).
- [ ] Audit every other `closeSeg`/`closeIdx`/`closeAndRemoveFiles`/detach caller in
      `caplin_state_snapshots.go`; each must be either unlink-retirement or close-only, and
      drain-gated if it can touch a segment a live view may pin.
- [ ] TDD under `-race`: (a) open a `CaplinStateView`, run `RemoveOverlaps` concurrently →
      pinned reader still reads correctly, covered files unlinked only after the view closes;
      (b) no-reader `RemoveOverlaps` unlinks by return (publishable/overlap re-scan callers
      unaffected); (c) **`Close` removes no files from disk**; (d) `OpenList` never exposes a
      transient generation missing the new files.

Acceptance: race tests green under `-race`; `db/snapshotsync` suite green; the
publishable/overlap integrity tests still pass.

---

### Task 7: lint, build, tests

- [ ] `make lint` (repeat until clean).
- [ ] `make erigon`.
- [ ] `go test ./db/snapshotsync/... ./cl/antiquary/...`
- [ ] `go test -race ./db/snapshotsync/...`
- [ ] hot-path alloc benchmark from Task 3 shows no regression.

Acceptance: all green, lint clean.

---

## Out of scope (follow-ups)

- The state **merge tier** (10k→100k→1m) — separate PR, now able to unlink live via this core.
- Folding block + state snapshot types onto one retire orchestrator (the full **erigondb
  retire substrate**); this delivers the shared visibility/refcount core it builds on.
