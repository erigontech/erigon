# Plan — DomainFileFamily: enforce .kv/.ef/.v atomic coupling

**Date:** 2026-08-21
**Branch:** `merge/main-into-feat-snapshot-flow-20260731`
**Ancestry:** supersedes the piecemeal wipe fix (`5ed76b8855`) approach for mode-D wrong-root.

## Problem

Every domain step S has a coupled set of files that MUST agree on keyspace and offsets:

- `.<S>-<S+1>.kv` — key/value snapshot
- `.<S>-<S+1>.ef` — inverted index (txN sequences per key touched in the step)
- `.<S>-<S+1>.v` — historical values referenced by the .ef
- `.<S>-<S+1>.bt` / `.kvi` / `.kvei` — accessors indexing the .kv by byte offset
- `.<S>-<S+1>.efi` / `.vi` — accessors indexing the .ef / .v

If **any** file in this set disagrees with the others — a key in .kv without a matching .ef entry, an accessor offset past the .kv's end, a .v value the .ef doesn't reference — reads produce silent wrong data. The trie folds it and emits a wrong state root. This is the mode-D wrong-root failure.

**Only `DomainRoTx.mergeFiles` at [db/state/merge.go:505](../../db/state/merge.go#L505) treats these as one atomic family**: it takes `domainFiles, indexFiles, historyFiles` together, builds them under a single `closeFiles=true` defer that cleans all three on failure, and returns `valuesIn, indexIn, historyIn` as a triple.

**Every other producer treats the files as independent.** The most consequential offender is `Domain.collate` at [db/state/domain.go:1020](../../db/state/domain.go#L1020):

```go
coll.HistoryCollation, err = d.History.collate(...)   // .ef + .v ← MDBX inverted-index tables
...
sources, err := d.stepSourcesForCollate(roTx, step)   // .kv    ← MDBX writable-shadow rows + v4 files
merged := newMergedStepSources(sources)
for { ... comp.Write(k); comp.Write(v); ... }
```

Two independent MDBX sources. Any shadow row planted outside `PutWithPrev` (which normally pairs the values write with an inverted-index write via `AddPrevValue`) reaches the `.kv` path but never the `.ef` path. The offenders:

1. **`DomainRoTx.unwind`** at [db/state/domain.go:1588-1667](../../db/state/domain.go#L1588-L1667) — direct cursor Put at `unwindStepBytes = ^step` during small Caplin FCU reorgs. Fires on every forward-exec that touches a canonical branch flip. Systematic accumulation over long soaks.
2. **`WipeWritableShadowPast.applyReplay`** at [db/state/wipe_writable_shadow.go:377-452](../../db/state/wipe_writable_shadow.go#L377-L452) — direct cursor Put during mode-D unwind restoration. Fires once per domain per unwind.
3. **`RegenerateBoundaryStepFile` / `WriteStateBoundaryFileV4` / `emitSplitStraddler`** in `node/components/storage/provider_unwind_state_regen*.go` — write `.kv` + accessors only, no `.ef`/`.v`.

Piecemeal-fixing each write site preserves the type-system silence — the compiler never asks whether the .kv write was paired. That silence is what let the wipe fix seem sufficient for one cycle and then fail on the next. **The correct fix is to make the type system speak.**

## Design

### 1. Declare the family as a type

New type in `db/state/file_family.go`:

```go
// DomainFileFamily is the coupled set of on-disk files that form one
// step-range slice of a domain: the .kv values file, the .ef inverted
// index, the .v history values, and every accessor that indexes into
// them by byte offset. All members share the same [fromStep, toStep)
// range and must be built, moved, or deleted as one — a partial family
// is a corruption invariant break.
type DomainFileFamily struct {
    Domain     kv.Domain
    FromStep   kv.Step
    ToStep     kv.Step

    Values     *seg.Decompressor  // .kv
    Index      *recsplit.Index    // .kvi (optional per domain)
    Btree      *btindex.BtIndex   // .bt  (optional per domain)
    Existence  *existence.Filter  // .kvei (optional per domain)

    History    HistoryFileFamily  // .ef + .v + accessors, coupled by construction
}

type HistoryFileFamily struct {
    EfDecomp   *seg.Decompressor  // .ef
    EfIdx      *recsplit.Index    // .efi
    VDecomp    *seg.Decompressor  // .v
    VIdx       *recsplit.Index    // .vi
}
```

- No public setters. The only way to construct a `DomainFileFamily` is through a `FamilyBuilder` (below).
- `Close()` closes every non-nil member. `RemoveFromDisk()` unlinks every path.
- `Paths()` enumerates all on-disk paths — one source of truth for Inventory tracking, downloader manifests, and reconcile-missing.

### 2. Builder discipline

```go
type FamilyBuilder interface {
    // AddEntry records that key K takes value V at txN. Writes go to
    // BOTH the .kv (last-value) and the .ef (all touched txNs) — the
    // builder maintains an internal aggregation so a single AddEntry
    // call cannot land in one without the other.
    AddEntry(k, v []byte, txN uint64) error

    // Finalize seals every member atomically: writes the .kv, .ef,
    // .v, and every configured accessor. Returns a fully-populated
    // DomainFileFamily or an error. On error, every partial file is
    // removed. Callers cannot obtain a partial family.
    Finalize(ctx context.Context) (*DomainFileFamily, error)

    // Abandon removes every temp file. Safe to call after Finalize.
    Abandon()
}
```

Every current `.kv` producer switches to `FamilyBuilder`:
- `Domain.collate` — one builder per step, fed from a union of shadow rows AND their (synthesized or read) IX entries.
- Regen paths — one builder per step, fed from the walk that materialises the boundary state.
- Merge — one builder per output range, fed from the k-way merge over input families.

The builder makes the invariant a **construction rule**: you can't write a K to .kv without also writing an IX entry for K to .ef. The type system enforces atomicity via the interface; the constructor enforces it via internal state.

### 3. Collate feeds the builder from a unified source

Replace `Domain.collate`'s two-source pattern with:

```go
func (d *Domain) collate(ctx, step, txFrom, txTo, roTx) (Collation, error) {
    fb := newDomainFamilyBuilder(d, step, ...)
    defer fb.Abandon()

    sources := d.unifiedStepSourcesForCollate(roTx, step) // NEW
    for {
        k, v, txN, ok, err := sources.Next()
        if !ok { break }
        if err := fb.AddEntry(k, v, txN); err != nil { return err }
    }
    family, err := fb.Finalize(ctx)
    ...
}
```

`unifiedStepSourcesForCollate` returns `(k, v, txN)` triples — the txN is the source of truth for the .ef entry. Sources:
- MDBX inverted-index at `[step*stepSize, (step+1)*stepSize)` gives every (K, txN) touched in the step.
- For each (K, txN), look up V in MDBX shadow rows OR v4 boundary files (whichever wins per current priority).
- Emit `(k, v, txN)` — builder writes both the .kv (K→V) and .ef (K→[txN…]).

**Crucially**: keys that appear in shadow rows but have NO IX entry for the step are either:
- **rejected** (build fails loud — force upstream to add the IX entry), or
- **synthesized** with a deterministic txN at the step boundary (S*stepSize), so the family stays consistent.

The choice — reject vs synthesize — is the design decision below. Recommend **synthesize with logging**, so soak converges immediately and the log tells us which write path is still shadow-only.

### 4. Shadow-write paths add IX entries

`applyReplay` and `DomainRoTx.unwind` each go through a small helper:

```go
// putShadowRow writes (K, V) at unwindStepBytes AND records the paired
// IX entry (K, txN) so the next d.History.collate for this step
// includes K. Fails atomically — either both writes land or neither.
func putShadowRow(tx kv.RwTx, dom kv.Domain, k, v []byte, step kv.Step, txN uint64) error
```

This closes the gap at the write side, so even if a future `Domain.collate` implementation lags the FamilyBuilder switch, it produces a consistent family from consistent inputs.

### 5. Inventory tracks families, not files

`Inventory.AddFile(name)` stays as the low-level API. New:

```go
Inventory.AddFamily(family *DomainFileFamily) // atomic; rejects partial families
Inventory.RemoveFamily(dom kv.Domain, from, to kv.Step)
Inventory.AllDomainFamilies(dom kv.Domain) []*DomainFileFamily
```

Retire, merge, regen, and reconcile-missing all switch to `AddFamily` / `RemoveFamily`. The bootstrap disk-scan validates that every `.kv` on disk has a full family; orphans (a `.kv` without paired `.ef`, or vice versa) are quarantined via the existing `quarantineCorruptStateFileFamily` path.

### 6. Retire asserts family consistency

Post-collate, retire calls `family.Validate()`:
- Every K in the `.kv` has a corresponding entry in the `.ef` at some txN in `[step*stepSize, (step+1)*stepSize)`.
- Every txN in every `.ef` sequence has a matching value in the `.v`.
- Every accessor offset points at a valid word in its target file.

Failure = refuse to publish the family. Fail loud. Better than silent wrong-root a week later.

## Sequencing

**Not a single commit.** Break into stages, each independently green:

| Stage | Change | Verification |
|---|---|---|
| S1 | Introduce `DomainFileFamily` + `HistoryFileFamily` types, no wiring | `make lint && make erigon`; unit test for `Close`/`Paths` |
| S2 | Introduce `FamilyBuilder` interface + default impl; unit-tested | RED-GREEN test for atomic finalize on error |
| S3 | Switch `Domain.collate` to `FamilyBuilder` via `unifiedStepSourcesForCollate`; new-shadow-row synthesis strategy | Existing collate tests pass; new test verifies shadow-only K produces an .ef entry |
| S4 | Add `putShadowRow` helper; migrate `applyReplay` and `DomainRoTx.unwind` | RED test: soak that previously fails at mode-D wrong-root converges |
| S5 | Switch regen paths (`RegenerateBoundaryStepFile` et al) to `FamilyBuilder` | Existing regen tests; end-to-end mode-D soak |
| S6 | Switch merge to `FamilyBuilder` (mechanical — merge already treats family atomically) | Existing merge tests |
| S7 | Inventory `AddFamily` / `RemoveFamily` wiring | Existing bootstrap + reconcile tests |
| S8 | Retire `family.Validate()` gate | RED-GREEN test that inconsistent family is rejected |
| S9 | Strip diagnostic instrumentation (reader.go, recompute_sdless.go, provider_unwind_state_regen_wire.go); commit clean | Loop passes 20 cycles with 0 wrong-root |

Each stage lands as its own commit. Each stage's tests must be RED-GREEN (see project TDD policy).

## Verification

**Terminal criterion**: 50 consecutive `legp-loop` cycles at ITER=5 RANDOMIZE_DEPTHS=true with zero deep-mode-D wrong-root failures. Current rate at HEAD `5ed76b8855` is ~100% failure over 21 cycles. Terminal criterion means a >4-nines improvement.

**Regression guard**: unit tests at S3/S4/S5/S8 must all continue to pass on every subsequent stage's HEAD.

**Determinism check**: on any wrong-root repro during stages, capture the family (`.kv` + `.ef` + `.v` + accessors) into `/erigon/tmp/repros/` alongside the existing five, and run `family.Validate()` against it — a passing Validate on a wrong-root-producing family means the invariant model is incomplete; a failing Validate means the invariant is correct and the write path leaked.

## Critical files

**New:**
- `db/state/file_family.go` — types + Close/Paths
- `db/state/file_family_builder.go` — FamilyBuilder interface + default impl
- `db/state/file_family_test.go`
- `db/state/shadow_write.go` — `putShadowRow` helper

**Modify:**
- `db/state/domain.go` — `Domain.collate` + `DomainRoTx.unwind`
- `db/state/history.go` — `History.collate` moves into `FamilyBuilder` internals
- `db/state/merge.go` — `DomainRoTx.mergeFiles` uses `FamilyBuilder`
- `db/state/wipe_writable_shadow.go` — `applyReplay` calls `putShadowRow`
- `node/components/storage/provider_unwind_state_regen.go` + `_v4.go` + `_wire.go` — regen uses `FamilyBuilder`
- `node/components/storage/snapshot/inventory.go` — `AddFamily` / `RemoveFamily`
- `node/components/storage/provider.go` — bootstrap validates families
- `execution/commitment/commitmentdb/reader.go` + `recompute_sdless.go` — strip diagnostic instrumentation (S9)

## Rules

- No stage skips its RED-GREEN test.
- Stages land independently; every commit builds clean and existing tests pass.
- Terminal criterion (50-cycle zero-failure soak) must complete before this plan is closed.
- Diagnostic instrumentation stripped only at S9 — kept in tree through S1-S8 so any surprise regression during migration is still traceable.

## Related memos

- [[mode-d-invariant-break-domainRoTxUnwind-2026-08-21]] — root-cause investigation that led here
- [[checkpoint-2026-08-21-mode-d-filefamily-invariant]] — session checkpoint that framed this
- [[feedback-fix-not-skip]] / [[feedback-never-avoid-problem]] — this plan replaces the piecemeal wipe fix
