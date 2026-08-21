# Plan — Reconstitute v4 boundary history family for mode-C/D unwind

**Date:** 2026-08-21
**Branch:** `merge/main-into-feat-snapshot-flow-20260731`

## Context

Provider.Unwind mode-C writes a v4 boundary `.kv` per state domain at range
`(baselineTxN, targetTxN]` — but leaves the paired step-full `.v` (history) and
`.ef` (inverted-index) intact. Those files were built during fresh-sync from a
different chain state; their history entries in `(targetTxN, step_end]` are
semantically undone by the unwind but never rewritten. A subsequent mode-C
compute that folds history through the mismatched range produces a wrong root.

**Physical evidence** (2026-08-21, `/erigon/tmp/mode-c-repro/cycle-002/snapshots-between-iters/`):
Iter-1 mode-C at target 3427792 (txN=121175226, baselineTxN=121093749):

- **Wrote** `v4.0-{domain}.121093750-121175226.kv` + `.bt`/`.kvi`/`.kvei` accessors — one per state domain.
- **Removed** per-step `.kv` files with `FromStep >= stepBoundary` (past-target).
- **Removed** `.v`/`.ef` for wholly-past-target steps.
- **KEPT** the straddler `v2.1-{domain}.310-311.v`/`v3.1-{domain}.310-311.ef` unchanged — byte-identical inode across the operation.

The `.310-311.v` spans txN `[121093750, 121484375)` — extending PAST the v4
`.kv`'s endTxN=121175226. Its history entries in the gap `[121175226, 121484375)`
are stale relative to the post-unwind chain state.

See [[mode-c-family-reconstitute-2026-08-21]] for the observation memo and
[[checkpoint-2026-08-21-mode-b-baseline-and-alternating-loop]] for the earlier
dual-root diagnostic run that first showed `boundedRoot == unboundedRoot != headerRoot`
(both readers see the same corrupt files).

## User's principle (verbatim)

"reconstitute the family for any file rewrite"

When Provider.Unwind produces a new `.kv`, it must also produce paired `.v`
and `.ef` (with their accessors) that match the new file's range. The straddler
history/index files must not be left behind as orphaned partial data.

## Design

### Sources of history in the truncated range

The user asked: if we don't write the history portion and need it later, where
does it come from?

Three candidate sources for history entries in `(baselineTxN, targetTxN]`:

1. **The pre-existing straddler `.v`/`.ef`** — a superset of our range. On disk
   at unwind time. Filter-copy: read old, keep entries with
   `historyTxN <= targetTxN`, write filtered subset as v4.
2. **MDBX `H.KeysTable` + `H.ValuesTable`** — the collate-from-MDBX path
   `BuildFilesInBackground` uses. Under `--prune.mode=minimal` MDBX only
   retains ~100k blocks; for deeper unwinds the range is pruned and this
   source is empty.
3. **Redownload from peers** — a network round-trip; peers have preverified
   full-step files, not arbitrary partial-step ones.

**Choice: option (1).** Straddler is guaranteed on-disk at unwind time (it's what
we're about to replace). No dependency on MDBX prune state, no network round-trip.
Filter-copy is bounded I/O + a re-encode.

### File naming

Match the existing v4 `.kv` convention:

- `v4.0-{domain}.{baselineTxN}-{targetTxN+1}.v`
- `v4.0-{domain}.{baselineTxN}-{targetTxN+1}.ef`
- `.vi` and `.efi` accessors as usual

The `+1` on endTxN matches `DomainKVFilePathV4(domain, fromTxN, lastTxNum+1)` — the
convention that a file's advertised endTxN is exclusive.

### Straddler location

For each state-domain v4 `.kv` produced by `regenerateBoundaryStepFiles`, find:

- The paired history straddler: `Inventory.AllDomainFiles(domain)` filtered by
  `Kind == KindHistory` and step range containing target.
- The paired index straddler: same, `Kind == KindIdx`.

If the straddler is absent for a domain (fresh-sync hasn't retired that step yet),
skip the history filter-copy for that step; the compute can still fold via MDBX.
The v4 `.v`/`.ef` is only needed when a straddler `.v`/`.ef` exists — that's the
condition under which the mismatch would cause corruption.

### FinalizeUnwind + AbortUnwind

Extend `pendingRegenState` to carry paired-history entries alongside the existing
`.kv` entries. FinalizeUnwind renames all three `.regen` files atomically and
removes the old straddler `.v`/`.ef` + their accessors. AbortUnwind cleans up
staged `.regen`s for all three.

## Interfaces

New methods on `StateAggregator`
(`node/components/storage/state_aggregator.go`):

```go
// HistoryFilePathV4 returns the v4.0 raw-txnum-named .v path for a
// domain — paired with DomainKVFilePathV4 for mode-C boundary emission.
HistoryFilePathV4(domain kv.Domain, fromTxN, toTxN uint64) string

// EFFilePathV4 returns the v4.0 raw-txnum-named .ef path.
EFFilePathV4(domain kv.Domain, fromTxN, toTxN uint64) string

// BuildHistoryAccessors builds the .vi sidecar for a v4 .v file.
BuildHistoryAccessors(ctx context.Context, domain kv.Domain, dataPath, finalPath string) error

// BuildIndexAccessors builds the .efi sidecar for a v4 .ef file.
BuildIndexAccessors(ctx context.Context, domain kv.Domain, dataPath, finalPath string) error
```

## Filter-copy primitives

New in `node/components/storage/provider_unwind_history_truncate.go`:

```go
// TruncateStraddlerHistoryFile reads oldEFPath + oldVPath, filters
// history entries to those with historyTxN <= targetTxN, and writes
// the filtered subset as new v4 .ef + .v files at newEFPath + newVPath.
// Uses the same multiencseq encoding as merge/collate for wire-compat.
func TruncateStraddlerHistoryFile(
    ctx context.Context,
    domain kv.Domain,
    oldEFPath, oldVPath string,
    newEFPath, newVPath string,
    targetTxN uint64,
    compression seg.FileCompression,
    tmpDir string,
    logger log.Logger,
) error
```

Reader/writer primitives already exist in `db/state/merge.go` (mergeFiles for both
InvertedIndexRoTx and HistoryRoTx). This function mirrors that structure but
with a single input file and per-entry txN filter.

## Wiring

In `provider_unwind_state_regen_wire.go::regenerateBoundaryStepFiles`, at the
`case action == actionRegenTruncate` (state domain) branch:

```go
// existing .kv emission
if err := WriteStateBoundaryFileV4(...); err != nil { ... }
pairs = append(pairs, regenPair{...})

// NEW: paired .v/.ef filter-copy
histFile := p.paireHistoryStraddler(kvDomain, fileEntry)  // NEW helper
if histFile != nil {
    vRegenPath := p.Aggregator.HistoryFilePathV4(kvDomain, fromTxN, lastTxNum+1) + ".regen"
    efRegenPath := p.Aggregator.EFFilePathV4(kvDomain, fromTxN, lastTxNum+1) + ".regen"
    if err := TruncateStraddlerHistoryFile(ctx, kvDomain,
        histFile.EFPath, histFile.VPath, efRegenPath, vRegenPath,
        lastTxNum, compression, p.snapTmpDir, p.logger); err != nil { ... }
    pairs = append(pairs, regenPair{regenPath: vRegenPath, ...})
    pairs = append(pairs, regenPair{regenPath: efRegenPath, ...})
}
```

Same wiring for `emitSplitStraddler` (mode-D) — its stub v4 also needs paired
history.

Commitment domain does NOT have history/index files, so this only fires for
non-commitment state domains (accounts, storage, code, receipt).

## Tests

### Unit (TDD-red first)

`node/components/storage/provider_unwind_history_truncate_test.go`:

1. `TestTruncateStraddlerHistoryFile_FiltersHistoryEntries` — RED first.
   Build a synthetic `.v`/`.ef` fixture with entries at txN=[100, 200, 300, 400].
   Call `TruncateStraddlerHistoryFile(..., targetTxN=250)`. Assert new `.v`/`.ef`
   contain entries at txN=[100, 200] only.

2. `TestTruncateStraddlerHistoryFile_EmptyResult` — targetTxN below every entry.
   Asserts the function emits empty but valid `.v`/`.ef`.

3. `TestTruncateStraddlerHistoryFile_PreservesEncoding` — filter-copy of a
   real (checked-in) .ef fixture and round-trip through `multiencseq.SequenceReader`
   produces expected txN set.

### Integration

Extend `provider_unwind_state_v4_test.go` and `provider_unwind_finalize_test.go`
to cover:

4. `TestRegenerateBoundaryStepFiles_EmitsPairedHistoryV4` — after
   `regenerateBoundaryStepFiles`, `pendingRegenState.pairs` includes both .kv and
   paired .v/.ef entries for state domains.

5. `TestFinalizeUnwind_RenamesPairedHistoryV4` — FinalizeUnwind atomically renames
   all three (.kv, .v, .ef) and removes the old straddler .v/.ef + accessors.

### End-to-end verification

The mode-C-after-mode-C repro
(`scratchpad/mode-c-repro-with-snapshots.sh`) must run 3× consecutively
without a `[dbg-dual-root]` mismatch. Compare pre/post snapshots at
`/erigon/tmp/mode-c-repro/cycle-*/snapshots-between-iters/` and assert:

- After iter-1 mode-C, `v4.0-{domain}.{baselineTxN}-{targetTxN+1}.v` and
  `.ef` exist for accounts, storage, code, receipt.
- Straddler `v2.1-{domain}.310-311.v` / `v3.1-{domain}.310-311.ef` are
  removed.
- Iter-2 mode-C at a target BETWEEN iter-1's target and pre-head succeeds.

## Sequencing

Stage this change to avoid the "big-write-broke-reads" pattern earlier this
session (see S3 revert `ad6402c285` in `checkpoint-2026-08-21-mode-b-baseline-and-alternating-loop`):

1. **Stage 1 — plan + red tests** (this commit).
2. **Stage 2 — filter-copy primitive** (TruncateStraddlerHistoryFile + its
   unit tests turn green).
3. **Stage 3 — aggregator interface + concrete impl** (path helpers + accessor
   builders + interface stub-test updates).
4. **Stage 4 — regen wire** (pending state entries emitted; integration test
   4 turns green).
5. **Stage 5 — finalize + abort** (rename + old-file removal wired;
   integration test 5 turns green).
6. **Stage 6 — end-to-end verify** (mode-C-after-mode-C repro passes 3× on
   fresh datadir).

Every stage builds+lints+tests clean. Stage boundaries are commit boundaries.

## Critical files

- `node/components/storage/state_aggregator.go` — interface extension.
- `node/components/storage/state_aggregator_impl.go` (or wherever concrete lives) — path helpers + accessor builders.
- **NEW** `node/components/storage/provider_unwind_history_truncate.go` — filter-copy.
- **NEW** `node/components/storage/provider_unwind_history_truncate_test.go` — unit tests.
- `node/components/storage/provider_unwind_state_regen_wire.go` — wire the emit.
- `node/components/storage/provider_unwind_finalize.go` — rename + remove.
- `node/components/storage/provider_unwind_state_v4_test.go` — integration.
- `node/components/storage/provider_unwind_finalize_test.go` — integration.

## Verification rules (from prior session feedback)

- No code change without a data claim citing the trace/snapshot that justified
  it — the `mode-c-family-reconstitute-2026-08-21` finding is the claim.
- Every stage is atomic (its own commit, its own tests, its own lint pass).
- Regression = immediate revert. Baseline (fresh-sync + mode-A tests) must stay
  green at every commit.
- Diagnostic instrumentation from prior investigation (
  `[dbg-dual-root]`, `[dbg-hist-reader]`, histogram) stays in tree until the
  end-to-end verification passes 3×, then gets stripped in a separate commit.

## Diagnostic infrastructure already landed

Session added shell-script gates for future targeted diagnostics (unstaged,
worth committing alongside this fix):

- `scripts/unwind-soak.sh` — `SNAPSHOT_BETWEEN_ITERS_DIR`, `SKIP_SCENARIOS_1_2`,
  `CONTINUE_ON_SCENARIO_3_FAIL`.
- `scripts/unwind-fresh-sync-then-soak.sh` — `SKIP_WIPE`.
- Session scratchpad: `mode-c-repro-with-snapshots.sh`.
