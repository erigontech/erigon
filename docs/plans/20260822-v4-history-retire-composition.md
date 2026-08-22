# Plan — Mirror v4 boundary-file composition from Domain to History + InvertedIndex

**Date:** 2026-08-22
**Branch:** `merge/main-into-feat-snapshot-flow-20260731`
**Follows:** [20260821-reconstitute-v4-history-family.md](20260821-reconstitute-v4-history-family.md) (stages 1-5 already landed)

## Context

Stages 1-5 (landed 2026-08-21) added mode-C's paired v4 `.v` + `.ef` emission
alongside the pre-existing v4 `.kv` emit. Mini-soak verify (2026-08-22)
proved the emit path works correctly on the first mode-C, but iter 2 mode-C
subsequently failed with wrong-root.

Root-cause (evidence at `/erigon/tmp/erigon-hoodi-modec-verify.cycle-002/`):

The Aug 8, 2026 wave (`07a1bbb6b6`, `fbb028b82b`, `2be971b96b`, and 6 sibling
commits) taught the aggregator's retire loop to be **v4-aware for state
domains (.kv)**:

- `Domain.stepSourcesForCollate` merges MDBX (post-target writes) + v4 `.kv`
  files (pre-target snapshot) into the step-aligned `.kv` output.
- `subsumedV4ItemsForStepLocked` retires the v4 `.kv` after the step-full
  is integrated.
- `rangeContainsV4Item` prevents the merger from firing over an unconsumed
  v4 range.
- `Domain.dirtyFilesEndTxNumMinimax` + `closeFilesAfterStep` pin + backward-
  walk bridge keep the v4 visible until subsumed.

**But every one of those commits is `Domain.*` (state .kv) only.** There is
**zero equivalent for `History.*` (.v) or `InvertedIndex.*` (.ef)**. So when
retire's collate fires for step 310 after mode-C has emitted a v4 pair:

- `Domain.collate` → merges MDBX + v4 `.kv` → step-full `.kv` **complete**.
- `History.collate` → iterates MDBX-only → step-full `.v` **missing v4's
  first-half history entries**.
- `InvertedIndex.collate` (paired inside `History.collate`) → also
  MDBX-only → step-full `.ef` **missing first-half txN entries**.

Then the (incomplete) step-full `.v` / `.ef` supersede the v4 pair in the
visible set. Subsequent mode-C compute walks the incomplete history → keys
whose only history entry falls in the v4's first-half range go unseen →
wrong root.

## User's direction (verbatim)

- "we hit the before the replacment step needs to be a merge of v4 + mdbx"
- "we should make sure mode-c is solid before moving on to mode d"

Retire's collate must merge v4 + MDBX (composition happens BEFORE the
step-full replaces the v4). Then the v4 can be safely retired because the
step-full contains the union.

## Design

Mirror the Aug 8 wave — but for history/idx instead of state:

### Stage 8a — v4FilesForStep on History + InvertedIndex

New methods parallel to `Domain.v4FilesForStep`:

- `History.v4FilesForStep(step) []string` — enumerate v4 `.v` files anchored
  at `step*stepSize` (from `h.dirtyFiles`).
- `InvertedIndex.v4FilesForStep(step) []string` — same for `.ef`.

Both are pure Scan+filter; no I/O beyond `dirtyFiles.Scan`. Guarded on
`isRawTxNItem` and `startTxNum == step*stepSize` matching the Domain
predicate.

### Stage 8b — mergeV4IntoHistoryCollation post-merge primitive

After `History.collate` returns its MDBX-derived `.v` / `.ef`, if any v4 pair
exists for this step, produce merged `.v` / `.ef` that concatenates
per-key: v4's txN sequence (first-half) + MDBX's txN sequence (second-half)
in single ascending-key order.

Signature (in a new file `db/state/history_v4_compose.go`):

```go
func (h *History) mergeV4IntoHistoryCollation(
    ctx context.Context,
    step kv.Step,
    coll HistoryCollation,
) (HistoryCollation, error)
```

Behaviour:
- If `v4FilesForStep(step)` returns empty, return `coll` unchanged.
- Open each v4 `.v` + `.ef` decompressor.
- Open the collate-output `.v` + `.ef` decompressor.
- Build a merged iterator: for each key present in either source, emit its
  txN sequence as (v4 txNs) ++ (MDBX txNs). Since v4 covers [step*ss,
  v4-endTxN] and MDBX covers [v4-endTxN+1, (step+1)*ss), the two are
  strictly disjoint — no per-key duplicates possible.
- Write merged output to a `.regen`-suffixed pair, then atomically rename
  over the collate output.

For simplicity the first cut supports a single v4 per step (which is what
`emitSplitStraddler` produces). Multi-v4-per-step (unlikely in practice)
returns an error rather than mis-merging.

### Stage 8c — wire mergeV4IntoHistoryCollation into History.collate

Two edits in `db/state/history.go:collate`:

- Before returning the completed `HistoryCollation`, call
  `h.mergeV4IntoHistoryCollation(ctx, step, coll)` and return its result.
- Guard: if `h.SnapshotsDisabled` or `Disable`, skip (matches existing
  early-out branches).

### Stage 8d — extend subsumedV4ItemsForStepLocked for history/idx

Currently `Aggregator.subsumedV4ItemsForStepLocked` scans `Domain.dirtyFiles`
only. Extend to also scan each Domain's `History.dirtyFiles` and
`History.InvertedIndex.dirtyFiles`, marking wholly-contained v4 items for
retirement.

Symmetric wrapper on History + InvertedIndex:

```go
func (h *History) retireSubsumedV4ItemsInRange(rangeStart, rangeEnd uint64) []*FilesItem
func (ii *InvertedIndex) retireSubsumedV4ItemsInRange(rangeStart, rangeEnd uint64) []*FilesItem
```

Then `subsumedV4ItemsForStepLocked` aggregates:

```go
items := []*FilesItem{}
items = append(items, d.retireSubsumedV4ItemsInRange(txFrom, txTo)...)
items = append(items, d.History.retireSubsumedV4ItemsInRange(txFrom, txTo)...)
items = append(items, d.History.InvertedIndex.retireSubsumedV4ItemsInRange(txFrom, txTo)...)
```

Same extension for `cleanAfterMergeLocked` / `subsumedV4ItemsFromMergeLocked`
(multi-step merge path).

### Stage 8e — merger guard for history/idx v4 items

The existing `rangeContainsV4Item` predicate operates on `[]*FilesItem`
generic to any file kind (per Explore's read at
[db/state/merge.go:205-220](../db/state/merge.go)). Verify that
`HistoryRoTx.mergeFiles` and `InvertedIndexRoTx.mergeFiles` route through
the same `findMergeRangeInFiles` guard. If not, add sibling guards.

Likely just a check + assertion — the guard machinery is generic.

### Stage 8f — end-to-end verify

Re-run `scripts/verify-modec-mini-soak.sh` — expect all 6 iters PASS with
zero `[dbg-dual-root] compute mismatch`. Then run 3 consecutive cycles for
the 3× criterion.

## Sequencing

Stage 8a → 8b → 8c → 8d → 8e → 8f, each as its own commit with TDD-red
first where the change has verifiable behavior:

- 8a: pure enumeration → unit test asserts correct v4 items returned.
- 8b: post-merge → unit test with synthetic v4 pair + synthetic collate
  output, asserts merged file contains union of tuples in correct order.
- 8c: wire the call → integration test with real History + v4 fixture.
- 8d: retire subsumed → integration test asserts v4 items dropped from
  dirtyFiles after IntegrateDirtyFiles.
- 8e: merger guard verification → probably no code change if guard is
  already generic; assert via a targeted test.
- 8f: mini-soak verify, 3× cycles.

## Critical files

- **NEW** `db/state/history_v4_compose.go` — the post-merge primitive.
- **NEW** `db/state/history_v4_compose_test.go` — unit tests.
- `db/state/history.go` — add `v4FilesForStep`, wire mergeV4 into `collate`.
- `db/state/inverted_index.go` — add `v4FilesForStep`, `retireSubsumedV4ItemsInRange`.
- `db/state/aggregator.go` — extend `subsumedV4ItemsForStepLocked` and
  `subsumedV4ItemsFromMergeLocked`.
- `db/state/merge.go` — verify `rangeContainsV4Item` covers history/idx;
  extend if needed.

## Verification rules

- Baseline stays green (all existing tests pass at every commit).
- Each stage's commit builds + lints clean.
- No stage lands without at least one unit test locking in its behavior.
- Diagnostic prints (`[dbg-dual-root]`, `[dbg-regen]`, etc.) stay in the
  tree until stage 8f passes 3×, then stripped in a separate commit.
- Cycle 1+2 evidence at `/erigon/tmp/erigon-hoodi-modec-verify.cycle-{001,002}/`
  preserved as diagnostic reference — do not delete until stage 8f is done.

## Related

- [20260821-reconstitute-v4-history-family.md](20260821-reconstitute-v4-history-family.md) — the plan for stages 1-5 (emit side).
- Aug 8 wave commits: `07a1bbb6b6`, `fbb028b82b`, `2be971b96b`, `0738a47986`,
  `4b9b1b4a61`, `2c941e8b4a`, `ebc4b40d92`, `906f8f3de1`, `d74097a374`.
