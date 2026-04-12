# Fix stale storage slot values after unwind

## Overview

Sepolia blocks fail validation with `gas used mismatch` (diff = -17100, exactly SSTORE_SET - SSTORE_RESET). Erigon reads a non-zero "original" value for a deleted storage slot, taking the wrong SSTORE gas path.

Two cooperating bugs in the domain state layer, both rooted in df770fadfe (Feb 25) which removed `prevStep` from the DomainDiff/unwind cycle:

1. **getLatestFromDb discards old-step deletion entries** — empty-value DB entries get discarded when their step is older than frozen files, causing fallthrough to `getLatestFromFiles` which returns stale pre-deletion data (frozen files have no tombstones). Already fixed on branch `origin/fix/getLatestFromDb-deletion-entries`.

2. **Unwind doesn't restore deletion markers in same-step scenario** — when a slot is deleted then re-written within the same step, the unwind check `len(value) > 0` skips restoring empty tombstones because both `nil` (different-step, skip) and `[]byte{}` (was-empty, should write) have len==0. V1 serialization already preserves the distinction (hasValue=0 vs hasValue=1+valLen=0) but the unwind collapses them.

## Context (from investigation)

- Traced on arb-dev1 with `awskii/trace-stale-slot` branch against Sepolia blocks 10619142-10624211
- Forward execution gas mismatch confirmed at multiple blocks with diff=-17100
- Snapshot files on this node carry stale values from prior corrupted chaindata (need re-download after fix)
- StorageDomain uses `LargeValues=false` (DupSort mode), stepSize=16 in tests
- 8-byte empty DupSort entries (step-only, no value content) are the standard deletion format used by `addValue(k, nil, step)` during forward execution

## Development Approach

- **testing approach**: Regular (code first, then tests)
- The fix is minimal and well-understood — two one-line changes plus one guard insertion
- Both V0 and V1 changeset formats handle correctly (see compatibility table in brainstorm plan)
- Existing `TestDomain_Unwind` must continue to pass

## Implementation Steps

### Task 1: Fix `getLatestFromDb` — treat deletion entries as authoritative

**Files:**
- Modify: `db/state/domain.go`

- [x] Add early return before the step-age guard at line 1647: when `len(v) == 0`, return `(v, foundStep, true, nil)` regardless of step age — deletion entries are authoritative because frozen files have no tombstones
- [x] Run `go test ./db/state/... -run TestDomain_Delete` — must pass

### Task 2: Fix `unwind()` — restore deletion markers for empty values

**Files:**
- Modify: `db/state/domain.go`

- [x] Line 1304 (LargeValues path): change `if len(value) > 0` to `if value != nil`
- [x] Line 1329 (DupSort path): change `if len(value) > 0` to `if value != nil`
- [x] Update comments at lines 1291, 1303, 1328 to reflect new nil-vs-empty semantics: nil = different step (entry remains at its own step position), non-nil including empty = restore at unwind step (tombstone prevents fallthrough to stale file data)
- [x] Run `go test ./db/state/... -run TestDomain_Unwind` — existing test must pass

### Task 3: Add regression tests

**Files:**
- Modify: `db/state/domain_test.go`

- [x] Add `TestDomain_DeletedKeyNotResurrectedByFiles` — Bug 1 regression test: write value at step 0, delete at step 1, build files covering both steps without pruning DB, verify GetLatest returns empty (not stale pre-deletion value from files)
- [x] Add `TestDomain_UnwindRestoresDeletionMarker` — Bug 2 regression test: write value at txNum 0, delete at txNum 1 (same step), re-write at txNum 2 (same step), build files for that step, call unwind() to revert txNum 2, verify GetLatest returns empty (not stale value from files)
- [x] Run `go test ./db/state/... -run "TestDomain_Deleted|TestDomain_UnwindRestores"` — both new tests pass

### Task 4: Verify full suite and lint

- [x] Run `go test ./db/state/...` — full package tests pass
- [x] Run `make lint` — clean (run multiple times per CLAUDE.md)

### Task 5: Remove tracing code from this branch

**Files:**
- Modify: `db/state/execctx/domain_shared.go` (remove isTraceTargetKey / traceTarget instrumentation added in ac9b21d89b)

- [x] Remove trace instrumentation added for the investigation (the `TRACE_TARGET_SLOT` code from commit ac9b21d89b)
- [x] Run `go test ./db/state/...` — still passes
- [x] Run `make lint` — clean

## Post-Completion

**Node recovery (arb-dev1):**
- Snapshot files on arb-dev1 were built from corrupted chaindata — they carry stale values
- After deploying the fix: re-download snapshots from the network or delete snapshots and re-sync
- This fix prevents NEW corruption; existing corrupted snapshots need replacement
