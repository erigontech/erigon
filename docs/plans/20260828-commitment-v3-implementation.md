# commitment.kv v3.0 — per-slot record data model

## Overview

Replace erigon's one-DB-record-per-trie-branch-row commitment format with one record per trie child
slot. Today changing a single child rewrites the whole row (~1 KB), because `CollectUpdate` reads the
previous row, merges the encoded delta back into it, and persists the full row. The delta already
exists at `EncodeBranch`; this change stops discarding it.

The prize is **per-block write volume and changeset volume**, not `.kv` file size — file size provably
grows (see the design doc's Problem section). Scope here is the implementation only; the measurement
experiments are deliberately out of scope.

Design of record, verified against `origin/main` @ `24c627d6a0`, committed in this worktree:
`docs/plans/20260827-commitment-per-slot-records.md`. **Read it first.** It carries the file:line
references, the invariants, the closed-form results and the rejected alternatives. Do not re-derive
or re-litigate what it settles.

## Context (from discovery)

- Worktree `/Users/awskii/org/wrk/wt/commitment-v3`, branch `awskii/commitment-v3`, off
  `origin/main` @ `0124ab5a0c`.
- Write path: `execution/commitment/commitment.go` (`CollectUpdate` :474/:483/:492/:500,
  `EncodeBranch` :567, `BranchMerger.Merge` :1056, `MergeHexBranches` :867).
- Read path: `execution/commitment/hex_patricia_hashed.go` (`unfoldBranchNode` :1458,
  `decodeBranchIntoRow` :1504, `hashRow` :1751, `foldBranch`, `computeCellHash` :1122).
- Codec: `execution/commitment/branch_decode.go` (`DecodeBranchInto` :32).
- Keys: `execution/commitment/nibbles/nibbles_v2.go` (`EncodeKeyV2`/`DecodeKeyV2`, already on main).
- Schema/versions: `db/state/statecfg/state_schema.go`, `db/state/statecfg/versions.yaml`.
- Converter: `db/state/commitment_convert.go`.
- Consumers: `db/integrity/commitment_integrity.go`, `cmd/integration/commands/commitment.go`.

## Development Approach

- **testing approach**: red-first wherever behaviour changes. A task adding a guard must name the
  assertion that fired and the value it saw — a non-zero exit code is not evidence.
- Complete each task fully before the next. Every task ends with the tree building and tests green.
- **Every task includes new/updated tests.** Not optional.
- New Go files carry a 2026 license header.
- Erigon naming: no Factory/Provider/Manager/Builder/`*Base`; `*Func` for registered function types.
- Comments: default to none. Write one only where a reader would otherwise guess wrong.
- **No task runs `git push`.** Pushing is manual.
- Every task must be self-contained from a clean git state.

## Testing Strategy

- Unit tests per task, success and error paths.
- Property/fuzz tests for the codec: encode/decode round-trip, and reconstruction parity against the
  current row format over generated cell mixes (leaf, branch-pointer, extension, tombstone).
- Final verification is byte parity of stored records against the sequential trie over N>=3
  incremental batches including at least one `.kv` merge — root parity alone is a weak oracle
  (invariant 14). Plus `StateRootVerifyByHistory`.
- Package gate: `go test ./execution/commitment/... ./db/state/...`.

## Progress Tracking

- Mark completed items `[x]` immediately.
- New tasks discovered mid-flight get a `+` prefix.
- Blockers get a `!` prefix.
- Update this file when scope changes.

## Solution Overview

- **Node record** — key `EncodeKeyV2(P)`, value = 16-bit present-mask (today's `afterMap`) plus
  inline leaf cells. No `touchMap`.
- **Slot record** — key `EncodeKeyV2(P) || (0x80|n)`, value = fields byte + payload. Branch-pointer
  children only (Variant A). Extension slots carry `(hashedExtension, hash)` together.
- **Deletion** — the domain's native zero-length value, not a bespoke tombstone byte.
- **Address hoist** — storage leaves carry only the 32-byte slot and inherit the plain account
  address from the enclosing account cell.
- **State blob** — drop the four per-row scratch arrays; keep flags plus root cell; version it.
- **Version** — `commitment.domain.kv` v2.2 -> v3.0, add `bt`/`kvei`, retire `kvi`, bump
  `hist.v`/`hist.vi`/`ii.ef`/`ii.efi`.

## Technical Details

- Slot key length is `ceil(d/2)+2` bytes at nibble depth `d`.
- A slot byte is `0x80|n`, which can never collide with a V2 parity byte (`0x00`/`0x01`).
- V2 gives clustering, not prefix-containment: foreign subtrees can sort inside a slot range, so any
  scan over a node's slots needs an **exact key-length filter**, not a count of 16.
- `KeyCommitmentState` is `[]byte("state")` and shares the key space; ordered scans must skip it.
- Record-count multiplier is at most 2x: every node except the root is pointed at by exactly one
  parent cell.

## What Goes Where

- **Implementation Steps**: code, tests, in-repo docs.
- **Post-Completion**: migration tooling, benchmarking on a real datadir, snapshot-fleet
  coordination, and the #21146 heap question — all outside this plan.

## Implementation Steps

### Task 1: Slot key encoding and the exact-length scan filter

**Files:**
- Create: `execution/commitment/nibbles/slotkey.go`
- Create: `execution/commitment/nibbles/slotkey_test.go`

- [ ] add `SlotKey(nodeKey []byte, nibble byte) []byte` appending `0x80|nibble` to an encoded node key
- [ ] add `IsSlotKey(k []byte) bool` and `SlotNibble(k []byte) (byte, bool)`
- [ ] add `NodeKeyLenForDepth(d int) int` returning `ceil(d/2)+1`, and `SlotKeyLenForDepth(d int) int` returning `ceil(d/2)+2`
- [ ] add `SlotRangeBounds(nodeKey []byte) (lo, hi []byte)` plus an exact-length predicate a scan uses to reject foreign-subtree keys inside the range
- [ ] write tests: round-trip over depths 0..128 both parities; slot byte never equals a V2 parity byte; `KeyCommitmentState` classifies as neither node nor slot
- [ ] write tests: the documented interleaving case — a descendant node key sorting inside a parent's slot range is rejected by the length filter
- [ ] run `go test ./execution/commitment/nibbles/...` — must pass before task 2

### Task 2: Per-slot record codec

**Files:**
- Create: `execution/commitment/record_v3.go`
- Create: `execution/commitment/record_v3_test.go`

- [ ] add `EncodeNodeRecord(afterMap uint16, cells *[16]cellEncodeData) []byte` emitting present-mask plus inline leaf cells only
- [ ] add `EncodeSlotRecord(cell *cellEncodeData) []byte` emitting fields byte plus payload for a branch-pointer or extension child
- [ ] add `DecodeNodeRecordInto` and `DecodeSlotInto` filling a `*[16]cell` grid row
- [ ] classify cells explicitly: an account cell carries `accountAddr` **and** a storage-root `hash` simultaneously — decide and document in one line whether its downward hash stays inline or becomes a slot, and make the codec consistent with that choice
- [ ] write round-trip tests over generated cell mixes: leaf-only, branch-pointer-only, mixed, extension cells, empty present-mask
- [ ] write a property test: a row reconstructed from `node + slots` equals what `DecodeBranchInto` produces for the same logical node
- [ ] run `go test ./execution/commitment/...` — must pass before task 3

### Task 3: Version plumbing for commitment.kv v3.0

**Files:**
- Modify: `db/state/statecfg/versions.yaml`
- Modify: `db/state/statecfg/state_schema.go`
- Create: `db/state/statecfg/commitment_v3_version_test.go`

- [ ] bump `commitment.domain.kv` current to v3.0; add `bt` and `kvei` entries; retire `kvi`; bump `hist.v`, `hist.vi`, `ii.ef`, `ii.efi`
- [ ] extend `commitmentKVWriteVersion` to stamp v3.0 when the per-slot format is active
- [ ] add a read-side gate function next to the existing version gates that reports whether a file is per-slot, keyed on file version
- [ ] write tests asserting the gate returns per-slot for v3.0 and bundled-row for every version below it
- [ ] write a test asserting a mixed-version file set resolves per file, not per datadir
- [ ] run `go test ./db/state/statecfg/...` — must pass before task 4

### Task 4: Refuse legacy row parsers on v3 values

**Files:**
- Modify: `execution/commitment/commitment.go`
- Modify: `cmd/integration/commands/commitment.go`
- Create: `execution/commitment/legacy_parse_guard_test.go`

- [ ] add an explicit refusal in `ReplacePlainKeys` for per-slot values — its only guard today is `len < 4`, so a 33-byte slot value is silently misparsed as `touchMap|afterMap|cells`
- [ ] audit and guard the other row parsers: `decodeCells`, `Validate`, `IsComplete`, `ChildCount`, `VerifyBranchHashes`
- [ ] make ordered scans skip `KeyCommitmentState` explicitly rather than assuming every in-range key is a node or slot
- [ ] write a red-first test feeding a 33-byte slot value to `ReplacePlainKeys` — name the assertion and the value it saw before the fix
- [ ] write tests for each guarded parser rejecting per-slot input
- [ ] run `go test ./execution/commitment/... ./cmd/integration/...` — must pass before task 5

### Task 5: Fix detectKeyEncoding for slot keys

**Files:**
- Modify: `db/state/commitment_convert.go`
- Modify: `db/state/commitment_convert_test.go`

- [ ] replace the two-state V1/V2 vote with a three-state result, or derive encoding from file version — a slot key ends `0x8n`, fails `ErrV2KeyParity`, and today votes V1
- [ ] ensure a converted v3.0 file is never classified as unconverted
- [ ] write a red-first test: sample slot keys through the current detector and assert the wrong verdict, then assert the corrected one
- [ ] write a test covering a mixed sample of node keys, slot keys and `KeyCommitmentState`
- [ ] run `go test ./db/state/...` — must pass before task 6

### Task 6: Write path emits node and slot records

**Files:**
- Modify: `execution/commitment/commitment.go`
- Modify: `execution/commitment/hex_patricia_hashed.go`
- Create: `execution/commitment/write_v3_test.go`

- [ ] make `CollectUpdate` emit one node record plus one record per changed branch-pointer slot, gated on the v3 format
- [ ] drop the `ctx.Branch(prefix)` prev read on the v3 path — `hashRow` already emits `cellEncodeData` for every present cell, so the node record re-encodes from memory
- [ ] delete `BranchMerger.Merge` and `MergeHexBranches` and their call sites on the v3 path
- [ ] stop persisting `touchMap`
- [ ] write tests asserting a single changed child produces exactly the expected record set and touches no other key
- [ ] write tests asserting no prev read occurs on the v3 write path
- [ ] run `go test ./execution/commitment/...` — must pass before task 7

### Task 7: Read path assembles a grid row from node plus slots

**Files:**
- Modify: `execution/commitment/hex_patricia_hashed.go`
- Modify: `execution/commitment/commitmentdb/commitment_context.go`
- Create: `execution/commitment/read_v3_test.go`

- [ ] make `unfoldBranchNode`/`decodeBranchIntoRow` assemble a row from the node record plus its slot records on the v3 path
- [ ] keep the bundled-row path intact for files below v3.0, selected by the task 3 gate
- [ ] preserve sibling preservation (invariant 4) and fold locality (invariant 2) — untouched siblings still come from disk
- [ ] write tests asserting an assembled row is byte-identical in grid terms to the bundled-row decode for the same node
- [ ] write tests for a node whose slots span multiple files
- [ ] run `go test ./execution/commitment/...` — must pass before task 8

### Task 8: Native zero-length deletion

**Files:**
- Modify: `execution/commitment/hex_patricia_hashed.go`
- Modify: `execution/commitment/commitment.go`
- Create: `execution/commitment/delete_v3_test.go`

- [ ] replace the 4-byte `{touchMap, afterMap=0}` delete record with the domain's native zero-length value on the v3 path
- [ ] update `IsTombstone` and every `len == 0` site to match
- [ ] make a cleared present-mask bit the authority on slot absence, and keep a missing slot lookup meaning "consult the ordinary fold", never "empty" (invariant 5)
- [ ] write tests asserting a deleted node is dropped at a bottom-most merge and retained otherwise
- [ ] write tests asserting a deleted slot does not resurrect from an older file
- [ ] run `go test ./execution/commitment/... ./db/state/...` — must pass before task 9

### Task 9: Address hoist for storage leaves

**Files:**
- Modify: `execution/commitment/commitment.go`
- Modify: `execution/commitment/hex_patricia_hashed.go`
- Create: `execution/commitment/hoist_test.go`

- [ ] stop persisting the 20-byte account address in storage leaf cells on the v3 path; store the 32-byte slot only
- [ ] make storage leaves inherit the plain address from the enclosing account cell during descent
- [ ] ensure the round-trip through `EncodeCurrentState`/`SetState` still holds (invariant 8) given a leaf now depends on its enclosing account cell being loaded first
- [ ] write tests over accounts whose storage subtree roots at varying depths, including one that diverges below depth 64 so no depth-64 record exists
- [ ] write a test asserting the hoist is not applied to account leaves
- [ ] run `go test ./execution/commitment/...` — must pass before task 10

### Task 10: Slim the root state blob

**Files:**
- Modify: `execution/commitment/hex_patricia_hashed.go`
- Create: `execution/commitment/state_blob_test.go`

- [ ] drop `Depths`, `TouchMap`, `AfterMap` and `BranchBefore` from `state.Encode`/`state.Decode`; keep root flags and the encoded root cell
- [ ] version the blob so an old blob still decodes
- [ ] confirm no path encodes with a row active — `EncodeCurrentState` panics on `currentKeyLen > 0` and `SetState` refuses `activeRows != 0`, so such a blob is already unloadable
- [ ] write tests for round-trip on the new blob and for decoding a legacy blob
- [ ] write a test asserting the encoded size drops by the expected ~656 bytes
- [ ] run `go test ./execution/commitment/...` — must pass before task 11

### Task 11: Deferred and concurrent path under per-slot keys

**Files:**
- Modify: `execution/commitment/commitment.go`
- Modify: `execution/commitment/commitmentdb/commitment_context.go`
- Create: `execution/commitment/deferred_v3_test.go`

- [ ] rework prefix-granular pending tracking: `CollectDeferredUpdate`, the per-goroutine `localCollector` ETL, and `readBranchAndCheckForFlushing`/`HasPendingPrefix` are keyed on the whole prefix and change meaning when a node's state spans a node record plus slots
- [ ] preserve invariant 9: prefix ownership stays disjoint, deferred updates still commute, newest-wins still resolves same-key duplicates
- [ ] write tests for concurrent workers writing disjoint slot sets under one parent
- [ ] write a test for the auto-flush at `DefaultMaxDeferredUpdates` mid-fold
- [ ] run `go test ./execution/commitment/...` — must pass before task 12

### Task 12: Changeset shape on the reorg path

**Files:**
- Modify: `db/state/execctx/domain_shared.go`
- Create: `db/state/changeset_commitment_v3_test.go`

- [ ] verify `DomainPut`'s `prevVal` flows into `DomainEntryDiff` correctly for node and slot records
- [ ] confirm unwind replays per-slot diffs to a byte-identical pre-state
- [ ] record measured entry count and byte size per block for the v3 path versus the bundled-row path in the test output
- [ ] write tests unwinding across a block that changed one slot, several slots, and a whole node
- [ ] write a test unwinding across a step boundary
- [ ] run `go test ./db/state/...` — must pass before task 13

### Task 13: Update integrity checks and CLI consumers

**Files:**
- Modify: `db/integrity/commitment_integrity.go`
- Modify: `cmd/integration/commands/commitment.go`
- Modify: `db/state/squeeze.go`

- [ ] update the `db/integrity` scans to understand per-slot records, or refuse them explicitly with a clear message where support is out of scope
- [ ] update the branch dump at `cmd/integration/commands/commitment.go` to render node and slot records
- [ ] audit `db/state/squeeze.go`'s row parsing for the same misparse hazard as task 4
- [ ] write tests for each updated check over a per-slot fixture
- [ ] write tests asserting refusals are explicit rather than silent misparses
- [ ] run `go test ./db/integrity/... ./cmd/integration/... ./db/state/...` — must pass before task 14

### Task 14: Enable the ordered accessor for commitment

**Files:**
- Modify: `db/state/statecfg/state_schema.go`
- Create: `db/state/commitment_accessor_test.go`

- [ ] make `AccessorBTree | AccessorExistence` the commitment default for v3.0 files, keeping `AGG_COMMITMENT_BT` as the escape hatch for the old shape
- [ ] verify `IteratePrefix`/`bindex.Seek` works against the commitment domain once `bindex` is populated
- [ ] write tests asserting a slot-range scan returns exactly a node's slots under the exact-length filter, with foreign subtree keys excluded
- [ ] write a test asserting `KeyCommitmentState` is skipped by the scan
- [ ] run `go test ./db/state/...` — must pass before task 15

### Task 15: Verify acceptance criteria

- [ ] verify every settled design decision in the Solution Overview is implemented
- [ ] assert stored-record byte parity against the sequential trie over N>=3 incremental batches including at least one `.kv` merge — batch-2 damage only surfaces as batch-3 divergence
- [ ] run `StateRootVerifyByHistory` over a sampled block range and confirm it rebuilds from accounts/storage history in a fresh `SharedDomains`
- [ ] verify a mixed-version datadir reads per file
- [ ] run the full suite: `go test ./execution/commitment/... ./db/state/... ./db/integrity/... ./execution/stagedsync/...`
- [ ] run `golangci-lint run ./execution/commitment/... ./db/state/...`

### Task 16: [Final] Update documentation

- [ ] update `docs/plans/20260827-commitment-per-slot-records.md` to mark implemented decisions and record anything the implementation forced to change
- [ ] note any new invariant discovered during implementation
- [ ] move this plan to `docs/plans/completed/`

## Post-Completion

*No checkboxes — external or manual.*

**Not in this plan:**
- The measurement experiments. File size provably grows; the write-volume win is unquantified.
- Migration tooling. The converter is `.kv`-only by design; history conversion is a separate tool.
- The #21146 heap question — unexplained +43% under a 32 GB cap, and V2 wiring is a hard predecessor.
- Snapshot-fleet coordination for the history-side version bumps.
- Benchmarking on a real datadir, and the `min: v3.0` versus mixed-version rollout decision.
- `git push` and PR creation — manual.
