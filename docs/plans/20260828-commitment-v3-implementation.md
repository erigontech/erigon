# commitment.kv v3.0 — one record per trie edge

## Overview

Replace erigon's one-DB-record-per-branch-row commitment format with one record per present child.
Today changing a single child rewrites the whole row, because `CollectUpdate` reads the previous row,
merges the encoded delta back into it, and persists the full row. The delta already exists at
`EncodeBranch`; this change stops discarding it.

A node's mask and hash live in the record for the edge that reaches it, one level up. There is no node
record. The root's mask goes in the state blob.

The prize is **per-block write volume and changeset volume**, not `.kv` file size — file size grows by
a derived ~25%, and record count by ~4.4x. Scope here is the implementation only; the measurement
experiments that decide whether it ships are deliberately out of scope.

Design of record, in this worktree: `docs/plans/20260827-commitment-per-slot-records.md`.
**Read it first.** It carries the file:line references, the invariants, the closed-form results, the
proof that no packed encoding puts the file in fold order, and the rejected alternatives. Do not
re-derive or re-litigate what it settles.

## Context (from discovery)

- Worktree `/Users/awskii/org/wrk/wt/commitment-v3`, branch `awskii/commitment-v3`, off
  `origin/main` @ `0124ab5a0c`.
- Write path: `execution/commitment/commitment.go` — `CollectUpdate` (:463), `EncodeBranch` (:567),
  `BranchMerger.Merge` (:1066), `MergeHexBranches` (:877), `IsTombstone` (:642).
- Fold: `execution/commitment/hex_patricia_hashed.go` — `fold` (:1972), `hashRow` (:1747), `computeCellHash`
  (:1122), `upCell.hashLen = 32` (:1733), `canEmbed` (:776), the extension `stateHashLen = 0` at
  :1203-1206.
- Read path: `unfoldBranchNode` (:1459), `decodeBranchIntoRow` (:1504), `unfold` (:1526),
  `deleteCell` (:2056).
- Codec: `execution/commitment/branch_decode.go`, `cell.fillFromFields` (`hex_patricia_hashed.go:597`).
- Keys: `execution/commitment/nibbles/nibbles_v2.go` (`EncodeKeyV2`/`DecodeKeyV2`, already on main).
- Context: `execution/commitment/commitmentdb/commitment_context.go`, `branch_cache.go`,
  `warmuper.go`.
- Accessor: `db/datastruct/btindex/btree_index.go` (`Cursor`, `resetNoRead`), `bps_tree.go` (`Seek`,
  lower-bound, `(nil, nil)` past the end at :350-357).
- Schema: `db/state/statecfg/state_schema.go`, `db/state/statecfg/versions.yaml`.
- Converter: `db/state/commitment_convert.go` (`detectKeyEncoding` :100-115).
- Consumers: `db/integrity/commitment_integrity.go`, `cmd/integration/commands/commitment.go`,
  `db/state/squeeze.go`.

## Development Approach

- **testing approach**: red-first wherever behaviour changes. A task adding a guard must name the
  assertion that fired and the value it saw — a non-zero exit code is not evidence.
- Complete each task fully before the next. Every task ends with the tree building and tests green.
- **Every task includes new/updated tests.** Not optional.
- Every task must be self-contained from a clean git state.
- **No task runs `git push`.**
- New Go files carry a 2026 license header.
- Erigon naming: no Factory/Provider/Manager/Builder/`*Base`; `*Func` for registered function types.
- Comments: default to none. Write one only where a reader would otherwise guess wrong.

## Testing Strategy

- Unit tests per task, success and error paths.
- Property/fuzz tests for the key encoding and the record codec: round-trip, and reconstruction parity
  against the current row format over generated cell mixes (branch, storage leaf, account leaf,
  account+storage, extension, tombstone).
- Final verification is stored-record byte parity against the sequential trie over N>=3 incremental
  batches including at least one `.kv` merge — root parity alone is a weak oracle (invariant 14) —
  plus `StateRootVerifyByHistory`.
- Package gate: `go test ./execution/commitment/... ./db/state/... ./db/datastruct/...`.

## Progress Tracking

- Mark completed items `[x]` immediately.
- New tasks discovered mid-flight get a ➕ prefix.
- Blockers get a ⚠️ prefix.
- Update this file when scope changes.

## Solution Overview

- **Key** — `key(P) = pack(P) || term`, `term = 0x00` if `len(P)` even, `0xf0|last(P)` if odd. No
  parity byte, no pad nibble. Node key is `floor(d/2)+1` bytes; record key is
  `key(P) || (0x80|n)` for child *n*, one more at `floor(d/2)+2`.
- **Records** — one per present child. No node record.
- **Mask** — in the parent's record. The root's mask is in the state blob.
- **Deletion** — clear the bit in the parent plus a zero-length value at the child key.
- **Read** — one `Seek` plus a short `Next` run per file per touched node, exact key-length filter,
  stop when `popcount(mask)` slots are covered. `TrieContext.Branch(prefix)` keeps its signature and
  synthesizes a row under the hood.
- **Accessor** — `AccessorBTree` required; add `RSeek`/`LSeek` on the cursor.
- **State blob** — re-keyed to `[0x00]`, four scratch arrays dropped, root mask added, versioned.

## Technical Details

```
flags  bit0 kind (0 branch, 1 leaf)   bit1 ext parity
       bit2 leaf kind (0 acct, 1 stor)   bit3 has storage   bit4 hash present

branch child        [flags][mask:2][hash:32][ext:tail]                      35..67
storage leaf        [flags][hash:32][plain:32]                              65
account leaf        [flags][hash:32][plain:20]                              53
account + storage   [flags][hash:32][sroot:32][mask:2][plain:20][ext:tail]  87..119
```

- All widths fixed; every uvarint gone. `fold` sets `hashLen = 32` unconditionally
  (`hex_patricia_hashed.go:1733`) and `EncodeBranch` gates `fieldStateHash` on `== 32` (:596), so a
  persisted hash is always exactly 32 bytes or absent.
- Extension is the record tail, so its length is implied and only parity needs a bit. `cell.extension`
  is `[64]byte` holding nibbles unpacked; packing two per byte halves it.
- `fieldHash` and `fieldStateHash` collapse into one field. An account's `sroot` is a different node's
  hash and stays separate.
- `hash present` is required: `canEmbed := !singleton && totalLen+pl < length.Hash` (:776) yields a
  short RLP that the `== 32` gate drops.
- Terminal byte ranges are disjoint — a trie record always ends `0x80..0x8f`, preceded by `0x00` or
  `0xf0..0xff`. Nothing else in the domain has that shape.
- Every trie key is >=2 bytes, so the 1-byte state key `[0x00]` sorts before all of them.
- Record-count multiplier is ~4.4x, not the <=2x of the superseded node-record model.

## What Goes Where

- **Implementation Steps**: code, tests, in-repo docs.
- **Post-Completion**: measurement, migration tooling, benchmarking, fleet coordination, rollout
  decisions, push and PR — all outside this plan.

## Implementation Steps

### Task 1: V3 edge key encoding

**Files:**
- Create: `execution/commitment/nibbles/nibbles_v3.go`
- Create: `execution/commitment/nibbles/nibbles_v3_test.go`

- [x] add `EncodeKeyV3(nibbles []byte) []byte` producing `pack(P) || term`, `term = 0x00` for even
      length and `0xf0|last` for odd; length `floor(d/2)+1`
- [x] add `DecodeKeyV3(k []byte) ([]byte, error)` with sentinel errors for an illegal terminal byte and
      for length out of range; there is no pad nibble, so no non-canonical-pad error
- [x] add `ChildKeyV3(nodeKey []byte, nibble byte) []byte` appending `0x80|nibble`, plus
      `IsChildKeyV3`, `ChildNibbleV3`, and `ChildKeyLenForDepth(d int) int` returning `floor(d/2)+2`
- [x] add an exact key-length predicate a run scan uses to reject foreign-subtree keys inside a child
      range, and `ChildRangeBoundsV3(nodeKey []byte) (lo, hi []byte)`
- [x] write round-trip tests over depths 0..128, both parities, including depth 0 (root) and depth 128
- [x] write tests asserting the three terminal byte classes are disjoint: `0x00`/`0xf0..0xff` end a
      node key, `0x80..0x8f` ends a child key, and no canonical key ends anything else
- [x] write a test for the documented intrusion cases — a descendant key sorting inside a parent's
      child range is rejected by the length filter, for even P (`subtree(P‖0‖0‖8)`) and odd P
      (`subtree(P'‖15‖a‖8)`)
- [x] write a test asserting `EncodeKeyV3` is one byte shorter than `EncodeKeyV2` at every odd depth
      and equal at every even depth
- [x] run `go test ./execution/commitment/nibbles/...` — must pass before task 2

### Task 2: RSeek and LSeek on the btindex cursor

**Files:**
- Modify: `db/datastruct/btindex/btree_index.go`
- Create: `db/datastruct/btindex/cursor_seek_test.go`

- [x] add `RSeek` returning the first key strictly greater than the argument — `Seek` then skip-if-equal
- [x] add `LSeek` returning the greatest key strictly less than the argument — `Seek` then step back
      one ordinal via `resetNoRead`, handling the past-the-end case where `Seek` returns `(nil, nil)`
      by positioning at `Count()-1`
- [x] add a doc line on `LSeek` stating it must not be used for ancestor lookup: an odd-length
      ancestor's key ends `0xf0|a` while its descendants carry `a<<4|b` at that position, so the
      ancestor sorts after its own subtree
- [x] write tests for both against a built index: hit, miss, first key, last key, past the end, before
      the first key, and empty index
- [x] write a test asserting `LSeek` on the first key returns no cursor rather than an out-of-bounds
      error
- [x] run `go test ./db/datastruct/btindex/...` — must pass before task 3

### Task 3: Edge record codec

**Files:**
- Create: `execution/commitment/record.go`
- Create: `execution/commitment/record_test.go`

- [ ] add the `flags` bit constants and `EncodeBranchChild(mask uint16, cell *cellEncodeData) []byte`
      emitting `[flags][mask:2][hash:32][ext:tail]` with the extension packed two nibbles per byte and
      its parity in bit 1
- [ ] add `EncodeLeafChild(cell *cellEncodeData) []byte` covering storage leaf, account leaf and
      account+storage, with bit 3 gating `[sroot:32][mask:2]` so an EOA carries no empty storage root
- [ ] add `DecodeRecordInto(rec []byte, c *cell) (mask uint16, err error)` filling one grid cell, and
      make it reject a record whose length disagrees with its flags
- [ ] set bit 4 from `stateHashLen == 32` so a `canEmbed` leaf is encoded without a hash and the
      reader knows to reload state
- [ ] write round-trip tests over each of the four shapes, with and without an extension, at odd and
      even extension lengths
- [ ] write a property test: a grid row rebuilt from a node's records equals what `DecodeBranchInto`
      produces for the same logical node from the current row format
- [ ] write tests for malformed input: truncated tail, flags claiming storage with no room for it,
      extension parity disagreeing with the tail length
- [ ] run `go test ./execution/commitment/...` — must pass before task 4

### Task 4: Version plumbing for commitment.kv v3.0

**Files:**
- Modify: `db/state/statecfg/versions.yaml`
- Modify: `db/state/statecfg/state_schema.go`
- Create: `db/state/statecfg/commitment_v3_version_test.go`

- [ ] add an explicit named write gate on `DomainCfg` — `EdgeRecordsInCommitment`, **defaulting
      false** — alongside `ReferencesInCommitmentBranches`. There is no "v3 format is active" signal in
      the codebase today; `commitmentKVWriteVersion` (`state_schema.go:192`) branches only on
      `ReferencesInCommitmentBranches`. Task 9 is what turns the gate on; nothing before it may stamp
      v3.0
- [ ] add `commitment.domain.kv` v3.0 to `versions.yaml` and make `commitmentKVWriteVersion` return it
      **only when `EdgeRecordsInCommitment` is set**, leaving current behaviour byte-identical until
      then; add `bt` and `kvei` entries; retire `kvi`; bump `hist.v`, `hist.vi`, `ii.ef`, `ii.efi`
- [ ] add a read-side gate function next to the existing version gates reporting whether a file holds
      edge records, keyed on file version
- [ ] write a test asserting `commitmentKVWriteVersion` still returns v2.2 with the gate off and v3.0
      with it on — this is the guard against stamping v3.0 over bundled rows between task 4 and task 9
- [ ] write tests asserting the read gate returns edge-record for v3.0 and bundled-row for every
      version below it
- [ ] write a test asserting a mixed-version file set resolves per file, not per datadir
- [ ] run `go test ./db/state/statecfg/...` — must pass before task 5

### Task 5: Enable the ordered accessor for commitment

**Files:**
- Modify: `db/state/statecfg/state_schema.go`
- Create: `db/state/commitment_accessor_test.go`

- [ ] set `Schema.CommitmentDomain.Accessors` to `AccessorBTree | AccessorExistence`. Which index type
      a domain uses is a schema property, not a per-file version gate — `d.Accessors` is domain-wide
      and existing `BuildMissedAccessors` machinery produces `.bt`/`.kvei` for files that lack them
- [ ] verify `bindex` is populated for the commitment domain and that `IteratePrefix` and
      `bindex.Seek` run against it — every existing call site targets `kv.StorageDomain` today
- [ ] write a test asserting a child-range scan returns exactly one node's records under the exact
      key-length filter, with an interleaved foreign-subtree key excluded
- [ ] write a test asserting the scan bails to a re-`Seek` at the next expected child key rather than
      walking a large foreign subtree
- [ ] run `go test ./db/state/...` — must pass before task 6

### Task 6: Exact encoding detection for the converter

**Files:**
- Modify: `db/state/commitment_convert.go`
- Modify: `db/state/commitment_convert_test.go`

- [ ] replace `detectKeyEncoding`'s two-state canonicality vote with an exact three-state test — a v3
      key ends `0x80..0x8f` with `0x00` or `0xf0..0xff` before it, which no canonical V1 or V2 key does
- [ ] ensure a converted v3.0 file is never classified as unconverted
- [ ] write a red-first test: sample v3 keys through the current detector, assert the wrong verdict it
      returns today and name the value that produced it, then assert the corrected verdict
- [ ] write a test over a mixed sample of V1 keys, V2 keys, v3 keys and the state key
- [ ] run `go test ./db/state/...` — must pass before task 7

### Task 7: Refuse legacy row parsers on v3 records

**Files:**
- Modify: `execution/commitment/commitment.go`
- Modify: `db/state/squeeze.go`
- Create: `execution/commitment/legacy_parse_guard_test.go`

- [ ] add an explicit refusal in `ReplacePlainKeys` for edge records — its only guard today is
      `len < 4`, so a 35-byte branch record is silently misparsed as `touchMap|afterMap|cells`
- [ ] audit and guard the other row parsers: `decodeCells`, `Validate`, `IsComplete`, `ChildCount`,
      `VerifyBranchHashes`, `DecodeBranchAndCollectStat` (`commitment.go:1231`)
- [ ] audit `db/state/squeeze.go`'s row parsing for the same misparse hazard
- [ ] write a red-first test feeding a 35-byte branch record to `ReplacePlainKeys` — name the
      assertion that fired and the value it saw before the guard exists
- [ ] write tests for each guarded parser rejecting edge-record input with a distinguishable error
- [ ] run `go test ./execution/commitment/... ./db/state/...` — must pass before task 8

### Task 8: Root state blob — re-key, slim, carry the root mask

**Files:**
- Modify: `execution/commitment/branch_cache.go`
- Modify: `execution/commitment/hex_patricia_hashed.go`
- Create: `execution/commitment/state_blob_test.go`

- [ ] re-key `KeyCommitmentState` from `[]byte("state")` to `[]byte{0x00}`. The ~29 files that
      reference the var pick the new value up automatically; audit only the sites that carry an
      independent assumption about its **length or sort position** — the ordered scans in
      `db/state/merge.go`, `db/state/squeeze.go`, `db/state/commitment_convert.go`,
      `db/integrity/commitment_integrity.go` and `commitmentdb/commitment_context.go`, which tasks 6,
      7 and 15 also touch
- [ ] drop `Depths`, `TouchMap`, `AfterMap` and `BranchBefore` from `state.Encode`/`state.Decode`;
      keep root flags and the encoded root cell
- [ ] add `[mask:2]` for the root's children, which under this model has nowhere else to live
- [ ] version the blob so a legacy blob still decodes
- [ ] confirm no path encodes with a row active — `EncodeCurrentState` panics on `currentKeyLen > 0`
      and `SetState` refuses `activeRows != 0`, so such a blob is already unloadable
- [ ] write tests for round-trip on the new blob and for decoding a legacy blob
- [ ] write a test asserting the encoded size drops by ~654 bytes
- [ ] write a test asserting `[0x00]` sorts before every v3 trie key and is classified as neither a
      node nor a child key
- [ ] run `go test ./execution/commitment/... ./execution/stagedsync/...` — must pass before task 9

### Task 9: Write path emits edge records

**Files:**
- Modify: `execution/commitment/commitment.go`
- Modify: `execution/commitment/hex_patricia_hashed.go`
- Create: `execution/commitment/write_v3_test.go`

- [ ] make `CollectUpdate` emit one record per changed child, gated on the v3 format, carrying the
      child's mask and hash into the record for the edge reaching it
- [ ] drop the `ctx.Branch(prefix)` prev read on the v3 path — `hashRow` already emits
      `cellEncodeData` for every present cell, so records re-encode wholly from memory
- [ ] delete `BranchMerger.Merge` and `MergeHexBranches` and their call sites on the v3 path
- [ ] stop persisting `touchMap` and the 4-byte row header
- [ ] write tests asserting a single changed child produces exactly the expected record set and
      touches no other key
- [ ] write a test asserting no prev read occurs on the v3 write path
- [ ] write a test asserting a node's mask is written into its parent's record and nowhere else, and
      that the root's mask lands in the state blob
- [ ] run `go test ./execution/commitment/...` — must pass before task 10

### Task 10: Read path assembles a grid row from a node's records

**Files:**
- Modify: `execution/commitment/hex_patricia_hashed.go`
- Modify: `execution/commitment/commitmentdb/commitment_context.go`
- Create: `execution/commitment/read_v3_test.go`

- [ ] make `TrieContext.Branch(prefix)` synthesize a row from the node's records while keeping its
      current signature, so `decodeBranchIntoRow`, `BranchCache`, the warmuper,
      `CollectDeferredUpdate` and `HasPendingPrefix` compile unchanged
- [ ] implement the run scan: one `Seek(key(P) || 0x80)` plus `Next` per file, exact key-length
      filter, mask-driven, stopping as soon as `popcount(mask)` slots are covered
- [ ] make the mask the sole authority on slot existence — a record present for a cleared bit is
      ignored, never treated as present
- [ ] keep the bundled-row path intact for files below v3.0, selected by the task 4 gate
- [ ] preserve sibling preservation (invariant 4) and fold locality (invariant 2) — untouched siblings
      still come from disk
- [ ] write tests asserting a synthesized row is identical in grid terms to the bundled-row decode for
      the same node
- [ ] write a test for a node whose records span multiple files, asserting the walk stops at mask
      coverage rather than at the first hit
- [ ] write a test asserting a stale record for a cleared mask bit in an older file is not resurrected
- [ ] run `go test ./execution/commitment/...` — must pass before task 11

### Task 11: Native zero-length deletion

**Files:**
- Modify: `execution/commitment/commitment.go`
- Modify: `execution/commitment/hex_patricia_hashed.go`
- Create: `execution/commitment/delete_v3_test.go`
- Create: `db/state/commitment_tombstone_test.go`

- [ ] on the v3 path, delete a child by clearing its bit in the parent's record and writing a
      zero-length value at the child key
- [ ] update `IsTombstone` and every `len == 0` site to match, and drop the 4-byte
      `{touchMap, afterMap=0}` delete record
- [ ] keep a missing record meaning "consult the ordinary fold", never "empty" — invariant 5 already
      makes absence non-informative, since single-child branches are never persisted
- [ ] write a red-first test asserting the current 4-byte delete record is copied forward at a
      bottom-most merge — name the assertion and the record it saw — then assert the zero-length form
      is dropped at `db/state/merge.go:506-509`
- [ ] write a test asserting a deleted child does not resurrect from an older file
- [ ] write a test asserting subtree deletion writes one tombstone per child the fold actually visited
      and does not enumerate untouched storage slots
- [ ] run `go test ./execution/commitment/... ./db/state/...` — must pass before task 12

### Task 12: Address hoist for storage leaves

**Files:**
- Modify: `execution/commitment/commitment.go`
- Modify: `execution/commitment/hex_patricia_hashed.go`
- Create: `execution/commitment/hoist_test.go`

- [ ] stop persisting the 20-byte account address in storage leaf records; store the 32-byte slot only
- [ ] make storage leaves inherit the plain address from the enclosing account cell during descent,
      which is already in hand because every entry into the trie starts at the root
- [ ] ensure the round-trip through `EncodeCurrentState`/`SetState` still holds (invariant 8) given a
      leaf now depends on its enclosing account cell being loaded first
- [ ] write tests over accounts whose storage subtree roots at varying depths, including one that
      diverges below depth 64 so no depth-64 record exists
- [ ] write a test asserting the hoist is not applied to account leaves
- [ ] write a test asserting a storage leaf record decoded without its enclosing account cell fails
      explicitly rather than yielding a wrong plain key
- [ ] run `go test ./execution/commitment/...` — must pass before task 13

### Task 13: Deferred and concurrent path under edge keys

**Files:**
- Modify: `execution/commitment/commitment.go`
- Modify: `execution/commitment/commitmentdb/commitment_context.go`
- Create: `execution/commitment/deferred_v3_test.go`

- [ ] **precondition** — check the state of the in-flight parallel-commitment branch before starting;
      it is restructuring exactly this code, and the design doc calls for coordinating rather than
      landing on top of it. Record what it is mid-change and stop with a ⚠️ if they conflict
- [ ] rework prefix-granular pending tracking: `CollectDeferredUpdate`, the per-goroutine
      `localCollector` ETL, and `readBranchAndCheckForFlushing`/`HasPendingPrefix` are keyed on the
      whole prefix, and a pending prefix now spans a run of records
- [ ] make last-write-wins in the ETL resolve per record rather than per row
- [ ] preserve invariant 9: prefix ownership stays disjoint, the coordinator owns `P`, and a worker
      owning `P‖n` returns exactly one cell whose mask and hash the coordinator writes
- [ ] write tests for concurrent workers writing disjoint child sets under one parent
- [ ] write a test for the auto-flush at `DefaultMaxDeferredUpdates` mid-fold
- [ ] write a test asserting deferred updates still commute and newest-wins resolves same-key
      duplicates
- [ ] run `go test ./execution/commitment/...` — must pass before task 14

### Task 14: Changeset shape on the reorg path

**Files:**
- Modify: `db/state/execctx/domain_shared.go`
- Create: `db/state/changeset_commitment_v3_test.go`

- [ ] verify `DomainPut`'s `prevVal` flows into `kv.DomainEntryDiff` correctly for edge records
- [ ] confirm unwind replays per-record diffs to a byte-identical pre-state
- [ ] assert per-block changeset **bytes** do not regress against the bundled-row path over the same
      update set, and that entry count rises by at most the record-count multiplier — a threshold, not
      a printout, since a bullet with no pass/fail is not a test
- [ ] write tests unwinding across a block that changed one child, several children, and a whole node
- [ ] write a test unwinding across a step boundary
- [ ] run `go test ./db/state/...` — must pass before task 15

### Task 15: Integrity checks and CLI consumers

**Files:**
- Modify: `db/integrity/commitment_integrity.go`
- Modify: `cmd/integration/commands/commitment.go`

- [ ] update the `db/integrity` scans to verify edge records — read a record, derive the target path
      from its key plus `ext`, read the target's run, recompute the branch hash, compare — or refuse
      them explicitly with a clear message where support is out of scope
- [ ] update the branch dump at `cmd/integration/commands/commitment.go:257` to render edge records
- [ ] make ordered scans skip the state key explicitly rather than assuming every key in range is a
      trie record
- [ ] write tests for each updated check over an edge-record fixture
- [ ] write tests asserting refusals are explicit rather than silent misparses
- [ ] run `go test ./db/integrity/... ./cmd/integration/... ./db/state/...` — must pass before task 16

### Task 16: Verify acceptance criteria

- [ ] verify every settled decision in the Solution Overview is implemented
- [ ] assert stored-record byte parity against the sequential trie over N>=3 incremental batches
      including at least one `.kv` merge — batch-2 damage only surfaces as batch-3 divergence
- [ ] run `StateRootVerifyByHistory` over a sampled block range and confirm it rebuilds from
      accounts/storage history in a fresh `SharedDomains`
- [ ] verify a mixed-version datadir reads per file
- [ ] run the full suite:
      `go test ./execution/commitment/... ./db/state/... ./db/integrity/... ./db/datastruct/... ./execution/stagedsync/...`
- [ ] run `golangci-lint run ./execution/commitment/... ./db/state/... ./db/datastruct/...`

### Task 17: [Final] Update documentation

- [ ] update `docs/plans/20260827-commitment-per-slot-records.md` to mark implemented decisions and
      record anything the implementation forced to change
- [ ] note any new invariant discovered during implementation
- [ ] move this plan to `docs/plans/completed/`

## Post-Completion

*No checkboxes — external or manual.*

**Not in this plan:**
- The measurement experiments. File size grows by a derived ~25% and record count by ~4.4x; the
  write-volume win is unquantified and is the only justification.
- Migration tooling. The converter is `.kv`-only by design; history conversion is a separate tool.
- The #21146 heap question — unexplained +43% under a 32 GB cap, and 4.4x the key count pushes on the
  same axis.
- Snapshot-fleet coordination for the history-side version bumps.
- Benchmarking on a real datadir, and the `min: v3.0` versus mixed-version rollout decision.
- The node-cursor read API and a `(prefix, nibble)`-keyed `BranchCache` — deliberately deferred so
  this change keeps `TrieContext.Branch`'s signature.
- `git push` and PR creation — manual.
