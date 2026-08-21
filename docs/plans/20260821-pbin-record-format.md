# PBin record format rework

## Overview

The PBin branch record spends bytes on fields that carry no information, and forces a random
state-domain read on every fold. This plan removes the dead bytes, removes the read, and then
stops paying a whole record's framing for every two-child node.

Grounding, both already in the repo — read them, do not re-derive:

- `docs/pbin-optimization-review.md` — the reviewed findings and their arithmetic
- `docs/pbin-mainnet-conversion.md` — the mainnet conversion measurements

The number that drives the ordering: bin carries 3.453x more records than hex at a nearly flat
121.66 B/record, while hex's record size inflates with range age. In the bulk regime record
*count* is the only lever that matters, which is why node blocking is the largest item and comes
last. Everything before it is a straight removal of bytes that carry no information.

No change in this plan may move an EIP-8297 root. The record is erigon's private storage of a
tree whose hashes the spec fixes; internal hashes are recomputed on read and never leave a record.

## Context (from discovery)

Files involved:

- `execution/commitment/pbin_branch.go` — `pbinBranchEncoder`, `pbinAppendCell`, `pbinDecodeBranch`,
  `pbinDecodeCell`, `pbinAppendLenAndVal`, `pbinDecodeFixedVal`, `pbinCheckCellMaps`
- `execution/commitment/pbin_state.go` — `EncodeCurrentState`, `SetState`, `pbinStateMarker`
- `execution/commitment/pbin_patricia_hashed.go` — `fold`, `foldBranch`, `foldPropagate`,
  `unfold`, `unfoldBranchNode`, `cellHash`, `hashRowCell`, `loadCellState`, `materializeBranch`,
  `dropSubtreeRecords`, `updateCell`
- `execution/commitment/pbin_hash.go` — `pbinHasher.cellHash`, `branchHash`, `leafCellHash`
- `execution/commitment/pbin_cell.go` — `pbinCell`, `pbinGrid`
- `execution/commitment/pbin_keys.go` — `pbinDigestCache`, the zone key lengths

Patterns to follow:

- `pbinCellFields` is a presence bitmap. It carries real optionality: not every leaf has a hash,
  and a cell carries `accountAddr` xor `storageAddr` xor a leaf value. It stays.
- `pbinHasher.cellHash` already has both hash paths — `childrenSet` recomputes via `branchHash`,
  otherwise the stored hash is used. `foldBranch` already sets `childrenSet` on the parent cell.
- `SetState` already refuses a blob whose marker is wrong, and `reconcileTrieVariant` already
  refuses a datadir whose `trie_hash` disagrees with the flag. The version guard copies that shape.

Dependencies: `pbinDecodeCell` today has no hash-suite dependency at all. Task 6 introduces one,
which is why it is scoped separately from the task that uses it.

## Development Approach

- **testing approach**: TDD — write the failing test first, then the change that makes it pass
- complete each task fully before moving to the next
- make small, focused changes
- **CRITICAL: every task MUST include new/updated tests** for code changes in that task
  - write unit tests for new functions
  - write unit tests for modified functions
  - add new test cases for new code paths
  - update existing test cases if behaviour changes
  - tests cover both success and error scenarios
- **CRITICAL: all tests must pass before starting the next task** — no exceptions
- **CRITICAL: never add `t.Skip` to hide a failure.** A red test is the finding
- **CRITICAL: update this plan file when scope changes during implementation**
- run `make lint` before every commit
- do not run `git push` — pushes are gated manually and no task here may push

## Testing Strategy

- **unit tests**: required for every task, as separate checklist items
- **root invariance is the acceptance gate.** These pin the EIP-8297 preimage and must stay green
  after every single task, unchanged:
  `pbin_conformance_test.go`, `pbin_specvectors_test.go`, `pbin_specroots_test.go`,
  `pbin_hash_test.go`, `pbin_hashsuite_test.go`.
  If any of them goes red, the change altered a root — revert it, do not adjust the test.
- **`pbin_zerovalue_test.go` and `pbin_witness_test.go` must stay green and unmodified.**
  `TestPBinStorageZeroOnUntouchedSiblingRefuses` pins the `errPBinDeleteUnsupported` backstop and
  `TestPBinWitnessServesRemoval` pins the untouched-leaf preimage. No task here should touch either;
  a change that makes one go red means the change removed a safety property the acceptance gate
  cannot see, since both failures leave every root-pinning test green.
- **byte-format tests will need rewriting** as the format changes:
  `pbin_cell_test.go` (`TestPBinBranchCodecRoundTripPrefixBitLengths`,
  `TestPBinBranchCodecRoundTripCellShapes`, `TestPBinBranchCodecIsCanonical`,
  `TestPBinBranchDecodeRejects`, `TestPBinBranchEncodeRejects`,
  `TestPBinBranchCodecDropsLoadedState`, `TestPBinBranchDecodeClearsReusedCells`),
  `pbin_fold_test.go`, `pbin_unfold_test.go`, `pbin_verify_test.go`
- package command: `go test ./execution/commitment/...`
- no e2e/UI tests in this project

## Progress Tracking

- mark completed items with `[x]` immediately when done
- add newly discovered tasks with ➕ prefix
- document issues/blockers with ⚠️ prefix
- update the plan if implementation deviates from the original scope

## Solution Overview

Four changes, in increasing order of risk.

**A format version in the state blob, not in the record.** `EncodeCurrentState` already writes
`pbinStateMarker` plus a flags byte plus a `uint16` length. A version byte goes there, so the cost
is one byte per file rather than one per 3.5G records, and a datadir written by the old format is
refused on open instead of being silently misread. This lands first so every later change is
behind a guard.

**The leaf hash is not in this plan, and the reason is a finding.** Caching a leaf's hash in its
parent record would delete one random state-domain read per fold. It cannot be done: taking the
stored hash skips `loadCellState`, and that call is the only source of `errPBinDeleteUnsupported`,
which fires when a record outlives its state. The only property separating a safe untouched sibling
from a dangerous one is whether the state still holds its value, and reading that *is* the call the
optimisation exists to delete. `TestPBinStorageZeroOnUntouchedSiblingRefuses` constructs the failure
on the ordinary tip path, so gating the fast path by regime does not rescue it either. Skipping the
read would also drop the leaf out from under `leafCellHash`'s `emitNode`, silently removing untouched
leaves from witnesses. Review item 2 is blocked until the backstop moves somewhere cheaper, which is
its own design problem.

**The dead bytes.** The record header is four bytes of `touchMap` and `afterMap`: `touchMap` is
discarded by every decode site, and every *persisted* `afterMap` is `0b11`. `foldBranch` guards
`bits.OnesCount16(...) != 2` and is the only caller whose output reaches disk. It is **not** the only
caller of `encode` — `pbinWitnessContext.branchRecord` is a second production caller, passing
`pbinCellBits` for both maps, but it synthesizes records in memory and never persists them. Any
signature change to `encode` must update that call site too. Separately,
`pbinAppendLenAndVal` writes a uvarint length before every fixed-size field and `pbinDecodeFixedVal`
then rejects any length that is not the compile-time constant. Presence is the bitmap's job; the
length byte carries nothing.

**The leaf prefix, storage leaves only.** A storage leaf's `storageAddr` is the full 52-byte
`addr || slot`, which determines its tree key outright through `pbinDigestCache.storageKey`, so its
prefix is recomputable and need not be stored.

**Account leaves keep their prefix, and this is not negotiable.** An account tree key is
`zone | 32-byte stem | sub-index`, and the *same* 20-byte `accountAddr` produces the BASIC_DATA,
CODE_HASH and DELEGATION leaves — `pbinUpdateStream.emitCodeLeaves` and `emitSibling` emit all three
under it, and `updateCell` sets `accountAddr` identically for each. `pbinCell` has no sub-index
field, and `pbinDigestCache.treeKey` hardcodes `accountKey(plainKey, pbinBasicDataLeafKey)`, so the
prefix is the only carrier of which leaf this is. Dropping it makes an account with zero basic data
and a non-empty code hash decode as sub-index 0, route through `pbinEncodeBasicData` instead of
`pbinCodeHashValue`, and **move the root**. Two header leaves in one stem also separate at the final
path bit, so their prefixes are empty and there was nothing to save. Code-chunk leaves keep theirs
too — `updateCell`'s empty-plainKey branch retains no `codeHash` or `chunkID` to rehash from.

**The root record and the state blob never omit a prefix.** `pbinAppendCell` is shared by three
producers: branch records, the root record from `storeRoot`, and the state blob from
`EncodeCurrentState`. Only branch records may omit, so the omission is a parameter of the encode
call rather than a property of the cell.

**Node blocking, last.** One record holds an entry branch node plus up to N-1 of its descendant
branch nodes, keyed by the entry path, exposing the resulting boundary child cells. A block of n
nodes has n+1 boundary edges, so it stores n+1 hashes where n separate records store 2n. Internal
hashes are recomputed on read. Frozen and merged files only: blocking rewrites a whole block when
one leaf changes, which is hex's behaviour and hex's pathology, and bin already wins at the tip.

## Technical Details

Current record: `touchMap uint16 | afterMap uint16 | cell | cell`.

Current cell: `fields byte | uvarint(prefix.bitLen) | packed prefix | [uvarint(20) accountAddr] |
[uvarint(52) storageAddr] | [uvarint(32) leafValue] | [uvarint(32) hash]`, presence per `fields`.

Target record: `cell | cell`, both always present.

Target cell: `fields byte | [uvarint(prefix.bitLen) | packed prefix] | [accountAddr] |
[storageAddr] | [leafValue] | [hash]` — presence still per `fields`, lengths implied by the field.
The prefix is omitted **only** when `fields` names `storageAddr` **and** the cell is being written
into a branch record. An `accountAddr` leaf, a code-chunk leaf, the root record and the state blob
all keep every prefix.

State blob: `pbinStateMarker | version | flags | uint16 rootLen | [root cell]`. `SetState` refuses
a blob whose version is not the current constant, and a pre-version blob fails the length and
marker checks it already performs.

Decode gains a dependency it does not have today: reconstructing an omitted leaf prefix needs the
digest cache and the descent depth. Task 6 plumbs that through before task 7 uses it.

## What Goes Where

- **Implementation Steps** (`[ ]` checkboxes): code and tests in this repo
- **Post-Completion** (no checkboxes): measurement runs and the rebuild, which need a real datadir

## Implementation Steps

### Task 1: Add a format version to the trie state blob and close the rebuild bypass

**Files:**
- Modify: `execution/commitment/pbin_state.go`
- Modify: `execution/commitment/pbin_state_test.go`
- Modify: `db/state/squeeze.go`

- [x] write a failing test that `SetState` refuses a blob whose version is not the current constant
- [x] write a failing test that a pre-version blob (old 4-byte header) is refused, not misread
- [x] add `pbinRecordFormat` in `pbin_state.go`, starting at the value for today's layout
- [x] write the version byte after `pbinStateMarker` in `EncodeCurrentState`, shifting the flags byte
      and the `uint16` root length, and reject a mismatch in `SetState` naming both versions
- [x] **close the bypass**: `RebuildCommitmentFiles` passes `execctx.WithoutCommitmentSeek()` for
      `VariantBinPatriciaTrie`, which skips restoring the state blob and therefore skips this guard —
      exactly the flow `--resume` uses over an inherited datadir. Add an explicit format check on the
      rebuild path that does not depend on `SetState` running
- [x] write a test that a rebuild over an old-format datadir refuses instead of decoding
- [x] run `go test ./execution/commitment/... ./db/state/...` — must pass before task 2

**Every later task that changes the record layout bumps `pbinRecordFormat` as part of that task.**
Tasks 2, 3, 5 and 6 each say so. A commit that changes the layout without bumping is the
silent-misread case this task exists to prevent.

### Task 2: Drop the touchMap/afterMap record header

**Files:**
- Modify: `execution/commitment/pbin_branch.go`
- Modify: `execution/commitment/pbin_patricia_hashed.go`
- Modify: `execution/commitment/pbin_witness_context.go`
- Modify: `execution/commitment/pbin_cell_test.go`
- Modify: `execution/commitment/pbin_fold_test.go`
- Modify: `execution/commitment/pbin_unfold_test.go`

- [x] write a failing test asserting the encoded record no longer begins with the four header bytes
- [x] drop the two `AppendUint16` calls from `pbinBranchEncoder.encode` and always encode both cells
- [x] make `pbinDecodeBranch` reconstruct `afterMap` as `0b11` rather than reading it
- [x] keep `pbinCheckCellMaps` as an encode-time guard on the in-memory maps
- [x] update **both** production callers of `encode` if its signature changes: `foldBranch` and
      `pbinWitnessContext.branchRecord` (signature unchanged; existing callers remain valid)
- [x] update `unfoldBranchNode`, `dropSubtreeRecords` and `materializeBranch` for the new decode signature
- [x] bump `pbinRecordFormat`
- [x] write tests that a truncated record and a record with trailing bytes are still rejected
- [x] verify the root-pinning tests stay green
- [x] run `go test ./execution/commitment/...` — must pass before task 3

### Task 3: Drop the uvarint length on every fixed-size field

**Files:**
- Modify: `execution/commitment/pbin_branch.go`
- Modify: `execution/commitment/pbin_state.go`
- Modify: `execution/commitment/pbin_cell_test.go`
- Modify: `execution/commitment/pbin_state_test.go`

`pbinAppendCell` and `pbinDecodeCell` are shared by branch records, the root record from `storeRoot`,
and the state blob from `EncodeCurrentState`. This task changes all three layouts at once.

- [x] write a failing test pinning the new per-cell byte cost for each field combination
- [x] write a failing test that the state blob and the root record round-trip under the new layout
- [x] replace `pbinAppendLenAndVal` with a plain append for `accountAddr`, `storageAddr`, `leafValue` and `hash`
- [x] replace `pbinDecodeFixedVal`'s length read with a bounds check against the field's constant
- [x] keep the uvarint on `prefix.bitLen` — that length is genuinely variable
- [x] bump `pbinRecordFormat`
- [x] write tests that a record truncated inside each fixed field is rejected with a clear error
- [x] verify `TestPBinBranchCodecIsCanonical` still holds — one cell state, one spelling
- [x] run `go test ./execution/commitment/...` — must pass before task 4

### Task 4: Give the decoder the descent depth and digest cache

**Files:**
- Modify: `execution/commitment/pbin_branch.go`
- Modify: `execution/commitment/pbin_patricia_hashed.go`
- Modify: `execution/commitment/pbin_state.go`
- Modify: `execution/commitment/pbin_cell_test.go`

- [ ] write a failing test that the decode entry point accepts a descent depth and a digest source
- [ ] thread both into `pbinDecodeBranch` and `pbinDecodeCell`
- [ ] pass the values already held at every call site. There are five: `unfoldBranchNode`,
      `materializeBranch`, `dropSubtreeRecords`, `loadRoot`, and **`SetState`**, which decodes the
      state blob's root cell. `loadRoot` and `SetState` pass depth 0 — a root cell's prefix is the
      whole tree key
- [ ] make no behaviour change in this task: the plumbing lands unused
- [ ] write tests that every existing decode case is unaffected by the new parameters
- [ ] run `go test ./execution/commitment/...` — must pass before task 5

### Task 5: Omit the prefix on storage leaf cells in branch records

**Files:**
- Modify: `execution/commitment/pbin_branch.go`
- Modify: `execution/commitment/pbin_patricia_hashed.go`
- Modify: `execution/commitment/pbin_state.go`
- Modify: `execution/commitment/pbin_witness_context.go`
- Modify: `execution/commitment/pbin_cell_test.go`
- Modify: `execution/commitment/pbin_state_test.go`

**Storage leaves only.** An account leaf's sub-index is not recoverable from `accountAddr` — see the
Solution Overview. Dropping an account prefix moves the root, and there is nothing to gain since two
header leaves in one stem separate at the final path bit and carry empty prefixes anyway.

`pbinAppendCell` has **four** production callers, and making omission a parameter changes its
signature at every one: `pbinBranchEncoder.encode`, `storeRoot`, `EncodeCurrentState`, and
`pbinWitnessContext.rootRecord`. Only the first may pass "omission permitted".

- [ ] write a failing test that a leaf cell with `storageAddr` encodes no prefix bytes
- [ ] write a failing test that a leaf cell with `accountAddr` **still encodes its prefix**
- [ ] write a failing test that a code-chunk leaf still encodes its prefix
- [ ] write a failing test that the root record and the state blob still encode every prefix, including
      a storage leaf root
- [ ] skip the prefix in `pbinAppendCell` only when `fields` names `storageAddr` and the caller permits omission
- [ ] rebuild it in `pbinDecodeCell` from `storageAddr` and the descent depth via `pbinDigestCache.storageKey`
- [ ] update all four callers for the signature change; witness-derived cells never set
      `storageAddrLen`, so omission cannot trigger there whichever value is passed
- [ ] bump `pbinRecordFormat`
- [ ] write a test that the rebuilt prefix equals the prefix the encoder dropped, across depths, and
      that a decoded-then-re-encoded record reproduces its bytes
- [ ] verify the root-pinning tests stay green
- [ ] run `go test ./execution/commitment/...` — must pass before task 6

### Task 6: Encode a spine of branch nodes as one record, with a discriminator

**Files:**
- Create: `execution/commitment/pbin_block.go`
- Create: `execution/commitment/pbin_block_test.go`
- Modify: `execution/commitment/pbin_branch.go`

**A block is a linear spine, not an arbitrary subtree.** The block holds branch nodes
`n1 -> n2 -> ... -> nk` where each `n(i+1)` is one child of `n(i)`; the other child of each is a
boundary cell, and the last node contributes two. A spine of k nodes therefore exposes k+1 boundary
cells — which is exactly the average hex node's child count at the measured sparsity (3.601 binary
nodes per hex node, 4.601 children), so the record-count estimate is unchanged.

The restriction is what makes task 7 possible: `pbinGrid` and `currentKey` represent a single
root-to-probe spine, so an arbitrary subtree's off-spine internal node has no grid row to occupy, and
its own record no longer exists because it was internalized. A spine has exactly one node per grid
depth.

A reader must tell a blocked record from an unblocked one by its first byte. After task 2 an unblocked
record starts with a cell's `fields` byte, valid values 1..63, so a bare node count would collide with
`pbinFieldLeaf` (1) and `pbinFieldBranch` (2). The discriminator must be a value no valid `fields`
byte can take.

- [ ] write a failing test that a blocked and an unblocked record are distinguishable by their first byte
- [ ] write failing tests for spines of 1, 2 and k nodes and the k+1 boundary cells each exposes
- [ ] write a failing test that the encoder refuses a node set that is not a linear spine
- [ ] define the layout: discriminator, node count, per-node branch bit and prefix, boundary cells in
      descent order
- [ ] implement the encoder, storing only boundary hashes and never an internal one
- [ ] implement the decoder, reconstructing internal nodes with `childrenSet` set and boundary cells
      with their stored hash
- [ ] route `pbinDecodeBranch` to the right grammar by the discriminator, so one datadir may hold both
- [ ] bump `pbinRecordFormat`
- [ ] write tests that a spine's recomputed entry hash equals the hash the unblocked nodes produce
- [ ] write tests rejecting a malformed shape, an impossible node count and trailing bytes
- [ ] run `go test ./execution/commitment/...` — must pass before task 7

### Task 7: Unfold several grid rows from one blocked record

**Files:**
- Modify: `execution/commitment/pbin_patricia_hashed.go`
- Modify: `execution/commitment/pbin_unfold_test.go`

- [ ] write a failing test that unfolding a blocked record fills every row the spine spans
- [ ] populate those rows in `unfoldBranchNode`, marking spine-internal cells `childrenSet`
- [ ] track `prevRecord` per block rather than per row, so the write path can rewrite the whole block
- [ ] keep the single-node path working — an unblocked record must still fill exactly one row
- [ ] update `materializeBranch`, which builds `key = path || c.prefix` and decodes with the unblocked
      decoder; under blocking that key may hold a block, or nothing because the node is spine-internal
- [ ] write tests for a descent that stops inside a spine, one that passes through it, and one that
      leaves the spine at a boundary cell
- [ ] verify the root-pinning tests stay green
- [ ] run `go test ./execution/commitment/...` — must pass before task 8

### Task 8: Re-block on the fold path, including the delete sweeps

**Files:**
- Modify: `execution/commitment/pbin_patricia_hashed.go`
- Modify: `execution/commitment/pbin_fold_test.go`

Three functions reach records by a single node's key and break under blocking. All three are in scope
here; tasks 2 and 4 already had to update the first two for a signature change, so they are known call
sites, not new discoveries.

- [ ] write a failing test that folding rows spanned by one spine writes exactly one record
- [ ] emit a block from `foldBranch` when the rows being folded form one spine
- [ ] fix `dropSubtreeRecords`: it decodes into `[2]pbinCell`, which cannot hold a spine's k+1 boundary
      cells, and it descends to spine-internal node keys where `ctx.Branch` returns nothing and the walk
      fails with `errPBinMissingBranch`. It must walk and delete per block
- [ ] fix `deleteRowRecord`, called from `foldPropagate` and `foldDelete` — under blocking a row no
      longer owns a record
- [ ] write a failing test that removing an account or a storage subtree inside a block leaves no
      orphaned records and does not error
- [ ] write a test that a delete inside a block rewrites the block without losing its siblings
- [ ] write a test that the root is identical whether a subtree was written blocked or unblocked
- [ ] run `go test ./execution/commitment/...` — must pass before task 9

### Task 9: Enable blocking on the rebuild path only

**Files:**
- Modify: `execution/commitment/pbin_patricia_hashed.go`
- Modify: `execution/commitment/config.go`
- Modify: `db/state/squeeze.go`
- Modify: `execution/commitment/pbin_fold_test.go`
- Create: `execution/commitment/pbin_block_policy_test.go`

**Rebuild only. Not merge.** The aggregator merge offers exactly one per-record hook on the commitment
domain, the transformer from `commitmentValTransformDomain`, and its type is
`func(val []byte, startTxNum, endTxNum uint64) ([]byte, error)` — one value in, one value out, never
the key, and it cannot drop or coalesce records. Blocking deletes k-1 keys and rewrites a value under a
different path, which that hook cannot express; the bin variant does not even run the transform, since
its payload is not `BranchData`. `RebuildCommitmentFiles` and `rebuildCommitmentShard` drive a real
fold and are the only place blocking can be produced.

The consequence is explicit and acceptable: records written unblocked at the tip stay unblocked through
collation and merge. Only rebuild output is blocked, so the 42-44% estimate applies to a rebuilt
datadir, not to one grown from the tip.

**The switch needs a signal the engine does not have.** `PatriciaContext` is four methods and carries
no rebuild flag; `rebuildCommitmentShard` only swaps the state reader via `SetStateReader`, which the
trie never sees; `WithoutCommitmentSeek` is not propagated into the trie; and
`InitializeTrieAndUpdates` builds the engine as `NewPBinPatriciaHashed(nil)`, so `TrieConfig` does not
even reach it. Wiring it is part of this task, caller side included.

- [ ] write a failing test that the tip fold path writes unblocked records
- [ ] write a failing test that the rebuild path writes blocked records
- [ ] add the switch selecting blocked or unblocked output, defaulting to unblocked
- [ ] wire it from `db/state/squeeze.go` through to the engine, and **write a test that the rebuild
      path actually sets it** — a switch that is defined but never set is the failure this task must
      not ship
- [ ] **revisit the task 8 assertions.** Task 8 was written while blocking was unconditional, so its
      "writes exactly one record" test exercised the default path. Flipping the default to unblocked
      turns it red. Re-point those assertions at the rebuild path rather than weakening them
- [ ] write a test that a datadir mixing blocked and unblocked records reads correctly, which the task 6
      discriminator makes possible
- [ ] verify the root-pinning tests stay green
- [ ] run `go test ./execution/commitment/... ./db/state/...` — must pass before task 10

### Task 10: Verify acceptance criteria

- [ ] verify every requirement in the Overview is implemented
- [ ] verify no change moved an EIP-8297 root:
      `go test ./execution/commitment/ -run 'TestPBin.*(Root|Hash|Conformance|Spec)' -v`
- [ ] **assert the filter actually ran the tests it claims.** `go test` exits 0 when a `-run` regex
      matches nothing. Count **top-level** results only — `grep -c '^--- PASS'`, unindented, since under
      `-v` every subtest prints its own indented `--- PASS` line and the matching set is heavily
      table-driven. Require at least 50; the regex matched 52 top-level functions when this plan was
      written, so a real drop means the filter stopped matching rather than that the suite shrank
- [ ] run the full package suite: `go test ./execution/commitment/...`
- [ ] run the wider suite the change can reach: `go test ./db/state/...`
- [ ] confirm `errPBinDeleteUnsupported` still fires — no task here may weaken it
- [ ] run `make lint` and fix everything it reports
- [ ] confirm no `t.Skip` was added anywhere in the diff
- [ ] confirm no task pushed to a remote

### Task 11: Update documentation

- [ ] record the new record and state-blob layout in `docs/pbin-optimization-review.md` under a
      "Landed" heading, and keep the note that review item 2 is blocked by the
      `errPBinDeleteUnsupported` invariant
- [ ] note in `AGENTS.md` that the record format is now versioned through the state blob, replacing the
      "not versioned on disk" statement for the record layout while keeping it for the embedding
- [ ] update `CLAUDE.md` if a new pattern emerged that future work should follow
- [ ] move this plan to `docs/plans/completed/`

## Post-Completion

*Items needing a real datadir or a long run — no checkboxes, informational only*

**Measurement, before claiming any disk win:**
- Tasks 1 through 5 remove roughly 23 B from a 121.66 B record — 12 B of dead header and length
  prefixes, plus ~11.33 B of storage-leaf prefix — so expect about a 19% shrink before any blocking
  lands. That figure is arithmetic from the review, not a measurement, and the storage-leaf share is
  the soft part of it.
- Tasks 6 through 9 are the large item, and after task 9 they shrink **rebuilt files only**. A datadir
  grown from the tip sees tasks 1 through 5 and nothing more.
- The review's savings are arithmetic, not measurements. Rebuild commitment from an existing state
  and compare the produced `.kv` sizes and record counts against the figures in
  `docs/pbin-mainnet-conversion.md`. Read record counts from the seg header: version byte, flags
  byte, then the 64-bit big-endian word count at offset 2, two words per record.
- The record-shape histogram over one bin `.kv` is still unrun. It settles the interior/leaf split
  and the real prefix length distribution, which bound tasks 5, 7 and 8.
- Whether seg's dictionary already collapsed the header bytes dropped in task 4 is unmeasured. The
  uncompressed win is certain; the on-disk win is not.

**Migration:**
- Every task here changes the record layout, so an existing bin datadir must be rebuilt from its
  state. That is the 109h13m operation described in the conversion doc, not a resync from genesis —
  no root moves, so block data is untouched.
- Old and new records cannot coexist in one datadir. The task 1 version guard makes that a refusal
  rather than a silent misread, on both the `SetState` path and the rebuild path that
  `WithoutCommitmentSeek` would otherwise let past. Each format-changing task bumps
  `pbinRecordFormat`, so a datadir built from an intermediate commit is refused too.
- Blocked and unblocked records **do** coexist by design, distinguished by the task 8 discriminator:
  the tip writes unblocked, a rebuild writes blocked, and a merge carries whatever it was given.

**Review:**
- The branch is `awskii/pbin-record-format`, off `binary-trie`. Pushing and opening a PR is manual.
