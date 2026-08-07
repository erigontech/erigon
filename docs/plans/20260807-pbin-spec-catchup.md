# PBin: catch up to EIP-8297 HEAD (code zone, delegation, reclamation)

## Overview

Erigon's `PBinPatriciaHashed` implements EIP-8297 as it stood several spec
commits ago. The tree it builds no longer agrees with the reference
implementation on any state containing code.

- Spec HEAD is `2c6da5e` on `ethereum/EIPs@master`. The commits this catch-up
  spans, oldest first: `a143951` (simpler tree pseudocode), `c6336fc`
  (migration points at EIP-8347), `7852514` (delete leaves on zeroization),
  `ab8081e` and `6c05450` (move all code chunks into the code zone, delete
  `CODE_OFFSET`), `2f0a8be` (delegation indicators in the header), `2c6da5e`
  (reserved header fields).
- The reference implementation is `8d258bc` on
  `ethereum/execution-specs@projects/binary-trie`, whose conformance vectors
  carry `source_commit` `58faeb0`.
- Erigon vendors vectors `4d18b59`, from before the code zone changed.

Zeroization (`7852514`) is already implemented. Three behavioural gaps and one
hygiene gap remain. Closing them puts erigon back on the reference root for
every case the corpus pins, and adds the adversarial coverage the corpus does
not carry.

The second half of the plan settles our open PRs against the spec repo, all of
which predate the same commits, and contributes back only the cases the
adversarial work proves are missing.

## Context (from discovery)

Verified this session by swapping the fresh vectors into the worktree and
running `go test ./execution/commitment/ -run TestPBinConformance`:

| Section | Result |
| --- | --- |
| `trie_roots` | 9/9 pass |
| `chunkify_code` | 4/4 pass |
| `encode_basic_data` | 3/3 pass |
| `embedding` | FAIL |
| `pbt_state` | 7/18 pass, 11 fail |

The eleven failing `pbt_state` cases: `code_with_push_data_spill`,
`code_and_boundary_storage`, `code_across_the_group_boundary`,
`full_header_occupancy`, `shared_bytecode_two_accounts`,
`short_shared_code_two_accounts`, `delegation_designator`,
`two_authorities_one_target`, `delegation_with_storage`,
`code_hash_starting_with_the_delegation_marker`,
`random_6_accounts_seed_8297`.

With the vendored vectors in place the whole suite is green
(`go test ./execution/commitment/... -run TestPBin`). That is the point: the
vendor bump is the Red step, not a chore. Nothing in the current suite can fail
on any of these gaps, because the corpus that would catch them predates them.

**Everything referencing the code-zone constants.** `pbinCodeOffset` and
`pbinHeaderCodeChunks` are referenced from ten files in `package commitment`
plus five in `package jsonrpc`. Go compiles all of a package's test files
together, so the code-zone change is one atomic edit — it cannot be split
across tasks without leaving the package unbuildable:

| File | Sites |
| --- | --- |
| `pbin_keys.go` | `:35`, `:219`, `:222`, `:229`, `:232`, `:278` |
| `pbin_code.go` | `:27-29` |
| `pbin_hash.go` | `:179` |
| `pbin_update_stream.go` | `:242`, `:243`, `:249`, `:314` |
| `pbin_witness_state.go` | `:168` |
| `pbin_fuzz_test.go` | `:39` |
| `pbin_process_test.go` | `:108` |
| `pbin_overflow_test.go` | `:34-147` (13 sites) |
| `pbin_specengine_test.go` | `:72` |
| `pbin_verify_test.go` | `:296` |
| `pbin_conformance_test.go` | `:142`, `:207` |
| `rpc/jsonrpc/pbin_witness_{e2e,altspec,clone,deploy,granularity}_test.go` | local `pbinHeaderCodeCapacity` at `pbin_witness_e2e_test.go:41` |

Note `pbin_conformance_test.go:207` reads `if i < pbinCodeOffset` where it means
`pbinHeaderCodeChunks`; both are 128 today, so the bug is invisible. It goes
away with the branch.

**Everything that assumes an account holds a CODE_HASH leaf.** The delegation
change breaks three consumers beyond the update stream:

- `PBinWitnessState.Account` (`pbin_witness_state.go:80`) uses the CODE_HASH
  leaf as the account-presence marker. A delegated account holds none, so it
  would read back absent. That propagates to
  `rpc/jsonrpc/pbin_witness_stateless.go:151-168`.
- `pbin_verify_test.go:282` rejects any account-zone leaf that is not
  sub-index 0 or 1.
- `pbinWitnessLeafState` (`pbin_witness_context.go:232-257`) produces flags
  only for sub-indices 0 and 1 and errors otherwise.
- The conformance harness's own oracle (`pbin_conformance_test.go:204-212`)
  unconditionally writes a code-hash leaf for every account, so the delegation
  vector cases cannot go green until it learns the rule.

**Verified absent, so not in scope.** No comment anywhere in
`execution/commitment/` or `rpc/jsonrpc/` cites EIP-4762 access-event keying,
witness branch-cost calibration, or EIP-7612/7748 as the migration path. The
spec deleted those sections, but erigon never carried them.

Untouched and already correct: zero-collapse deletion semantics, and the
single-child branch merge the spec formalized under "Insertion and deletion"
(`foldPropagate`, `pbin_patricia_hashed.go:708`).

## Development Approach

- **testing approach**: TDD (Red → Green → Refactor), as `CLAUDE.md` requires
  for behaviour changes. Task 3 makes the gaps red; each later task turns a
  named subset green.
- **the package must build at the end of every task.** Deleting a constant and
  leaving a reference behind is a build break, not a failing test — a build
  break has no Red/Green signal and blocks every later gate.
- **every task ends with a named test gate, a `make lint` run, and a commit.**
  `go test -run <pattern>` exits 0 when the pattern matches nothing, so each
  gate names the exact test functions the task adds and is run with `-v` to
  confirm they actually executed.
- **no gate may include a suite the plan expects to still be red.**
- **never add `t.Skip` or any other test muting** to hide a failure — the sole
  exception in this repo is the canonical `testing.Short()` guard, and no task
  here needs it
- all EIP-8297 identifiers keep the `pbin` / `PBin` prefix
- comments default to none; when one is warranted keep it concise and
  *why*-focused, with no PR numbers, dates, or incident narration
- commit messages prefix the package: `execution/commitment: ...`. No signing.
  Commit on the existing branch `binary-trie-witness`, never on main.
- **do not push the erigon branch.** Spec-catchup and witness work stays local
  until explicitly released. The spec-repo branches in Part B may be pushed;
  they are already public.
- the linter is non-deterministic, so run `make lint` repeatedly until clean

## Testing Strategy

- **unit tests**: required in every code task, listed as separate checklist
  items, with the test function names written into the plan so the gate can
  name them.
- **conformance vectors**: the vendored `binary_trie_vectors.json` is the
  reference oracle. `TestPBinConformancePBTState` runs each case through both
  the in-repo canonical-rebuild oracle and the engine, so the harness oracle
  has to learn every embedding rule the engine learns.
- **differential fuzz**: `FuzzPBinProcessMatchesOracle` compares engine roots
  against `pbinOracleRoot` over generated corpora. Extended, not replaced.
- **adversarial cases**: named tests for inputs the corpus does not reach, each
  pinning a rule an implementation could plausibly get backwards.
- **round-trip property**: for every case, engine root == oracle root, and ==
  the vector root wherever the corpus pins one.
- **witness suite**: `rpc/jsonrpc/pbin_witness_*_test.go` is re-baselined after
  the code zone moves, never deleted. Its size numbers are measurements — they
  are asserted as *relations* (binary versus hex, one chunk set versus two),
  not as observed constants written back into their own assertions.
- no e2e/UI suite applies to this work.

## Progress Tracking

- mark completed items with `[x]` immediately when done
- add newly discovered tasks with ➕ prefix
- document issues/blockers with ⚠️ prefix
- update this plan when scope changes

## Solution Overview

**Gap A — all code chunks content-addressed (EIPs `ab8081e` + `6c05450`).**
`CODE_OFFSET` is deleted. Every chunk lives in `CODE_ZONE`, keyed
`CODE_ZONE ‖ key_hash(code_hash ‖ tree_index) ‖ sub_index` with
`tree_index = chunk_id // STEM_SUBTREE_WIDTH` and
`sub_index = chunk_id % STEM_SUBTREE_WIDTH`. Header sub-indices 128–255 become
unallocated. `HEADER_STORAGE_SLOTS = 64` replaces the arithmetic that derived
the header storage span from `CODE_OFFSET`; the invariant is now
`HEADER_STORAGE_OFFSET + HEADER_STORAGE_SLOTS <= STEM_SUBTREE_WIDTH`.

In erigon this deletes a code path rather than adjusting one. The
account-header chunk branch disappears, so `codeChunkKey`/`codeOverflowKey` and
their package-level wrappers `pbinTreeKeyCodeChunk`/`pbinTreeKeyCodeOverflow`
collapse into one content-addressed deriver, and `pbinPendingCode` with its
stem-exit flush machinery goes with it.

**Gap B — delegation indicators in the header (EIPs `2f0a8be`).**
`DELEGATION_LEAF_KEY = 2`. An account whose code is exactly 23 bytes beginning
`0xef0100` stores that indicator at header sub-index 2, right-padded with nine
zero bytes to 32, with `code_size = 23`, and holds no `code_hash` leaf and no
`CODE_ZONE` chunks. Every other account holds a `code_hash` leaf and no
delegation leaf. Both removals are unconditional on every account write: an
account that has just delegated still carries the code-hash leaf it held a
moment ago, and one that has just cleared still carries its delegation leaf.

Classification is a function of the code bytes alone, never of the hash. The
spec gives the grinding motive for allocating a separate sub-index rather than
telling the two apart by leading bytes; the behavioural consequence is that a
contract whose *hash* begins `0xef0100` is still code, which the corpus pins as
`code_hash_starting_with_the_delegation_marker`.

Clearing a delegation (authorization to the zero address) restores a
`code_hash` leaf holding `keccak("")` and zeroes `code_size`. A code read takes
the leading `code_size` bytes of the delegation leaf, and `EXTCODEHASH` hashes
them. Because the delegation leaf now also marks an account present, account
presence is "code-hash leaf **or** delegation leaf", not code-hash alone.

**Gap C — code reclamation on account deletion.** The spec requires an
account's `CODE_ZONE` leaves to be removed when no account in the resulting
state holds that `code_hash`, and kept otherwise. Its locality argument is
per-transaction: a leaf that *predates* the transaction is held by an account
the transaction cannot delete, and a leaf the transaction *inserted* is held
only by accounts the transaction wrote it to.

Erigon's commitment batch is not a transaction — it spans a block, often more.
Transplanting the spec's argument unchanged is wrong, and so is the naive
reading "drop when the code hash is absent from the batch's referenced set":
if account `A` deploys code `C` and self-destructs within the batch while a
pre-existing, untouched account `B` already runs `C`, `C` never appears in the
batch's referenced set and `B`'s code would be deleted.

The condition that is both correct and local, independent of batch span:

> Drop a removed account's chunk leaves iff the leaves were **absent from the
> parent state** at batch start **and** no account in the batch's post-state
> holds that `code_hash`.

The first clause is the spec's "inserted by this transaction", generalized to
the batch, and it is one parent-state read at the account's first chunk key.
If the leaves pre-existed the batch, some holder pre-existed too, and per the
spec's own argument that holder cannot have been deleted with code. If they did
not pre-exist, only accounts the batch wrote can hold them, so the batch's
post-state is the whole world. Delegation indicators are exempt: they live in
the header stem and go with it.

Erigon must also source the removed account's `code_hash` and `code_size`,
which the deletion `Update` does not carry — they come from a parent-state read
of the account being removed.

**Gap D — reserved sub-index discipline.** The in-use header sub-indices are
0, 1, 2, and 64–127. The fresh vectors dropped `header_sub_index_255_key` and
added `delegation_key`, so the conformance loader moves with them. Folded into
Task 3, since the harness must compile against the fresh file first.

**Gap E — stale spec citations.** `eip:NNN` and `eip:NNN-NNN` line citations
appear in 16 files under `execution/commitment/`. Every one now points at the
wrong lines: the spec's pseudocode moved from an `insert`-based `BinaryTree`
class to `binarize`/`state_root`, an "Insertion and deletion" section was
added, and the Access-events section was deleted. Each becomes a section-name
citation (`eip:"Code"`, `eip:"Zero values and deletion"`) so they stop rotting
on every spec edit.

## Technical Details

Constants after Gaps A and B, in `execution/commitment/pbin_keys.go`:

```
pbinBasicDataLeafKey     = 0
pbinCodeHashLeafKey      = 1
pbinDelegationLeafKey    = 2
pbinHeaderStorageOffset  = 64
pbinHeaderStorageSlots   = 64        // replaces pbinCodeOffset arithmetic
pbinStemSubtreeWidth     = 256
```

`pbinCodeOffset` and `pbinHeaderCodeChunks` are deleted. Two rewrites that look
like behaviour changes are pure renames, because `pbinCodeOffset` already
equals `pbinHeaderStorageOffset + pbinHeaderStorageSlots`:

- `pbinSlotInHeader` (`pbin_keys.go:278`)
- the storage-header arm of the leaf-value dispatch (`pbin_hash.go:179`) and of
  `pbin_specengine_test.go:72`

Sub-indices 128–255 already fall through to the verbatim-value path, which is
correct for a reserved sub-index; the dispatch is not turned into an error.

Chunk key derivation collapses to one function:

```
chunkKey(codeHash, chunkID):
    treeIndex = chunkID / pbinStemSubtreeWidth
    subIndex  = chunkID % pbinStemSubtreeWidth
    position  = hash(codeHash ‖ uint256(treeIndex))
    return pbinTreeKey(pbinCodeZone, position, subIndex)
```

Delegation helpers:

```
pbinDelegationMarker     = {0xef, 0x01, 0x00}
pbinDelegationCodeLength = 23

pbinIsDelegation(code)     -> len(code) == 23 && code[:3] == marker
pbinEncodeDelegation(code) -> 32-byte value, code right-padded with zeros
```

This is *not* the chunk encoding: a chunk reserves byte 0 for a PUSHDATA count,
which an indicator, never executed as code, does not carry. That distinction
also decides Task 8's open question — whether the delegation leaf is
record-resident (verbatim 32 bytes, like a chunk) or packed from account
fields. It carries no field the `Update` holds, so record-resident is the
answer, and `pbinLeafValue` / `pbinWitnessLeafState` must agree on it.

Account write ordering in `pbinUpdateStream.processKey`, per account:

1. emit `BASIC_DATA`
2. if the code is a delegation: emit sub-index 2 with the packed indicator, and
   delete sub-index 1
3. otherwise: emit sub-index 1 with the code hash, delete sub-index 2, and
   queue every chunk into the code zone

Both deletions are unconditional in their branch, since the stream is told
nothing about what the account held before.

A chunk is absent only when the whole 32-byte value is zero — 31 zero code
bytes **and** a zero PUSHDATA count in byte 0. Zero bytes continuing PUSHDATA
from an earlier chunk do not qualify.

## What Goes Where

- **Implementation Steps** (`[ ]` checkboxes): everything in this repository and
  in the execution-specs checkout — code, tests, vendored vectors, PR bodies.
- **Post-Completion** (no checkboxes): EL-layer work outside `package
  commitment`, and anything needing a maintainer's decision.

## Implementation Steps

### Task 1: Commit the in-flight witness work to get a clean baseline

Every later task must start from a clean tree — this plan is ralphex-executable
and cannot depend on transient working-tree state.

**Files:**
- Modify: (commit only, no edits) `execution/commitment/`, `rpc/jsonrpc/`, `docs/`

- [x] review `git status` and `git diff` on branch `binary-trie-witness`
- [x] stage every modified file plus all nine untracked ones:
      `docs/pbin-encoding.md`, `docs/plans/20260807-pbin-spec-catchup.md`,
      `execution/commitment/pbin_conformance_test.go`,
      `execution/commitment/pbin_witness_codezone_test.go`,
      `execution/commitment/testdata/binary_trie_vectors.json`,
      `rpc/jsonrpc/pbin_witness_altspec_test.go`,
      `rpc/jsonrpc/pbin_witness_clone_test.go`,
      `rpc/jsonrpc/pbin_witness_deploy_test.go`,
      `rpc/jsonrpc/pbin_witness_whale_test.go`
- [x] run `make lint` until clean
- [x] commit as `execution/commitment, rpc/jsonrpc: pbin witness fixes and conformance harness`
- [x] verify `git status --porcelain` is empty
- [x] run `go test ./execution/commitment/... -run TestPBin -count=1` — must be
      green on the old vectors

### Task 2: Establish a durable execution-specs checkout

Task 3 needs the fresh vectors as an input, and the only checkout on this
machine is under `/tmp`. All four PR branches are already on `fork`, so nothing
unique lives there.

**Files:**
- Create: `~/org/wrk/espr` (clone, outside this repository)

- [x] clone `https://github.com/ethereum/execution-specs.git` to `~/org/wrk/espr`
- [x] add remote `fork` → `https://github.com/awskii/execution-specs.git`, fetch
      both remotes
- [x] check out `projects/binary-trie` and confirm HEAD is at or past `8d258bc`
      (HEAD is exactly `8d258bc`)
- [x] confirm `~/org/wrk/espr/tests/binary_trie/vectors/binary_trie_vectors.json`
      reports `source_commit` `58faeb0`
- [x] verify the toolchain: `uv run python tests/binary_trie/vectors/dump_vectors.py`
      regenerates the file with no diff — vector content byte-identical; the
      only changed line is the `source_commit` stamp, which the generator sets
      to the current HEAD (`8d258bc`), restored after the check
- [x] run `uv run pytest tests/binary_trie/ -q` to confirm a green base —
      204 passed

### Task 3: Vendor the fresh vectors and make the harness build against them

The delegation constant is defined here rather than in Task 7, because
`pbinDelegationLeafKey` appears in this task's assertions and an undefined
identifier is a build break, not a Red test.

**Files:**
- Modify: `execution/commitment/testdata/binary_trie_vectors.json`
- Modify: `execution/commitment/pbin_conformance_test.go`
- Modify: `execution/commitment/pbin_keys.go`

- [x] copy `~/org/wrk/espr/tests/binary_trie/vectors/binary_trie_vectors.json`
      over `execution/commitment/testdata/binary_trie_vectors.json`
- [x] add `pbinDelegationLeafKey = 2` to the embedding constants
- [x] replace the `HeaderSubIndex255Key` field (`pbin_conformance_test.go:41`)
      and its assertion (`:132`) with `DelegationKey`, asserted against
      `keys.accountKey(addr, pbinDelegationLeafKey)`
- [x] leave the two chunk-placement sites (`:142` in the embedding assertion and
      `:207` in the `pbt_state` oracle) untouched for now — Task 4 rewrites both
      in the same atomic edit that deletes the constants they use
- [x] run `go test ./execution/commitment/ -run TestPBinConformance -count=1`;
      the package must **build**, and the failing set must be
      `TestPBinConformanceEmbedding` plus the eleven `pbt_state` cases in
      Context. Record any deviation in this plan before continuing.
      (Verified: failing set is exactly `TestPBinConformanceEmbedding` plus the
      eleven Context cases; `pbt_state` 7/18 pass. No deviation.)
- [x] run `make lint` until clean; commit as
      `execution/commitment: vendor EIP-8297 conformance vectors from 58faeb0`

### Task 4: Move every code chunk into the code zone

One atomic edit across the whole package. Splitting it leaves `package
commitment` unbuildable, which would block every later gate.

**Files:**
- Modify: `execution/commitment/pbin_keys.go`
- Modify: `execution/commitment/pbin_code.go`
- Modify: `execution/commitment/pbin_hash.go`
- Modify: `execution/commitment/pbin_update_stream.go`
- Modify: `execution/commitment/pbin_witness_state.go`
- Modify: `execution/commitment/pbin_conformance_test.go`
- Modify: `execution/commitment/pbin_overflow_test.go`
- Modify: `execution/commitment/pbin_process_test.go`
- Modify: `execution/commitment/pbin_specengine_test.go`
- Modify: `execution/commitment/pbin_verify_test.go`
- Modify: `execution/commitment/pbin_fuzz_test.go`
- Modify: `execution/commitment/pbin_keys_test.go`
- Modify: `execution/commitment/pbin_code_test.go`
- Modify: `execution/commitment/pbin_hash_test.go`

- [x] delete `pbinCodeOffset` and `pbinHeaderCodeChunks`; add
      `pbinHeaderStorageSlots = 64` and rewrite `pbinSlotInHeader`
      (`pbin_keys.go:278`), `pbin_hash.go:179` and `pbin_specengine_test.go:72`
      against it — all three are renames, not behaviour changes
- [x] merge `codeChunkKey`/`codeOverflowKey` (`pbin_keys.go:218-238`) and their
      wrappers `pbinTreeKeyCodeChunk`/`pbinTreeKeyCodeOverflow` (`:108`, `:117`)
      into one `chunkID / pbinStemSubtreeWidth` deriver taking a code hash
- [x] delete `pbinPendingCode`, `flushPendingCode` and `flushPendingCodeBefore`
      from `pbin_update_stream.go`; `queueCode` routes every chunk through the
      surviving code-zone path, and `pbinOverflowChunk` loses the "overflow"
      name that no longer denotes anything (now `pbinCodeChunk`)
- [x] rewrite the witness chunk loop (`pbin_witness_state.go:164-172`), the
      verify position check (`pbin_verify_test.go:296`), the conformance
      embedding assertion (`:142`) and the conformance `pbt_state` oracle
      (`:206-212`), and re-express `pbinFuzzCodeSizes` (`pbin_fuzz_test.go:39`)
      and `pbinTestChunkKey` (`pbin_process_test.go:108` — deleted outright;
      callers use `pbinTreeKeyCodeChunk` directly)
- [x] write `TestPBinChunkKeyMatchesVectorIndices` pinning chunk ids 0, 1, 255,
      256, 257, 511, 512 and 2114 against `embedding.code_chunk_keys` — that is
      the id set the fresh corpus carries — and
      `TestPBinChunkKeyIgnoresAddress`
- [x] write `TestPBinSharedBytecodeEmitsOneChunkSet` and
      `TestPBinChunksCrossGroupBoundary` (256 and 257 chunks, not the planned
      129 — stale arithmetic from the old split: the group boundary sits at
      `STEM_SUBTREE_WIDTH` = 256, matching the 257-chunk vector case)
- [x] write `TestPBinZeroChunkEmitsNoLeaf`, covering that a chunk is absent only
      when byte 0's PUSHDATA count is zero too, and that `code_size` still
      delimits the code
- [x] gate: `go test ./execution/commitment/ -count=1 -v -run 'TestPBinChunkKey|TestPBinSharedBytecode|TestPBinChunksCross|TestPBinZeroChunk|TestPBinTreeKeyEIPVectors|TestPBinChunkifyCode|TestPBinCellHash|TestPBinLeafValueRoutesByZone|TestPBinProcess|TestPBinConformanceEmbedding'`
      — confirm from `-v` that the new tests ran, and that
      `TestPBinConformanceEmbedding` and the seven code-shaped `pbt_state`
      cases are green (28 pass, 0 fail; eight `pbt_state` cases went green —
      `code_hash_starting_with_the_delegation_marker` is code-shaped, not a
      delegation case, so Task 7's remaining red set is three: `delegation_designator`,
      `two_authorities_one_target`, `delegation_with_storage`)
- [x] `make lint` until clean; commit as
      `execution/commitment: address every code chunk by code hash`

➕ Fixed a latent subtree-drop bug the move exposed (`pbin_patricia_hashed.go`,
`needUnfolding`): a removal prefix ending strictly inside a stored node's
prefix was misclassified as a Split and errored at `updateCell`. The old
header chunks at sub-indices ≥ 0x80 forced a branch exactly at bit 264 in
every tested removal, masking it. Red was `TestPBinAccountRemovalDropsBothSubtrees`.
➕ Re-baselined `TestPBinAccountRemovalDropsBothSubtrees`
(`pbin_zerovalue_test.go`): a removed account's chunks are content-addressed
and stay, so the expected root now includes them.
➕ Also touched beyond the planned file list: `pbin_witness_test.go` and
`pbin_witness_prune_test.go` (chunk-key call sites), `pbin_witness_codezone_test.go`
and `pbin_zerovalue_test.go` (stale header-split comments/expectations).
Renames: `TestPBinEngineEmitsHeaderCodeChunks` → `TestPBinEngineEmitsCodeChunks`;
`TestPBinShorteningRedeployKeepsStaleChunks` + `TestPBinGrowingRedeployReplacesChunks`
→ `TestPBinRedeployKeepsOldCodeChunks` (under content addressing every redeploy
keeps the old chunk set); `TestPBinCodeOverflowKeyMatchesSpec` →
`TestPBinChunkKeyMatchesSpec`; `TestPBinOverflowChunksAreSharedByIdenticalCode`
→ `TestPBinSharedBytecodeEmitsOneChunkSet`; `TestPBinOverflowChunksFollowEveryAccountZoneKey`
→ `TestPBinCodeChunksFollowEveryAccountZoneKey`.

### Task 5: Re-baseline the witness measurement suite

The measurement cases are parametrised on the 128-chunk header split, which no
longer exists. They are measurements, not spec assertions, so they are
re-measured — but asserted as relations, since writing an observed number into
its own assertion proves nothing.

**Files:**
- Modify: `rpc/jsonrpc/pbin_witness_e2e_test.go`
- Modify: `rpc/jsonrpc/pbin_witness_granularity_test.go`
- Modify: `rpc/jsonrpc/pbin_witness_altspec_test.go`
- Modify: `rpc/jsonrpc/pbin_witness_clone_test.go`
- Modify: `rpc/jsonrpc/pbin_witness_deploy_test.go`
- Modify: `rpc/jsonrpc/pbin_witness_whale_test.go`

- [x] re-express the local `pbinHeaderCodeCapacity` (`pbin_witness_e2e_test.go:41`,
      currently `31 * (256 - 128)`) as a chunk count; `pbinStemSubtreeWidth` is
      unexported in `package commitment` and not reachable from here, so the
      constant stays local (now `pbinCodeGroupChunks = 256`)
- [x] drop the `gated()` column in `pbin_witness_altspec_test.go` — the
      "keep code in the header when it fits" alternative it priced is gone from
      the spec
- [x] state the expected direction before measuring, then assert relations:
      binary witness exceeds hex at every size, and the ratio grows with chunk
      count (asserted over the 1 → 128 → 256 → 793 subsequence: adjacent close
      sizes may wobble — measured 128 gives 3.74x against 129's 3.69x, because a
      different code hash scatters the stem elsewhere in the zone — while the
      decade steps grow strictly; the zero-padded case must undercut hex)
- [x] add `TestPBinWitnessClonesProveOneChunkSet`, asserting that two accounts
      sharing bytecode prove one chunk set rather than two — the behaviour
      content addressing was adopted for, and previously false (clone block
      proves exactly 257 chunk leaves, distinct block 8×257)
- [x] record the re-measured hex-versus-binary table in this plan, not in the
      assertions (below)
- [x] gate: `go test ./rpc/jsonrpc/ -count=1 -v -run TestPBinWitness` and
      confirm `TestPBinWitnessClonesProveOneChunkSet` appears in the output
      (all pass; the new test ran)
- [x] `make lint` until clean; commit as
      `rpc/jsonrpc: re-baseline pbin witness measurements on the code zone`

Re-measured on the code zone (call executing 8 bytes; chunk counts 1, 128, 129,
256, 793 reproduce execution-specs#3286's sizes, `group_spill` 257 replaces the
header-split `spill_4216`):

| case | code B | chunks | hex tot | bin tot | bin/hex | blob-variant | /hex |
| --- | --- | --- | --- | --- | --- | --- | --- |
| single_chunk | 31 | 1 | 1184 | 2016 | 1.70x | 1711 | 1.45x |
| chunks_128 | 3968 | 128 | 5120 | 19132 | 3.74x | 5714 | 1.12x |
| chunks_129 | 3999 | 129 | 5151 | 18997 | 3.69x | 5543 | 1.08x |
| group_full | 7936 | 256 | 9089 | 35947 | 3.96x | 9480 | 1.04x |
| group_spill | 7967 | 257 | 9120 | 36418 | 3.99x | 9646 | 1.06x |
| max_code_size | 24576 | 793 | 25729 | 108339 | 4.21x | 26255 | 1.02x |
| max_zero_padded | 24576 | 793 | 25730 | 2114 | 0.08x | 26323 | 1.02x |

Clone dedup, 8 contracts of 257 chunks each called in one block: clones
hex 10280 / bin 38591 (3.75x), distinct hex 66132 / bin 279752 (4.23x) —
shared bytecode proves 17219 chunk bytes against distinct's 137752.

➕ Also touched beyond the planned file list: `pbin_witness_bytesplit_test.go`
(`isCodeChunkKey` loses its dead account-zone arm), `pbin_witness_size_test.go`
(`pbinWitnessCorpus` shape names follow the e2e chain rename),
`pbin_witness_stateless_test.go` (stale header-chunk corpus comment).
Renames: `TestPBinWitnessConsecutiveSpillingDeploys` →
`TestPBinWitnessConsecutiveDeploys`; granularity cases `header_full` /
`first_overflow` / `deep_overflow` → `chunks_128` / `chunks_129` /
`group_full`. The granularity bin arm now asserts account-zone leaves stay
inside the allocated sub-indices (0, 1, 64–127), pinning Gap D's reserved-range
discipline in the witness path.

### Task 6: Add the delegation classifier and encoder

**Files:**
- Modify: `execution/commitment/pbin_keys.go`
- Modify: `execution/commitment/pbin_values.go`
- Create: `execution/commitment/pbin_delegation_test.go`

- [x] add `pbinDelegationMarker = {0xef,0x01,0x00}` and
      `pbinDelegationCodeLength = 23` (`pbinDelegationLeafKey` landed in Task 3;
      everything new sits in `pbin_values.go` — `pbin_keys.go` needed no edit)
- [x] add `pbinIsDelegation(code []byte) bool`, matching on length 23 and the
      three-byte prefix, and nothing else
- [x] add `pbinEncodeDelegation(code []byte) [pbinValueLength]byte`,
      right-padding with nine zero bytes
- [x] write `TestPBinIsDelegationClassifiesByBytes`: a valid indicator; 23 bytes
      without the marker; the marker at a length other than 23; and code whose
      *keccak hash* begins `0xef0100`, which must classify as code (the vendored
      `code_hash_starting_with_the_delegation_marker` value covers the last two
      at once: 23 bytes, no marker prefix, keccak opens `0xef0100`)
- [x] write `TestPBinEncodeDelegationPadsToThirtyTwo`, round-tripping the
      leading 23 bytes and asserting the encoding differs from the chunk
      encoding of the same bytes
- [x] gate: `go test ./execution/commitment/ -count=1 -v -run 'TestPBinIsDelegation|TestPBinEncodeDelegation'`
      and confirm both ran (both ran, both pass; Red confirmed first as
      undefined-identifier build failure)
- [x] `make lint` until clean; commit as
      `execution/commitment: classify and encode EIP-7702 delegation indicators`

### Task 7: Write exactly one of the code-hash and delegation leaves

**Files:**
- Modify: `execution/commitment/pbin_update_stream.go`
- Modify: `execution/commitment/pbin_hash.go`
- Modify: `execution/commitment/pbin_witness_context.go`
- Modify: `execution/commitment/pbin_verify_test.go`
- Modify: `execution/commitment/pbin_conformance_test.go`
- Modify: `execution/commitment/pbin_delegation_test.go`

- [ ] at the code-hash sibling emission (`pbin_update_stream.go:142-149`),
      branch on `pbinIsDelegation`: emit sub-index 2 and delete sub-index 1, or
      emit sub-index 1 and delete sub-index 2 — both deletions unconditional
      within their branch
- [ ] skip `queueCode` entirely for a delegated account; it has no `CODE_ZONE`
      leaves
- [ ] treat the delegation leaf as record-resident in `pbinLeafValue`
      (`pbin_hash.go`) and `pbinWitnessLeafState` (`pbin_witness_context.go:232-257`),
      which today produce flags only for sub-indices 0 and 1; the value carries
      no field an `Update` holds
- [ ] admit sub-index 2 in the verify position check (`pbin_verify_test.go:282`)
- [ ] teach the conformance `pbt_state` oracle (`pbin_conformance_test.go:204-212`)
      to write a delegation leaf and no code-hash or chunk leaves for a
      delegated account — the four delegation vector cases cannot go green
      otherwise
- [ ] write `TestPBinDelegationLeafIsExclusive`: delegation set on a fresh EOA;
      delegation replacing contract-shaped code; delegation cleared to
      `keccak("")` with `code_size` zeroed; two authorities delegating to one
      target producing two distinct header leaves and no shared leaf
- [ ] gate: `go test ./execution/commitment/ -count=1 -v -run 'TestPBinDelegationLeafIsExclusive|TestPBinConformancePBTState'`
      — `TestPBinConformancePBTState` must now be 18/18
- [ ] `make lint` until clean; commit as
      `execution/commitment: hold delegation indicators in the account header`

### Task 8: Serve delegated accounts from the witness

A delegated account holds no code-hash leaf, so the presence marker at
`pbin_witness_state.go:80` reports it absent.

**Files:**
- Modify: `execution/commitment/pbin_witness_state.go`
- Modify: `rpc/jsonrpc/pbin_witness_stateless.go`
- Modify: `execution/commitment/pbin_witness_codezone_test.go`

- [ ] make `PBinWitnessState.Account` treat "code-hash leaf **or** delegation
      leaf" as the presence marker, and synthesize `CodeHash` for a delegated
      account by hashing the leading `code_size` bytes of the leaf
- [ ] make the code reader (`pbin_witness_state.go:145`) return those leading
      `code_size` bytes before it reaches the chunk loop
- [ ] confirm `preStateAccount` (`rpc/jsonrpc/pbin_witness_stateless.go:151-168`)
      no longer returns nil for a delegated account
- [ ] write `TestPBinWitnessDelegatedAccountIsPresent` and
      `TestPBinWitnessDelegatedAccountCarriesNoChunks`
- [ ] write `TestPBinWitnessReassemblesCodeAcrossGroups`, reassembling a
      257-chunk contract byte-for-byte across two `tree_index` groups
- [ ] gate: `go test ./execution/commitment/ ./rpc/jsonrpc/ -count=1 -v -run 'TestPBinWitnessDelegated|TestPBinWitnessReassembles'`
      and confirm all three ran
- [ ] `make lint` until clean; commit as
      `execution/commitment, rpc/jsonrpc: read delegated accounts from a witness`

### Task 9: Reclaim code-zone leaves the batch inserted and nothing still holds

**Files:**
- Modify: `execution/commitment/pbin_update_stream.go`
- Create: `execution/commitment/pbin_reclaim_test.go`

- [ ] source the removed account's `code_hash` and `code_size` from a
      parent-state read; the deletion `Update` carries neither
- [ ] collect the code hashes still referenced after the batch during the
      existing account walk
- [ ] in `removeAccount` (`pbin_update_stream.go:166-177`), drop the chunk
      leaves iff the account's first chunk key is **absent from the parent
      state** and its `code_hash` is absent from the surviving set and is not
      `keccak("")`; keep them in every other case
- [ ] leave delegation indicators alone — they go with the header stem
- [ ] write `TestPBinReclaimDropsCodeWithNoSurvivor`: an account created and
      self-destructed inside the batch, no other holder — every chunk leaf gone
- [ ] write `TestPBinReclaimKeepsCodeForBatchSibling`: the same, with a sibling
      written in the same batch running identical bytecode — leaves remain
- [ ] write `TestPBinReclaimKeepsCodeForPreexistingHolder`: the same, with the
      holder pre-existing and untouched by the batch — leaves remain. This is
      the case the parent-state clause exists for, and the one a
      referenced-set-only rule gets wrong.
- [ ] gate: `go test ./execution/commitment/ -count=1 -v -run 'TestPBinReclaim|TestPBinConformancePBTState'`
      and confirm all three new tests ran
- [ ] `make lint` until clean; commit as
      `execution/commitment: reclaim unreferenced code chunks on account removal`

### Task 10: Pin the adversarial cases Tasks 6-9 do not already cover

Tasks 6, 7 and 9 already pin hash-grinding, delegation exclusivity and the
three reclamation directions. What remains are the intra-batch sequences and
the group-boundary shape.

**Files:**
- Create: `execution/commitment/pbin_adversarial_test.go`

- [ ] `TestPBinDelegationSetAndClearedInOneBatch` — the account ends with a
      code-hash leaf of `keccak("")` and no delegation leaf
- [ ] `TestPBinDelegationRepointedInOneBatch` — the delegation leaf is rewritten
      and no code-hash leaf ever appears
- [ ] `TestPBinZeroChunkAloneInItsGroup` — a zero chunk that is the only chunk
      in its `tree_index`, so the group has no leaf at all
- [ ] `TestPBinSharedCodeOutlivesOneHolder` — two accounts sharing code, one
      deleted, the other keeps it, root matches the oracle
- [ ] assert engine root == `pbinOracleRoot` in every case, and == the vector
      root wherever the corpus pins one
- [ ] gate: `go test ./execution/commitment/ -count=1 -v -run TestPBin` and
      confirm all four new tests ran
- [ ] `make lint` until clean; commit as
      `execution/commitment: pin adversarial pbin embedding cases`

### Task 11: Extend the differential fuzz corpus over the new shapes

**Files:**
- Modify: `execution/commitment/pbin_fuzz_test.go`
- Modify: `execution/commitment/pbin_process_test.go`

- [ ] extend `pbinFuzzCorpus` (`pbin_fuzz_test.go:54`) to generate delegated
      accounts, accounts sharing bytecode, all-zero chunks, and chunk counts
      straddling the 255/256 and 511/512 group boundaries
- [ ] extend `pbinFuzzBatches` to delete accounts inside a batch, so reclamation
      is exercised against the oracle and not only by hand-written cases
- [ ] confirm `pbinTestCorpus.entries` (`pbin_process_test.go:74`) already
      describes the delegation leaf after Task 7; extend it only if the oracle
      and the engine disagree
- [ ] add seed corpus entries for each Task 10 case
- [ ] gate: `go test ./execution/commitment/ -count=1 -run FuzzPBinProcessMatchesOracle`
      then `go test ./execution/commitment/ -fuzz FuzzPBinProcessMatchesOracle -fuzztime=5m`
- [ ] `make lint` until clean; commit as
      `execution/commitment: fuzz pbin over delegation, sharing and group boundaries`

### Task 12: Cite the spec by section instead of by line number

**Files:**
- Modify: `execution/commitment/pbin_keys.go` (9), `pbin_hash.go` (6),
  `pbin_values.go` (3), `pbin_code.go` (2), `pbin_patricia_hashed.go` (1),
  `pbin_update_stream.go` (1), `pbin_witness_decode.go` (1)
- Modify: `execution/commitment/pbin_oracle_test.go` (6), `pbin_keys_test.go` (2),
  `pbin_process_test.go` (2), `pbin_overflow_test.go` (2), `pbin_code_test.go` (1),
  `pbin_hash_test.go` (1), `pbin_hashsuite_test.go` (1), `pbin_unfold_test.go` (1),
  `pbin_witness_decode_test.go` (1)
- Modify: `docs/pbin-encoding.md`

- [ ] replace every `eip:` citation with a section name, e.g. `eip:"Code"`,
      `eip:"Zero values and deletion"`, `eip:"Delegation"` — including the
      single-line form `eip:132` at `pbin_values.go:31`
- [ ] refresh `docs/pbin-encoding.md` for the code zone, the delegation leaf and
      the reserved sub-index ranges (3–63 and 128–255)
- [ ] confirm `grep -rn 'eip:[0-9]' execution/commitment/` returns nothing
- [ ] gate: `go test ./execution/commitment/... -run TestPBin -count=1`
- [ ] `make lint` until clean; commit as
      `execution/commitment: cite EIP-8297 by section, not by line`

### Task 13: Close execution-specs#3286 as superseded by the code-zone move

Done ahead of the run. Nothing here for the executor.

**Files:**
- Modify: (GitHub only) PR `ethereum/execution-specs#3286`

- [x] confirm the PR's five cases are all parametrised on the header/overflow
      split that `ab8081e`/`6c05450` deleted — the branch reads
      `Spec.CODE_OFFSET`, which `spec.py` no longer defines, so the file does
      not import against the current base
- [x] post a short comment
- [x] close the PR
- [x] confirm branch `tests/binary-tree-witness-growth` stays on `fork` at
      `b2382c5`
- [ ] record in this plan which of its measurements Task 5 reproduces

The five sizes to reproduce in Task 5, re-expressed as chunk counts now that
the header holds none: 1, 128, 129, 256, and 793 (`MAX_CODE_SIZE`).

### Task 14: Rebase and reframe execution-specs#3305

Its first case argued a client could apply the zero rule in one zone and not
the other. There is no second zone holding chunks now, so that framing is dead
— but the shared-zero-chunk case becomes more central, not less.

**Files:**
- Modify: `tests/binary_trie/vectors/dump_vectors.py` (in `~/org/wrk/espr`)
- Modify: `tests/binary_trie/vectors/README.md`
- Modify: `tests/binary_trie/vectors/binary_trie_vectors.json`

- [ ] rebase `tests/binary-trie-zero-chunk-vectors` onto current
      `projects/binary-trie`, resolving the generated JSON by regenerating it
- [ ] reframe case one as a zero chunk crossing a `STEM_SUBTREE_WIDTH` group
      boundary (`tree_index >= 1`), and rename both cases away from "overflow"
- [ ] regenerate with `uv run python tests/binary_trie/vectors/dump_vectors.py`
      and update the README case table
- [ ] verify both roots against erigon's engine and the canonical-rebuild oracle
- [ ] rewrite the PR body: problem paragraph first with no "Summary" heading,
      then `## Changes`; no Testing section, no AI mentions
- [ ] push and reply to the rebase request

### Task 15: Rebase execution-specs#3316

**Files:**
- Modify: `tests/binary_tree/eip8297_partitioned_binary_tree/test_multi_block.py` (in `~/org/wrk/espr`)

- [ ] rebase `tests/binary-tree-consecutive-spilling-deploys` onto current
      `projects/binary-trie`
- [ ] **re-size the three contracts.** They are `(129, 137, STEM_SUBTREE_WIDTH)`
      chunks, sized when 128 was the header/code-zone edge. That edge is gone,
      so 129 and 137 are ordinary interior sizes and nothing in the test crosses
      a boundary. The only boundary left is the group edge at
      `STEM_SUBTREE_WIDTH`, so use `(255, 256, 257)`: last chunk of group 0,
      exactly-full group 0, and first chunk of group 1. The 257-chunk contract
      is the only one holding two stems, which is what makes a later deploy
      disturbing an earlier one visible.
- [ ] keep every size under 308 chunks — the filler fails deterministically at
      309 and above, unexplained and unrelated to this test
- [ ] update the test's docstring: the point is now sharing one code zone across
      the group boundary, not spilling out of a header
- [ ] fill the test on the rebased base and confirm all cases pass
- [ ] rewrite the PR body for the new sizes; keep it dead short, problem
      paragraph first, no "Summary" heading, no Testing section, no AI mentions
- [ ] push and reply to the rebase request
- [ ] confirm CI is green apart from any known unrelated `fork.py` drift

### Task 16: Diff the adversarial case list against the existing corpus

Nothing gets proposed upstream that the corpus already pins. The current corpus
is 18 `pbt_state` vector cases and 49 fixture test functions.

**Files:**
- Create: `docs/plans/notes/20260807-pbin-corpus-gap.md`

- [ ] create `docs/plans/notes/` — it does not exist
- [ ] list every case from Tasks 6, 7, 9 and 10 beside its nearest existing
      equivalent, checking at minimum `test_code_sharing.py`
      (`test_shared_code_survives_sibling_same_tx_selfdestruct`,
      `test_shared_designator_survives_peer_redelegation`,
      `test_contract_hashing_to_the_delegation_marker_executes_as_code`) and
      `test_code_chunking.py` (`test_delegated_eoa_executes_chunked_delegate`)
- [ ] mark each covered, partially covered, or absent, with the file and test
      name backing the verdict
- [ ] confirm the expected shortlist: reclamation with no surviving holder, and
      a zero chunk alone in its group
- [ ] drop anything already covered — do not repackage an existing case
- [ ] record the shortlist in this plan before opening anything

### Task 17: Contribute the absent cases back to the corpus

**Files:**
- Modify: `tests/binary_trie/vectors/dump_vectors.py` (in `~/org/wrk/espr`)
- Modify: `tests/binary_tree/eip8297_partitioned_binary_tree/test_code_sharing.py` (in `~/org/wrk/espr`)

- [ ] open one PR per coherent theme, not one per case
- [ ] for a vector case, regenerate `binary_trie_vectors.json` and update the
      README case table in the same commit
- [ ] for a fixture case, fill it and confirm it passes on `projects/binary-trie`
- [ ] cross-check every proposed root against erigon's engine before pushing
- [ ] write each body dead short: problem paragraph first, no "Summary" heading,
      no Testing section, no AI mentions
- [ ] push to `fork` and open against `projects/binary-trie`

### Task 18: Verify acceptance criteria

- [ ] `TestPBinConformancePBTState` is 18/18 and `TestPBinConformanceEmbedding`
      passes against vectors `58faeb0`
- [ ] `grep -rn 'pbinCodeOffset\|pbinHeaderCodeChunks\|pbinHeaderCodeCapacity' .`
      returns nothing outside this plan
- [ ] `grep -rn 'eip:[0-9]' execution/commitment/` returns nothing
- [ ] every test function named in Tasks 4-11 exists and runs — confirm by name
      in `go test -v` output, not by an exit code
- [ ] the fuzz target runs 5 minutes clean
- [ ] run the full suite: `go test ./execution/commitment/... ./rpc/jsonrpc/... -count=1`
- [ ] run `make lint` repeatedly until clean, then `make erigon integration`
- [ ] confirm the erigon branch is committed and **not** pushed

### Task 19: [Final] Update documentation

- [ ] confirm `docs/pbin-encoding.md` matches the shipped layout
- [ ] update `CLAUDE.md` only if a new convention emerged that future work needs
- [ ] record the spec HEAD this catch-up targets (`2c6da5e` / `8d258bc` /
      vectors `58faeb0`) in this plan's Overview so the next drift is measurable
- [ ] append the settled facts to `~/org/mode/e/research-8297-witness.org` per
      the research-log skill
- [ ] move this plan to `docs/plans/completed/`

## Post-Completion

*Items requiring manual intervention or external systems — informational only.*

**Out of scope here, tracked for later:**

- **EIP-7610 non-empty-storage predicate.** Newly specified: an address has
  non-empty storage exactly when a leaf exists at one of its header sub-indices
  64–127, or anywhere in its storage bucket. The MPT's `storage_root` check has
  no analogue in this tree. This lives in the EL state layer, not `package
  commitment`, so it belongs to the M1 EL-integration work.
- **Gas schedule.** The EIP deleted its Access-events section outright. Erigon
  never carried EIP-4762 keying or branch-cost comments, so there is nothing to
  remove — but nothing should be added until a schedule is specified.
- **Migration.** The Fork section now points at EIP-8347. Erigon has no
  conversion path; that is its own programme.

**Maintainer decisions we do not control:**

- Whether execution-specs#3305's reframed cases are wanted now that every chunk
  is content-addressed — the reviewer may prefer them folded into an existing
  case.
- Whether the reclamation-with-no-survivor case belongs in the vector corpus or
  the fixture suite; it needs a transaction to express, which argues for
  fixtures.

**Deliberately deferred:**

- The unexplained deterministic deploy-fill failure at ≥310 chunks (9,610 bytes)
  in the spec repo's fixture filler, reproducible across runs while 308 chunks
  passes. It blocked nothing here and execution-specs#3316 was sized around it.
