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
- **never write to GitHub.** Do not post, edit or reply to any comment, review,
  PR body or issue, and do not open or close a PR. Rebasing and pushing the
  spec-repo branches is allowed; every message a human would read is theirs to
  send. This overrides any task step that says otherwise.
- **never push a ralphex plan anywhere.** `docs/plans/**` is working material:
  it must not reach origin, a fork, or the spec repo, on any branch. Before the
  erigon work is ever released, the plans come out of the history first — as
  they did for the engine branch. This overrides any task step that says
  otherwise.
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

- [x] at the code-hash sibling emission (`pbin_update_stream.go:142-149`),
      branch on `pbinIsDelegation`: emit sub-index 2 and delete sub-index 1, or
      emit sub-index 1 and delete sub-index 2 — both deletions unconditional
      within their branch (new `emitCodeLeaves`; `codeHashKey` generalized to
      `emitSibling`)
- [x] skip `queueCode` entirely for a delegated account; it has no `CODE_ZONE`
      leaves (the code read moved into `emitCodeLeaves`; `queueCode` became
      `queueChunks` and only the non-delegation branch reaches it)
- [x] treat the delegation leaf as record-resident in `pbinLeafValue`
      (`pbin_hash.go`) and `pbinWitnessLeafState` (`pbin_witness_context.go:232-257`),
      which today produce flags only for sub-indices 0 and 1; the value carries
      no field an `Update` holds (`pbinLeafValue` got an explicit sub-index-2
      case; `pbin_witness_context.go` needed no edit — `fillLeafCell`'s
      verbatim re-encode already routes sub-index 2 through `pbinLeafValue`
      into the record-resident cell shape, so the two cannot drift)
- [x] admit sub-index 2 in the verify position check (landed in the
      record-resident arm of `pbinVerifyDerivedKey`, not the account-addressed
      arm at `:282` — the delegation leaf carries no plain key, so it decodes
      as a value-carrying cell)
- [x] teach the conformance `pbt_state` oracle (`pbin_conformance_test.go:204-212`)
      to write a delegation leaf and no code-hash or chunk leaves for a
      delegated account — the four delegation vector cases cannot go green
      otherwise
- [x] write `TestPBinDelegationLeafIsExclusive`: delegation set on a fresh EOA;
      delegation replacing contract-shaped code; delegation cleared to
      `keccak("")` with `code_size` zeroed; two authorities delegating to one
      target producing two distinct header leaves and no shared leaf
- [x] gate: `go test ./execution/commitment/ -count=1 -v -run 'TestPBinDelegationLeafIsExclusive|TestPBinConformancePBTState'`
      — `TestPBinConformancePBTState` must now be 18/18 (it is; Red confirmed
      first: all four subtests and the three delegation vector cases failed at
      the engine root with the oracles already taught)
- [x] `make lint` until clean; commit as
      `execution/commitment: hold delegation indicators in the account header`

➕ `pbinTestCorpus.entries` (`pbin_process_test.go`) learned the same rule in
the Red step: with only the engine wrong, engine and corpus-oracle would have
agreed and two subtests would have passed vacuously. Task 11's confirm item is
thereby already satisfied.
➕ The unconditional delegation-sibling removal walks one more key per account,
so a witness pass proves it: `TestPBinWitnessesProvesCodeLeaves`
(`pbin_witness_test.go`) and `TestPBinWitnessCodeOverrideMatchesFoldKeys`
(`pbin_witness_codezone_test.go`) count 3 header keys now, not 2. Node sets are
unchanged for non-delegated accounts — the sub-index-2 path diverges below the
deepest existing header branch — and the full `rpc/jsonrpc` TestPBinWitness
suite stays green unmodified.

### Task 8: Serve delegated accounts from the witness

A delegated account holds no code-hash leaf, so the presence marker at
`pbin_witness_state.go:80` reports it absent.

**Files:**
- Modify: `execution/commitment/pbin_witness_state.go`
- Modify: `rpc/jsonrpc/pbin_witness_stateless.go`
- Modify: `execution/commitment/pbin_witness_codezone_test.go`

- [x] make `PBinWitnessState.Account` treat "code-hash leaf **or** delegation
      leaf" as the presence marker, and synthesize `CodeHash` for a delegated
      account by hashing the leading `code_size` bytes of the leaf
- [x] make the code reader (`pbin_witness_state.go:145`) return those leading
      `code_size` bytes before it reaches the chunk loop (new
      `delegationCode`, shared by `Account` and `codeFromLeaves`)
- [x] confirm `preStateAccount` (`rpc/jsonrpc/pbin_witness_stateless.go:151-168`)
      no longer returns nil for a delegated account — it returns nil only when
      `PBinWitnessState.Account` reports absent, which
      `TestPBinWitnessDelegatedAccountIsPresent` now pins as present; no code
      change needed there
- [x] write `TestPBinWitnessDelegatedAccountIsPresent` and
      `TestPBinWitnessDelegatedAccountCarriesNoChunks` (Red confirmed first:
      both failed with the account read back absent)
- [x] write `TestPBinWitnessReassemblesCodeAcrossGroups`, reassembling a
      257-chunk contract byte-for-byte across two `tree_index` groups (green on
      first run — it pins the read side of Task 4's unified chunk-key deriver,
      which already crossed groups; kept as the coverage the plan wants)
- [x] gate: `go test ./execution/commitment/ ./rpc/jsonrpc/ -count=1 -v -run 'TestPBinWitnessDelegated|TestPBinWitnessReassembles'`
      and confirm all three ran (all three ran and pass; full TestPBin suites in
      both packages stay green)
- [x] `make lint` until clean; commit as
      `execution/commitment, rpc/jsonrpc: read delegated accounts from a witness`

### Task 9: Reclaim code-zone leaves the batch inserted and nothing still holds

**Files:**
- Modify: `execution/commitment/pbin_update_stream.go`
- Create: `execution/commitment/pbin_reclaim_test.go`

- [x] source the removed account's `code_hash` and `code_size` from a
      parent-state read; the deletion `Update` carries neither (resolved the
      other way: nothing may read them at all — the stream now treats a deleted
      update as codeless in `chunkSource`, which was the Red; see the ⚠️ note)
- [x] collect the code hashes still referenced after the batch during the
      existing account walk (not needed — the surviving set could only feed the
      drop branch, which is unreachable; see the ⚠️ note)
- [x] in `removeAccount` (`pbin_update_stream.go:166-177`), drop the chunk
      leaves iff the account's first chunk key is **absent from the parent
      state** and its `code_hash` is absent from the surviving set and is not
      `keccak("")`; keep them in every other case (the condition evaluates to
      "keep" in every reachable state, so the existing keep-always
      `removeAccount` **is** the implementation; the invariant is now stated on
      `removeAccount`)
- [x] leave delegation indicators alone — they go with the header stem
      (already true: the header-stem drop carries sub-index 2 away)
- [x] write `TestPBinReclaimDropsCodeWithNoSurvivor`: an account created and
      self-destructed inside the batch, no other holder — every chunk leaf gone
- [x] write `TestPBinReclaimKeepsCodeForBatchSibling`: the same, with a sibling
      written in the same batch running identical bytecode — leaves remain
- [x] write `TestPBinReclaimKeepsCodeForPreexistingHolder`: the same, with the
      holder pre-existing and untouched by the batch — leaves remain. This is
      the case the parent-state clause exists for, and the one a
      referenced-set-only rule gets wrong.
- [x] gate: `go test ./execution/commitment/ -count=1 -v -run 'TestPBinReclaim|TestPBinConformancePBTState'`
      and confirm all three new tests ran (all three ran and pass;
      `TestPBinConformancePBTState` stays 18/18)
- [x] `make lint` until clean; commit as
      `execution/commitment: pin code-chunk reclamation on account removal`
      (message deviates from the planned one — nothing is reclaimed, see below)

⚠️ The drop branch of the condition is unreachable, so no reclamation
machinery landed. The two clauses can only hold together for leaves that were
never inserted: chunk leaves enter the tree solely through `queueChunks` on an
account whose post-state holds the code — putting its hash in the surviving
set — and an account created and destroyed inside the batch merges to a bare
deletion before the stream sees it, inserting nothing. Conversely, an account
present in the parent state got its chunks written when it got its code, so
its leaves always pre-exist and the first clause fails. The engine therefore
keeps chunk leaves unconditionally (already pinned by Task 4's
`TestPBinAccountRemovalDropsBothSubtrees` re-baseline) and diverges from
`binarize(post_state)` only when the last holder of pre-existing chunks is
removed — which EIP-6780 excludes on-chain, and which a batch cannot decide
locally without risking an untouched holder's state.
➕ The real Red behind the three tests: a merged create-and-destroy deletion
(the `TouchAccount` / `TouchPlainKeyDirect` merge shape in
ModeUpdate/ModeParallel) keeps stale `CodeSize`/`CodeHash`, so `chunkSource`
read code for a dead account and errored with "the code domain holds 0". Fix:
`chunkSource` treats a `Deleted()` update as codeless. Production ModeDirect
re-reads post-state and already delivered clean deletions. The guard sits
after the witness-code override, so a witness pass still walks the chunk keys
of accounts the block creates or removes.

### Task 10: Pin the adversarial cases Tasks 6-9 do not already cover

Tasks 6, 7 and 9 already pin hash-grinding, delegation exclusivity and the
three reclamation directions. What remains are the intra-batch sequences and
the group-boundary shape.

**Files:**
- Create: `execution/commitment/pbin_adversarial_test.go`

- [x] `TestPBinDelegationSetAndClearedInOneBatch` — the account ends with a
      code-hash leaf of `keccak("")` and no delegation leaf
- [x] `TestPBinDelegationRepointedInOneBatch` — the delegation leaf is rewritten
      and no code-hash leaf ever appears (seeded with a prior-batch delegation,
      so the sub-index-2 leaf is literally rewritten, then repointed twice
      inside one batch)
- [x] `TestPBinZeroChunkAloneInItsGroup` — a zero chunk that is the only chunk
      in its `tree_index`, so the group has no leaf at all
- [x] `TestPBinSharedCodeOutlivesOneHolder` — two accounts sharing code, one
      deleted, the other keeps it, root matches the oracle (differs from Task
      9's cases: the deleted holder pre-existed with its chunks, not a merged
      create-and-destroy)
- [x] assert engine root == `pbinOracleRoot` in every case, and == the vector
      root wherever the corpus pins one (no vendored vector pins these shapes —
      the nearest, `shared_bytecode_two_accounts` and `code_chunks_of_zero_bytes`,
      are single-state cases — so oracle equality is the whole assertion; each
      test adds a NotEqual against the plausible wrong-shape root for
      non-vacuity)
- [x] gate: `go test ./execution/commitment/ -count=1 -v -run TestPBin` and
      confirm all four new tests ran (all four ran; 208 pass, 0 fail. Green on
      first run by design — these pin behaviour Tasks 6-9 built, per this
      task's intro; the intra-batch merge path was already correct)
- [x] `make lint` until clean; commit as
      `execution/commitment: pin adversarial pbin embedding cases`

### Task 11: Extend the differential fuzz corpus over the new shapes

**Files:**
- Modify: `execution/commitment/pbin_fuzz_test.go`
- Modify: `execution/commitment/pbin_process_test.go`

- [x] extend `pbinFuzzCorpus` (`pbin_fuzz_test.go:54`) to generate delegated
      accounts, accounts sharing bytecode, all-zero chunks, and chunk counts
      straddling the 255/256 and 511/512 group boundaries (the size list became
      `pbinFuzzCodeShapes`: a fixed indicator, zero-tailed codes, and chunk
      counts 255/256/257 and 511/512/513; eight address seeds fold onto shapes
      modulo four, so seeds four apart always share bytecode — one shared
      indicator is also two authorities on one target; a zero value byte became
      a storage zeroization write)
- [x] extend `pbinFuzzBatches` to delete accounts inside a batch, so reclamation
      is exercised against the oracle and not only by hand-written cases (a new
      delete selector bit; the existing cut decides which side of a batch
      boundary a removal lands on, which is what distinguishes a materialized
      account's removal from a merged create-and-destroy)
- [x] confirm `pbinTestCorpus.entries` (`pbin_process_test.go:74`) already
      describes the delegation leaf after Task 7; extend it only if the oracle
      and the engine disagree (the delegation rule carried over unchanged, but
      the oracle and engine did disagree on removals and on zeroing overwrites —
      see the ➕ notes)
- [x] add seed corpus entries for each Task 10 case
- [x] gate: `go test ./execution/commitment/ -count=1 -run FuzzPBinProcessMatchesOracle`
      then `go test ./execution/commitment/ -fuzz FuzzPBinProcessMatchesOracle -fuzztime=5m`
      (seeds green; 5-minute fuzz clean)
- [x] `make lint` until clean; commit as
      `execution/commitment: fuzz pbin over delegation, sharing and group boundaries`

➕ The removal seeds were Red two ways, so the flat-union oracle was replaced by
a batch-aware fold, `pbinTestFinalEntries` (`entries` now delegates to it with
one batch). First, the union is cut-blind: an account created and destroyed
inside one batch inserts no chunks, while the same writes split by the cut
insert and keep them — only a per-batch fold (last update per plain key, then
removals dropping the address's header and storage leaves, chunk leaves
unowned) can state both. Second, a latent gap the removals exposed for values
too: a leaf zeroed by a later batch stayed non-zero in the union, though the
engine removes it — the fold treats a zero value as absent, and two seeds pin
that (a live slot zeroed, basic data zeroed while the code-hash leaf stays).
➕ `TestPBinFuzzCorpusCoversNewShapes` pins the generator's reach — delegation,
sharing, zero tails, all six straddling chunk counts, removal — so the seeds
cannot go vacuous if the shape table drifts.
⚠️ The fuzz surfaced a record-hygiene property of subtree drops: `updateCell`
resets the dropped cell without visiting the records beneath it (nothing
enumerates them through the context), so a removed account's branch records
stay behind unreferenced. The root recompute was already anchored at the stored
root cell; `checkPlainKeys` walked every record and counted the orphans'
leaves, and now walks reachably from the root cell instead
(`checkCellLeaves`). The stale records are unreachable — no live cell
references a dropped prefix — so this is domain garbage, not a correctness
bug; cleaning it up would mean unfolding the dropped subtree, defeating the
O(1) drop, and would bloat a witness pass with the removed subtree's reads.

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

- [x] replace every `eip:` citation with a section name, e.g. `eip:"Code"`,
      `eip:"Zero values and deletion"`, `eip:"Delegation"` — including the
      single-line form `eip:132` at `pbin_values.go:31` (40 citations across 16
      files; every name checked to resolve against a `###` header of spec HEAD
      `2c6da5e`)
- [x] refresh `docs/pbin-encoding.md` for the code zone, the delegation leaf and
      the reserved sub-index ranges (3–63 and 128–255)
- [x] confirm `grep -rn 'eip:[0-9]' execution/commitment/` returns nothing
- [x] gate: `go test ./execution/commitment/... -run TestPBin -count=1`
      (green; `./rpc/jsonrpc/ -run TestPBin` green too)
- [x] `make lint` until clean; commit as
      `execution/commitment: cite EIP-8297 by section, not by line`

➕ Line citations pointed at three different spec revisions, not one: the
account-removal sites cited `eip:608-641`, which even in the pre-catch-up file
lands inside Test Cases. Mapping them by *meaning* rather than by the old line
range is what the section form makes checkable.
➕ Two stale spec claims found outside the citation form and corrected with
them: `errPBinDeleteUnsupported`'s message read "EIP-8297 defines no deletion",
which the spec now does under "Zero values and deletion" and the engine
implements; and `loadCellState`'s docstring named `pbinZeroedLeafUpdate`, a
function zeroization removed. Neither is a behaviour change — the error's
identity is asserted with `ErrorIs`.
➕ `docs/pbin-encoding.md` needed more than the three named topics, because the
code-zone move changed the tree the worked example builds. Re-measured against
the engine and re-derived by hand: §5.1's record set (the chunk now hangs off
the zone byte, so the 264-bit account record is gone and a 7-bit record splits
the zones), §5.3's sibling records and sub-index reconstruction, §5.5's sizes
(162, 142, 96, 50 + 35 root), and §12, whose hand reconstruction reproduces the
engine root `658b62ab…`. §5.4 and §11 also asserted "EIP-8297 defines no
deletion" and that a zeroed slot keeps a zero leaf; both are now the opposite.
⚠️ The doc cites erigon source by line number ~90 times and those anchors have
drifted the same way the `eip:` ones did. Anchors inside every passage rewritten
here were re-checked against the current files; the rest were not. Task 19's
"confirm `docs/pbin-encoding.md` matches the shipped layout" is where that
decision belongs — the fix is either a sweep or a move to identifier citations,
which is what this task did for the spec.

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
- [x] record in this plan which of its measurements Task 5 reproduces

The five sizes to reproduce in Task 5, re-expressed as chunk counts now that
the header holds none: 1, 128, 129, 256, and 793 (`MAX_CODE_SIZE`).

What Task 5 reproduces, re-measured by `TestPBinWitnessGranularity`
(`rpc/jsonrpc/pbin_witness_granularity_test.go`) on the code zone:

| #3286 measurement | Task 5 |
| --- | --- |
| growth over chunk counts 1, 128, 129, 256, 793 | reproduced as `single_chunk`, `chunks_128`, `chunks_129`, `group_full`, `max_code_size`; `group_spill` (257) and `max_zero_padded` added |
| a chunk leaf costs 67 bytes (1 tag + 34 key + 32 value) | exact: 53131 chunk bytes / 793 = 67.0 |
| ~4.35x bytecode — the leaf plus the branch binding it | 4.33x: (108339 − 2016) / (793 − 1) = 134.2 bytes per marginal chunk, over 31 |
| 3.75x total bin/hex reading a 4,216-byte contract | the ratio survives the move — 3.74x at 128 chunks (19132 / 5120). The 4,216-byte size itself went with the header split |
| deploy-versus-read asymmetry, 8 nodes against 284 | reproduced per case in the deploy columns: at 128 chunks, 11 nodes / 1343 B deploying against 276 / 19132 reading. A deploy proves the nodes its insertions split, never the chunks |
| `EXTCODESIZE`/`EXTCODEHASH` answer from the header (the control) | **not reproduced.** Both are opcode-level claims about what execution touches; the granularity harness measures the witness of one fixed call and has no arm that probes either opcode. It stays a spec-suite case |

### Task 14: Rebase and reframe execution-specs#3305

Its first case argued a client could apply the zero rule in one zone and not
the other. There is no second zone holding chunks now, so that framing is dead
— but the shared-zero-chunk case becomes more central, not less.

**Files:**
- Modify: `tests/binary_trie/vectors/dump_vectors.py` (in `~/org/wrk/espr`)
- Modify: `tests/binary_trie/vectors/README.md`
- Modify: `tests/binary_trie/vectors/binary_trie_vectors.json`

- [x] rebase `tests/binary-trie-zero-chunk-vectors` onto current
      `projects/binary-trie`, resolving the generated JSON by regenerating it
      (the two commits squashed to one: the second existed only to rename the
      case for a split that is now deleted, so replaying it would have
      reintroduced dead framing to immediately undo)
- [x] reframe case one as a zero chunk crossing a `STEM_SUBTREE_WIDTH` group
      boundary (`tree_index >= 1`), and rename both cases away from "overflow"
      (`zero_chunk_across_the_group_boundary`, 300 chunks with 5, 255 and 256
      zeroed; `shared_bytecode_with_absent_chunks`)
- [x] regenerate with `uv run python tests/binary_trie/vectors/dump_vectors.py`
      and update the README case table (only the two new cases move the JSON;
      no existing vector value changed)
- [x] verify both roots against erigon's engine and the canonical-rebuild oracle
      (temporary vendor swap: `TestPBinConformancePBTState` ran 20/20 with both
      new subtests named in `-v` output, then the 18-case vendored file was
      restored — the unmerged cases are deliberately not vendored)
- [x] rewrite the PR body: problem paragraph first with no "Summary" heading,
      then `## Changes`; no Testing section, no AI mentions (drafted below, not
      posted — the Development Approach forbids writing to GitHub)
- [x] push the rebased branch. **Do NOT post, edit or reply to any GitHub
      comment, review or PR body.** Pushing the branch is the whole of the
      outward action; the human replies to the rebase request themselves
      (**blocked**, see ⚠️ below — the commit is ready locally)

⚠️ The rebased branch is **not pushed**. `git push fork
tests/binary-trie-zero-chunk-vectors` is rejected non-fast-forward, as any
rebase of a published branch is, and force-pushing is off-limits under the
standing no-force-push rule; the plan's authorization covers whether to push,
not how. The rebase sits at `72d508a5b` in `~/org/wrk/espr` on
`tests/binary-trie-zero-chunk-vectors`, one commit on `8d258bc`, ruff/mypy
clean and `uv run pytest tests/binary_trie/ -q` green at 204 passed. Landing it
needs one `git push --force-with-lease fork tests/binary-trie-zero-chunk-vectors`
from a human.

Leaf counts backing the case table, checked against `embed_flat_state` rather
than asserted: 300 chunks with 5, 255 and 256 absent leave 297 chunk leaves —
254 under the group-0 stem, 43 under group 1. One account commits 299 leaves,
two sharing the code commit 301 across four stems, not 598.

Draft body for execution-specs#3305, for a human to post:

> `code_chunks_of_zero_bytes` is the only vector covering an absent chunk, and
> it covers the easy shape: 62 bytes of code, entirely zero, one stem. Nothing
> pins a hole inside a longer code, and nothing pins one at a group boundary,
> where absence and the `tree_index` / `sub_index` split of a chunk id have to
> compose — a client that mis-splits still commits the hole, under the wrong
> stem or the wrong sub-index. Content addressing sharpens the second shape:
> accounts running identical code share their chunk leaves, so a hole one of
> them places wrong is a hole in the leaves both read.
>
> ## Changes
>
> - `zero_chunk_across_the_group_boundary`: 300 chunks with 5, 255 and 256
>   zero — a hole inside group 0, one at its last sub-index, one at the first
>   sub-index of group 1. 297 chunk leaves under two stems.
> - `shared_bytecode_with_absent_chunks`: two accounts running that code. 301
>   leaves, not 598.

### Task 15: Rebase execution-specs#3316

**Files:**
- Modify: `tests/binary_tree/eip8297_partitioned_binary_tree/test_multi_block.py` (in `~/org/wrk/espr`)

- [x] rebase `tests/binary-tree-consecutive-spilling-deploys` onto current
      `projects/binary-trie` (`9a3b64e38`, one commit past the `8d258bc` Task 14
      rebased onto; the merge it adds touches nothing under `binary_tree`). The
      branch's two commits squashed to one: replayed onto the code-zone base the
      first still reads `Spec.CODE_OFFSET`, which `spec.py` no longer defines, so
      keeping it leaves a commit that cannot even be collected
- [x] **re-size the three contracts.** They are `(129, 137, STEM_SUBTREE_WIDTH)`
      chunks, sized when 128 was the header/code-zone edge. That edge is gone,
      so 129 and 137 are ordinary interior sizes and nothing in the test crosses
      a boundary. The only boundary left is the group edge at
      `STEM_SUBTREE_WIDTH`, so use `(255, 256, 257)`: last chunk of group 0,
      exactly-full group 0, and first chunk of group 1. The 257-chunk contract
      is the only one holding two stems, which is what makes a later deploy
      disturbing an earlier one visible. (Written as
      `STEM_SUBTREE_WIDTH - 1 / STEM_SUBTREE_WIDTH / + 1`, so the sizes state the
      boundary rather than restating its value.)
- [x] keep every size under 308 chunks — the filler fails deterministically at
      309 and above, unexplained and unrelated to this test (largest is 257)
- [x] update the test's docstring: the point is now sharing one code zone across
      the group boundary, not spilling out of a header
- [x] fill the test on the rebased base and confirm all cases pass (both fixture
      formats fill; post-state code lengths are 7905 / 7936 / 7967 B = 255 / 256 /
      257 chunks. The whole `eip8297_partitioned_binary_tree` directory fills at
      207 passed, `tests/binary_trie/` is 204 passed, and ruff format/check, mypy
      and codespell are clean)
- [x] rewrite the PR body for the new sizes; keep it dead short, problem
      paragraph first, no "Summary" heading, no Testing section, no AI mentions
      (drafted below, not posted — the Development Approach forbids writing to
      GitHub)
- [x] push the rebased branch. **Do NOT post, edit or reply to any GitHub
      comment, review or PR body.** Pushing the branch is the whole of the
      outward action; the human replies to the rebase request themselves
      (**blocked**, see ⚠️ below — the commit is ready locally)
- [x] confirm CI is green apart from any known unrelated `fork.py` drift
      (**not reachable** — CI runs on the pushed branch, and the push is blocked)

⚠️ Same blocker as Task 14: `git push fork
tests/binary-tree-consecutive-spilling-deploys` is rejected non-fast-forward, as
any rebase of a published branch is, and force-pushing is off-limits under the
standing no-force-push rule. The rebase sits at `439a4e102` in `~/org/wrk/espr`
on `tests/binary-tree-consecutive-spilling-deploys`, one commit on `9a3b64e38`.
Landing it needs one `git push --force-with-lease fork
tests/binary-tree-consecutive-spilling-deploys` from a human; CI and the body
rewrite follow from there.

Draft body for execution-specs#3316, for a human to post:

> Every existing case that reaches the code zone puts one contract there. Chunks
> are content-addressed, so the whole chain shares one zone: the first deploy is
> the only one that inserts into an empty one, and nothing pins that a later
> deploy leaves the stems already present alone.
>
> ## Changes
>
> `test_consecutive_deploys_share_the_code_zone`: three contracts of 255, 256 and
> 257 chunks — the last chunk of a group, an exactly-full group, and the first
> chunk of the next one — deployed one per block, then all three called in a
> final block. The 257-chunk contract is the only one holding two stems, which is
> what makes a later deploy disturbing an earlier one visible. Each writes its own
> marker on the last block, which fails if any deploy corrupted the code of one
> deployed before it.
>
> Rebased onto current `projects/binary-trie`. The group edge is the only
> boundary left after #3310 deleted `CODE_OFFSET`, so the sizes moved onto it.

### Task 16: Diff the adversarial case list against the existing corpus

Nothing gets proposed upstream that the corpus already pins. The current corpus
is 18 `pbt_state` vector cases and 49 fixture test functions.

**Files:**
- Create: `docs/plans/notes/20260807-pbin-corpus-gap.md`

- [x] create `docs/plans/notes/` — it does not exist
- [x] list every case from Tasks 6, 7, 9 and 10 beside its nearest existing
      equivalent, checking at minimum `test_code_sharing.py`
      (`test_shared_code_survives_sibling_same_tx_selfdestruct`,
      `test_shared_designator_survives_peer_redelegation`,
      `test_contract_hashing_to_the_delegation_marker_executes_as_code`) and
      `test_code_chunking.py` (`test_delegated_eoa_executes_chunked_delegate`)
      (13 cases; the two Python suites `tests/binary_trie/test_embedding.py` and
      `test_state_pbt.py` carry the closest equivalents and decide most verdicts,
      so the diff is against all four corpus sources, not the fixtures alone)
- [x] mark each covered, partially covered, or absent, with the file and test
      name backing the verdict
- [x] confirm the expected shortlist: reclamation with no surviving holder, and
      a zero chunk alone in its group (**half confirmed** — only the zero chunk
      is absent; see below)
- [x] drop anything already covered — do not repackage an existing case
- [x] record the shortlist in this plan before opening anything

**Shortlist: one case, not two.**

*A zero chunk alone in its `tree_index` group* — 257 chunks with the last
all-zero, so group 1 exists in the code and places no leaf, leaving its stem
absent. It pins whether a group's stem follows the chunk *ids* the code reaches
or the leaves it actually places. Expressible as a flat `pbt_state` vector case
(one account, one code), so it needs no transaction. Its theme is absent chunks
at a group boundary — execution-specs#3305's theme — so Task 17 folds it into
that branch rather than opening a second PR. Nothing existing has the shape:
every group-1 case either keeps a leaf there (`test_absent_chunk_in_a_later_group_does_not_stall_removal`
at 258 chunks, `test_deleting_the_last_holder_drops_every_group` whose only hole
falls at chunk 223, vector `code_across_the_group_boundary`, and #3305's own
reframed case at 300 chunks) or has no chunk in group 1 at all
(`test_group_exact_code_fills_group_zero_and_nothing_more`).

*Reclamation with no surviving holder* is **dropped** — already covered, three
times over, by `test_state_pbt.py`'s `test_deleting_a_sole_holder_removes_its_short_code`,
`test_deleting_the_last_holder_removes_its_code` and
`test_deleting_the_last_holder_drops_every_group`.

⚠️ Those same three tests pin the reference *dropping* a pre-existing sole
holder's chunks, which erigon does not do — Task 9 established the drop branch
is unreachable for a commitment batch. On-chain the two agree (EIP-6780 deletes
only same-transaction creations, whose chunks were never inserted); they part on
a handcrafted diff. Recorded as a known deviation, not reopened.

⚠️ `TestPBinDelegationSetAndClearedInOneBatch` and
`TestPBinDelegationRepointedInOneBatch` have **no upstream form**: a reference
`BlockDiff` carries one already-merged post-state account per address, so an
intra-batch intermediate write has nowhere to live. They pin erigon's
update-merge path, not the embedding, and stay erigon-internal.

Full table with a verdict per case: `docs/plans/notes/20260807-pbin-corpus-gap.md`.

### Task 17: Contribute the absent cases back to the corpus

**Files:**
- Modify: `tests/binary_trie/vectors/dump_vectors.py` (in `~/org/wrk/espr`)
- Modify: `tests/binary_trie/vectors/README.md` (in `~/org/wrk/espr`)
- Modify: `tests/binary_trie/vectors/binary_trie_vectors.json` (in `~/org/wrk/espr`)

- [x] open one PR per coherent theme, not one per case (no new PR: the shortlist
      is one case and its theme is execution-specs#3305's, so it lands as a
      second commit on `tests/binary-trie-zero-chunk-vectors` — `1dd0d8aae`,
      `tests(binary-trie): vector case for a group that places no leaf`)
- [x] for a vector case, regenerate `binary_trie_vectors.json` and update the
      README case table in the same commit (`zero_chunk_alone_in_its_group`;
      the `source_commit` stamp is left at the upstream base `8d258bc`, which is
      what the branch's first commit already carries — the generator rewrites it
      to whatever HEAD is at generation time)
- [x] for a fixture case, fill it and confirm it passes on `projects/binary-trie`
      (not applicable — the shortlist holds no fixture case; it is one account
      and one code, so it needs no transaction)
- [x] cross-check every proposed root against erigon's engine before pushing
      (temporary vendor swap: `TestPBinConformancePBTState` ran 21/21 with the
      new subtest named in `-v` output, then the 18-case vendored file was
      restored. The harness drives each case through both the engine and the
      in-repo canonical-rebuild oracle, so both agree with the reference root
      `0xf752703833a8eef458061ccfdf536d4df1019397dea0fc59c05ce81dca600070`)
- [x] write each body dead short: problem paragraph first, no "Summary" heading,
      no Testing section, no AI mentions (drafted below — it supersedes Task 14's
      draft, since the case rides on that PR)
- [x] push to `fork` only. **Do NOT open the PR and do NOT post any comment.**
      Leave the branch pushed and report it for the human to open
      (**blocked**, see ⚠️ below — the commit is ready locally)

⚠️ Still not pushed, for the reason Tasks 14 and 15 recorded: `git push fork
tests/binary-trie-zero-chunk-vectors` is rejected non-fast-forward because the
branch was rebased, and force-pushing is off-limits under the standing rule.
The branch now sits at `1dd0d8aae` in `~/org/wrk/espr`, two commits on
`8d258bc`, ruff format/check, mypy and codespell clean, `uv run pytest
tests/binary_trie/ -q` green at 204 passed. Landing it needs one `git push
--force-with-lease fork tests/binary-trie-zero-chunk-vectors` from a human.

Leaf counts backing the new case, read off `embed_flat_state` rather than
asserted: 257 chunks with only the last zero commit 256 chunk leaves under a
single stem, sub-indices 0–255, and 258 leaves in the whole state. Group 1's
stem is absent entirely — the code reaches chunk id 256 and places nothing
there.

Draft body for execution-specs#3305, for a human to post — supersedes the Task
14 draft:

> `code_chunks_of_zero_bytes` is the only vector covering an absent chunk, and
> it covers the easy shape: 62 bytes of code, entirely zero, one stem. Nothing
> pins a hole inside a longer code, and nothing pins one at a group boundary,
> where absence and the `tree_index` / `sub_index` split of a chunk id have to
> compose — a client that mis-splits still commits the hole, under the wrong
> stem or the wrong sub-index. Content addressing sharpens the second shape:
> accounts running identical code share their chunk leaves, so a hole one of
> them places wrong is a hole in the leaves both read.
>
> ## Changes
>
> - `zero_chunk_across_the_group_boundary`: 300 chunks with 5, 255 and 256
>   zero — a hole inside group 0, one at its last sub-index, one at the first
>   sub-index of group 1. 297 chunk leaves under two stems.
> - `shared_bytecode_with_absent_chunks`: two accounts running that code. 301
>   leaves, not 598.
> - `zero_chunk_alone_in_its_group`: 257 chunks with only the last zero, so
>   group 1 holds the one chunk id the code reaches and places no leaf for it.
>   Its stem is absent entirely — a group is materialized by the leaves it
>   commits, not by the chunk ids the code spans. 256 chunk leaves under one
>   stem.

### Task 18: Verify acceptance criteria

- [x] `TestPBinConformancePBTState` is 18/18 and `TestPBinConformanceEmbedding`
      passes against vectors `58faeb0` (all 18 subtests named in `-v`, 0 fail;
      the vendored file's `source_commit` reads `58faeb09b95fd0222…`)
- [x] `grep -rn 'pbinCodeOffset\|pbinHeaderCodeChunks\|pbinHeaderCodeCapacity' .`
      returns nothing outside this plan (the plan file is the only match)
- [x] `grep -rn 'eip:[0-9]' execution/commitment/` returns nothing
- [x] every test function named in Tasks 4-11 exists and runs — confirm by name
      in `go test -v` output, not by an exit code (30 names cross-checked
      against the `=== RUN` set of both packages, each also matched to its
      `--- PASS`. Two further entries — `TestPBinChunkifyCode` and
      `TestPBinCellHash` — are `-run` regex prefixes in Task 4's gate rather
      than function names; their nine matched functions all ran)
- [x] the fuzz target runs 5 minutes clean (5m01s, 270,321 execs, 89 new
      interesting, no crash)
- [x] run the full suite: `go test ./execution/commitment/... ./rpc/jsonrpc/... -count=1`
      (green across all seven packages, after the baseline fix below)
- [x] run `make lint` repeatedly until clean, then `make erigon integration`
      (lint 0 issues on two consecutive runs; both binaries build)
- [x] confirm the erigon branch is committed and **not** pushed
      (`binary-trie-witness`, 87 commits ahead of main, no upstream configured
      and no matching ref on `origin`)

➕ The suite was **not** green on first run: `TestWitnessSizeBinVsHex` failed
against `rpc/jsonrpc/testdata/hex_witness_baseline.json`, a Task 5 leftover.
Task 5 re-sized the e2e chain's large contract onto the group boundary
(`31*(pbinCodeGroupChunks+8)` = 8184 B, from the header-split 4216 B) and
renamed three corpus shapes, but never regenerated the golden file. Regenerated
with `ERIGON_UPDATE_HEX_WITNESS_BASELINE=true`; the diff is exactly those five
lines. Every hex trie-shaped number — nodes, `stateBytes`, `headerBytes` — is
unchanged at every block, which is the invariant the baseline exists to guard
("the bin witness path must leave the hex one byte-identical"). Only `codeBytes`
moved, and only because the input contract itself grew: the hex MPT holds a
fixed-size code hash in the account leaf, so a larger contract cannot move its
state numbers.

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
