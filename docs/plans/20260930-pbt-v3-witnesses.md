# PBT witnesses on pbt-v3

## Overview

- `debug_executionWitness` on `awskii/pbt-v3` refuses every binary block with `ErrBinCommitmentUnsupported`. On hex+bin
  datadirs it also refuses hex blocks, because the witness plumbing requires `*commitment.HexPatriciaHashed` and the
  dual hex arm is the v3 hex trie.
- This plan:
  - merges `awskii/v3-commitment`, which already builds hex witnesses on the v3 engine;
  - serves hex witnesses on hex+bin datadirs;
  - adds a PBT witness built from pbt-v3's 16-cell rows, with erigon's own stateless verifier;
  - adds a request parameter that selects the MPT or the PBT witness.
- The PBT witness uses geth-pbt's blob format (leaf, branch and group blobs keyed by path). Its node set follows
  erigon's rules: proofs for the state erigon's execution loads and writes, never a copy of another client's access
  pattern.
- `awskii/v3-commitment` may land on main before this work. The branch merges it (never cherry-picks) and keeps its own
  edits to v3-owned files limited to integration and test adaptations, so the later squash landing stays mechanical.

## Context (from discovery)

- Branches and trees:
  - `awskii/pbt-v3` at `8a36bda94a9` (worktree `~/org/wrk/wt/pbt-v3`); last merged `awskii/v3-commitment` at `2292cf7f287`.
  - `awskii/v3-commitment` at `9fdf5a9777b` (worktree `~/org/wrk/wt/v3-commitment`) adds hex witnesses on the v3
    engine: `b25831056ad`, `32b2423a8b1`, `04065c255e2`, `953e1e72d61`, `0f884eebdf7`, `b4254f260bc`, `40398e20778`,
    commitment `GetAsOf` fixes `f3adfaf779d`, `c4831b73d38`, `327048e0362`, and a v3 prefetcher BAL mode
    (`23185423b46`, `9fdf5a9777b`). `awskii/witness-inflight` `addc2797fb8` is not merged into it yet.
  - `binary-trie` at `f2df0cc0bf4` (worktree `~/org/wrk/wt/binary-trie`) held a pair-record bin witness stack that
    pbt-v3 deleted: `execution/commitment/pbin_witness*.go`, `execution/state/triedb_state.go`,
    `rpc/jsonrpc/pbin_witness_stateless.go` and the `rpc/jsonrpc/pbin_witness_*_test.go` files.
  - geth-pbt (`CPerezz/go-ethereum`, local clone `~/org/wrk/go-ethereum`, ref `origin/pbt` `793dedb`) is the reference for
    the blob format only: `trie/bintrie/encoding.go`, `node.go`, `bits.go`, `core/stateless/encoding.go`.
- Witness plumbing after the merge:
  - `commitmentdb` finds a witness-capable trie through the unexported interface `witnessTrie` (`WitnessesByHash`) in
    `execution/commitment/commitmentdb/commitment_context.go`;
  - `trie.WitnessNodesForKeysByHash` (RLP) prunes hex witnesses;
  - `rpc/jsonrpc/debug_execution_witness.go` holds `RecordingState`, `accessedState`, `buildWitnessResult`,
    `resolveWitnessMode`, `serveFromWitnessCache` and `replayBlockOverWitness`;
  - `rpc/jsonrpc/commitment_reconstruction.go` reads commitment history as of a block;
  - `rpc/jsonrpc/witness_cache*.go` holds the witness cache;
  - `rpc/jsonrpc/witness_v3_parity_test.go` (`TestWitnessesMatchHPHUnderCommitmentV3`) arrives with the merge.
- PBT engine: `execution/commitment/v3/pbt` (`fold.go` with `foldRange`/`foldChild`, `unfold.go`, `bucket.go`, `record.go`
  with `DecodeRecord`, `verify.go`), engine-free rules in `execution/commitment/eip8297` (`hash.go` with `LeafPreimage`,
  `BranchPreimage`, `AppendBitPrefix`, `HashBytes`; `keys.go`; `code.go`; `reference.go`).
- Dual mode:
  - `kv.CommitmentDomain` is hex and `kv.CommitmentBinDomain` is bin on hex+bin datadirs; a bin-only datadir keeps bin
    rows in `kv.CommitmentDomain`;
  - `chain.Config.IsBinaryTrie(time)` decides the canonical trie of a block;
  - `rawdb.ReadShadowStateRoot` holds the shadow root of each block;
  - a domain that stopped advancing is visible through `rawdb.ReadCommitmentDomainStopped`,
    `Aggregator.IsDomainFrozen` and `ErigonDBSettings.FrozenAt` (`frozen_at_txnum` in `erigondb.toml`).
- A trial merge of `9fdf5a9777b` into `8a36bda94a9` conflicts in 9 files:
  - `db/state/execctx/commitment_put_test.go`, `db/state/execctx/options.go`;
  - `execution/commitment/commitmentdb/commitment_context.go`;
  - `execution/commitment/v3/trie_test.go`, `execution/commitment/v3/wipe_test.go`,
    `execution/commitment/zz_statehash_test.go`;
  - `execution/stagedsync/branch_prefetch.go`, `execution/stagedsync/branch_prefetch_test.go`,
    `execution/stagedsync/committer.go`.

  The merge also keeps, without a conflict, pbt-v3's early return for `VariantCommitmentV3` in `buildWitnessResult`,
  which breaks v3's parity test.

## Development Approach

- **testing approach**: TDD.
  - Every behaviour change starts with a test that fails for the missing behaviour: read the assertion that fires, not
    the exit code.
  - For a new API, first add its signature, or a stub returning a sentinel error, so the red is the named assertion and
    not a compile error.
  - Behaviour that the merge itself enables gets guard tests. Each guard is mutation-checked: copy the file to the
    scratchpad, break the guarded code, run the whole package, name the `file:line` that went red, copy the file back.
    Never `git checkout` or `git restore` a file holding uncommitted work.
- complete each task fully before moving to the next; small, focused changes.
- **CRITICAL: every task MUST include new/updated tests** for code changes in that task.
- **CRITICAL: all tests must pass before starting next task** - no exceptions.
- **CRITICAL: update this plan file when scope changes during implementation.**
- conventions:
  - no code comments;
  - new files carry the 2026 license header;
  - identifiers in package `commitment` carry the `pbin`/`PBin` prefix;
  - Go names without Java-style suffixes (erigon uses `*Func` for function types);
  - benchmarks live in `*_bench_test.go`.
- Tests restore every process-global they change in `t.Cleanup` and never call `t.Parallel`:
  - the PBT selection (`statecfg.ExperimentalBinCommitment`, `statecfg.BinCommitmentHash`,
    `commitment.SetPBinHashSuite`);
  - `statecfg.ExperimentalCommitmentV3` and `statecfg.Schema`;
  - `statecfg.EnableHistoricalCommitment`;
  - `dbg.AssertEnabled`.

  PBT witness tests select the BLAKE3 suite explicitly.
- commits:
  - at the end of each task: subject line only, at most 120 characters, erigon package prefix, no body, no trailer, no
    attribution;
  - the merge commit in task 1 uses git's merge subject and names the merged sha;
  - before each commit: `make lint` until clean (it is non-deterministic) and `make erigon integration`.
- all tasks are committed on `awskii/pbt-v3-witness` (worktree `~/org/wrk/wt/pbt-v3-witness`), branched from
  `awskii/pbt-v3` with this plan. Task 1's merge commit is where `awskii/pbt-v3-snapshot` starts.

## Testing Strategy

- **unit tests**: required for every task.
- **integration tests**: RPC tests in `rpc/jsonrpc` over test chains built with the existing test helpers; dual-mode
  tests follow `execution/tests/pbt_dual_commitment_test.go`.
- no UI or browser e2e suites exist in this repository.
- package runs: `go test ./execution/commitment/... ./db/state/... ./execution/stagedsync/... ./rpc/jsonrpc/...`; full
  gate `make lint && make erigon integration`.

## Progress Tracking

- mark completed items with `[x]` immediately when done.
- add newly discovered tasks with ➕ prefix.
- document issues/blockers with ⚠️ prefix.
- update the plan if implementation deviates from original scope.

## Solution Overview

1. Merge `awskii/v3-commitment` and serve hex witnesses on hex+bin datadirs for every block whose hex commitment
   exists. `eth_getWitness` and `eth_getProof` stay MPT-only and serve blocks where hex is canonical.
2. `debug_executionWitness(block, mode?, trie?)`. `trie` is `"mpt"` or `"pbt"`; omitted, it is the trie canonical at the
   requested block. `mode` (legacy, canonical) applies to mpt only. One rule anchors every witness: each root comes
   from the header when the requested trie is canonical at that root's block, otherwise from the recorded shadow root.
3. An engine-free node model in `execution/commitment/eip8297/witness` holds geth-pbt's stored-node model and blob
   format and implements EIP-8297 operations on a lazily resolved tree. One driver applies a block's reads and writes
   in one fixed order. The builder resolves nodes from pbt-v3 rows; the verifier resolves them from witness blobs.
   Because both run the same code in the same order, the witness is exactly the node set the verifier loads.
4. The builder in `execution/commitment/v3/pbt` resolves any node position from 16-cell rows through the engine's own
   fold code. The verifier replays the block over the witness with erigon's execution and checks the post-root.

## Technical Details

### RPC contract

| `trie` | `mode` | result |
|---|---|---|
| omitted | omitted / legacy / canonical | the trie canonical at the block (`IsBinaryTrie(block.Time)`); `mode` with a pbt default is an error |
| `"mpt"` | omitted / legacy / canonical | MPT witness from the hex domain |
| `"pbt"` | omitted | PBT witness from the bin domain |
| `"pbt"` | any value | error: mode applies to the MPT witness only |
| other | any | invalid params |

Anchors for trie X at block B with parent P:

| root | taken from the header when | header value | otherwise |
|---|---|---|---|
| pre-state | X is canonical at P | `header(P).Root` | `ReadShadowStateRoot(P)` |
| self-check post-state | X is canonical at B | `header(B).Root` | `ReadShadowStateRoot(B)` |

At the first bin block, for example, the pbt pre-state root is the shadow root of P, while the pbt self-check root is
the header root of B.

Trie X is available at B when every condition below holds; otherwise the RPC returns an error naming the trie and
the failed condition, and never falls back to the other trie:
- the datadir holds X's domain (hex-only, bin-only or hex+bin);
- X's commitment covers P: X was running at P, is not frozen before P (`Aggregator.IsDomainFrozen`,
  `ErigonDBSettings.FrozenAt`), and was not stopped before P (`rawdb.ReadCommitmentDomainStopped` plus the domain's
  progress);
- X's commitment history is retained from P (`HistoryStartFrom(domain)`);
- every shadow root the anchor table needs exists.

A standalone rpcdaemon reads the same state through its database and settings; where a getter is not reachable from
`rpc/`, task 4 exposes it and extends its Files block.

The witness cache holds the default-trie witness of each block. A request for the non-default trie is built on
demand; a cache-only node serves the default trie only and returns a distinct error for the other.

### PBT response

- `keys`: node paths; `state`: node blobs; parallel arrays sorted by the encoded path bytes.
- `codes`: full bytecode of every non-empty code the execution read (content-keyed set, including code read through
  a code-size access, delegation designators and code deployed earlier in the block), sorted by bytes. The set is built
  in the pbt adapter only; MPT `codes` output is unchanged.
- `headers`: erigon's existing RLP encoding.
- an empty witness has `keys` null and `state`, `codes` as empty arrays.

### Blob format (geth-pbt `trie/bintrie`)

- leaf: `0x00 ‖ stem ‖ sub ‖ value(32)` = `eip8297.LeafPreimage(key = stem‖sub, value)`; the key is 34 or 66 bytes.
- branch: `0x01 ‖ u16BE prefix bit length ‖ packed prefix bits ‖ left(32) ‖ right(32)` = `eip8297.BranchPreimage`.
- group record: `0x02 ‖ u16BE pos ‖ stem length(1) ‖ stem ‖ bitmap(32) ‖ values(32·k)`, k ≥ 2, bit `sub` set when
  `bitmap[sub>>3] & (1<<(7-sub&7))`, values full 32-byte words in ascending sub order. A stem holding one value is a
  leaf blob. Decoding rejects a stem length outside the allowed set and a stem that fails geth's `validateStem` rules.
- hashes: leaf and branch blobs hash with `HashBytes` over the blob. A group hashes by a positional fold: its top
  branch folds the remaining stem bits `[pos, stemBits)` followed by the common leading sub bits, lower branches fold
  the remaining shared sub bits, and leaves keep full keys.
- the node hash suite and key derivation both use BLAKE3.
- paths: the root path is empty bytes (`"0x"` in JSON); every other path is `AppendBitPrefix(walk)`.
- witness values are full 32-byte words; erigon's compact leaf values and database path encodings never appear.

### Node model operations and driver

- read: walk to the stem or to the divergence point; the walked nodes prove presence or absence.
- insert and update:
  - header stem subs 0/1/2 hold basic data, code hash and delegation;
  - storage slots 0-63 live at header subs 64-127;
  - slots from 64 up live in overflow storage stems.
- delete with collapse: an emptied branch is replaced by its survivor, resolved at its old path; a surviving branch
  absorbs the parent prefix and edge; a surviving group re-hashes at its new position.
- account deletion: remove the header stem, then the overflow storage prefix by walking to the cut point only (the
  subtree below the cut is dropped without resolving it).
- code: a deploy writes the non-zero chunks of the new code (content-addressed; never removed).
- nodes created during the block never enter the witness; only blobs resolved from the pre-state do.
- driver order, identical in builder and verifier:
  1. reads (a set; order does not change the resolved nodes);
  2. storage pass: addresses ascending, slots ascending, a zero value deletes;
  3. account updates ascending, including code writes on deploy;
  4. account deletions ascending.

### Row resolver

- a branch position inside a row comes from `foldRange`/`foldChild`, which rebase a stored `L‖R‖prefix` to the
  requested position; a position below a row follows the containing window into the child row.
- fixed bucket root records are erigon descriptors, not nodes: their extension is relative to bit 264 and combines with
  the position consumed above them.
- a group collects every value under its stem: at most 17 row reads (one row at the stem end plus up to 16 below),
  plus suffix-bearing leaves sitting above the stem.
- a leaf comes from `DecodeRecord` (full key, 32-byte value).
- rows are read as of the parent block through the reconstruction view: `CommitmentBinDomain` on hex+bin,
  `CommitmentDomain` on bin-only.

### Verifier

- resolves nodes from `keys`/`state` by path; each blob must hash to its parent's pointer (groups folded at their
  stored position).
- reads code from `codes` plus in-block overlays.
- replays the block through `replayBlockOverWitness` with overlays and account-lifecycle bookkeeping ported from
  `binary-trie`'s `rpc/jsonrpc/pbin_witness_stateless.go`.
- erigon's synthetic system-caller touch is suppressed in replay; a genuine user access to that address still needs its
  proof.
- a CREATE into an existing account that holds storage wipes it, as erigon's execution does; a missing storage proof
  there is an error, never "empty".
- applies the block's writes through the driver and compares the root with the self-check anchor; a mismatch or a
  missing blob is an RPC error and nothing is served.

### Merge resolutions (task 1)

- `commitment_context.go`:
  - collapse tracer: keep pbt-v3's `tracer != nil` panic for the bin variant, then v3's interface dispatch;
  - `BranchChildCount`: v3's read closure (staged-unwind check, per-trie dispatch, HPH fallback) reading
    `sdc.CommitmentDomain()`, after pbt-v3's bin rejection.
- `db/state/execctx/options.go`: v3's `VariantParallelHexPatricia` demotion plus pbt-v3's `WithHexCommitmentOnly`.
- `execution/stagedsync/committer.go`: one conditional `ResetBlockFlags` (not mid-block), placed after both feeds are
  prepared; keep `asOfReader.txNum = t.lastTxNum + 1` before `BinFeed()`. The trial merge keeps pbt-v3's unconditional
  reset, which would undo v3's mid-block fix. Re-check the file against v3's head at merge time; it changed again after
  `d6150aa2315`.
- `execution/stagedsync/branch_prefetch.go` and its test: keep pbt-v3's per-domain list and bin flags (`domains`,
  `bin`) and v3's BAL mode and counters (`hits`, `misses`, `dropped`, `drained`); imports are the union.
- `db/state/execctx/commitment_put_test.go`: v3's `runner.CloneDeltas` with pbt-v3's explicit domain arguments.
- `execution/commitment/v3/trie_test.go`: v3's folded `TestTrieAPI` with pbt-v3's initializer error check.
- `execution/commitment/v3/wipe_test.go`, `execution/commitment/zz_statehash_test.go`: accept v3's deletions. Move
  pbt-v3's direct `DeleteUpdate` setup (from `TestDeleteThenWriteClearsWipe`) into v3's self-destruct case in
  `phase_a_test.go`; otherwise the test package cycles through `execution/state -> execctx -> commitmentdb -> v3`.
- `rpc/jsonrpc/debug_execution_witness.go`: delete pbt-v3's `VariantCommitmentV3` early return in `buildWitnessResult`
  (no textual conflict; without it v3's `TestWitnessesMatchHPHUnderCommitmentV3` fails).
- `rpc/jsonrpc/debug_execution_witness_bin_test.go`: `TestPBinDualV3HexExecutionWitnessRefuses` asserts the refusal
  text of the HPH-only path; turn it into a test that the pre-fork hex witness is served.
- v3's post-state values in `touchNonZeroKeys` and its `GetAsOf` fixes merge without conflicts and must survive.
- any further conflict at a newer v3 head follows the same rule: v3 owns hex-engine and witness behaviour; pbt-v3
  keeps only bin integration and test adaptations, re-checked at every later merge.

## What Goes Where

- **Implementation Steps** (`[ ]` checkboxes): code, tests and documentation in this repository.
- **Post-Completion** (no checkboxes): devnet runs on remote hosts, later merges of `awskii/v3-commitment`, its squash
  landing on main, reports to other client teams.

## Implementation Steps

### Task 1: Merge awskii/v3-commitment

**Files:**
- Modify: `execution/commitment/commitmentdb/commitment_context.go`
- Modify: `db/state/execctx/options.go`
- Modify: `execution/stagedsync/committer.go`
- Modify: `execution/stagedsync/branch_prefetch.go`, `execution/stagedsync/branch_prefetch_test.go`
- Modify: `rpc/jsonrpc/debug_execution_witness.go`, `rpc/jsonrpc/debug_execution_witness_bin_test.go`
- Modify: `db/state/execctx/commitment_put_test.go`
- Modify: `execution/commitment/v3/trie_test.go`, `execution/commitment/v3/phase_a_test.go`
- Delete: `execution/commitment/v3/wipe_test.go`, `execution/commitment/zz_statehash_test.go`

- [x] merge `awskii/v3-commitment` at its head at plan start (`9fdf5a9777b`) into `awskii/pbt-v3-witness`; record the
      merged sha in the plan
- [x] resolve every conflicted file as listed under Merge resolutions, re-checking `committer.go` against the merged head
- [x] delete the `VariantCommitmentV3` early return in `buildWitnessResult` and turn
      `TestPBinDualV3HexExecutionWitnessRefuses` into a served-witness test
- [x] run v3's witness and history suites on the merged tree:
      - `execution/commitment/v3`;
      - `db/state` (`GetAsOf`);
      - `rpc/jsonrpc`: `TestWitnessesMatchHPHUnderCommitmentV3` and the witness cache tests
- [x] run the step-boundary and dual committer tests:
      - `TestHandleMessage_BlockEndStateFollowsMidBlockStepCheckpoint`;
      - `TestDualCompletionStopsShadowOnReplayError`;
      - `TestCommitmentCalculatorDualFold`;
      - `TestStoppedShadowSurvivesCalculatorReplacement`.

      Mutation-check the `ResetBlockFlags` resolution: an unconditional reset turns the mid-block test red.
- [x] run the affected packages, `make lint` and `make erigon integration`; commit the merge - must pass before task 2
- [x] ➕ fix BAL prefetch metadata for binary domains, update the queued-item assertion, and cover storage/code rows
      through `handleBlockRequest`; mutation-check the new fields

### Task 2: Guards for hex witnesses on hex+bin datadirs

**Files:**
- Modify: `rpc/jsonrpc/witness_v3_parity_test.go`
- Create: `rpc/jsonrpc/pbt_hex_witness_dual_test.go`
- Modify: `rpc/jsonrpc/eth_call.go`, `rpc/rpchelper/commitment.go` (only if a guard below exposes a gap)

- [x] extend `witness_v3_parity_test.go` with a hex+bin arm:
      - the dual globals and `statecfg.EnableHistoricalCommitment` are set and restored in `t.Cleanup`;
      - the bin domain is active, with a dual genesis as in `execution/tests/pbt_dual_commitment_test.go`;
      - legacy, canonical and `eth_getWitness` output is byte-identical to the hex-only arm for pre-fork blocks.
- [x] add `eth_getProof` parity for a pre-fork block on hex+bin
- [x] port from `binary-trie`:
      - `TestPBinGetWitnessRefusesBin` and `TestPBinHexOnlyCallersStillRefuse` (`pbin_witness_reachable_test.go`);
      - `TestPBinDualPostFlipProofAndWitnessRefuse` and `TestPBinFrozenHexHistoricalWitnessAndProof`
        (`pbin_witness_dual_test.go`).

      These cover post-fork and bin-only refusals of `eth_getWitness`/`eth_getProof`, and historical pre-fork reads
      after the fork and after a hex freeze.
- [x] add a test for the pruned hex commitment history error
- [x] mutation-check each guard: re-inserting the early return, and bypassing the canonical-hex check in `eth_call.go`,
      each turn a named assertion red
- [x] run tests - must pass before task 3

### Task 3: trie parameter and cache routing

**Files:**
- Modify: `rpc/jsonrpc/debug_api.go`
- Modify: `rpc/jsonrpc/debug_execution_witness.go`
- Modify: `rpc/jsonrpc/witness_cache.go`
- Create: `rpc/jsonrpc/debug_execution_witness_trie_test.go`

- [x] add the optional `trie *string` parameter to `ExecutionWitness` in the `DebugAPI` interface and the
      implementation; pbt returns a not-yet-served sentinel error until task 11
- [x] write the table test for the RPC contract table (defaults on both sides of the fork, explicit values, pbt with
      `mode`, unknown values); confirm it fails at its first resolution assertion
- [x] implement the resolution: default through `IsBinaryTrie(block.Time)`; reject unknown values and `mode` together
      with pbt
- [x] route the cache: `serveFromWitnessCache` serves only the default trie; a cache-only node returns a distinct error
      for the other
- [x] write tests for cache hit and miss by trie and for the cache-only refusal
- [x] run tests - must pass before task 4

### Task 4: MPT anchors and availability

**Files:**
- Modify: `rpc/jsonrpc/debug_execution_witness.go`
- Modify: `rpc/jsonrpc/debug_execution_witness_trie_test.go`

- [x] write tests:
      - mpt for a post-fork block inside the transition window, anchored at shadow roots;
      - for both a hex stop and a hex freeze, the last parent that is served and the first that is refused;
      - mpt on a bin-only datadir;
      - a missing shadow root and an independently opened rpcdaemon database.

      Confirm they fail at the anchor or availability assertion.
- [x] replace the `binTrie && !IsBinaryTrie(parent)` special case with the anchor table
- [x] implement the availability rules through `Aggregator.IsDomainFrozen`, `ErigonDBSettings.FrozenAt`,
      `rawdb.ReadCommitmentDomainStopped`, domain progress and `HistoryStartFrom`, with errors naming the failed
      condition. Expose any getter not reachable from `rpc/` and list its file here
- [x] write a test for a standalone rpcdaemon reading the same state
- [x] run tests - must pass before task 5

### Task 5: Node model blob format and hashing

**Files:**
- Create: `execution/commitment/eip8297/witness/blob.go`
- Create: `execution/commitment/eip8297/witness/blob_test.go`
- Create: `execution/commitment/eip8297/witness/testdata/geth_blobs.json`
- Create: `execution/commitment/eip8297/witness/testdata/README.md`

- [x] generate format vectors (not witness fixtures) with a program in a scratch module outside this repository, pinned
      to geth-pbt `origin/pbt` `793dedb`: blobs and hashes of hand-built nodes (leaf; branch with and without prefix;
      groups at position 0 and above; a one-value stem); commit only the JSON and a README naming the ref and how the
      vectors were made
- [x] add the codec signatures with stubs; write tests that decode each vector and recompute its hash; confirm they fail
      at the hash comparison
- [x] implement leaf and branch blobs through `eip8297.LeafPreimage` / `BranchPreimage`, and group record encode and
      decode. Decoding rejects:
      - k < 2;
      - a length mismatch;
      - a bitmap/value count mismatch;
      - a position beyond the stem;
      - a stem length outside the allowed set;
      - a stem failing geth's `validateStem` rules
- [x] implement the positional group fold and path encoding (empty root path, `AppendBitPrefix` otherwise, ordering by
      encoded bytes)
- [x] write tests:
      - round trips and rejects;
      - group hashes equal the root of an `eip8297` reference tree built from the same leaves with the consumed prefix
        removed;
      - `go list -deps` confirms the package imports no engine package
- [x] run tests - must pass before task 6

### Task 6: Node model operations and driver

**Files:**
- Create: `execution/commitment/eip8297/witness/tree.go`
- Create: `execution/commitment/eip8297/witness/driver.go`
- Create: `execution/commitment/eip8297/witness/tree_test.go`
- Create: `execution/commitment/eip8297/witness/driver_test.go`

- [x] add the tree and driver signatures with stubs. Write a test that a tree built by inserting random leaf sets hashes
      to the `eip8297` reference root; the sets cover all zones, header and overflow storage, code stems and groups of
      1 to 256 values. Confirm it fails at the root comparison
- [x] implement the tree with a `ResolveFunc` (path to blob) that loads nodes lazily, checks each blob against its
      parent's pointer, and records each resolved path once; the root is resolved for a non-empty tree; nodes created
      during the block are never recorded
- [x] implement read, insert/update, delete with collapse, account deletion to the cut point, and code chunk writes
- [x] implement the driver in the fixed order (reads, storage pass, account updates, account deletions) returning the
      post-root and the resolved set
- [x] write tests on the post-root:
      - collapse cascades (two deletions under one branch);
      - absent reads;
      - header slots against overflow slots;
      - account deletion with storage in both places;
      - delegation set and clear;
      - deploys sharing code.

      After the driver runs, the root equals the reference root of the post-state leaves.
- [x] write tests on the resolved set:
      - an account deletion with overflow storage resolves only the nodes down to the cut point;
      - a collapse whose survivor was inserted earlier in the same block resolves nothing new
- [x] run tests - must pass before task 7

### Task 7: Row resolver from 16-cell rows

**Files:**
- Create: `execution/commitment/v3/pbt/witness_nodes.go`
- Create: `execution/commitment/v3/pbt/witness_nodes_test.go`

- [x] add the resolver signature with a stub. Write the rows-against-model oracle:
      - trees are built by the engine from random leaf sets, covering both storage zones, bucket forms, suffix leaves and
        groups of 1 to 256 values; `recordFuzzSeeds` holds malformed record bodies and is not a source;
      - for every node path of the model built from the same leaves, the row resolver returns the identical blob, and
        the roots are equal.

      Confirm it fails at the blob comparison.
- [x] implement node resolution at a path: branches through `foldRange`/`foldChild` with prefix rebasing, bucket
      descriptors resolved through, groups collected from rows, leaves through `DecodeRecord`, empty positions
- [x] write a test that rows are read as of the parent block: after a later block rewrites a row, the resolver still
      returns the parent-block blob; confirm it fails before the history read exists
- [x] read rows through a `PatriciaContext` over bin commitment history as of the parent block
- [x] write error tests: missing row, corrupt row, a group whose depth disagrees with its path
- [x] run tests - must pass before task 8
- [x] ➕ rework the resolver to read path-local rows, validate stored pointers, resolve bucket descriptors, reject
      impossible group probes early, and cover systematic shapes, corruption, and root/bucket read bounds
- [x] ➕ repair overflow-row routing and terminal-group probes, anchor the global root, reject missing bucket
      descriptors, and cover overflow shapes, descriptor corruption, and group-root read bounds

### Task 8: PBT witness builder and dispatch

**Files:**
- Create: `execution/commitment/v3/pbt/witness.go`
- Create: `execution/commitment/v3/pbt/witness_test.go`
- Create: `execution/commitment/commitmentdb/pbt_witness.go`
- Create: `execution/commitment/commitmentdb/pbt_witness_test.go`

- [x] add `(*Trie).Witness(ctx, input)` with a stub; write a test that the builder's output for a small block equals the
      node set the model resolves when driven over the same pre-state; confirm it fails at the set comparison
- [x] implement `Witness`, running the driver over the row resolver and returning paths and blobs sorted by path plus the
      post-root
- [x] dispatch to it from `commitmentdb` in `pbt_witness.go`, next to v3's `witnessTrie`, for the bin variant only
- [x] write tests for the dispatch on bin-only and hex+bin datadirs and for the refusal on hex-only
- [x] run tests - must pass before task 9

### Task 9: Recorder provenance and the pbt input adapter

**Files:**
- Modify: `rpc/jsonrpc/debug_execution_witness.go`
- Create: `rpc/jsonrpc/pbt_witness_input.go`
- Create: `rpc/jsonrpc/pbt_witness_input_test.go`

- [x] add the adapter signature with a stub. Write tests:
      - a read served by the in-block overlay is not a pre-state load;
      - a read inside a reverted call is;
      - two versions of one address's code both reach the pbt `codes` set.

      Confirm they fail at those assertions.
- [x] record provenance in `RecordingState` for account and storage reads (pre-state reader or overlay); keep reads in
      reverted calls
- [x] build the content-keyed `codes` set in the pbt adapter only:
      - full code for code-size reads;
      - modified code the block never read is left out;
      - `AccessedCode` and MPT `codes` stay as they are.
- [x] implement the rest of the adapter: reads, net writes against pre-block values, account deletions, code deploys,
      delegation set and clear
- [x] write tests:
      - the adapter over transfers, storage deletes, deploys, self-destruct in the creation transaction and delegation
        changes;
      - MPT output unchanged, through `TestWitnessesMatchHPHUnderCommitmentV3` and the task 2 parity arm
- [x] run tests - must pass before task 10

### Task 10: Stateless verifier

**Files:**
- Create: `rpc/jsonrpc/pbt_witness_stateless.go`
- Create: `rpc/jsonrpc/pbt_witness_stateless_test.go`

- [x] add the verifier signature with a stub; write tests: a correct witness reproduces the post-root; a witness missing
      one needed blob fails; confirm they fail at the root comparison and at the missing-blob error
- [x] implement the witness resolver (path to blob from `keys`/`state`, hash-checked) and code lookup from `codes` plus
      in-block overlays
- [x] port the overlays and account-lifecycle bookkeeping from `binary-trie`'s `rpc/jsonrpc/pbin_witness_stateless.go`:
      - replay through `replayBlockOverWitness`;
      - suppress the synthetic system-caller touch;
      - wipe storage on a CREATE into an existing account that holds storage, returning an error when its storage proof
        is missing.
- [x] compare the post-root with the self-check anchor; a mismatch or a missing blob is an error
- [x] port the behaviours of `binary-trie`'s `pbin_witness_stateless_test.go` to the new format:
      - genuine and synthetic system-address access;
      - CREATE over storage;
      - delete and recreate in one block;
      - `TestPBinWitnessStatelessHasStorage`;
      - `TestPBinWitnessStatelessMissingNodeErrors`
- [x] run tests - must pass before task 11

### Task 11: Serve trie=pbt

**Files:**
- Modify: `rpc/jsonrpc/debug_execution_witness.go`
- Modify: `rpc/jsonrpc/witness_cache_builder.go`
- Modify: `execution/commitment/commitmentdb/commitment_context.go`
- Modify: `rpc/jsonrpc/debug_execution_witness_trie_test.go`
- Modify: `rpc/jsonrpc/debug_execution_witness_bin_test.go`
- Modify: `rpc/jsonrpc/witness_cache_builder_test.go`

- [x] write tests: a pbt witness for a block on a bin-only datadir, for a pre-fork block on hex+bin (shadow anchors),
      and for the first bin block under both tries; confirm they fail on the not-yet-served sentinel
- [x] build the pbt path:
      - bin domain selection and the parent history check;
      - the anchors;
      - the hex environment gate and the empty-access early return bypassed;
      - the root blob always present.
- [x] shape the response (parallel `keys`/`state` sorted by path, content-keyed `codes`, RLP `headers`, empty-witness
      form) and run the verifier on every pbt witness before serving it
- [x] apply the availability rules to pbt; the eager cache builder builds the default trie (pbt after the fork)
- [x] write tests:
      - pbt refusals: hex-only datadir, bin shadow not running at the parent, pruned bin history, missing shadow root;
      - cache behaviour after the fork
- [x] run tests - must pass before task 12

### Task 12: End-to-end witness coverage

**Files:**
- Create: `rpc/jsonrpc/pbt_witness_e2e_test.go`
- Create: `rpc/jsonrpc/pbt_witness_wipe_test.go`
- Create: `rpc/jsonrpc/pbt_witness_dual_test.go`
- Create: `rpc/jsonrpc/pbt_witness_phases_test.go`
- Create: `rpc/jsonrpc/pbt_witness_ported_test.go`

- [x] every block of test chains verifies, and dropping any single blob makes verification fail. The chains cover:
      transfers to new accounts, storage writes and deletes including cascades, account deletion with storage,
      EIP-161 touch deletion, self-destruct in the creation transaction, deploys with shared code, EIP-7702 delegation
      set and clear, system calls, withdrawals, `BLOCKHASH`, and reverted calls
- [x] port the behaviours of `binary-trie`'s witness tests, without pair-node, preimage-key or code-chunk-reader
      assumptions:
      - `pbin_witness_e2e_test.go`, `pbin_witness_wipe_test.go`, `pbin_witness_phases_test.go`;
      - `pbin_witness_deploy_test.go` (`TestPBinWitnessConsecutiveDeploys`);
      - `pbin_witness_dual_test.go` (`TestPBinDualExecutionWitness`, `TestPBinHeadCaptureWithoutCommitmentHistory`)
- [x] corrupt cases: a blob that does not hash to its pointer, a blob under the wrong path, missing code, group depth
      disagreeing with its path, the empty root
- [x] dual matrix: bin-only; hex+bin before, at and after activation; canonical and shadow anchors; retained and pruned
      history; head capture; blocks that touch no state
- [x] run tests - must pass before task 13
- [x] ➕ enforce exact node-set consumption in the verifier, remove builder-only pre-state code-chunk proofs, and cover persisted empty accounts with header and overflow storage
- [x] ➕ authenticate codeless accounts through their code-hash leaf, compare builder post-roots with block anchors, cover delegation clearing, and delete zero-valued BASIC_DATA leaves

### Task 13: Witness documentation

**Files:**
- Create: `docs/pbt-witness.md`

- [x] document the RPC parameters, anchors and availability rules, the blob format (with the geth-pbt ref it was taken
      from), the node-set rules and the verifier
- [x] list where erigon's witness differs from geth-pbt's and why:
      - geth's prefetcher dedups PBT reads by slot, not owner;
      - geth's witness carries a deleted account's whole storage subtree;
      - CREATE into an account that holds storage wipes it in erigon and keeps it in geth.
- [x] cite code by name, never by line number
- [x] run `make lint` - must pass before task 14

### Task 14: Verify acceptance criteria

**Files:**
- none (verification only)

- [ ] every requirement in the Overview is implemented
- [ ] edge cases in the RPC contract, anchor and availability tables are covered by tests
- [ ] run the full suites: `go test ./execution/commitment/... ./db/state/... ./execution/stagedsync/... ./rpc/jsonrpc/...`
- [ ] run `make lint` until clean and `make erigon integration`
- [ ] mutation-check the key guards; each must turn a named test red when reverted:
      - the anchor rule;
      - the collapse survivor resolution;
      - the provenance filter;
      - resolution of an account deletion down to the cut point.

### Task 15: [Final] Update documentation

**Files:**
- Modify: `docs/pbin-dual-commitment.md`
- Modify: `CLAUDE.md`

- [ ] replace the refusal statements in `docs/pbin-dual-commitment.md` (witnesses on binary and v3-hex blocks) with the
      served behaviour, citing code by name
- [ ] update `CLAUDE.md` with the PBT witness entry points if new conventions were introduced
- [ ] move this plan to `docs/plans/completed/`

## Post-Completion

*Items requiring manual intervention or external systems - no checkboxes, informational only*

**Manual verification**:
- run an EIP-8347 migration lap on a remote devnet host with witness requests on both tries before, at and after the
  fork, with `--prune.experimental.include-commitment-history` set on the erigon participants.
- measure witness build time on mainnet-sized PBT state on a remote host.

**External system updates**:
- merge `awskii/v3-commitment` again whenever it moves (including `awskii/witness-inflight` once it lands there). When it
  squash-lands on main: prove the squash commit's tree equals the v3 head this branch merged, `git merge -s ours
  <squash>`, then merge main normally.
- the owner decides whether to report geth-pbt's slot-dedup and deleted-subtree behaviours to `CPerezz/go-ethereum`.
