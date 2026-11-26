# Witness and proof RPCs on commitment v3 only

## Overview
- `eth_getProof`, `eth_getWitness`, `eth_getTxWitness`, `debug_executionWitness`, `debug_executionWitnesses`
  and the engine payload witness (`ValidatedBlockWitness`) use three trie implementations today:
  - node capture: HPH or v3, chosen per process by `ERIGON_COMMITMENT_V3` (default off);
  - lean prune, proof extraction, op-stream encoding and every self-check: the erigon2 MPT in
    `execution/commitment/trie`, for both variants;
  - on a v3 node, collapse siblings come from `collapseSim` (`execution/commitment/v3/collapse.go`), a
    simulation of HPH's grid fold that keeps HPH's output byte for byte.
- Goal: on a v3 node every serving step is v3 code or a format-only helper with no trie in it. HPH keeps its
  current code, frozen, until HPH is deleted. The erigon2 MPT stays only as an independent oracle in tests and
  under `ERIGON_ASSERT`.
- No API change. Output bytes stay the same, except the outputs that carry legacy collapse siblings after Task 7:
  legacy `debug_executionWitness` and `eth_getWitness`/`eth_getTxWitness` may gain nodes, never lose them.

## Context (from discovery, main `e91aca7f9bf`, 2026-11-25)
- Entry points: `rpc/jsonrpc/eth_call.go` (`GetProof` :457, `getWitness` :665), `rpc/jsonrpc/debug_execution_witness.go`
  (`ExecutionWitness` :805, `buildWitnessResult` :1074, `detectCollapseSiblings` :1439, `buildWitnessTrie` :1529,
  `ValidatedBlockWitness` :1059, `execBlockStatelessly` :2279), `rpc/jsonrpc/witness_cache_builder.go`,
  `rpc/jsonrpc/witness_subscription.go`; engine caller `execution/engineapi/engine_server.go:1258`.
- Dispatch: `execution/commitment/commitmentdb/commitment_context.go`: `witnessTrie` (one method, `WitnessesByHash`),
  `v3Trie` (7 methods), `WitnessNodesByHash` :410, `WitnessNodes` :422, `WitnessLean` :444, `TraceCollapses` :478,
  `BranchChildCount` :490 (HPH `BranchData` fallback :519).
- Hard-wired erigon2 calls on the serving path, both variants:
  - `trie.WitnessNodesForKeysByHash` (`commitment_context.go:431`): the lean prune; it decodes into erigon2 node types;
  - `trie.ProofFromNodes` (`eth_call.go:577`, `:610`; `trie/proof.go:662`): uses only `rlp`, `nibbles` and `common`;
  - `trie.RLPDecode` → `*trie.Trie` → `UpdateAccountCode` → `ExtractWitness` → `WriteInto` (`commitment_context.go:449-465`,
    `eth_call.go:845-851`): the eth_getWitness op-stream; `emptyWitnessBytes` (`eth_call.go:901`);
  - ASSERT only: `trie.VerifyAccountProof`/`VerifyStorageProof`, the op-stream round trip (`eth_call.go:854-870`),
    `newWitnessStateless` (`*trie.Trie`, `debug_execution_witness.go:1811`).
- Collapse today:
  - HPH fires at delete time on the row's live child count, in hashed key order (`hex_patricia_hashed.go:2230-2277`),
    so legacy holds transient siblings: delete a and insert c under one branch, and b is reported although the
    post-state branch keeps 2 children. The spec: "legacy keeps every traced collapse-sibling — legacy is thus the
    superset" (`docs/plans/witness-legacy-mode-spec.md:105`). Canonical drops candidates whose post-state branch has
    ≥ 2 children (`BranchChildCount`, `debug_execution_witness.go:1511`).
  - eth_getWitness builds with legacy siblings (`eth_call.go:763`, `:818`); the engine route is canonical (`:1064`).
  - v3 runs `traceCollapses` (`collapseSim`) in the `sorted` callback over the sorted feed, before the phases
    (`v3/trie.go:136`, `:153`). No post-state child count is exposed; a collapsed node is a deletion delta.
  - Routes: on-demand legacy and canonical, eth_getWitness and the engine route use `SetCollapseTracer` +
    `ComputeCommitment`; only head-capture legacy with parent records uses `TraceCollapsesRoot`.
  - A stale HPH sibling path after an upward propagate exists, but it did not lose a node in a probe: leaf b stayed in
    the lean witness because the delete-time event reports its full path first.
- Test chain: `collectChainWitnesses` (`debug_execution_witness_test.go:1103`) uses `rpcdaemontest.CreateTestExecModule`,
  shared by 33 test files (13 blocks, Berlin). Blocks 1-12 have 0 collapse candidates, block 13 has 2, 0 are transient;
  78 of 78 `detectCollapseSiblings` calls had `traced=false`. It cannot gate a collapse change.
- v3 today: `Trie.WitnessesByHash` (`v3/witness.go:59`) walks records along each proved key and returns the on-path
  superset; `Trie.BranchChildCount` is native. v3 non-test code does not import `execution/commitment/trie`.
- Parity tests that use HPH as oracle: `v3/witness_test.go` `TestWitnessMatchesHPH` :361, `TestCollapseTracerMatchesHPH` :493;
  `rpc/jsonrpc/debug_execution_witness_test.go` `TestWitnessesMatchHPHUnderCommitmentV3` :1145 (legacy, canonical and
  eth_getWitness bytes; no eth_getProof).
- No v3-pinned test: eth_getProof (latest, historical), eth_getTxWitness, a witness at a past block through the history
  reader, `ValidatedBlockWitness`, the durable cache builder.
- Output contract: `docs/plans/witness-legacy-mode-spec.md` (legacy for the prover, canonical equal to the EEST zkevm
  v0.4.0 corpus). Oracle: reth `stateless`; erigon's `WITNESS_STRICT_VERIFY` is weaker.
- Related PRs: draft #26810 moves HPH to `execution/commitment/v2` and changes 20 of the 24 non-test files in
  `execution/commitment/v3`, plus `commitment_context.go` and `eth_call_test.go`; open #26276 (v3 history convert)
  produces real v3 history files and touches no file in this plan.
- Stale comment: `debug_execution_witness.go:1106-1108` says witness building needs the sequential HPH.

## Decisions
- Scope and bar:
  - HPH nodes keep today's code behind the existing dispatch, with no new HPH work. The HPH branches are deleted in
    Task 9, together with the HPH tree, after v3 is the default.
  - Parity bar: prune, proof and op-stream steps are byte-identical to today's output (golden fixtures). The collapse
    step is gated by validity: reth stateless, `erigon_only=0` against reth, the EEST canonical corpus, the spec.
  - eth_getWitness keeps its API. A node-list encoder builds the op-stream; `*trie.Trie` leaves the serving path.
- Design:
  - The stateless verifier stays on the erigon2 MPT, under `ERIGON_ASSERT` and in tests: a v3 verifier would check v3
    with v3.
  - eth_getProof keeps `ProofFromNodes` and moves it to a format-only package. Rejected: a v3-native `Proof`, which
    rewrites a 70-line RLP walker and adds byte risk.
  - Legacy collapse uses a deletes-only rule; its witness node set is a superset of today's legacy node set. Canonical
    is that candidate set through the existing `BranchChildCount` filter. Rejected: the net rule for legacy, which makes
    legacy smaller.
- Order: Tasks 1-8 are built on main now; whichever of this branch and #26810 lands second rebases. Task 9 waits for
  its gate.

## Development Approach
- Tests first. Tasks 1-2 land first and fix today's bytes. In each task the test steps come before the code steps
  and run red first. A defect gets a red test in the real code path, in the same PR as its fix.
- Pure moves (Tasks 4 and 5's format move) have no behaviour change: the existing tests and the golden fixtures are
  the safety net, and the PR body says so (AGENTS.md "When pragmatism applies").
- One PR per task group, cut from fresh main: PR 1 = Tasks 1-2, PR 2 = Task 3, PR 3 = Task 4, PR 4 = Task 5,
  PR 5 = Tasks 6-8, PR 6 = Tasks 9-11 (gated).
- Every task that changes code runs its package tests with v3 on and off (`statecfg.SetCommitmentV3` in the test, and
  the CI `ERIGON_COMMITMENT_V3` leg). The HPH branch stays green against its fixtures.
- Each task ends with its tests green before the next task starts. A failing case found in Task 6 is recorded, not
  committed red; Task 7 adds it as a test with the fix, and a mutation check shows it red without the fix.

## Testing Strategy
- Golden fixtures (Task 2) are the byte gate for Tasks 3-5 and outlive HPH. A dedicated fixture chain carries collapse
  shapes, so the Task 7 gates have cases to judge.
- Differential tests use the erigon2 MPT as an independent oracle for v3-native code.
- Validity (Tasks 6-7): stateless re-execution of every fixture block in both modes, asserted in the test on the
  returned post-state root (the RPC path only logs a mismatch); the EEST zkevm canonical corpus (`make test-fixtures-zkevm`,
  `execution/tests/testutil/witness_block_test_util.go`); a mainnet window against reth (Post-Completion).
- No UI, no e2e suite.

## Progress Tracking
- Mark done items `[x]` at once. New tasks get a ➕ prefix, blockers ⚠️.
- Update this file when scope changes.

## Solution Overview
- v3 gets two native outputs, each a walk over v3 records along the proved keys:
  - `WitnessNodes`: the lean node set, without the erigon2 prune;
  - collapse siblings from a structural rule over the pre-state records, not from a simulation of HPH.
- Format-only helpers with no trie in them move out of `execution/commitment/trie`:
  - `ProofFromNodes` to `execution/commitment/witness`;
  - the op-stream operators, marshalling, reader and a new encoder from the lean node list to
    `execution/commitment/witness/opstream`.
  The erigon2 trie keeps `WitnessBuilder` and `BuildTrieFromWitness` and imports `opstream`.
- `commitmentdb` dispatches: v3 calls the native methods; HPH keeps the current path unchanged.
- After v3 is the default and HPH is deleted, the HPH branches go, and the erigon2 prune becomes a test-only oracle.

## Technical Details
- Lean set (Task 3): the same set `trie.WitnessNodesForKeysByHash(byHash, root, provedKeys, exclusion)` returns from v3's
  superset. Rules the walk must keep:
  - on-path nodes per proved key, root first;
  - the node behind a diverging extension only with exclusion proofs, never behind the account-root extension
    (`v3/witness.go:182`), and only when the key continues past the branch nibble (`witness.go:213`);
  - for a partial key that ends exactly at an extension end, the branch behind the extension only with exclusion proofs
    (`trie/proof.go:314`);
  - a node under 32 bytes stays inside its parent, except the account root (`proof.go:253`).
  RPC outputs sort or rebuild the set, so set equality plus the golden bytes decide. `RLPDecode` takes `nodes[0]` as the
  root (`trie/trie.go:865`), so root-first order is kept for the op-stream input.
- Proof (Task 4): `ProofFromNodes` and `childNode` move as is. Behaviour stays: an absent account or an empty storage
  root returns `Proof: []` (`eth_call.go:592-597`); the `0x80` line (`:625`) is for a present account whose storage walk
  returns no node.
- Op-stream (Task 5): `witness.go` (header, `Witness`, `NewWitness`, `WriteInto`, `NewWitnessFromReader`),
  `witness_operators.go`, `witness_marshalling.go` and `witness_stats.go` move; they import no trie code. The encoder
  walks the node RLPs from the root in the order `WitnessBuilder` (`trie/witness_builder.go`) walks a trie, and
  reproduces the state a `*trie.Trie` carries:
  - a hash reference outside the set is `OperatorHash`;
  - a code-bearing account without a code read gets `CodeSize = codeSizeUncached` (`trie/trie.go:1008-1011`);
  - storage roots resolve by hash in one global map (`trie.go:1016`): an account that shares a storage root with a
    walked account gets its storage expanded too;
  - an account with empty root and empty code hash has no storage op (`witness_builder.go:171-172`); nil storage is
    `EMPTY_ROOT` (`:175-176`); blinded storage is a hash op;
  - code attaches by account hash from `codeReads`; an absent account and empty code are skipped, as `WitnessLean` does
    today, and a code hash mismatch is an error (`trie.go:300-303`).
  The gate is byte equality with today's `ExtractWitness(false, nil)` + `WriteInto` output and with `emptyWitnessBytes`.
- Collapse (Task 7): one read-only walk over the pre-state records along the sorted feed keys, in v3's `sorted` callback
  where `traceCollapses` runs now. For every pre-state branch on a touched path, count the children whose subtree
  survives when only the block's deletes are applied. If exactly 1 survives, report it to the `CollapseTracer` at its
  full path (prefix + nibble + its extension). A branch whose survivors drop to 0 removes itself from its parent's count,
  so cascades follow. The rule does not depend on key order. The candidate set can differ from HPH's (for example, HPH
  reports b in a branch whose children are all deleted while an insert keeps it alive; b's path is a proved delete key,
  so its node is in the witness anyway). The gate is therefore on witness node sets, not on tracer events. Legacy keeps
  every candidate. Canonical keeps the current RPC filter, `BranchChildCount` ≥ 2 drops the candidate, which removes the
  transient ones. Both routes reach the walk: `SetCollapseTracer` + `ComputeCommitment`, and `TraceCollapsesRoot`. The
  `CollapseTracer` interface and the RPC code stay as they are.
- Fixtures (Task 2): one file per block and method, with a per-variant override directory, empty at first, for the
  legacy-sibling outputs (legacy `debug_executionWitness`, `eth_getWitness`). After Task 7 the v3 files of these outputs
  that change move to the override.
- Readers: unchanged. Durable on-demand uses `NewSplitHistoryReader`/`NewHistoryStateReader`; head-capture uses the
  pinned parent and parent records; the engine route uses `parentState`.

## What Goes Where
- Implementation Steps: code, tests, fixtures and the spec doc in this repo.
- Post-Completion: the mainnet reth parity window, the heads-up to the prover team, the research-log update.

## Implementation Steps

### Task 1: v3-pinned tests for every serving path
**Files:**
- Modify: `rpc/jsonrpc/eth_call_test.go`
- Modify: `rpc/jsonrpc/debug_execution_witness_test.go`
- Modify: `rpc/jsonrpc/witness_cache_builder_test.go`
- Modify: `execution/engineapi/engine_api_payload_witness_test.go`

- [x] run `TestGetProof` and the request-shape tests with v3 on and off as subtests (`statecfg.SetCommitmentV3`)
- [x] add a historical `eth_getProof` test under v3 on a chain with v3 commitment history (`newV3HistoryTestExecModule`)
- [x] add `eth_getTxWitness` and an on-demand `debug_executionWitness` at a past block under v3 (both modes)
- [x] add v3 runs of `TestWitnessCacheBuilderParity` (durable, which also feeds `debug_executionWitnesses`) and
      `TestEngineApiPayloadWitness*` (`ValidatedBlockWitness`), unless they already pin a variant
- [x] no red result in the added v3 coverage; no defect entry is required
- [x] run `go test ./rpc/jsonrpc/ ./execution/engineapi/` with `ERIGON_COMMITMENT_V3` unset and `=true`: all green

### Task 2: golden fixtures from today's output, on a dedicated collapse chain
**Files:**
- Create: `rpc/jsonrpc/witness_golden_test.go`
- Create: `rpc/jsonrpc/testdata/witness_golden/`
- Modify: `rpc/jsonrpc/debug_execution_witness_test.go` (`collectChainWitnesses` takes the chain to run on)

- [x] build a dedicated chain in `witness_golden_test.go` with `GenerateChain` on `chain.TestChainBerlinConfig`; the shared
      `rpcdaemontest` chain stays unchanged
- [x] its blocks hold: a delete and an insert under one branch (transient collapse, with the insert both before and after
      the delete in key order), a cascade, a storage-plane collapse, a storage trie that becomes empty, a self-destruct,
      an inline storage leaf as survivor; assert collapse candidates > 0 for each shape except transient-before, where HPH
      reports none today
- [x] per block write `debug_executionWitness` legacy and canonical, `eth_getWitness` bytes, and `eth_getProof` for a fixed
      key set (present, absent, storage present and absent, empty storage, deleted account); this test holds the
      eth_getProof byte check
- [x] capture on main before any later task, under HPH and under v3; assert both captures are equal at capture time
- [x] add the per-variant override directory for the legacy-sibling outputs (empty)
- [x] the test regenerates the files only under an explicit update flag; by default it compares bytes
- [x] one-time mutation check: change one byte in a temp copy, confirm the failure names block and method, paste the
      failure text in the PR body
- [x] run the golden test with v3 on and off: green

### Task 3: v3-native lean node set
**Files:**
- Modify: `execution/commitment/v3/witness.go`
- Modify: `execution/commitment/v3/witness_test.go`
- Modify: `execution/commitment/commitmentdb/commitment_context.go`
- Modify: `rpc/jsonrpc/debug_execution_witness.go`

- [x] write the differential test first, red: `Trie.WitnessNodes` against `trie.WitnessNodesForKeysByHash` over v3
      `WitnessesByHash`, as sets, on the `TestWitnessMatchesHPH` shapes and seeds, both exclusion settings, and each edge
      key alone (full, partial at a child boundary, partial at an extension end, absent, under the account-root extension)
- [x] write the error-case tests: nil trie context, corrupt record (`errWitnessHash`)
- [x] add `Trie.WitnessNodes(ctx, updates, produceExclusionProofs) (nodes [][]byte, root []byte, err error)` with the rules in
      Technical Details
- [x] `SharedDomainsCommitmentContext.WitnessNodes` calls it for v3; the HPH path keeps `trie.WitnessNodesForKeysByHash`
- [x] correct the stale comment at `debug_execution_witness.go:1106-1108`
- [x] run v3 and commitmentdb tests, then Task 1 and Task 2 tests, v3 on and off: green

### Task 4: move ProofFromNodes to a format-only package
**Files:**
- Modify: `execution/commitment/witness/` (add the proof walker and its tests)
- Modify: `execution/commitment/trie/proof.go`, `execution/commitment/trie/proof_test.go`
- Modify: `rpc/jsonrpc/eth_call.go`, `execution/commitment/witness_test.go`

- [x] move `ProofFromNodes` and `childNode` as is; update every caller; `trie` keeps no copy
- [x] move the tests that cover them; add none; the PR body states the TDD skip for a pure move
- [x] run trie, witness, `TestGetProof*` and Task 2 golden tests, v3 on and off: green

### Task 5: op-stream format package and node-list encoder
**Files:**
- Create: `execution/commitment/witness/opstream/` (header, `Witness`, operators, marshalling, reader, stats, encoder)
- Move: `execution/commitment/trie/witness.go`, `witness_operators.go`, `witness_marshalling.go`, `witness_stats.go`,
  `witness_operators_test.go`, `witness_test.go` to `opstream`
- Modify: `execution/commitment/trie/witness_builder.go`, `trie_from_witness.go`, `trie_witness.go`
- Modify: `execution/commitment/commitmentdb/commitment_context.go`
- Modify: `rpc/jsonrpc/eth_call.go`

- [x] move the format-only code and its tests to `opstream` in one commit; `trie` keeps `WitnessBuilder` and
      `BuildTrieFromWitness` and imports `opstream` (check with `go list -deps`)
- [x] write the encoder byte test first, red: encoder bytes equal `RLPDecode` + `UpdateAccountCode` + `ExtractWitness` +
      `WriteInto` on the Task 3 shapes and on: two accounts with one storage root and one touched slot, a code-bearing
      account without a code read, an account with code and empty storage, an EIP-7702 account, the empty set, a single
      leaf, a root extension
- [x] write the round-trip test: `opstream.NewWitnessFromReader` + `trie.BuildTrieFromWitness` rebuilds the parent root
- [x] add the encoder from the root-first lean node list and the code reads, with the state list in Technical Details;
      `emptyWitnessBytes` uses `opstream`
- [x] `getWitness` uses the encoder for both variants; `WitnessLean` and its `*trie.Trie` return go
- [x] run trie, opstream, Task 1 and Task 2 tests, v3 on and off: green

### Task 6: collapse validity harness
**Files:**
- Modify: `execution/commitment/v3/witness_test.go`
- Modify: `rpc/jsonrpc/witness_golden_test.go`
- Modify: `rpc/jsonrpc/witness_cache_builder_test.go`

- [ ] trie-level search over delete-heavy random batches (both planes, cascades, self-destruct). Oracle: `trie.RLPDecode` of
      the lean set plus the siblings, then apply the batch insert-first and delete-first; a `HashNode` resolve fails the
      case. If a case fails, record its shape, the assertion that fires and the value it sees; Task 7 adds it as a test
- [ ] on the Task 2 chain, call `execBlockStatelessly` in the test for every block, both modes, under v3, and assert the
      returned post-state root
- [ ] run `TestHeadCaptureOverParentRecordsMatchesDurablePerBlock` on the Task 2 chain, so the `TraceCollapsesRoot` route sees
      candidates > 0 per shape
- [ ] run the EEST zkevm canonical corpus under v3 and record the result; if its runner has no v3 mode, add one
- [ ] if no failing case is found, record that Task 7 removes HPH mechanics from v3 and fixes no validity defect

### Task 7: v3-native collapse siblings
**Files:**
- Modify: `execution/commitment/v3/collapse.go`, `execution/commitment/v3/trie.go`, `execution/commitment/v3/witness_test.go`
- Create: `execution/commitment/v3/collapse_bench_test.go`
- Modify: `rpc/jsonrpc/debug_execution_witness_test.go`
- Modify: `rpc/jsonrpc/testdata/witness_golden/`

- [ ] write the rule tests first, red: every candidate is the single deletes-only survivor of a pre-state branch; cascades;
      a branch that drops to 0; both planes; key order of a delete and an insert does not change the result
- [ ] replace `collapseSim` with the deletes-only walk in Technical Details; keep the `sorted` callback hook, the
      `CollapseTracer` interface and the shared record reads of `TraceCollapsesRoot`
- [ ] delete `TestCollapseTracerMatchesHPH`
- [ ] `TestWitnessesMatchHPHUnderCommitmentV3`: legacy `debug_executionWitness` node set of v3 ⊇ HPH per block;
      `eth_getWitness`: decode both op-streams, v3 node set ⊇ HPH, and the v3 stream rebuilds the parent root; canonical
      stays byte-equal; the Task 2 golden test keeps eth_getProof byte-equal
- [ ] add the Task 6 failing case, if any, as a test in the real RPC path: red with `collapseSim`, green with the new walk;
      Task 6 stateless re-execution stays green; the EEST canonical corpus stays byte-identical
- [ ] move the v3 files of the legacy-sibling outputs that change to the override directory; list them in the PR body
- [ ] benchmark per-block collapse cost against the `collapseSim` arm on the Task 2 chain in `collapse_bench_test.go`;
      bar: `ab-verdict` gives v3-native not worse
- [ ] run v3, rpc and engineapi tests, v3 on and off: green

### Task 8: PR 5 acceptance and spec
**Files:**
- Create: `rpc/jsonrpc/witness_callsites_test.go`
- Modify: `docs/plans/witness-legacy-mode-spec.md`

- [ ] a test lists every `trie.` call site in `rpc/jsonrpc/eth_call.go`, `debug_execution_witness.go`, `witness_cache_builder.go`
      and `commitmentdb/commitment_context.go`, and allows only: the ASSERT verifier, the ASSERT round trip
      (`opstream.NewWitnessFromReader` + `trie.BuildTrieFromWitness`), and the frozen HPH branch (`trie.WitnessNodesForKeysByHash`);
      `execution/types/hashing.go` and `execution/state/dump.go` are out of scope
- [ ] spec: the collapse-sibling rule for legacy and canonical, the eth_getWitness note, and the code map
- [ ] if `docs/site` changes, regenerate both `llms-full.txt` files
- [ ] `make lint` on the touched packages; golden fixtures, Task 6 and Task 7 tests green, v3 on and off

### Task 9: delete the HPH branches (⚠️ gated: v3 is the default and the HPH tree is being deleted)
**Files:**
- Modify: `execution/commitment/commitmentdb/commitment_context.go`
- Modify: `rpc/jsonrpc/eth_call.go`, `debug_execution_witness.go`, `witness_cache_builder.go`
- Delete: HPH witness code (`WitnessesByHash`, `witness_node_set.go`, `captureExtensionDivergence`) with the HPH tree
- Modify: `execution/commitment/trie/proof.go` and its tests

- [ ] replace `TestWitnessMatchesHPH` and `TestWitnessesMatchHPHUnderCommitmentV3` with the golden fixtures and the erigon2
      oracle tests, before the delete
- [ ] drop the `witnessTrie` assertion, the `BranchData` child-count fallback, the `AcceptsFeed` touch split and the variant
      branches in the cache builder
- [ ] `trie.WitnessNodesForKeysByHash` loses its serving caller; keep it only where a test uses it as an oracle
- [ ] remove the HPH entry from the Task 8 allowlist
- [ ] run `go test ./rpc/... ./execution/commitment/... ./execution/engineapi/...`: green

### Task 10: verify acceptance criteria
- [ ] the Task 8 call-site test allows only the two ASSERT entries
- [ ] golden fixtures green; EEST canonical corpus byte-identical; stateless re-execution green for every fixture block
- [ ] `make lint` and the rpc, commitment and engineapi packages green

### Task 11: [Final] update documentation
- [ ] `docs/plans/witness-legacy-mode-spec.md`: code map without HPH
- [ ] move this plan to `docs/plans/completed/`

## Post-Completion
**Manual verification**
- Mainnet window on a remote box after Task 7: v3 node with head-capture cache next to a reth endpoint. Gate:
  reth stateless re-executes every block, `erigon_only=0`, codes and keys identical. Host-side diff by raw node
  bytes. Before the run, check that the script records these three numbers per block and that the window holds
  enough blocks with deletes to reach a verdict.
- Build time per block under v3 against main on the same window.

**External**
- Tell the prover team that legacy `debug_executionWitness` and `eth_getWitness` may add collapse siblings after
  Task 7. The output gets larger, never smaller, and stays valid.

## Open risks
- #26810 changes most v3 files that Tasks 3, 5 and 7 touch, and `eth_call_test.go` that Task 1 touches. Whichever lands
  second takes the rebase.
- No v3 witness or proof has run on real v3 history files; #26276 produces them. Task 1 covers the test chain only.
- The deletes-only legacy node set as a superset of HPH's is derived, not tested; Task 7's per-block check is the gate.
- The replay order of reth stateless and of the downstream prover decides if transient siblings are needed; legacy
  keeps them in either case.
- No validity defect is known in today's collapse output. If Task 6 finds none, Task 7 is a simplification: it removes
  the HPH simulation from v3 at the cost of a legacy output change.
- Byte equality of the op-stream depends on matching `WitnessBuilder`'s traversal and trie state, including account-leaf
  detection and inline nodes; the golden fixtures are the gate.
- Task 9 depends on the v3 default gate (tip latency below HPH) and on #26810.
