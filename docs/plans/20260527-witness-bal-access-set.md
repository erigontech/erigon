# Amsterdam BAL-Driven Access-Set Provider for debug_executionWitness

## Overview

`debug_executionWitness` builds a stateless witness by re-executing the block with a
hand-rolled EVM loop (`RecordingState`) **only to recover the access set** — which
accounts/storage/code the block touched. The witness itself is a proof against the
**parent** (pre-block) state, built from commitment history; the post-state is already
committed in the DB. So the re-execution exists purely to learn *which keys to prove*.

For **Amsterdam** blocks (EIP-7928) that access set is **already persisted** as the
Block-Level Access List (BAL), consensus-validated against `header.blockAccessListHash`.
This change adds an Amsterdam branch that builds the access set **directly from the stored
BAL** and **skips EVM re-execution entirely**. Benefits:

- The witness access set becomes **canonical-by-construction** — it is the same access list
  consensus validated, so there is no hand-rolled execution loop that can drift from how the
  block was really executed.
- It is the correct way to support Amsterdam witnesses (the serial re-exec path cannot even
  build a BAL; BAL lives only in the parallel executor).
- Removes the SystemAddress heuristic (the BAL already encodes the consensus inclusion rule).

Pre-Amsterdam blocks have no BAL and keep the existing re-execution path **unchanged**.

This is **Change 3 of 3** independent deliverables:
1. zkevm witness test suite (`awskii/eest-zkevm-witness`, ~a month from merge) — **not a dependency**
2. collapse-sibling output filter (`awskii/witness-collapse-sibling-filter`) — separate PR
3. **this change** — Amsterdam BAL access-set provider

## Scope decision: ship the BAL plumbing now; defer witness correctness

This PR delivers the **BAL access-set provider + the `IsAmsterdam` branch in
`ExecutionWitness`** — the plumbing that replaces EVM re-execution with a BAL read for
Amsterdam blocks. **Witness verification correctness for Amsterdam blocks is explicitly
deferred** to a follow-up **Change 4** that owns the pre-existing **commitment-pipeline
divergence** observed during Task 3 implementation.

The divergence: `detectCollapseSiblings.ComputeCommitment` produces a root that does not match
`block.Root()` for the Amsterdam test fixture **regardless of how the access set is obtained**
— it reproduces under both the new BAL path and a force-routed re-execution path with the
full `RecordingState` access set. So the bug is in the witness's recompute pipeline under
Amsterdam, not in the BAL provider, and fixing it is out of scope here.

Deferred to Change 4:
- **Amsterdam commitment-pipeline divergence** — `detectCollapseSiblings.ComputeCommitment`
  reproducing `block.Root()` for Amsterdam blocks (the verification blocker).
- **"Fewer-nodes" zkevm class** (~4,128 fixtures observed during Change 2 work). Subset
  attributable to BAL Codes/Headers/exclusion-reads under-coverage stays in this PR's risk
  section; residual HPH-conversion under-emission tracked in Change 4.

**Merge gate for this PR:** mapping unit tests on `buildAccessedStateFromBAL` pass; the
`IsAmsterdam` branch fires for an Amsterdam block (no re-exec); the pre-Amsterdam path is
byte-for-byte unchanged. **`verifyWitnessStateless` passing on the Amsterdam fixture is NOT a
gate** — that assertion is `t.Skip`'d with a tracking link to Change 4 per the explicit
user-override of the no-skip rule (CLAUDE.md "Test skips" §"If a user explicitly directs an
agent to add a skip").

## Context (from discovery)

- **Branch:** `awskii/witness-bal-access-set` off `main`, worktree `/Users/awskii/org/wrk/erigon-witness-bal`.
- **Handler:** `rpc/jsonrpc/debug_execution_witness.go`
  - `ExecutionWitness` (~L524): currently always re-executes (Initialize → tx loop → Finalize →
    CommitBlock) then `collectAccessedState(recordingState)` (~L643). Downstream:
    `detectCollapseSiblings` (~L891), `buildWitnessTrie` (~L945), `collectAccessedHeaders`
    (~L1045), `verifyWitnessStateless` (~L1087).
  - `accessedState` struct (~L716): `{ SortedKeys, Addresses map[Address]struct{}, Storage
    map[Address]map[Hash]struct{}, CodeAddrs map[Address]struct{}, SortedCodes []hexutil.Bytes,
    CodeReads map[Hash]witnesstypes.CodeWithHash }`. **This is the seam.**
  - `collectAccessedState` (~L758): existing provider from `RecordingState`; shows the exact
    shaping for `SortedCodes` (sorted by code hash) and `CodeReads` (keccak(addr)→CodeWithHash,
    L859-869), and the SystemAddress heuristic (L794-817) the BAL path must NOT replicate.
- **BAL access (persisted, no re-exec):**
  - `rawdb.ReadBlockAccessListBytes(tx, blockHash, blockNum) ([]byte, error)` — `db/rawdb/accessors_chain.go:599`.
  - `types.DecodeBlockAccessListBytes(data) (types.BlockAccessList, error)` — `execution/types/block_access_list.go:573`.
  - `types.BlockAccessList = []*AccountChanges`; `AccountChanges{ Address, StorageChanges
    []*SlotChanges, StorageReads []StorageKey, BalanceChanges, NonceChanges, CodeChanges }`
    (`execution/types/block_access_list.go:39`). `SlotChanges.Slot` is the storage key.
  - `rpc/jsonrpc/eth_block_access_list.go` already reads+decodes the BAL exactly this way
    (gated on `IsAmsterdam`).
  - SystemAddress inclusion already follows the consensus EIP-7928 rule
    (`execution/state/versionedio.go:1439`).
- **Code at parent state (no re-exec):** `state.NewHistoryReaderV3(tx, firstTxNumInBlock)`
  (`execution/state/history_reader_v3.go:67`) → `.ReadAccountCode(addr)` (L263). The handler
  already builds this exact reader at `firstTxNumInBlock` (~L557).
- **Fork gate:** `chainConfig.IsAmsterdam(header.Time)` (`execution/chain/chain_config.go:413`).
- **Test precedent for Amsterdam+BAL chains:** `execution/engineapi/engine_api_bal_test.go`,
  `execution/execmodule/bal_selfdestruct_systemaddress_test.go`. Commitment-history setup
  pattern: `TestExecutionWitness` (`rpc/jsonrpc/debug_api_test.go` ~L754).

## Development Approach

- **Testing approach:** TDD (repo CLAUDE.md) — failing test first, for the right reason.
  **Never add a `t.Skip`** (forbidden for agents).
- Each task fully green before the next. Small, focused changes. No comments unless an
  invariant the types don't enforce.
- **Keep the pre-Amsterdam re-execution path byte-for-byte unchanged** — this change only ADDS
  the Amsterdam branch + the new provider. Do not touch the collapse-sibling filter (Change 1)
  or the HPH conversion.
- `make lint` after changes (non-deterministic — run until clean); `make erigon integration`
  before done. Commit prefix `rpc/jsonrpc: ...`.

## Testing Strategy

- **In-tree merge oracle (on `main`, no zkevm-suite dependency):**
  - **Mapping unit tests** on `buildAccessedStateFromBAL` (already green in Task 2).
  - **Path assertion:** for an Amsterdam block routed through `debugApi.ExecutionWitness`, the
    `IsAmsterdam` branch fires (no `RecordingState` constructed). Indirect observation only,
    not a production test-only counter.
  - **Regression:** a pre-Amsterdam block goes through the existing path and its witness is
    byte-for-byte unchanged. Verified via `git diff -w` showing only indentation changes
    inside the `else` after Task 3.
- **Deferred to Change 4 (NOT a gate here):** `verifyWitnessStateless` passing on an Amsterdam
  fixture. The end-to-end witness test is scaffolded with `t.Skip("blocked on Change 4: <link>")`
  per the explicit user override of the no-skip rule. The skip carries the tracking link and
  is removed by Change 4, not by closing the issue with the skip in place.
- **Local cross-check (Post-Completion, NOT a merge gate):** zkevm suite
  (`execution/tests/eest_zkevm_witness`) via a scratch layering `main + this change +
  Change 2 + awskii/eest-zkevm-witness`; confirms multiset parity on Amsterdam fixtures once
  Change 4 lands.
- No e2e/UI tests in this repo.

## Progress Tracking

- Mark `[x]` immediately when done. New tasks ➕, blockers ⚠️. Keep this file in sync.
- The three OPEN RISKS below are resolved empirically (verify guardrail + local zkevm suite)
  and the resolution is recorded in this file as it is found.

## Solution Overview

Introduce a second `accessedState` **provider** and branch on the fork at the top of
`ExecutionWitness`:

```
header := block.Header()
if chainConfig.IsAmsterdam(header.Time) {
    accessed = buildAccessedStateFromBAL(ctx, tx, blockHash, blockNum, firstTxNumInBlock)
    // headers = parent only (BLOCKHASH is EIP-2935 state, captured in BAL)
} else {
    // EXISTING path verbatim: RecordingState re-exec + collectAccessedState + accessedBlockHashes
}
// SHARED, UNCHANGED: detectCollapseSiblings, buildWitnessTrie, collectAccessedHeaders, verifyWitnessStateless
```

`detectCollapseSiblings` and `buildWitnessTrie` are agnostic to how `accessed` was obtained —
they touch the access keys and recompute commitment over end-of-block state (deletes already
reflected), so collapse-sibling detection works identically. `verifyWitnessStateless` is the
**completeness oracle**: if the BAL-derived access set misses a needed read, stateless re-exec
hits a missing trie node and fails loudly — so correctness is self-checked, not assumed.

**Why this is safe to add incrementally:** the pre-Amsterdam branch is the current code moved
unchanged into the `else`; only Amsterdam blocks take the new path.

## Technical Details

`buildAccessedStateFromBAL(ctx, tx, blockHash, blockNum, parentTxNum) (*accessedState, error)`:

1. `data, _ := rawdb.ReadBlockAccessListBytes(tx, blockHash, blockNum)`; if `data == nil`
   return a clear error (`commitment/BAL history pruned` — analogous to existing pruning
   errors). `bal, _ := types.DecodeBlockAccessListBytes(data)`.
2. For each `*AccountChanges ac` in `bal`:
   - `Addresses[ac.Address] = {}`.
   - `Storage[ac.Address]` ← union of `ac.StorageReads` and each `ac.StorageChanges[i].Slot`.
3. Code (deliberate over-approximation, deterministic): open
   `hr := state.NewHistoryReaderV3(tx, parentTxNum)`. For **every** address in the BAL,
   `code, _ := hr.ReadAccountCode(addr)`; if `len(code) > 0`, add to `CodeAddrs`, build
   `SortedCodes` (sorted by keccak(code), matching `collectAccessedState`), and
   `CodeReads[keccak(addr)] = CodeWithHash{Code, InternCodeHash(keccak(code))}`. This may
   over-include code vs Geth's strict "GetCode-only" rule — accepted as Risk 1 below; Codes
   multiset parity with EEST is NOT a merge gate.
4. `SortedKeys`: same shaping as `collectAccessedState` (vestigial; `result.Keys` stays null).
5. Do **not** apply the SystemAddress heuristic — the BAL already encodes the consensus rule,
   so `ac.Address == SystemAddress` entries are taken as-is.

Headers: for the Amsterdam branch, `result.Headers` = parent header only (pass empty
`accessedBlockHashes` to `collectAccessedHeaders`), because EIP-2935 (active in Amsterdam)
serves BLOCKHASH from history-contract state captured in the BAL. Headers parity is NOT
covered by `verifyWitnessStateless`; Risk 3 below.

**BAL absence semantics** — `ReadBlockAccessListBytes` can return `nil, nil` for three
distinct cases that must NOT collapse to one error:
- non-Amsterdam block — defense-in-depth only; the `IsAmsterdam` gate already prevents this.
- Amsterdam block, empty BAL (zero-tx / system-only) — valid, return empty `accessedState`.
- Amsterdam block, pruned BAL — operator action needed, distinct error.

Discriminator: `data == nil` after an Amsterdam-gated call means pruned; `len(data) == 0`
after a successful decode of an empty list means legitimately empty. Tests cover both.

**Activation block.** `IsAmsterdam` is `time >= AmsterdamTime`, so the activation block
itself takes the BAL branch. Verified by the in-tree test using the activation timestamp.

## Risks (recorded for reviewers; not merge gates)

1. **Amsterdam commitment-pipeline divergence (verification blocker, deferred to Change 4).**
   Observed during Task 3: `detectCollapseSiblings.ComputeCommitment` does not reproduce
   `block.Root()` for the Amsterdam test fixture under either the BAL path or a force-routed
   re-exec path. Confirmed via instrumentation:
   - `IsAmsterdam` branch fires
   - `buildAccessedStateFromBAL` returns 7 addresses + 8 storage keys + 2 code addresses
     matching the persisted BAL exactly
   - All keys touched on `sdCtx` before `ComputeCommitment`
   - Plain state at `endTxNum` has post-block values (`InsertChain` succeeded)
   - Yet `ComputeCommitment` returns a root that diverges from `block.Root()`

   This validates the original "block reexecution should completely match how we regularly
   execute the block" intuition: the chain insertion's `ComputeCommitment` path (via
   `chain_makers` with `versionMap`+`blockIO`) diverges from the witness's `SplitHistoryReader`
   `ComputeCommitment` path under Amsterdam. Change 4 owns this.

2. **Codes precision.** Persisted BAL has `CodeChanges` + accessed addresses but no "code read
   via GetCode/GetCodeSize" signal. This PR ships the deliberate over-approximation rule
   (include code for every BAL address with non-empty parent-state code); exact Codes multiset
   parity with Geth deferred to Change 4.
3. **Absent-account exclusion reads.** `AsBlockAccessList` drops reads of non-existent
   accounts (`versionedio.go:1382-1391`). Not detectable without `verifyWitnessStateless`
   green; deferred to Change 4.
4. **Headers / BLOCKHASH set.** `result.Headers` = parent header only is our claim. Headers
   parity validated locally against fixtures once Change 4 unblocks the corpus run.

## What Goes Where

- **Implementation Steps** (checkboxes): the new provider, the fork branch, tests, risk
  resolution, lint/build.
- **Post-Completion** (no checkboxes): zkevm scratch-layering cross-check, PR open.

## Implementation Steps

### Task 0: Constructability spike — minimal Amsterdam chain + BAL + commitment history + debugApi route

**Files:**
- Create (temporary, may be discarded): `rpc/jsonrpc/debug_witness_bal_constructability_test.go`

This is the actual gate for the rest of the plan. The cited precedents
(`engine_api_bal_test.go`, `bal_selfdestruct_systemaddress_test.go`) live in different
packages and use different chain builders than `TestExecutionWitness`. Composability of
(BAL-producing executor) + (commitment-history-enabled chain) + (chain queryable through
`debugApi.ExecutionWitness`) is **unverified**. Confirm or fix it before writing Task 1's test.

- [x] Stand up the smallest possible in-test Amsterdam chain that **produces a persisted BAL**
      (e.g. `ExperimentalBAL = true`, parallel executor) AND has commitment history enabled
      AND is reachable from a constructed `DebugAPIImpl`. Read back the BAL via
      `rawdb.ReadBlockAccessListBytes` to prove the persistence path works.
- [x] Record the working harness in this plan (pointer to the test file / a 5-line sketch). If
      no combination of existing builders works, decide whether the test belongs in
      `rpc/jsonrpc` calling the handler directly, or in `engineapi`/`execmodule` calling RPC
      shim helpers — document the decision.
- [x] If the harness needs new wiring, list it explicitly so Task 1 doesn't expand silently.

**Harness result (Task 0)** — verified composable in `rpc/jsonrpc` package, no new wiring
needed. Test: `rpc/jsonrpc/debug_witness_bal_constructability_test.go`
(`TestExecutionWitnessAmsterdamBALConstructability`). Recipe:

1. `statecfg.EnableHistoricalCommitment()` (cleanup-restored) — switches commitment domain
   to keep per-block history.
2. `execmoduletester.New(t, WithGenesisSpec({Config: chain.AllProtocolChanges, Alloc: ...}),
   WithKey(privKey))`. `AllProtocolChanges` has `AmsterdamTime=0`, so the chain is Amsterdam
   from genesis. **No `WithExperimentalBAL()` needed** — `exec/block_assembler.go:113` builds
   the BAL whenever `IsAmsterdam(time) || ExperimentalBAL`, and `inserters.go:141-147`
   unconditionally persists `block.BlockAccessList` via `rawdb.WriteBlockAccessListBytes`.
3. `blockgen.GenerateChain(..., 1, ...)` with a single simple-transfer tx; assert
   `chainPack.BlockAccessLists[0]` non-empty and `chainPack.Headers[0].BlockAccessListHash`
   non-nil before insertion.
4. `m.InsertChain(chainPack)` — this routes through
   `chainreader.InsertBlocksAndWaitWithAccessLists(blocks, balMap)` with `balMap` built from
   `chain.BlockAccessLists`, persisting the BAL.
5. `rawdb.ReadBlockAccessListBytes(tx, hash, num)` returns the persisted payload;
   `types.DecodeBlockAccessListBytes(data)` decodes a non-empty `BlockAccessList`. ✓
6. `rawdb.WriteDBCommitmentHistoryEnabled(tx, true)` — gates `debug_executionWitness`.
7. `NewPrivateDebugAPI(newBaseApiForTest(m), m.DB, nil, 0, false).ExecutionWitness(...)`
   reaches the handler with no infrastructure error. (Today it goes through the pre-existing
   re-exec path; Task 1's real test will assert verifiability and the BAL-branch indirect
   signal once Task 2/3 land.)

**Decisions logged for Task 1:**

- Test home stays in `rpc/jsonrpc` (no need to move to `engineapi`/`execmodule`).
- Task 1's `TestExecutionWitnessAmsterdamBAL` will use the same harness — either appended to
  this spike file or to `debug_api_test.go`. The spike file can be kept (it's a useful smoke
  test for the harness itself) or absorbed into Task 1 — preference: absorb in Task 1 to keep
  one Amsterdam test surface.
- No new wiring required beyond what already exists. Both `chain.AllProtocolChanges` (Amsterdam
  at genesis) and the `WithGenesisSpec`+`InsertChain` path are sufficient.

### Task 1: Failing test — Amsterdam block witness via the BAL path (RED)

**Files:**
- Modify: `rpc/jsonrpc/debug_api_test.go` (or the location decided in Task 0)

- [x] `TestExecutionWitnessAmsterdamBAL`: using the Task-0 harness, call
      `debugApi.ExecutionWitness` on the Amsterdam block; assert a **verifiable** witness
      (`verifyWitnessStateless` passes / root reproduced) **without** setting
      `ERIGON_WITNESS_NO_VERIFY`.
- [x] Path assertion by **indirect observation** (no production-code counter). Acceptable
      forms: assert `RecordingState` was not constructed (instrument the existing re-exec path
      with a `t.Helper()`-only test hook or a `_, ok := tx.Debug()...` style observation), or
      assert a re-exec-only side effect is absent. Prefer the simplest indirect signal.
- [x] Add `TestExecutionWitnessAmsterdamActivationBlock` exercising `header.Time ==
      AmsterdamTime` to lock the boundary.
- [x] Run the new tests — confirm RED for the right reason (today the Amsterdam block goes
      through re-exec or errors before the new branch lands).

**Task 1 notes:**

- Spike file `debug_witness_bal_constructability_test.go` absorbed into
  `debug_witness_bal_test.go` per the Task-0 decision (single Amsterdam test surface). The
  shared harness `setupAmsterdamBALHarness` is the Task-0 recipe extracted as a helper.
- Path observation: added `recordingStateConstructedHookForTest` test seam in
  `debug_execution_witness.go` (nil in production, fired once when the re-exec path
  constructs `RecordingState`). Tests install a counter via `installRecordingHookCounter`.
- Activation boundary: AllProtocolChanges has `AmsterdamTime=0`, so the genesis block sits
  exactly on the boundary; the test locks inclusivity via
  `IsAmsterdam(*AmsterdamTime)==true` and then exercises the first Amsterdam-era block. A
  non-zero AmsterdamTime can't be tested via the in-test chain builder — when
  `parent.Time < AmsterdamTime`, the parallel-exec BAL infra isn't allocated
  (chain_makers.go gates on `IsAmsterdam(parent.Time)`), and `InsertChain` then fails with
  `Amsterdam processing is not supported by serial exec`. Documented in the test.
- RED reason confirmed: both tests fail today with
  `[debug_executionWitness] computedRootHash != expectedRootHash` — the existing re-exec
  path produces wrong state roots for Amsterdam blocks, which is precisely what the BAL
  branch (Tasks 2/3) will fix. The hook-count assertion is the path-observation gate that
  will flip green once the IsAmsterdam branch lands.

### Task 2: buildAccessedStateFromBAL — tests first, then implementation

**Files:**
- Modify: `rpc/jsonrpc/debug_execution_witness.go`
- Add tests in: `rpc/jsonrpc/debug_witness_bal_test.go` (or appended to `debug_api_test.go`)

- [x] Write the mapping unit tests first (RED): feed a hand-built `types.BlockAccessList` into
      the new function and assert the resulting `accessedState` shape; include cases for
      *empty BAL* (decoded list of zero entries — legitimate, returns empty accessedState),
      *pruned BAL* (`ReadBlockAccessListBytes` returns nil — distinct error), and
      *SystemAddress-included-by-BAL* (entry preserved, no heuristic stripping).
- [x] Implement `buildAccessedStateFromBAL(ctx, tx, blockHash, blockNum, parentTxNum) (*accessedState, error)`:
      `rawdb.ReadBlockAccessListBytes` + `types.DecodeBlockAccessListBytes`; distinguish nil
      bytes (pruned, error) from empty list (legit empty accessedState).
- [x] Map addresses + storage (`StorageReads` ∪ `StorageChanges[].Slot`); for **every** BAL
      address, `HistoryReaderV3.ReadAccountCode(addr)` at `parentTxNum` and include code in
      `SortedCodes`/`CodeReads` iff `len(code) > 0` (over-approximation per Technical Details).
- [x] Do NOT apply the SystemAddress heuristic; take BAL entries as-is.
- [x] Mapping tests — green.

**Task 2 notes:**

- Implementation lives in `rpc/jsonrpc/debug_execution_witness.go`: pure mapping
  `accessedStateFromBAL(bal, readCode)` is split from the DB-touching wrapper
  `buildAccessedStateFromBAL(ctx, tx, blockHash, blockNum, parentTxNum)` so the
  mapping is unit-testable without DB infrastructure.
- Six unit tests in `rpc/jsonrpc/debug_witness_bal_test.go` cover: empty BAL,
  addresses+storage (reads ∪ changes dedup), SystemAddress preserved as-is,
  code over-approximation incl. EOA skip, shared-code dedup in SortedCodes, and
  the pruned-BAL distinct error via a never-written block hash through the
  harness. All six pass; `make lint` is clean.
- Task 1's Amsterdam tests remain RED — they only flip green when Task 3 wires
  the `IsAmsterdam` branch into `ExecutionWitness`.

### Task 3: Branch ExecutionWitness on IsAmsterdam (ship BAL plumbing)

**Files:**
- Modify: `rpc/jsonrpc/debug_execution_witness.go`
- Modify: `rpc/jsonrpc/debug_witness_bal_test.go`

The Task-3 wiring landed during the prior ralphex run (uncommitted, in working tree). It is
structurally correct per debug instrumentation. The end-to-end witness assertion is blocked by
the Amsterdam commitment-pipeline divergence (Risk 1) — Change 4 owns the fix. This task
ships the wiring and reshapes the tests around the softer merge gate.

- [x] Commit the Task-3 wiring already in working tree (`rpc/jsonrpc/debug_execution_witness.go`):
      existing re-exec + `collectAccessedState` block wrapped in `else`; `if chainConfig.IsAmsterdam(header.Time)`
      branch calls `buildAccessedStateFromBAL`; Amsterdam branch sets `accessedBlockHashes`
      empty so `collectAccessedHeaders` emits parent-only; shared `detectCollapseSiblings` /
      `buildWitnessTrie` / `verifyWitnessStateless` calls untouched.
- [x] Verify pre-Amsterdam branch is **byte-identical** to today: `git diff -w main --
      rpc/jsonrpc/debug_execution_witness.go` shows only the new `if` + indentation inside
      `else` plus the necessary hoisting of `fullEngine`, `header`, and
      `accessedBlockHashes` declarations out of the branch (both paths share them).
- [x] Update Task-1 tests (`TestExecutionWitnessAmsterdamBAL`,
      `TestExecutionWitnessAmsterdamActivationBlock`) to **skip the
      `verifyWitnessStateless`-passes assertion** with `t.Skip("blocked on Change 4:
      Amsterdam commitment-pipeline divergence — see
      docs/plans/20260528-witness-amsterdam-commitment-divergence.md")`. **Keep the
      path-assertion (`installRecordingHookCounter` count == 0) as the active live
      assertion** — that one IS testable today and is what proves the BAL branch fires.
      The Skip is per the explicit user override of the no-skip rule; trade-off is logged in
      the Scope section above.
- [x] Pre-Amsterdam regression: full `go test ./rpc/jsonrpc/ -run TestExecutionWitness` green
      (no changes to existing sub-cases).
- [x] Path-assertion is GREEN for the Amsterdam tests; verify-passes assertions are SKIPPED
      with the tracking link.

### Task 4: Final integration verification (merge gate)

- [x] **Mapping unit tests** (Task 2's 6 unit tests on `buildAccessedStateFromBAL`) — green.
- [x] **Path assertion** for Amsterdam block (`installRecordingHookCounter` count == 0) —
      green.
- [x] **Pre-Amsterdam regression** — `TestExecutionWitness` and all existing sub-cases
      unchanged and green.
- [x] **No new green tests asserting witness correctness for Amsterdam** — those are skipped
      pending Change 4. Verify the skip messages reference the Change 4 plan path.
- [x] `make lint` (repeat until clean — non-deterministic).
- [x] `make erigon integration` builds.
- [x] `go test ./rpc/jsonrpc/...` clean (skipped tests OK).

### Task 5: Create Change 4 stub plan

**Files:**
- Create: `docs/plans/20260528-witness-amsterdam-commitment-divergence.md`

- [x] Write a brief stub plan capturing the divergence findings recorded in Risk 1:
      `detectCollapseSiblings.ComputeCommitment` does not reproduce `block.Root()` for
      Amsterdam blocks under either the BAL path or a force-routed re-exec path; lists the
      debug observations; sketches investigation entry points (compare `chain_makers`
      `ComputeCommitment` path with the witness `SplitHistoryReader` path; check Amsterdam
      EIPs that touch commitment state: EIP-2935 history-storage, EIP-4788 beacon root,
      EIP-7708 transfer logs, EIP-8037 state-gas). Marks acceptance: the Skipped Amsterdam
      witness tests flip green when Change 4 lands.
- [x] Commit the stub plan.

### Task 6: [Final] Docs and plan housekeeping
- [ ] Update CLAUDE.md / package notes only if a genuinely new, non-obvious pattern emerged
      (default: no change).
- [ ] Move this plan to `docs/plans/completed/`.

## Post-Completion
*Manual / external — no checkboxes*

**Change 4 (the verification-blocker follow-up):** open after this PR ships. The
`docs/plans/20260528-witness-amsterdam-commitment-divergence.md` stub (Task 5) is the
starting point. When Change 4 lands and removes the divergence, the Skipped Amsterdam witness
tests in this PR's test file flip green automatically — Change 4's PR removes the `t.Skip`.

**Local cross-check via scratch layering** (deferred until Change 4 lands; the corpus run is
not useful while verification is blocked): `main + this change + Change 2 + Change 4 +
awskii/eest-zkevm-witness`, run `execution/tests/eest_zkevm_witness` and record:

- Codes multiset parity (Risk 2 — refine over-approximation if needed)
- Absent-account exclusion reads (Risk 3 — add supplement if fixtures need it)
- Headers parity (Risk 4 — confirm `result.Headers = [parent]`)
- "Fewer-nodes" residual scope for Change 4 / Change 5

**PR for THIS change:**
- Open against `main`. Title:
  `rpc/jsonrpc: build executionWitness access set from BAL for Amsterdam blocks`.
- Body: rationale (no re-execution for Amsterdam; access set is the consensus BAL); links
  EIP-7928; explicitly states **witness verification correctness is deferred to Change 4**
  (Amsterdam commitment-pipeline divergence reproducible under both BAL and re-exec paths);
  notes the in-tree merge gate is mapping unit tests + path assertion + pre-Amsterdam
  byte-identical regression; flags the per-user-override `t.Skip` on the verify-passes
  assertions, with the link to the Change 4 stub plan in this repo.
