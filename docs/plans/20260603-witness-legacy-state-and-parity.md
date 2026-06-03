# debug_executionWitness legacy 1:1 — state nodes, keys/codes over-inclusion, mode param

## Overview
Follow-up to `completed/20260603-witness-legacy-keys-codes`. Close the last gaps so
erigon's `debug_executionWitness` matches the reference **legacy** witness 1:1 (legacy is
the default the downstream prover consumes). Four gaps, smallest-first:

1. **keys over-inclusion** — erigon emits address keys for accessed-but-nonexistent
   accounts; emit an address key only when the account exists post-state.
2. **codes empty-edge** — the single empty-`0x` code entry is emitted too eagerly.
3. **state under-inclusion** — legacy keeps nodes canonical prunes (~46/block:
   storage-root nodes for untouched storage, empty nodes, extra collapse siblings).
4. **`mode` RPC param** — accept mode as an optional request parameter (API parity), env
   var stays the default.

KISS: minimal, mode-gated, guarded changes. **The canonical path must stay byte-identical
(state-root-critical).**

## Context (from discovery)
- Producer `rpc/jsonrpc/debug_execution_witness.go`: `RecordingState`,
  `collectAccessedState` (builds `WitnessKeys` + codes, already mode-aware),
  `ExecutionWitness(ctx, blockNrOrHash)` (L548 — the single RPC entry; handles number AND
  hash; there is **no** `…ByBlockHash`), `resolveWitnessMode()` (L674, reads
  `ERIGON_WITNESS_MODE`, default `legacy`), `detectCollapseSiblings` whose
  `childCount >= 2` **drop loop at ~L1049-1064 is #21569's minimization** (it lives HERE,
  in the producer — not in the commitment trie), `verifyWitnessStateless` (~L736, in-process
  state-root check), system-address existence pattern using `rs.inner.ReadAccountData` (~L875).
- Builder `execution/commitment/hex_patricia_hashed.go`: `GenerateWitness` (~L2606),
  `toWitnessTrie` (~L1440). The account node sets `Storage: trie.NewHashNode(storageRootHash)`
  at **L1434** — the storage root is a **hash node, not expanded**; `toWitnessTrie` stops
  at the account boundary (~L1473). Materializing the storage-root *node* therefore needs an
  explicit resolve/fetch, not a resident grab.
- `sdCtx.Witness(...)` (`commitmentdb/commitment_context.go` ~L279) has **5 callers**:
  `eth_call.go:500,556` (eth_getProof), `eth_call.go:755` (computeWitness),
  `debug_execution_witness.go:1103,1304`. Adding a `mode` parameter touches all 5; the
  three non-`debug_executionWitness` callers must pass the value reproducing today's output.
- Already landed (this branch, local): `witnessMode` switch, legacy codes, `WitnessKeys`,
  EIP-7702 designators. keys/codes already mode-aware in `collectAccessedState`.
- Oracle: `~/dev/wit-oracle/` (`reth-legacy/`, `erigon-accept/`, blocks 25230831–850;
  exclude flaky 25230834/841/845/846). Corpus `eest_zkevm_witness` present via temporary
  merge (commit `d988f667`, reverted later).

## Development Approach
- **Testing**: every code task has a *real automated gate* runnable from clean git state —
  no task is "done" on build+lint alone:
  - Gap 1 (`accountExists`) and Gap 2 (empty-code trigger) are pure predicates → **unit
    tests (Red→Green)** in the jsonrpc package.
  - Gap 3 legacy state → **in-process `verifyWitnessStateless` run in legacy mode** (asserts
    the legacy witness still reconstructs the correct state root — catches root corruption),
    plus an extended `Test_WitnessTrie_GenerateWitness` (run **without** `-short`; it
    `t.Skip`s under `-short`).
  - Canonical regression corpus (must stay green) for every task.
- The exhaustive 1:1 node-set match is the **manual oracle** (Post-Completion) — the
  automated gates above are necessary conditions the executor CAN run; the oracle is the
  final human check.
- Smallest-first (1 → 2 → 3 → 4). `make lint` before each commit (non-deterministic; the
  ~55 pre-existing issues are in a sibling worktree, not introduced here).

## Testing Strategy
- **Unit** (fast): `accountExists` and empty-code-trigger tests in
  `rpc/jsonrpc/..._test.go`.
- **Witness builder**: `go test -run Test_WitnessTrie_GenerateWitness ./execution/commitment/`
  (no `-short`) with a legacy storage-root case.
- **Legacy state-root gate**: `verifyWitnessStateless` exercised in legacy mode (in the
  existing witness flow or test).
- **Canonical regression**: `ERIGON_WITNESS_MODE=canonical go test -count=1 -timeout 40m
  -run 'TestExecutionSpecWitness/for_amsterdam' ./execution/tests/eest_zkevm_witness/...`
  (default 600s timeout false-fails the large corpus).
- **Acceptance (manual, Post-Completion)**: oracle unique-set diff, all fields under=0 over=0.

## Progress Tracking
- `[x]` when done; ➕ new tasks; ⚠️ blockers; keep in sync.

## Solution Overview
- **#1 keys**: `RecordingState.accountExists(addr)` — overlay non-nil, else not in
  `DeletedAccounts` and `rs.inner.ReadAccountData` non-nil (use the *inner* reader, mirroring
  L875, so existence checks don't re-mark `AccessedAccounts`). In `collectAccessedState`,
  add a 20B address to `WitnessKeys` only if it exists. Slots and the unique global-dedup
  set unchanged.
- **#2 codes**: narrow the `emptyCodeAccessed` trigger to when empty bytecode is
  materialized for execution (a code-load path), not every empty-code account data-read.
  Tune until legacy emits exactly one empty `0x` and canonical is unchanged.
- **#3 state** (mode-gated legacy; canonical byte-identical): (a) gate the
  `detectCollapseSiblings` `childCount>=2` drop on `mode==canonical` (legacy keeps the
  siblings); (b) materialize per-account storage-root nodes in the builder — `0x80` when
  `storageRoot==emptyRoot`, else resolve the storage-root node and attach it instead of the
  `NewHashNode` at L1434; (c) include legacy empty nodes. Guard every change with the
  invariant that the witness `RootHash()` is **unchanged** vs canonical.
- **#4 mode param**: optional `mode *string` 2nd param on `ExecutionWitness`;
  `resolveWitnessMode(mode)` precedence param > env > legacy (reject unknown). Thread the
  resolved mode end-to-end (`collectAccessedState`, `detectCollapseSiblings`,
  `sdCtx.Witness`→`GenerateWitness`).

## Technical Details
- Existence (Gap 1) covers in-block created (overlay non-nil → exists), deleted
  (`DeletedAccounts` → not exists), created-then-deleted (not exists). Note: an extra
  `inner.ReadAccountData` per read-only accessed address — acceptable.
- Mode threading default (Gap 4): the 3 non-`debug_executionWitness` `.Witness()` callers
  (`eth_getProof` ×2, `computeWitness`) must pass the mode that reproduces today's output.
  Determine which (canonical vs legacy) their current `GenerateWitness` output corresponds to
  and pass it explicitly; do not change their behavior.
- Thread mode through as a **no-op first** (canonical and legacy identical until Gap 3
  lands) so each task compiles without unused-variable lint.
- `mode` JSON values lowercase `"legacy"`/`"canonical"`; absent ⇒ env default.

## What Goes Where
- **Implementation Steps**: the four code changes + their automated gates + canonical corpus.
- **Post-Completion**: manual oracle acceptance (live node); revert temporary corpus merge;
  extract clean PR off `main`.

## Implementation Steps

### Task 1: Keys — emit address keys only for existing accounts

**Files:**
- Modify: `rpc/jsonrpc/debug_execution_witness.go`
- Modify/Create: `rpc/jsonrpc/debug_execution_witness_test.go` (or nearest existing jsonrpc test)

- [ ] write a failing unit test for `RecordingState.accountExists`: cases exists / deleted /
      nonexistent (nil inner) / created-in-block (overlay) / created-then-deleted
- [ ] add `RecordingState.accountExists(addr) bool`: overlay non-nil; else not in
      `DeletedAccounts` AND `rs.inner.ReadAccountData(addr)` non-nil (use the inner reader,
      mirror L875)
- [ ] in `collectAccessedState`, gate 20B address entries in `WitnessKeys` on
      `accountExists`; leave 32B slots and the unique global-dedup set unchanged
- [ ] tests pass; build + `make lint` clean

### Task 2: Codes — narrow the empty-`0x` trigger

**Files:**
- Modify: `rpc/jsonrpc/debug_execution_witness.go`
- Modify: `rpc/jsonrpc/debug_execution_witness_test.go`

- [ ] write a failing unit test asserting the empty-`0x` entry appears only when empty
      bytecode is materialized for execution (not on a plain empty-account data read)
- [ ] move/narrow the `emptyCodeAccessed` trigger from `ReadAccountData` to the
      execution code-load path; canonical unaffected
- [ ] tests pass; build + `make lint` clean

### Task 3: State — legacy node materialization (mode-gated)

**Files:**
- Modify: `execution/commitment/hex_patricia_hashed.go`
- Modify: `execution/commitment/commitmentdb/commitment_context.go`
- Modify: `rpc/jsonrpc/debug_execution_witness.go`
- Modify: `execution/commitment/hex_patricia_hashed_test.go`

- [ ] thread `witnessMode` (legacy/canonical) from the producer through `sdCtx.Witness`
      into `GenerateWitness`/`toWitnessTrie`; pass canonical from the 3 non-witness
      `.Witness()` callers (`eth_call.go:500,556,755`) so they are byte-unchanged
- [ ] gate the `detectCollapseSiblings` `childCount>=2` drop (L1049-64) on
      `mode==canonical`; legacy keeps the collapse siblings
- [ ] **first verify** whether the storage-root node bytes are resident in the builder for
      an untouched-storage account; if not, resolve them (read the storage-root branch via
      the trie context) — document which path is used
- [ ] in legacy: for each witnessed account emit its storage-root node — `0x80` when
      `storageRoot==emptyRoot`, else the resolved storage-root node instead of
      `trie.NewHashNode(storageRootHash)` (L1434)
- [ ] in legacy: include empty nodes that canonical prunes
- [ ] **invariant**: assert the witness `RootHash()` is identical in legacy and canonical
      (these changes add nodes, never change the root) — fail loudly otherwise
- [ ] keep the canonical path byte-identical (no diff when `mode==canonical`)
- [ ] extend `Test_WitnessTrie_GenerateWitness` (run **without** `-short`) with a legacy
      case asserting the storage-root node is materialized for an untouched-storage account
- [ ] run `verifyWitnessStateless` in legacy mode (necessary state-root gate); build +
      `make lint` clean

### Task 4: Mode RPC parameter (API parity)

**Files:**
- Modify: `rpc/jsonrpc/debug_execution_witness.go`

- [ ] add optional `mode *string` 2nd parameter to `ExecutionWitness` (lowercase
      `"legacy"`/`"canonical"`); confirm the RPC registration accepts an optional trailing param
- [ ] change `resolveWitnessMode(mode *string)`: precedence param > env > legacy; reject
      unknown values with a clear error
- [ ] thread the resolved mode into `collectAccessedState` (done) and the builder path
      (Task 3); no-arg call still defaults to env/legacy
- [ ] build + `make lint` clean; manual sanity: `mode=canonical` reproduces canonical output

### Task 5: Canonical corpus regression
**Files:** (no source changes)
- [ ] `make erigon integration` + `make lint` clean
- [ ] `ERIGON_WITNESS_MODE=canonical go test -count=1 -timeout 40m -run
      'TestExecutionSpecWitness/for_amsterdam' ./execution/tests/eest_zkevm_witness/...` —
      no new failures vs baseline

### Task 6: Verify acceptance criteria (code-level)
- [ ] keys emit only existing-account addresses; unit test green
- [ ] codes legacy empty `0x` emitted once; unit test green; canonical codes unchanged
- [ ] legacy state materializes storage-root + empty + sibling nodes; `RootHash()` unchanged;
      `Test_WitnessTrie_GenerateWitness` (no `-short`) + legacy `verifyWitnessStateless` green
- [ ] `mode` param overrides env; no-arg defaults preserved
- [ ] canonical corpus green; build + lint clean

### Task 7: [Final] Documentation and plan close-out
- [ ] update the witness memory note with outcomes
- [ ] move this plan to `docs/plans/completed/`

## Post-Completion
*Manual / external — no checkboxes*

**Oracle acceptance (manual, human — needs the live mainnet node):**
- Rebuild, restart the node (`--datadir ~/dev --chain mainnet --prune.mode=full
  --prune.include-commitment-history --http --http.api=eth,debug,erigon,web3,net,trace`),
  wait for witness-readiness (poll a real `debug_executionWitness` call), re-capture
  `erigon-accept/` for blocks 25230831–850 (C-style bash loop, not `seq`; absolute paths;
  retries), unique-set diff vs `reth-legacy/`. Assert every field under=0 AND over=0 across
  the 16 blocks (exclude the 4 flaky). Spot-check `mode=canonical` reproduces canonical.

**Branch hygiene**
- Revert the temporary corpus merge (commit `d988f667`).
- Extract the witness-mode work to a clean PR off `main`.
