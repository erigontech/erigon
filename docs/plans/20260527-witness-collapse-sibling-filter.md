# Collapse-Sibling Output Filter for debug_executionWitness

## Overview

`debug_executionWitness` currently emits **more trie nodes than Geth** for the same
block. Per the analysis in erigontech/erigon#21312, the residual extra nodes are all
**sibling collapse nodes** — storage or account leaf nodes at full-depth (128-nibble)
paths that Erigon's HexPatriciaHashed (HPH) witness builder emits as explicit entries
so the parent branch hashes correctly. Geth's standard MPT keeps the sibling hash
**inline** inside the parent branch RLP and never emits the sibling leaf as a separate
witness node. Across the issue's test table Erigon is +1..+18 nodes vs Geth.

This change removes the **redundant** collapse-sibling nodes from the witness output so
Erigon's `state` array matches Geth's, **without** weakening the witness: a node is only
dropped when stateless re-execution still reproduces the block's state root with it
removed. We do **not** touch the HPH conversion (`toWitnessTrie`) — the fix is a filter
at the output boundary plus the existing stateless-verification guardrail run on the
filtered set.

This is **Change 1 of 3** separate deliverables:
1. zkevm witness test suite (separate branch `awskii/eest-zkevm-witness`, not a dependency here)
2. **this change** — collapse-sibling output filter
3. Amsterdam BAL access-set provider (separate plan)

Scope is strictly the filter + its in-tree test. No BAL/Amsterdam work, no changes to the
re-execution loop.

## Context (from discovery)

- **Branch:** `awskii/witness-collapse-sibling-filter` off `main`, worktree `/Users/awskii/org/wrk/erigon-exec-witness`.
- **Handler:** `rpc/jsonrpc/debug_execution_witness.go`
  - `ExecutionWitness` (~L524): re-executes block via `RecordingState`, collects access set, then:
    - `detectCollapseSiblings` (~L891, STEP 1): commitment fold/unfold over a split reader; records every collapse path via `sdCtx.SetCollapseTracer(...)` into `siblingPaths`.
    - `buildWitnessTrie` (~L945, STEP 2): re-seeks parent state, `touchAll` + `TouchHashedKey(siblingPath)` for each sibling path, calls `sdCtx.Witness(...)` → `witnessTrie.RLPEncode()`; returns `encodedNodes`. Result assigned to `result.State` (~L695).
  - `verifyWitnessStateless` (~L1087): decodes `result.State`, re-executes statelessly, asserts resulting root == `block.Root()`. Gated off by `ERIGON_WITNESS_NO_VERIFY=true`. **Already runs on `result.State` after it is set**, so filtering before this point means the guardrail validates the filtered set for free.
- **Witness trie / encoding:** `execution/commitment/trie/trie.go`
  - `(*Trie).RLPEncode()` (L1450): DFS over merged trie, hashes each node, **dedups by keccak hash** (`seen` map), appends RLP for `ShortNode`/`DuoNode`/`FullNode` and `AccountNode.Storage`; `ValueNode`/`HashNode` emit nothing. Output is a flat hash-deduped `[][]byte` in **DFS order, root first**.
  - `RLPDecode` (L1542): inverse, used by the stateless verifier. **Hard invariant:** `encodedNodes[0]` must be the root (`rootHash := crypto.Keccak256Hash(encodedNodes[0])`, ~L1559). Any filter must preserve slice order and never drop/move element 0.
- **HPH witness build:** `execution/commitment/hex_patricia_hashed.go` — `GenerateWitness` → `toWitnessTrie`. **Not modified by this change.**
- **Existing test harness:** `rpc/jsonrpc/debug_api_test.go`:
  - `TestExecutionWitness` (~L754) is the **canonical commitment-history setup** to copy: `statecfg.EnableHistoricalCommitment()` + `rawdb.WriteDBCommitmentHistoryEnabled(tx, true)`.
  - **It uses `rpcdaemontest.CreateTestExecModule` which inserts a FIXED canned 13-block chain** (`cmd/rpcdaemon/rpcdaemontest/test_util.go:127`, generator hard-coded ~L181) — it CANNOT author a controlled collapse. Use a bespoke-chain variant instead: model on `CreateTestExecModuleForTraces` / `...Collision` (test_util.go ~L463 / ~L573), or build directly via `execmoduletester.New(...)` + `blockgen.GenerateChain` with a caller-supplied block closure.
  - The zkevm runner (`execution/tests/eest_zkevm_witness/...`) lives on the separate `awskii/eest-zkevm-witness` branch and is **not present in this worktree** — do not grep for it; it is a local cross-check only.

## Development Approach

- **Testing approach:** TDD (per repo CLAUDE.md) — failing test first, for the right reason, then minimal fix. **Never add a `t.Skip`** (forbidden for agents).
- Complete each task fully (tests passing) before the next.
- Small, focused changes. No comments unless an invariant the types don't enforce.
- `make lint` after changes (non-deterministic — run until clean); `make erigon integration` before done.
- Commit prefix: `rpc/jsonrpc: ...`.

## Testing Strategy

- **In-tree unit tests (the merge oracle, on `main`, no zkevm-suite dependency):**
  - account-trie 2→1 collapse → redundant sibling absent, exact multiset, verify passes;
  - storage-trie 2→1 collapse → same (different path/parent shape, not interchangeable);
  - no-collapse block → filter is a strict no-op (golden output unchanged);
  - load-bearing sibling → specific sibling hash **present**, verify passes (over-filter guard).
  None set `ERIGON_WITNESS_NO_VERIFY`. Exact multiset assertions carry the minimality claim
  (verify only proves safety, not minimality). This is what CI runs.
- **Local cross-check (NOT in the PR):** run the zkevm suite
  (`execution/tests/eest_zkevm_witness`) from a *scratch* layering `main + this change +
  awskii/eest-zkevm-witness` to confirm exact `state` multiset parity on Amsterdam fixtures.
  The zkevm branch must never become a dependency of this PR.
- No e2e/UI tests in this repo.

## Progress Tracking

- Mark `[x]` immediately when done. New tasks ➕, blockers ⚠️. Keep this file in sync.

## Solution Overview

Filter the encoded node list inside `buildWitnessTrie`, where both the built `witnessTrie`
and `siblingPaths` are in scope:

1. Build the witness trie exactly as today.
2. Compute the **candidate** set: the RLP nodes that correspond to the collapse
   `siblingPaths` (walk the trie to each path, hash that node — it is in the encoded set
   because it was force-touched via `TouchHashedKey`).
3. Determine which candidates are **redundant** — their content is not required for a
   standard-MPT verifier to reproduce the post-state because the parent branch already
   carries the sibling's 32-byte hash inline and the branch does not actually collapse to a
   single child during stateless re-execution. Drop only those.
4. Return the reduced node list, **preserving DFS order with the root at index 0**.
   `verifyWitnessStateless` then runs on the reduced `result.State` (existing call, no
   relocation needed) and asserts the root still matches.

**Guardrail asymmetry (important):** the verifier re-roots by doing real deletes
(`s.t.Delete`/`DeleteSubtree`) then `s.t.Hash()` (debug_execution_witness.go ~L1703-1802), so
a wrongly-dropped *load-bearing* sibling fails verification loudly. But verify only catches
**over-filtering** (dropped a needed node). It does **not** catch **under-filtering** (kept a
redundant node → verify still passes). So the verify guardrail proves *safety*, not
*minimality*. The node-count/Geth-parity goal is enforced **solely** by the tests' exact
multiset assertions — which therefore must cover the relevant collapse shapes.

The precise redundancy criterion is the genuinely hard part (a real 2→1 collapse *does* need
its surviving sibling). It is derived and **written down in Task 2 as an explicit decision**
before any filter code is written, tied to how the stateless verifier re-roots, and stated
with the set of collapse shapes it is proven against. We prefer a **sound structural
criterion** over drop-and-retry so the result is deterministic and verify stays a pure
assertion.

## Technical Details

- `siblingPaths [][]byte` are hashed-key nibble paths (1..128 nibbles) from the collapse
  tracer. **Two distinct shapes, handled differently:**
  - *Account-trie sibling*: path at `addrHash` depth, a leaf in the main trie.
  - *Storage-trie sibling*: path at full `addrHash||slotHash` depth, living under an
    `AccountNode.Storage` subtrie (`RLPEncode` recurses through the `AccountNode` case,
    trie.go:1513-1518). The path→node walk and the "which branch is the parent" question
    differ from the account case. Both must be covered by tests; they are NOT interchangeable.
- A node is identified in `result.State` by `keccak256(nodeRLP)` (the dedup key in
  `RLPEncode`). The filter works in hash space: for each sibling path, resolve the node at
  that path in `witnessTrie`, hash it, and decide redundancy via the Task-2 criterion.
- **Ordering invariant:** the filter MUST preserve slice order and MUST NOT drop element 0
  (the root). Implement by computing a drop-set of hashes, then rebuilding the slice in the
  original order skipping dropped hashes — never via map iteration. Assert the root hash
  survives.
- **No-collapse fast path:** when `siblingPaths` is empty the filter is a strict no-op and
  output must equal today's byte-for-byte (regression-guarded by a test).
- Filtering lives inside `buildWitnessTrie` (both `witnessTrie` and `siblingPaths` in scope),
  before the `encodedNodes` slice is returned.

## What Goes Where

- **Implementation Steps** (checkboxes): the failing test, the filter, wiring, validation, docs.
- **Post-Completion** (no checkboxes): zkevm scratch-layering cross-check, PR open.

## Implementation Steps

### Task 1: Bespoke-chain test harness + deterministic collapse fixtures (RED)

**Files:**
- Modify: `rpc/jsonrpc/debug_api_test.go`
- Possibly Modify: `cmd/rpcdaemon/rpcdaemontest/test_util.go` (add a custom-chain constructor if none fits)

- [x] Stand up a witness test that authors its **own** chain (the canned
      `CreateTestExecModule` 13-block chain cannot trigger a controlled collapse). Reuse the
      bespoke-chain pattern from `CreateTestExecModuleForTraces`/`...Collision`, or
      `execmoduletester.New(...)` + `blockgen.GenerateChain` with a block closure. Copy the
      commitment-history setup verbatim from `TestExecutionWitness` (~L754):
      `statecfg.EnableHistoricalCommitment()` + `rawdb.WriteDBCommitmentHistoryEnabled`.
      → `rpc/jsonrpc/debug_witness_collapse_test.go`, helper `newWitnessTester`.
- [x] **Deterministic shared-prefix key selection:** pick two addresses (account case) and two
      storage slots (storage case) whose **keccak hashes share a leading nibble** so they sit
      under one branch. Brute-force/search candidate keys in the test setup and assert the
      shared-prefix precondition (so the test is self-validating, not relying on luck).
      → `findSubtreeSiblingAddresses` / `findSubtreeSiblingHashes`; self-validating via
      `require` on the divergence nibble. Address search starts above the precompile range
      (≥0x100000) — initial attempt landed on `0x05` (modexp precompile), so the call ran the
      precompile instead of the contract's selfdestruct and no deletion occurred.
- [x] `TestExecutionWitnessCollapseSiblingAccount`: block selfdestructs one account in a
      2-child account-trie branch whose **other child is a 2-account subtree** → branch
      collapses onto the subtree (subtree root needed only by hash = redundant).
- [x] `TestExecutionWitnessCollapseSiblingStorage`: contract with three slots forming a
      2-child storage branch (one slot vs a 2-slot subtree); block zeroes the lone slot →
      branch collapses onto the subtree.
- [x] For each: assert `verifyWitnessStateless` passes (block valid) **without** setting
      `ERIGON_WITNESS_NO_VERIFY`, and assert the witness is **minimal** via a criterion-free
      probe (`probeRedundantNodes`: a node is redundant iff removing it still re-roots to the
      block root) plus an exact node-count pin. Buggy counts recorded below.
- [x] `TestExecutionWitnessNoCollapseUnchanged`: a block with no deletes (plain value
      transfer) → witness already minimal; filter must be a strict no-op.
- [x] Run the new tests — confirm RED on the collapse cases for the right reason; the no-op
      case should already pass. **Confirmed:** account RED (removable node `[5]`, 6→5),
      storage RED (removable node `[6]`, 8→7), no-collapse PASS.

➕ **Key finding (feeds Task 2 & Task 4):** a plain **2-leaf** 2→1 collapse's surviving
  sibling is **load-bearing**, NOT redundant — the standard verifier needs the sibling leaf's
  preimage to merge it up on collapse (probe finds zero removable nodes). The redundant
  collapse-sibling (the actual #21312 bug) only appears when the surviving sibling is a
  **subtree** (≥2 children) referenced by hash, so its preimage is never descended into. Task 1
  fixtures therefore use subtree siblings. The 2-leaf shape is the **load-bearing** counter-case
  for Task 4.

➕ **Oracle choice:** instead of a hardcoded golden multiset, minimality is asserted with a
  criterion-free removal probe (remove each non-root node, re-run `execBlockStatelessly`, a
  removal that still reproduces the root means the node was redundant). This is robust and
  directly encodes the Geth-parity/minimality claim; the verifier may *panic* on a missing
  load-bearing node, so the probe recovers and treats a panic as "needed". Exact counts (5/7)
  are also pinned.

### Task 2: Derive and document the redundancy criterion (decision, before any filter code)

**Files:**
- Modify: `docs/plans/20260527-witness-collapse-sibling-filter.md` (record the decision here)

- [x] Using the RED fixtures + reading the stateless re-root path
      (`witnessStateless.Finalize` → `Delete`/`DeleteSubtree` → `Hash`, ~L1703-1802), write
      down the exact rule for "redundant sibling = safe to drop." Candidate (from initial
      discovery): *the sibling's parent branch retains ≥2 live children after the block's
      deletes (no structural collapse).* **This candidate was wrong** — it described the
      no-collapse case (the tracer never even fires there). The correct, code-grounded rule is
      below (DECISION).
- [x] State explicitly which collapse shapes the rule is **proven** against
      (account 2→1, storage 2→1) and which are **out of proven scope** (cascading/multi-level
      collapse, 3→2 branch shrink that is not a collapse). Document cascading collapse as a
      known limitation if not covered by a test, with the conservative behavior (keep the
      sibling when unsure — safety over minimality). → see DECISION §"Proven scope".
- [x] Confirm the rule is computable from `witnessTrie` + `siblingPaths` alone (no extra
      re-execution). → see DECISION §"Computability".

---

#### DECISION — redundancy criterion (proven against account 2→1 and storage 2→1)

**The rule.** A collapse-sibling node is **redundant (safe to drop)** iff the node it resolves
to at its `siblingPath` in the witness trie is a **branch node** (`*FullNode` or `*DuoNode`,
i.e. ≥2 children). It is **load-bearing (must keep)** in every other case — when the path
resolves to a `*ShortNode` (leaf or extension), an `*AccountNode`, or a `ValueNode`.

**Why this is exactly the right line — tied to how the verifier re-roots.** The stateless
verifier's `Finalize` (debug_execution_witness.go ~L1703-1802) applies the block's deletes via
`s.t.Delete` / `s.t.DeleteSubtree` and then calls `s.t.Hash()`. Those deletes run the standard
MPT collapse in `execution/commitment/trie/trie.go`: when a `FullNode`/`DuoNode` drops to a
single remaining child, the trie calls `convertToShortNode(child, pos)` (trie.go:1056). That
function inspects the surviving child **only** through one type assertion:

```go
if short, ok := child.(*ShortNode); ok {
    // merge: prepend the collapse nibble to short.Key — READS the ShortNode preimage
    return NewShortNode(append([]byte{pos}, short.Key...), short.Val)
}
// child is a branch (FullNode/DuoNode) or a HashNode: wrapped by reference, NEVER inspected
return NewShortNode([]byte{pos}, child)
```

- Surviving sibling is a **branch**: after `RLPDecode` a dropped subtree-root appears as a bare
  `HashNode`; a kept one appears as a `FullNode`/`DuoNode`. Neither is a `*ShortNode`, so
  `convertToShortNode` takes the second return and folds the **32-byte hash inline** without
  ever reading the subtree's contents. Its preimage is therefore never needed → **droppable**.
- Surviving sibling is a **ShortNode** (leaf or extension): `convertToShortNode` merges by
  reading `short.Key`/`short.Val`, so the preimage **must** be present → **keep**. A bare
  `HashNode` here would make the verifier fail/panic — exactly the over-filter the guardrail
  catches.

This matches the Task 1 probe ground truth: the subtree-root node is removable (account `[5]`,
storage `[6]`), while a plain 2-leaf collapse's surviving leaf is **not** removable.

**Mapping `siblingPath` → node.** `siblingPath` is built in HPH as
`currentKey[:depth] + survivingNibble + survivingCell.hashedExtension`
(hex_patricia_hashed.go:2394 / :2418). A leaf survivor carries a full-depth `hashedExtension`,
so its path reaches terminal depth and resolves to a `ShortNode`/`AccountNode`/`ValueNode`. A
branch survivor's path stops at the branch depth and resolves to a `FullNode`/`DuoNode`. So
path length corroborates the type, but the **node type is the criterion** — do not key off
length. The node is guaranteed present in `witnessTrie` because STEP 2 force-touches it via
`TouchHashedKey(siblingPath)` before `RLPEncode`.

**Computability.** Decidable from `witnessTrie` + `siblingPaths` alone, **no extra
re-execution**: for each `siblingPath`, descend the witness trie along the nibble path, read the
concrete node type at that path, and drop its keccak hash iff it is a `*FullNode`/`*DuoNode`.
The verify guardrail (already running on the filtered `result.State`) remains a pure
post-condition that catches any over-drop.

**Proven scope.**
- ✅ Account-trie 2→1 collapse onto a branch subtree — `TestExecutionWitnessCollapseSiblingAccount`.
- ✅ Storage-trie 2→1 collapse onto a branch subtree — `TestExecutionWitnessCollapseSiblingStorage`.
- ✅ Load-bearing counter-case (surviving sibling is a `ShortNode` leaf) — kept; Task 4 pins it.
- ✅ No-collapse block — tracer never fires, `siblingPaths` empty, strict no-op.

**Out of proven scope (conservative behavior = keep).**
- **Cascading / multi-level collapse:** `detectCascadingCollapseAtRow` (hex_patricia_hashed.go:2412)
  also emits sibling paths for collapses propagated upward by `fold()`. The branch-vs-ShortNode
  rule is applied **per sibling path** and is inherently conservative — a cascading survivor
  that is a `ShortNode` is automatically kept — but this shape is **not** yet covered by a test,
  so it is a documented known limitation. Safety over minimality holds: an over-drop is still
  caught by verify, and the rule only drops unambiguous branch nodes.
- **Extension-node survivor:** a `ShortNode` whose `Val` is a branch (not a leaf) is treated as
  **load-bearing** (the rule only drops `FullNode`/`DuoNode`), which is correct — `convertToShortNode`
  merges its key.
- **3→2 branch shrink (not a collapse):** the tracer fires only when `afterMap` has exactly one
  remaining child (`bits.OnesCount16 == 1`, hex_patricia_hashed.go:2207); a 3→2 shrink keeps a
  `DuoNode` and emits **no** sibling path, so it never reaches the filter. Not applicable.

### Task 3: Implement the collapse-sibling filter in buildWitnessTrie

**Files:**
- Modify: `rpc/jsonrpc/debug_execution_witness.go`

- [x] Add a helper (one-line docstring) that, given `witnessTrie`, `siblingPaths`, and the
      encoded node slice, returns the reduced slice. Map each sibling path → node → keccak
      hash; apply the Task-2 criterion to build a **drop-set of hashes**.
      → `filterRedundantCollapseSiblings` (debug_execution_witness.go); node→hash via new
      `(*trie.Trie).NodeHash` so the dedup key matches `RLPEncode`.
- [x] Rebuild `encodedNodes` in original DFS order skipping dropped hashes; **never drop
      index 0**; assert the root hash survives. No map-iteration ordering. → drop-set excludes
      `keccak(encoded[0])`; output rebuilt by iterating `encoded` in order.
- [x] Wire it into `buildWitnessTrie` before returning; leave `verifyWitnessStateless`
      untouched so it validates the filtered `result.State`. A verify failure is a hard error
      — no silent fallback to the unfiltered set.
- [x] Run Task 1 collapse tests — confirm GREEN (minimal set, verify passes). Account 6→5,
      storage 8→7; `probeRedundantNodes` now empty for both.
- [x] Run `TestExecutionWitnessNoCollapseUnchanged` — still a no-op.

### Task 4: Over-filtering counter-test (load-bearing sibling retained)

**Files:**
- Modify: `rpc/jsonrpc/debug_api_test.go`

- [ ] Construct/identify a case where the surviving sibling **is** load-bearing (genuine 2→1
      collapse whose promotion needs the preimage). Assert the **specific** sibling hash is
      **present** in `state` (exact multiset, not just "verify passed") and verify passes.
      This makes a degenerate "drop nothing" or "drop everything" filter fail.
- [ ] Run all witness tests — green.

### Task 5: Refactor and verify acceptance criteria

**Files:**
- Modify: `rpc/jsonrpc/debug_execution_witness.go`

- [ ] Tidy the helper; no scenario comments; consistent naming.
- [ ] Pre-Amsterdam `TestExecutionWitness` (all existing sub-cases) still pass unchanged.
- [ ] All new tests green: account collapse minimal, storage collapse minimal, no-op
      unchanged, load-bearing retained.
- [ ] `make lint` (repeat until clean — non-deterministic).
- [ ] `make erigon integration` builds.
- [ ] `go test ./rpc/jsonrpc/...` and `go test ./execution/commitment/...` green.

### Task 6: [Final] Docs and plan housekeeping
- [ ] Update CLAUDE.md / package notes only if a genuinely new, non-obvious pattern emerged
      (default: no change).
- [ ] Move this plan to `docs/plans/completed/`.

## Post-Completion
*Manual / external — no checkboxes*

**Local cross-check (not part of the PR):**
- Create a scratch worktree layering `main + this change + awskii/eest-zkevm-witness`; run
  `execution/tests/eest_zkevm_witness` (`make test-fixtures-zkevm` first) and confirm exact
  `state` multiset parity on Amsterdam fixtures. Record the before/after node counts.

**PR:**
- Open against `main`. Title: `rpc/jsonrpc: filter redundant collapse-sibling nodes from executionWitness`.
- Body links erigontech/erigon#21312 and notes the verify-guardrail correctness argument.
- Note: full Amsterdam node-exactness also needs Change 3 (BAL access set); this change is
  independent and lands on its own.
