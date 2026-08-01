# debug_executionWitness for the EIP-8297 binary trie

## Overview

`debug_executionWitness` currently refuses the bin commitment variant. This plan implements it:
the same pipeline the hex trie uses, with a different collector.

The value is measurement. Binary-trie witness sizes are currently argued from estimates; a working
API turns them into numbers taken from real blocks.

The binary trie makes the collector simpler than hex's:

- Every branch has exactly two children, both hashes always present in the node preimage. There is
  no sibling materialization problem.
- There is no extension node, so no exclusion-proof / divergence-capture machinery.
- There is no deletion, so the whole collapse-sibling detection phase has no trigger.

It is *not* simpler on one axis the first draft of this plan missed: the acceptance gate needs a
**post**-state root, which means a mutable trie over blinded nodes, not just a read-only walk.
Task 5 exists for that.

## Context (from discovery)

Verified by survey and by an adversarial plan review; every citation below was checked against the
tree at `ff808c2334a`.

**Reusable unchanged**
- `execution/commitment/witness_node_set.go:26-58` — `witnessNodeSet` is hash-agnostic (`byHash`
  map, `onNode(rlp, hash)`), never decodes. Use as-is.
- `rpc/jsonrpc/debug_execution_witness.go:45,607,1064` — `RecordingState`, `buildAccessedState`,
  `collectAccessedState` touch no commitment code. Reusable verbatim.

**The blocker**
- `execution/commitment/commitmentdb/commitment_context.go:353-359` — `witnessCapture` type-asserts
  `*commitment.HexPatriciaHashed`. Consumers: `WitnessNodes` (:364), `Witness` (:379),
  `WitnessLean` (:408).

**Where the collector goes**
- The node preimages exist **only inside** `pbinHasher.branchHash` (`pbin_hash.go:106-111`) and
  `leafCellHash` (`:134-158`). Neither returns its buffer, and `foldBranch`
  (`pbin_patricia_hashed.go:640-676`) receives only the resulting hash. The tap therefore belongs in
  `pbinHasher`, mirroring where hex puts it — not in `foldBranch`.
- Nodes are also hashed outside `foldBranch`: sibling cells via `hashRowCell` → `cellHash`
  (`pbin_patricia_hashed.go:738-763`) and the root cell via `RootHash()` (`:299`) when no branch
  fold ran. A tap in `pbinHasher` catches all three; a tap in `foldBranch` would miss two.
- Preimages: branch `0x01 || encode_bit_prefix(prefix) || left(32) || right(32)`; leaf
  `0x00 || packed_key || value(32)`. An absent child is `pbinEmptyTreeHash` (32 zero bytes) in a
  fixed slot, never omitted.
- `execution/commitment/hex_patricia_hashed.go:2414` — the signature to mirror; `witnessTracer`
  interface at `:60-63`.

**Missing prerequisite**
- There is no bin decoder. `trie.RLPDecode`, `trie.WitnessNodesForKeysFromNodes`
  (`execution/commitment/trie/proof.go:216`), `Trie.Get/GetAccount/Prove` are keccak+RLP+MPT-shaped
  and cannot consume bin nodes.
- Hex gets its post-state root from a fully **mutable** in-memory MPT over blinded nodes —
  `witnessStateless.Finalize()` (`debug_execution_witness.go:1933-2033`) calls `UpdateAccount`,
  `Update`, `Delete`, `DeepHash`, then `Hash()` at `:2032`. Bin has no equivalent. See Task 5.

**provedKeys are not the HashSort key stream**
- Hex collects proved keys in the `HashSort` callback (`hex_patricia_hashed.go:2421`). Under bin,
  `pbinUpdateStream.processKey` (`pbin_update_stream.go:93-124`) expands **one** account touch into
  BASIC_DATA + CODE_HASH + N code-chunk leaves, none of which appear in `HashSort`'s stream.
  Collect provedKeys at the **emit sink**, or pruning silently drops every code leaf.

**Must not be reached under bin**
- `commitment_context.go:436` `SetCollapseTracer` — **panics**.
- `commitment_context.go:448` `BranchChildCount` — errors; canonical-mode only.
- HAZARD (mechanism confirmed, behaviour not run): `sdCtx.TouchHashedKey(siblingPath)` stores an
  empty plain key, and `pbin_update_stream.go:225` `stateOf("")` falls through to a zero-length
  storage read. Whether the domain getter errors on a zero-length key was not verified — treat as a
  hazard to guard, not as established behaviour.

**Facts the first draft got wrong — do not reintroduce**
- `verifyWitnessStateless` is **not** unconditionally on: `debug_execution_witness.go:1459` returns
  early when `ERIGON_WITNESS_NO_VERIFY=true`.
- `rpc/jsonrpc/pbin_hex_only_test.go:59,63` assert `GetProof` and `SimulateV1` refuse bin. Both are
  **correct and must stay**. Likewise `db/state/execctx/pbin_options_test.go:79-80`
  (`TestPBinHexOnlyCommitmentRefusesBin`). Neither is a "refusal test that now asserts the wrong
  thing".
- `WithHexCommitmentOnly` (`db/state/execctx/options.go:70-75`) also applies
  `WithoutParallelCommitment()`. Removing it outright re-admits `VariantParallelHexPatricia` on the
  hex witness path, which `witnessCapture` cannot serve (see the comment at
  `debug_execution_witness.go:900-902`).
- `resolveWitnessMode` (`:590-596`) returns `witnessModeLegacy` for a nil or empty mode — legacy
  **is** the default. Rejecting "a legacy request" under bin would reject every request.
- The stale "witness is hex-only" prose lives at `db/state/execctx/options.go:66-69` and
  `node/eth/backend.go:377`. The `pbin_patricia_hashed.go` package doc does **not** mention witness.

**In-repo oracle**
- `execution/commitment/pbin_oracle_test.go` — an independent EIP-8297 transcription
  (`pbinOracleInsert`:93, `pbinOracleMerkelizeWith`:172) hashing via `x/crypto/sha3`. It takes full
  key/value pairs and has no blinded-child concept, so it is used for permutation independence, not
  as a witness decoder.

## Development Approach

- **testing approach**: Regular (code first, then tests) — set deliberately for this plan. The work
  is mostly a new collector over an engine whose behaviour is already pinned by 67 passing EEST
  fixtures, so the tests are characterisation rather than specification. The acceptance gate
  (Task 9) is the real specification and is written before the code it gates in Task 5.
- complete each task fully before moving to the next
- **every task MUST include new/updated tests**, listed as separate checklist items
- **all tests must pass before starting the next task** — no exceptions
- update this plan file when scope changes
- the hex witness path must stay byte-identical after every task

## Testing Strategy

- **unit tests**: required per task
- **acceptance gate**: there is NO external oracle for binary-trie witnesses; EEST publishes no
  binary-tree witness fixtures. The gate is that a stateless validator re-executes the block from
  the witness alone and reproduces the **post**-state root. A witness that fails must NEVER be
  returned.
- **strict by default**: for bin an unresolved node is unambiguous (a hash you don't have, versus
  `pbinEmptyTreeHash`). Bin deliberately diverges from hex here: hex's strictness is opt-in via
  `WITNESS_STRICT_VERIFY` and its whole verify step is skippable via `ERIGON_WITNESS_NO_VERIFY`
  (`:1459`). Under bin, strict is the only mode and the verify step is not skippable. Record this
  divergence in the code.
- no CI shard in scope; in-repo Go tests are the entire gate.

## Progress Tracking

- mark completed items `[x]` immediately
- add newly discovered tasks with ➕
- document blockers with ⚠️

## Solution Overview

1. A witness tracer tap **in `pbinHasher`**, emitting each node's exact consensus preimage and hash.
2. A `Witnesses` method on `PBinPatriciaHashed` matching the hex signature, collecting provedKeys at
   the update-stream emit sink and reproducing the **parent** root.
3. An interface in place of the `*HexPatriciaHashed` type assertion.
4. A bin node-set decoder (preimage → node → re-merkelize).
5. A witness-backed `PatriciaContext` so `PBinPatriciaHashed` itself serves as the mutable trie for
   the post-state root — rather than writing a second, independent mutable binary trie.
6. Pruning, guards, stateless reader, verify gate.
7. Un-refusal of the witness path **last**, once everything behind it works.

`produceExclusionProofs` is accepted and ignored under bin — there is no extension node for it to
act on. Bin has one witness mode; the legacy/canonical split is hex-specific.

## Technical Details

**Decoder contract**
- input: `[][]byte` node preimages, root first (the `witnessNodeSet.nodes(root)` contract)
- a child hash with no matching preimage is *blinded* — legal, opaque during re-merkelize
- a blinded child on a path a proved key must traverse is an error (strict)

**Mutable trie strategy (Task 5)** — chosen over writing a second binary trie implementation:
convert each decoded branch preimage into the branch record format `pbinBranchEncoder` produces,
serve them from an in-memory `PatriciaContext` (`Branch`/`PutBranch`/`Account`/`Storage`, plus the
unexported `pbinCodeContext.Code` at `pbin_update_stream.go:51`), and drive the existing
`PBinPatriciaHashed`. This reuses leaf-splitting, branch creation, BASIC_DATA packing, the CODE_HASH
leaf and code chunking instead of reimplementing all of it.

**Processing flow under bin** (vs hex)
| stage | hex | bin |
|---|---|---|
| `buildAccessedState` | shared | shared, unchanged |
| `detectCollapseSiblings` | runs | **skipped** (no deletion ⇒ no trigger) |
| `TouchHashedKey(siblingPath)` | per sibling | **never called** (guarded) |
| `buildWitnessTrie` | `WitnessNodes` | `WitnessNodes` via interface |
| exclusion proofs | legacy only | **N/A**, asserted no-op |
| `0x80` empty-storage append | legacy only | **skipped** (MPT artifact) |
| verify | `trie.RLPDecode`, skippable | bin decoder, **not** skippable |

## Implementation Steps

### Task 1: Witness tracer tap in pbinHasher

**Files:**
- Create: `execution/commitment/pbin_witness.go`
- Create: `execution/commitment/pbin_witness_test.go`
- Modify: `execution/commitment/pbin_hash.go`
- Modify: `execution/commitment/pbin_patricia_hashed.go`

- [x] add a tracer field to `pbinHasher` (`pbin_hash.go`), reusing the existing `witnessTracer`
      interface at `hex_patricia_hashed.go:60-63` — do not define a second interface
- [x] emit from inside `branchHash` (`pbin_hash.go:106-111`) and `leafCellHash` (`:134-158`), where
      the preimage buffer already exists; emit the buffer as-is, do not rebuild it
- [x] confirm by test that the tap also covers nodes hashed outside `foldBranch` — sibling cells via
      `hashRowCell`→`cellHash` (`pbin_patricia_hashed.go:738-763`) and the root cell via `RootHash()`
      (`:299`)
- [x] ensure `Reset()` and `Release()` detach the tracer so a pooled engine never carries one
      (`Release` calls `Reset`, which now clears it)
- [x] write tests: tracer nil = zero emissions and an unchanged root
- [x] write tests: tracer set = every node emitted once, `hash == H(preimage)` for both tags —
      asserted as: one hash never carries two preimages, every emission hashes to its own preimage,
      and the whole reference-tree node set is covered. The engine emits a **superset**: a branch
      folded under a short prefix is re-hashed with the canonical prefix when its parent row
      propagates, so the earlier emission survives in the set.
- [x] write tests: a tree whose root is a leaf (single-key corpus) still emits its root node
- [x] write tests: pooled reuse after `Release()` starts with no tracer
- [x] run tests - must pass before task 2

### Task 2: Witnesses method returning the parent root

**Files:**
- Modify: `execution/commitment/pbin_witness.go`
- Modify: `execution/commitment/pbin_update_stream.go`
- Modify: `execution/commitment/pbin_witness_test.go`

- [x] implement `Witnesses(ctx context.Context, updates *Updates, produceExclusionProofs bool,
      logPrefix string) (nodes [][]byte, provedKeys [][]byte, rootHash []byte, err error)` mirroring
      `hex_patricia_hashed.go:2414`
- [x] collect provedKeys at the `pbinUpdateStream` **emit sink**, not from `HashSort`: one account
      touch expands into BASIC_DATA + CODE_HASH + N code-chunk leaves (`pbin_update_stream.go:93-124`)
      and only the sink sees all of them
- [x] the returned root MUST be the **parent / pre-state** root — `buildWitnessTrie:1339` compares it
      against `expectedParentRoot`. The pass drives `seek` (fold + unfold, extracted from
      `followAndUpdate`) and never `updateCell`, so nothing is applied
- [x] accept and ignore `produceExclusionProofs`, with a comment naming the reason (no extension node)
- [x] ➕ wrap the context in `pbinWitnessReadOnly` for the pass: `foldBranch` writes a row's record
      back as it folds, and this pass folds rows it never modified. The wrapper also has to forward
      the unexported `pbinCodeContext.Code`, or code chunking breaks behind it
- [x] write tests: the returned root equals the pre-state root for a corpus with pending updates
- [x] write tests: provedKeys include the code-chunk and CODE_HASH leaves of a touched contract,
      not just its BASIC_DATA key
- [x] write tests: `produceExclusionProofs` true vs false give byte-identical output — compared as
      sets plus root-first, since `witnessNodeSet.nodes` orders the tail by map iteration
- [x] ➕ write tests: the pass leaves the commitment records untouched
- [x] write tests: an empty update set returns no nodes and no error
- [x] run tests - must pass before task 3

### Task 3: Replace the HexPatriciaHashed type assertion with an interface

**Files:**
- Modify: `execution/commitment/commitmentdb/commitment_context.go`
- Create: `execution/commitment/commitmentdb/pbin_witness_test.go`

- [x] define an interface for the `Witnesses` method and assert at compile time that both
      `*HexPatriciaHashed` and `*PBinPatriciaHashed` satisfy it
- [x] replace the type assertion at `commitment_context.go:353-359`, keeping a clear error for a trie
      satisfying neither
- [x] note in a comment that `WitnessNodes` (:364) still routes to the hex pruner until Task 6; no
      bin caller reaches it before Task 10
- [x] write tests: `witnessCapture` succeeds under bin instead of erroring
- [x] write tests: hex output is unchanged — capture output pinned against `HexPatriciaHashed.Witnesses`
      called directly over the same corpus
- [x] write tests: a trie implementing neither produces a clear error
- [x] run tests - must pass before task 4

### Task 4: Binary witness node decoder

**Files:**
- Create: `execution/commitment/pbin_witness_decode.go`
- Create: `execution/commitment/pbin_witness_decode_test.go`

- [x] decode a node preimage: tag `0x00` = leaf (key + 32-byte value), `0x01` = branch (bit prefix +
      two 32-byte child hashes)
- [x] reject malformed input explicitly: unknown tag, truncated prefix, bit count exceeding the
      encodable path, a leaf key matching no zone's length — plus non-canonical prefix padding, so
      one prefix has exactly one preimage
- [x] build a set keyed by `H(preimage)`, root first per the `witnessNodeSet.nodes` contract
- [x] re-merkelize from the root, treating a child hash absent from the set as opaque, returning the
      computed root
- [x] ➕ `pbinDecodeWitness` takes the capture's root rather than deriving it from the first entry:
      deriving it would silently re-root the tree if that entry went missing. Recursion is bounded by
      the 528-bit path, so a cyclic set errors instead of hanging
- [x] write tests: round-trip — capture from a real fold (Task 1), decode, re-merkelize, root matches
- [x] write tests: each malformed-input case errors rather than yielding a wrong root
- [x] write tests: a blinded child is preserved and does not change the root
- [x] write tests: permutation independence — the same corpus inserted in a different order gives the
      same root, cross-checked against `pbinOracleMerkelizeWith` (`pbin_oracle_test.go:172`)
- [x] write tests: removing one node makes re-merkelize fail rather than silently differ — asserted
      over every single-node removal: each one either errors or reproduces the same root, and exactly
      one (the root node's) errors
- [x] run tests - must pass before task 5

### Task 5: Witness-backed PatriciaContext for the mutable trie

**Files:**
- Create: `execution/commitment/pbin_witness_context.go`
- Create: `execution/commitment/pbin_witness_context_test.go`

- [x] implement a `PatriciaContext` (`Branch`, `PutBranch`, `Account`, `Storage` —
      `commitment.go:134-144`) plus the unexported `pbinCodeContext.Code`
      (`pbin_update_stream.go:51`) backed by a decoded witness node set
- [x] convert decoded branch preimages into the record format `pbinBranchEncoder` produces, so
      `PBinPatriciaHashed` can unfold into them unmodified
- [x] return a clear "blinded" error when a `Branch` read needs a node absent from the witness —
      never an empty record, which would silently build a wrong subtree
- [x] confirm the existing engine can then apply updates and produce a post-state root over this
      context, so no second mutable binary trie is written
- [x] ➕ a record cannot carry a BASIC_DATA or CODE_HASH value verbatim — both are packed from
      account fields at hash time. Those cells carry a handle (the leaf's node hash, truncated to
      plain-key width) that `Account` resolves to the state the value unpacks to; every other leaf
      round-trips as a record-resident value. Which one applies is decided by re-encoding through
      `pbinLeafValue`, so it cannot drift from the hasher
- [x] ➕ code arrives from outside the node set (`setCode`): the witness carries code as blobs, and
      the update stream reads it by plain key to chunk it. Task 8 picks the owner
- [x] write tests: applying a known update set over the witness context yields the same root as
      applying it over `MockState` with full state
- [x] write tests: a read of a blinded branch errors and names the path
- [x] write tests: a witness sufficient for the touched keys but missing untouched subtrees still
      produces the correct root
- [x] run tests - must pass before task 6

### Task 6: Proof-path pruning for binary witnesses

**Files:**
- Create: `execution/commitment/pbin_witness_prune.go`
- Create: `execution/commitment/pbin_witness_prune_test.go`
- Modify: `execution/commitment/commitmentdb/commitment_context.go`

- [x] implement the bin analogue of `trie.WitnessNodesForKeysFromNodes`
      (`execution/commitment/trie/proof.go:216`): walk from the root along each proved key's bit path,
      keeping every node on the path
- [x] route `WitnessNodes`' pruning to the bin pruner under the bin variant; leave hex untouched
- [x] ➕ the decoded node keeps its preimage, so the pruner emits the bytes it was given and orders
      them by walk order (root first) instead of by map iteration
- [x] ➕ a proved key of no zone errors instead of panicking in `pbinPathFromBytes`
- [x] write tests: the pruned set still reconstructs the root and still satisfies Task 5's context for
      the proved keys
- [x] write tests: a proved key whose path hits a blinded child stops cleanly, no panic — the key is
      built from a path the witness is known to blind, so the case cannot stop being one
- [x] write tests: nodes off every proved path are dropped and the result is a strict subset —
      compared against on-path membership derived from arrival-path prefixes, not from a second
      per-key descent
- [x] write tests: code-chunk leaves survive pruning (they are proved keys per Task 2)
- [x] ➕ write tests: `WitnessNodes` prunes with each variant's own walker (in `commitmentdb`)
- [x] run tests - must pass before task 7

### Task 7: Skip the hex-only phases under bin, with guards

**Files:**
- Modify: `rpc/jsonrpc/debug_execution_witness.go`
- Create: `rpc/jsonrpc/pbin_witness_phases_test.go`

- [x] skip `detectCollapseSiblings` entirely under bin — no deletion means no collapse trigger; never
      call `SetCollapseTracer` (`commitment_context.go:436` **panics**) or `BranchChildCount` (:448)
- [x] guard `TouchHashedKey` so it cannot be reached under bin (hazard: an empty plain key reaches
      `stateOf("")`, `pbin_update_stream.go:225`) — `buildWitnessTrie` errors on a non-empty
      `siblingPaths` under bin before touching anything, so the loop is unreachable
- [x] skip the legacy `0x80` empty-storage append (`:967-977`) under bin — an MPT artifact
- [x] reject only an **explicit** `"canonical"` mode under bin. Legacy is the default for a nil or
      empty mode (`resolveWitnessMode:590-596`), so rejecting legacy would reject every request.
      ➕ an explicit `""` now resolves to legacy too (it previously errored under both variants),
      so "no mode requested" has one meaning
- [x] check whether `serveFromWitnessCache` (`:770`, gated on `mode != witnessModeLegacy` at `:771`)
      and `buildWitnessResultHeadCapture` (`:861`) interact with the above; both funnel into
      `buildWitnessResult`, so state explicitly whether they are covered or out of scope —
      **both covered, no change needed**: the cache serve runs no pipeline phase and its
      `mode != witnessModeLegacy` gate never fires under bin (legacy is the only reachable mode);
      head-capture funnels into `buildWitnessResult`, which now branches on the variant, and both
      cache-builder call sites already pass `witnessModeLegacy`
- [x] write tests: a bin witness build performs no collapse-detection work and does not panic
- [x] write tests: an explicit `"canonical"` request under bin errors; a nil/empty mode succeeds
- [x] write tests: bin witness output contains no `0x80` node
- [x] run tests - must pass before task 8

### Task 8: Stateless reader and writer over a decoded binary witness

**Files:**
- Create: `rpc/jsonrpc/pbin_witness_stateless.go`
- Create: `rpc/jsonrpc/pbin_witness_stateless_test.go`
- Create: `execution/commitment/pbin_witness_state.go`
- Modify: `execution/commitment/pbin_witness_context.go`

- [x] implement a `StateReader`/`StateWriter` over a decoded bin witness, resolving accounts, storage
      and code by deriving the tree key and walking the node set
- [x] implement the post-state root via Task 5's context driving `PBinPatriciaHashed` — the analogue
      of hex's `witnessStateless.Finalize()` (`debug_execution_witness.go:1933-2033`)
- [x] make strict resolution the default and non-optional: a blinded node on a required path is an
      error, never an empty read. Document the deliberate divergence from hex's opt-in
      `WITNESS_STRICT_VERIFY`
- [x] decide and document where code comes from under bin — **the witness's own chunk leaves**, not
      `result.Codes`. They are committed by the root and re-checked against the CODE_HASH leaf on
      reassembly, and the fold re-chunks every account it touches, so pruning keeps a chunk leaf
      wherever the post-state pass needs code. `Codes` is keyed by code *reads*, a strictly narrower
      set — a contract credited without a call (contract coinbase, withdrawal recipient) has no blob
      but does have leaves. Code a block deploys has no pre-state leaves and arrives through
      `UpdateAccountCode`, as under hex
- [x] ➕ the exported seam is `commitment.PBinWitnessState` (decode + leaf resolution by address/slot
      + `Root`): the leaf walk, tree-key derivation and `Updates` construction all need
      package-private pbin state, and the alternative was exporting them one by one
- [x] ➕ `pbinWitnessContext.Code` falls back to the chunk leaves when no code was set explicitly, so
      the code owner is decided in one place rather than at every call site
- [x] write tests: a complete witness resolves every accessed account, slot and code
- [x] write tests: a witness with a node removed errors on the read that needs it — asserted over
      every single-node removal, since the pruned set holds only on-path nodes
- [x] write tests: an absent account and an absent slot both resolve correctly
- [x] write tests: applying a block's writes yields the expected post-state root — cross-checked
      against the same writes applied over full state, and covering a deploy, a storage write, a
      zeroed slot the witness holds and a zeroed slot it proves absent
- [x] ➕ write tests: an on-tree account delete is refused; one the witness proves absent is not
- [x] run tests - must pass before task 9

### Task 9: Wire the stateless verification gate

**Files:**
- Modify: `rpc/jsonrpc/debug_execution_witness.go`
- Modify: `rpc/jsonrpc/pbin_witness_stateless_test.go`

- [ ] route `verifyWitnessStateless` (`:963`, body `:1452`) to the bin verifier under bin;
      `trie.RLPDecode` cannot consume bin nodes
- [ ] re-execute from the witness alone and assert the post-state root equals `block.Root()`; on
      failure return `errWitnessVerifyFailed` and never return the witness
- [ ] make the bin path ignore `ERIGON_WITNESS_NO_VERIFY` (`:1459`) — under bin the gate is the only
      correctness evidence that exists
- [ ] keep `checkWitnessKeysComplete` behaviour
- [ ] write tests: a good witness verifies and is returned
- [ ] write tests: a truncated witness fails and no witness is returned
- [ ] write tests: `ERIGON_WITNESS_NO_VERIFY=true` does not disable verification under bin, and still
      does under hex
- [ ] run tests - must pass before task 10

### Task 10: Un-refuse the witness path under the bin variant

**Files:**
- Modify: `db/state/execctx/options.go`
- Modify: `rpc/jsonrpc/debug_execution_witness.go`
- Create: `rpc/jsonrpc/pbin_witness_reachable_test.go`

- [ ] on the witness path (`debug_execution_witness.go:903`), replace `WithHexCommitmentOnly()` with
      `WithoutParallelCommitment()` plus a bin allowance. A plain removal re-admits
      `VariantParallelHexPatricia`, which `witnessCapture` cannot serve (`:900-902`)
- [ ] leave `WithHexCommitmentOnly` itself unchanged — every other caller still needs it
- [ ] verify these still refuse: `eth_getProof` (`eth_call.go:482`), `eth_getWitness` (:781),
      `eth_simulateV1` (`eth_simulation.go:167`), receipts (`receipts_generator.go:333,553`),
      `rpchelper/commitment.go:97`, `db/integrity` (215, 1112, 1180, 1194)
- [ ] do NOT modify `rpc/jsonrpc/pbin_hex_only_test.go` or `db/state/execctx/pbin_options_test.go` —
      both assert refusals that remain correct
- [ ] write tests: `debug_executionWitness` is reachable and returns a verifying witness under bin
- [ ] write tests: the hex witness path still refuses the parallel variant
- [ ] write tests: each still-refusing caller listed above continues to refuse under bin
- [ ] run tests - must pass before task 11

### Task 11: End-to-end witness over a bin chain

**Files:**
- Create: `rpc/jsonrpc/pbin_witness_e2e_test.go`

- [ ] build a chain on `execmoduletester` under `ExperimentalBinCommitment` with BLAKE3, following
      the working pattern at `execution/tests/testutil/block_test_util.go:231-241`
- [ ] cover blocks exercising: a plain transfer, a contract deploy (header code chunks), a deploy
      large enough to reach CODE_ZONE overflow, a storage write, and an SSTORE-to-zero
- [ ] assert stateless re-execution reproduces the post-state root for every block
- [ ] write tests: a block with no state change produces a well-formed witness
- [ ] write tests: two consecutive blocks each verify independently
- [ ] run tests - must pass before task 12

### Task 12: Measure binary vs hex witness size

**Files:**
- Create: `rpc/jsonrpc/pbin_witness_size_test.go`
- Create: `rpc/jsonrpc/testdata/hex_witness_baseline.json`
- Modify: `docs/plans/20260801-pbin-execution-witness.md`

- [ ] build the same block sequence twice — once hex, once bin — from identical genesis and identical
      transactions
- [ ] the two arms mutate process-global state (`statecfg.ExperimentalBinCommitment`,
      `commitment.SetPBinHashSuite`); sequence them explicitly with save/restore and no `t.Parallel`,
      matching the note at `block_test_util.go:231-241`
- [ ] commit the hex arm's per-block node count and byte totals as a golden fixture, so Task 13 can
      check hex is unchanged without checking out another branch
- [ ] measure per block: witness node count, total bytes, and codes/headers separately so the
      trie-node contribution is isolated
- [ ] apply Task 8's code-ownership decision consistently, or the comparison counts code twice on one
      side and the table is meaningless
- [ ] record the measured table in this plan under a "Measured witness sizes" heading, stating the
      corpus (block count, account count, storage density) — sizes without a corpus mean nothing
- [ ] write tests: both arms produce a verifying witness
- [ ] run tests - must pass before task 13

### Task 13: Update the stale "witness is hex-only" documentation

**Files:**
- Modify: `db/state/execctx/options.go`
- Modify: `node/eth/backend.go`

- [ ] update the `WithHexCommitmentOnly` doc comment (`options.go:66-69`), which lists witness first
      among the hex-only callers
- [ ] update the startup warning at `node/eth/backend.go:377`
- [ ] grep for any other comment or doc still claiming the witness path refuses bin, and fix it
- [ ] run tests - must pass before task 14

### Task 14: Verify acceptance criteria

- [ ] verify every requirement in Overview is implemented
- [ ] verify the hex witness path is unchanged, against Task 12's committed golden fixture
- [ ] verify no witness failing stateless verification can be returned under either variant
- [ ] verify the still-refusing callers from Task 10 all still refuse
- [ ] run the full test suite: `make test-short`
- [ ] run the EIP-8297 blocktest fixtures and confirm still 67/70, the same three known
      non-conformant failures
- [ ] run `make lint`

### Task 15: [Final] Update documentation

- [ ] update `README.md` if needed
- [ ] update `CLAUDE.md` if new patterns were discovered
- [ ] move this plan to `docs/plans/completed/`

## Post-Completion

**Manual verification**
- Take a witness on a longer bin dev chain and sanity-check against the Task 12 table; a large
  divergence means the corpus was unrepresentative, not that the code is wrong.
- The measured sizes are the artefact worth sharing upstream — they replace estimates in the
  EIP-8297 discussion with numbers from a real client.

**External**
- No external oracle exists. If EEST later publishes binary-tree `executionWitness` fixtures, add a
  CI shard modelled on `zkevm-witness` (`tools/eest-spec-shards.yml`) and treat it as the
  conformance gate.
- `eth_getProof` and `eth_getWitness` remain refused under bin; each needs its own follow-up.
