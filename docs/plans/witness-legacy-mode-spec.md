# `debug_executionWitness` — Legacy Witness Format

## Abstract

`debug_executionWitness` returns a **stateless witness** for the execution of a single block: the pre-state
Merkle-Patricia proof, the contract bytecodes, the preimages of the accessed keys, and the ancestor headers
needed to re-execute that block and re-derive its post-state root with no access to the full state. This
document specifies the **legacy** output — the default, full form consumed by the downstream prover. A second
mode, **canonical**, emits the minimized form the `ethereum/execution-spec-tests` **zkevm@v0.4.0** corpus
encodes (the `executionWitness` field of those fixtures), matching reth's `canonical` mode; it is
state-root-critical and is referenced here only where it differs from legacy.

## Method

```
debug_executionWitness(block, mode?) -> ExecutionWitnessResult
```

- `block` — block number, tag, or hash (`rpc.BlockNumberOrHash`).
- `mode` — optional string, `"legacy"` or `"canonical"`. When omitted the mode is `legacy`.
- The call requires the historical-commitment schema (`rawdb.ReadDBCommitmentHistoryEnabled`); without it the
  call returns an error.

`resolveWitnessMode` derives the mode from the `mode` parameter alone — there is no environment-variable
override. Absent ⇒ `legacy`; `"legacy"`/`"canonical"` ⇒ that mode; any other value ⇒ error.

## Result object

All four members are arrays of hex-encoded byte strings (`hexutil.Bytes`); `keys` and `headers` are omitted
when empty.

```jsonc
{
  "state":   ["0x…", …],  // RLP-encoded MPT nodes of the pre-state proof
  "codes":   ["0x…", …],  // pre-state contract bytecodes
  "keys":    ["0x…", …],  // preimages of accessed keys (addresses + slots)
  "headers": ["0x…", …]   // RLP-encoded ancestor headers
}
```

### `state` — pre-state proof nodes

The set of RLP-encoded trie nodes proving every account and storage slot the block reads or writes, drawn from
the account trie and from the storage tries of touched accounts. It is produced by folding the commitment trie
over the accessed hashed keys (`GenerateWitness` → `toWitnessTrie`): each key contributes the nodes on its
root→leaf path together with the branch sibling hashes required to recompute every node hash up to the root.
The accessed-key set is first augmented by a collapse-detection pass (`detectCollapseSiblings`, run against a
split reader — parent commitment plus end-of-block state) so the proof also covers branch siblings that a
transaction transiently collapsed; the witness is then assembled against the parent-state reader.

- **Node encoding** — standard Ethereum MPT node RLP (branch / extension / leaf); unresolved subtrees are
  represented by their 32-byte hash (a `HashNode`).
- **Untouched storage roots** — for a witnessed account whose storage was not touched in this block, the
  storage root is emitted as a bare `HashNode` (the 32-byte root) and is **never** expanded into its sub-trie
  nodes. This holds in both modes.
- **Ordering** — the array is sorted ascending by node bytes after self-verification, so the root is not
  positionally first. A consumer identifies the root as the node whose Keccak-256 equals the parent block's
  `stateRoot`; a decoder that assumes `state[0]` is the root (as erigon's own `RLPDecode` does on the pre-sort
  array) must instead resolve it by hash.
- Legacy appends one empty-storage marker node — see *Legacy additions*.

### `codes` — pre-state bytecode

The bytecode the block needs as pre-state: every bytecode loaded (read) during execution — via `GetCode` /
`GetCodeSize`, matching Geth's `witness.AddCode` semantics — unioned with the modified and pre-state copies and
deduplicated by code hash. Each entry is the raw bytecode; a single empty (`0x`) entry is included iff some
empty-code account was loaded.

The pre-state copy is load-bearing for **EIP-7702**: the accessed-code map is keyed by address, so a delegated
account's designator (`0xef0100‖address`) is overwritten there by its resolved target code and survives only via
the pre-state copy.

Canonical narrows this to non-empty **pre-state** code only, excluding bytecode created in-block (a stateless
verifier reconstructs that by replaying the transactions).

### `keys` — accessed-key preimages

The unhashed preimages of every accessed key:

- **account addresses** — 20 bytes, included only when the account exists in post-state;
- **storage slots** — 32 bytes, for every accessed slot.

The EIP-7928 system address `0xff…fe` is excluded unless it has a real state change: it is touched as the
`msg.sender` of the per-block system call, which on its own is not a state access. Entries are deduplicated and
sorted ascending. Populated in both modes.

### `headers` — ancestor headers

The contiguous chain of RLP-encoded block headers from the target block's parent back to the oldest ancestor
reached by a `BLOCKHASH` opcode during execution. Used to answer `BLOCKHASH` under stateless re-execution.

## Legacy additions (relative to canonical)

1. **Empty storage-trie node.** A single `{0x80}` node (`0x80` is the RLP of the empty trie, whose hash is
   `EmptyRoot`) is appended once to `state` when any emitted node's bytes contain the 32-byte `EmptyRoot` — in
   practice an account leaf's empty storage-root field. It is appended *after* stateless verification: the bare
   node is unreachable from the root, so re-execution would reject it.
2. **Collapse siblings retained.** During replay a branch can transiently collapse toward a single child;
   `detectCollapseSiblings` traces the affected siblings. Canonical drops a traced sibling when its post-state
   branch still has ≥2 children (the collapse was transient, so the sibling is not structurally required);
   legacy keeps every traced collapse-sibling — legacy is thus the superset.

The account and storage proofs are otherwise built identically in the two modes; these two points are the only
structural differences.

## Verification invariant

Before returning, the producer self-verifies the witness: `verifyWitnessStateless` re-executes the block using
only the witness as state, and asserts that the resulting post-state root equals `block.Root()`. A witness that
fails this check is never returned — the call errors. The check is on by default and can be disabled for
diagnostics with `ERIGON_WITNESS_NO_VERIFY=true`. `WITNESS_STRICT_VERIFY=true` tightens it further, making
re-execution error on any unresolved (missing) trie node instead of treating it as empty.

A witness is a **sufficient** proof, not a canonical-minimal one: re-execution to the correct root is the only
correctness condition, so two conforming producers may legitimately differ in which redundant nodes or codes
they include.

## Errors

- block not found or not canonical;
- `mode` outside `{legacy, canonical}`;
- historical-commitment schema disabled;
- stateless re-execution root mismatch (no witness is returned).

## Code map

- **Producer** — `rpc/jsonrpc/debug_execution_witness.go`: `ExecutionWitness` (entry), `resolveWitnessMode`,
  `collectAccessedState` (codes/keys), `collectAccessedHeaders`, `detectCollapseSiblings`, `buildWitnessTrie`,
  `verifyWitnessStateless`, the `{0x80}` append.
- **Builder** — `commitmentdb.SharedDomainsCommitmentContext.Witness` →
  `execution/commitment/hex_patricia_hashed.go` `GenerateWitness` / `toWitnessTrie`, `witnessCreateAccountNode`
  (untouched storage root = `HashNode`).
