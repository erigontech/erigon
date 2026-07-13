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
- The call requires the historical-commitment schema (`rawdb.ReadDBCommitmentHistoryEnabled`) to recompute a
  witness on demand; without it the call returns an error — unless the node runs in **head-capture** serving
  mode (see *Head-capture serving*), which serves recent blocks from an in-memory cache built without any
  commitment history.

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
the account trie and from the storage tries of touched accounts. It is produced by an on-the-fly fold over the
accessed hashed keys (`HexPatriciaHashed.Witnesses` captures the superset node set, `WitnessNodes` prunes it to
the lean set): each key contributes the nodes on its root→leaf path together with the branch sibling hashes
required to recompute every node hash up to the root.
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

The EIP-7928 system address `0xff…fe` is excluded unless it has a real state change or is accessed by a user
transaction: it is touched as the `msg.sender` of the per-block system call, which on its own is not a state
access, so that system-call touch alone does not include it. Entries are deduplicated and sorted ascending.
Populated in both modes.

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

## Head-capture serving (minimal nodes)

A node pruned without commitment-domain history (`--prune.mode=minimal`) cannot recompute a witness on demand,
because the parent commitment plane is only reachable through commitment history it no longer keeps. **Head-capture**
serving fills that gap for the last N blocks: with `--witness.cache.head-capture` set (plus a non-zero
`--witness.cache.blocks`), the node eagerly builds each new head block's witness as it commits and stores it in
the same block-hash-keyed LRU the durable path uses. The commitment parent state is read from a *pinned parent
RO snapshot* (a temporal tx lagging the tip by one committed block, whose commitment-latest equals the parent's
commitment); the account/storage/code parent and block-end state come from the history a minimal node still
retains. Each built witness passes the same self-verification gates before it is cached, so a stale or
incomplete view fails closed and simply leaves that block uncached — never a wrong witness.

Flags (embedded RPC only):

- `--witness.cache.head-capture` — enable head-capture serving on a node without commitment history. Ignored
  (no-op) when commitment history is present: such a node keeps the durable recompute-on-miss path.
- `--witness.cache.blocks` — window size N; the number of recent blocks retained, capped at 96.
- `--witness.cache.maxmb` — resident-memory cap in MB. Eviction triggers on whichever of the block count
  (`--witness.cache.blocks`) or this byte budget binds first. `0` = count-only (no byte cap).

Serving semantics in head-capture mode:

- **Cache-only.** A hit is served from cache verbatim; a miss returns a typed out-of-window error and **never**
  falls through to a history recompute (there is no commitment history to recompute from).
- **Tip-only.** Only blocks within the trailing window are servable. A request ahead of the tip, or for a block
  older than the window, is out-of-window.
- **By-hash canonical check.** A by-hash request whose block number is no longer canonical (reorged out) returns
  the distinct `reorged-away` error rather than serving a stale orphan as canonical.
- **Cold after restart.** The cache is in-memory only and cannot be back-filled from history. After a restart it
  is empty and re-warms forward, so the last ~N blocks are out-of-window until N new blocks have been committed.
- **`eth_getWitness`.** In head-capture mode this RLP/uncached endpoint also returns the out-of-window error
  instead of attempting a recompute; on a commitment-history node its behavior is unchanged.

## Errors

- block not found or not canonical;
- `mode` outside `{legacy, canonical}`;
- historical-commitment schema disabled and head-capture serving off;
- head-capture cache miss — requested block is out of the trailing window, or its hash was reorged away;
- stateless re-execution root mismatch (no witness is returned).

## Code map

- **Producer** — `rpc/jsonrpc/debug_execution_witness.go`: `ExecutionWitness` (entry), `resolveWitnessMode`,
  `collectAccessedState` (codes/keys), `collectAccessedHeaders`, `detectCollapseSiblings`, `buildWitnessTrie`,
  `verifyWitnessStateless`, the `{0x80}` append.
- **Builder** — `commitmentdb.SharedDomainsCommitmentContext.Witness` / `WitnessNodes` →
  `execution/commitment/hex_patricia_hashed.go` `Witnesses` (untouched storage root emitted as a bare
  `HashNode`), pruned to the lean set by `trie.WitnessNodesForKeysFromNodes`.
- **Head-capture cache** — `rpc/jsonrpc/witness_cache.go` (LRU with count + byte cap, mode fields),
  `rpc/jsonrpc/witness_cache_builder.go` (`WitnessCacheMode`, rolling-pin lifecycle, `buildAndCacheHeadCapture`),
  `rpc/jsonrpc/debug_execution_witness.go` (`headCaptureSource`, `buildWitnessResultHeadCapture`,
  `errWitnessOutOfWindow` / `errWitnessReorgedAway`), enabled in `node/eth/backend.go`.
