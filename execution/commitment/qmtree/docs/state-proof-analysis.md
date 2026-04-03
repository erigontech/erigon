# QMTree State Proof Analysis & Design

## Context

The qmtree is implemented and integrated into Erigon's execution pipeline.
Core metrics: ~21x smaller than the existing commitment domain, append-only
O(1) writes, ~1120-byte Merkle proofs. The tree is built from four-component
leaves:

```
leaf = keccak256(preStateHash || stateChangeHash || transitionHash || previousLeafHash)
```

Where:
- `preStateHash` = DeriveSha MPT root over all state reads (BAL-ordered)
- `stateChangeHash` = DeriveSha MPT root over all state writes (BAL-ordered)
- `transitionHash` = rolling keccak256 over 11 record types (embeds exec hash)
- `previousLeafHash` = hash chain linking to prior leaf

The 10-method `qm_` RPC namespace, execution/transition hashing, KeyIndex
with exclusion proofs, and SharedDomains integration are all implemented.

**Deployment strategy**: Rollup-first. The qmtree replaces MPT in a rollup
where we control the state commitment scheme. Mainnet adoption follows via
two related EIPs: (1) linearisation / transaction numbering, (2) the new
tree as state commitment. Both EIPs are prerequisites for each other.

---

## Question 1: Is the State Root Sufficient?

**Yes, with caveats around negative proofs and key lookup.**

### What the MPT root provides today

| Capability | MPT | QMTree |
|---|---|---|
| Positive inclusion proof ("key K has value V") | Yes — trie path | Yes — Merkle path to leaf containing (K,V) in stateChangeHash |
| Negative/exclusion proof ("key K does not exist") | Yes — branch/extension | Yes — KeyIndex ExclusionProof (sorted commitment over all written keys) |
| Latest-value proof ("V is *current* value of K") | Implicit — stores only latest | KeyIndex inclusion proof (key → latest txNum) + qmtree witness |
| Block-level binding | Root in block header | Root after final txnum of block |
| Light client sync | Yes (snap sync) | Yes — proof size ~3-4x smaller |

### Remaining gaps

1. **KeyIndex unwind** — KeyIndex does not support incremental reorg unwind;
   affected keys require full replay from genesis. Medium priority.

2. **KeyIndex persistence** — currently in-memory only, rebuilt on
   `LoadFromDisk`. Plan: RecSplit + segmented data files. See
   [keyindex-persistence-plan.md](keyindex-persistence-plan.md).

3. **RPC exposure of exclusion proofs** — `KeyIndex.GetProof()` implements
   both inclusion and non-membership proofs but they are not yet exposed
   via the `qm_` RPC namespace.

### Verdict

The root is sufficient for a rollup where we control the commitment scheme.
The remaining gaps are engineering work, not fundamental blockers.

---

## Question 2: Key Validity Proof

Given (account key, block number), prove inclusion:

1. **Resolve txnum**: inverted index → latest txnum T modifying key before block end. O(1).
2. **Get leaf data**: LeafData for SN=T. O(1).
3. **Get Merkle proof**: `tree.GetProof(T)` — 14-20 levels. O(log N).
4. **Reconstruct DeriveSha trie**: load all writes for txnum T from history files;
   build `DeriveSha(writes)`; confirm `Root() == stateChangeHash`. O(writes in tx).
5. **Get trie branch**: trie proof for the requested key. O(log N_keys).
6. **Verify**: recompute leaf hash, walk qmtree path, compare root; walk SMT
   branch, confirm key→value is included.

Full proof = qmtree witness (~1.1 KB) + trie branch (~O(log N_keys) × 32B).
For a tx touching 100 keys, trie depth ~7, branch ~224B. Total ~1.3 KB.

| Metric | QMTree + SMT | MPT |
|---|---|---|
| Proof generation | ~100-500 μs | ~1-50 ms |
| Proof size | ~1.3 KB | ~3-5 KB |
| Verification | ~20-50 μs | ~100-500 μs |

*Note: `preStateHash` and `stateChangeHash` are now DeriveSha MPT roots
(not flat keccak hashes), so per-key SMT branch proofs are possible. See
the "Canonical Format" section under Question 3b for details.*

---

## Question 3: Pre/Post State Changeset Retrieval

The leaf embeds preStateHash and stateChangeHash — cryptographic commitments
over the actual changesets. Retrieval:

1. Query Erigon's existing history tables (HistoryRange) for the txnum
2. Hash the returned data, verify against the leaf's preStateHash / stateChangeHash
3. Return changesets + Merkle proof

No new storage needed — reuse existing history. The qmtree hashes bind it.

---

## Question 3b: Proving an Individual Key

**Resolved.** `preStateHash` and `stateChangeHash` are now computed as
DeriveSha MPT roots over the read/write sets — not flat keccak256 hashes.
This means:

- A short O(log N_keys) trie branch proves any single key's inclusion
- No need to reveal the entire read/write set
- The trie is reconstructed on-demand from history files per RPC request

**Implemented: DeriveSha MPT roots replace flat hashes**

`preStateHash` and `stateChangeHash` are now computed as `DeriveSha` MPT
roots — the same trie format Ethereum uses for transaction roots, receipt
roots, and withdrawal roots:

```
preStateHash    = DeriveSha({ (domain, key) → value } for all reads)
stateChangeHash = DeriveSha({ (domain, key) → value } for all writes)
```

A state proof for a specific key is:
1. qmtree witness for the tx (proves the leaf is committed in the tree)
2. MPT branch within `preStateHash` for the requested key

This gives an O(log N_keys) proof for any single key, without revealing
the full read/write set.

### Storage Strategy

The per-tx DeriveSha trie is **not stored permanently** — it is reconstructed
on-demand from Erigon's existing history files:

- Flow: `qm_getAccountStateProof(address, keys, block)` →
  1. Inverted index → txnum T touching address near block
  2. `HistoryRange(T)` → all reads/writes for tx T
  3. Build DeriveSha trie in memory from those reads/writes
  4. Verify `Root() == witness.preStateHash` (proves trie matches leaf)
  5. Return: qmtree witness + trie branch for requested key
- No new permanent storage required

**This approach aligns with Erigon's existing architecture**: history files are
already queryable per txnum, and archival nodes are the ones expected to serve
detailed state proofs. Non-archival nodes without history files serve the
qmtree witness only; archival nodes additionally provide the trie branch.

### Format Change (completed)

`preStateHash` and `stateChangeHash` were changed from flat keccak256 to
DeriveSha MPT roots. This was a **breaking format change** to the entry file —
all pre-existing qmtree data was regenerated.

### Canonical Format (implemented in `execution/exec/qmtree_reader.go`)

The Merkle root uses **`DeriveSha`** — the same Merkle Patricia Trie format
Ethereum uses for transaction roots, receipt roots, and withdrawal roots.
This ensures format consistency across all Ethereum commitment structures and
allows existing Ethereum tooling to verify state change proofs.

**Ordering (Block Access List / EIP-7928 rules):**

State operations are sorted before building the trie, using the same strict
ordering defined by EIP-7928 Block Access Lists:
- Primary sort: domain (AccountsDomain=0 < StorageDomain=1 < CodeDomain=2)
- Secondary sort: key bytes ascending
  - AccountsDomain key = 20-byte address
  - StorageDomain key = 20-byte address || 32-byte slot (52 bytes total)
  - CodeDomain key = 20-byte address
- Result: addresses strictly increasing; within each address, storage slots
  strictly increasing — exactly matching BAL canonical ordering

**Leaf encoding:**

Each item in the trie is RLP-encoded as a 3-element list:
```
RLP([domain_uint, key_bytes, value_bytes])
```
- `domain_uint`: RLP uint (0 = accounts, 1 = storage, 2 = code)
- `key_bytes`: RLP byte array — address (20B) or address+slot (52B)
- `value_bytes`: RLP byte array — serialised account, storage value, or code
                 (nil/empty bytes for deletions)

**Trie keys:** sequential integers 0, 1, 2 … (same as transactions/receipts).

**Empty set:** returns `trie.EmptyRoot`
(`0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421`)
rather than the zero hash — consistent with DeriveSha on an empty list.

**Reproduction:** any implementation that:
1. Collects (domain, key, value) triples for all reads (preStateHash) or
   writes (stateChangeHash) executed by a transaction
2. Sorts them by the BAL ordering above
3. RLP-encodes each as `[domain_uint, key_bytes, value_bytes]`
4. Passes them to `DeriveSha` (MPT with sequential index keys)

will produce the same root as stored in the qmtree entry file.

---

## Question 4: RPC Re-Execution Proof

Yes. The leaf already commits to the full execution:
- **preStateHash** — what was read (inputs)
- **transitionHash** (embeds execHash) — what process ran
- **stateChangeHash** — what was written (outputs)

A verifier checks: given inputs + process + outputs, the leaf is in the tree
at the root committed in the block header. This covers eth_call,
debug_traceTransaction, and receipt verification.

---

## Question 5: RPC APIs

### `qm_getProof(address, storageKeys[], blockNumberOrHash)`

State inclusion proof. Returns:

```json
{
    "address": "0x...",
    "balance": "0x...", "nonce": "0x...", "codeHash": "0x...",
    "storageValues": {"0xkey1": "0xval1"},
    "txNum": 12345,
    "leafData": {
        "preStateHash": "0x...",
        "stateChangeHash": "0x...",
        "transitionHash": "0x...",
        "previousLeafHash": "0x..."
    },
    "merkleProof": "0x...",
    "qmtreeRoot": "0x...",
    "stateChangePreimage": [
        {"domain": 0, "key": "0x...", "value": "0x..."}
    ]
}
```

Verification: recompute leaf → walk Merkle path → compare root → hash
preimage → compare stateChangeHash → find (address, value) in preimage.

### `qm_getWitness(blockNumberOrHash)` / `qm_getTxWitness(block, txIndex)`

Execution witness using RangeWitness. Only 2 Merkle proofs for an entire
block — the hash chain (previousLeafHash) links intermediate leaves.

```json
{
    "blockNumber": 12345,
    "qmtreeRoot": "0x...",
    "rangeWitness": {
        "firstProof": "0x...",
        "lastProof": "0x...",
        "leaves": [{
            "serialNum": 100,
            "txHash": "0x...",
            "preStateHash": "0x...",
            "stateChangeHash": "0x...",
            "transitionHash": "0x...",
            "previousLeafHash": "0x...",
            "preState": [...],
            "stateChanges": [...]
        }]
    }
}
```

---

---

## Question 6: Provable `eth_call` (Read-Only Execution Proving)

`eth_call` is currently read-only and unproved: the node executes the call
against current state and returns a result, but provides no cryptographic
evidence that the result is correct.

With qmtree, a **provable call** becomes possible:

### Concept

A provable call is a **synthetic transaction** — a read-only execution that:
1. Reads some state (captured in `preStateHash` — an MPT root over reads)
2. Runs code (captured in `transitionHash`)
3. Produces outputs but makes **no state changes** (so `stateChangeHash`
   covers only the return value / output)

The proof binds the call inputs, execution, and outputs to the committed
qmtree root, giving a verifier cryptographic evidence that "given state S,
calling contract C with args A returns value V."

### How it works

1. Execute the `eth_call` with qmtree instrumentation enabled
2. Record reads (`preStateHash` = MPT root over all state accessed)
3. Record the execution trace (`transitionHash`)
4. Record the return value as the sole "write" (`stateChangeHash` = MPT root
   over `{output_key → return_value}`)
5. Anchor to the current qmtree root (which commits all historical state)
6. Return: qmtree witness for the call's txnum + SMT branches for any
   input/output keys the caller wants to verify

### ZK composability

This is particularly powerful in a ZK context: the qmtree execution proof
can be linked to other proof sources (other contracts, off-chain
computations, oracle attestations). The `preStateHash` proves "these were
the exact inputs from blockchain state", the `transitionHash` proves "this
exact EVM code ran on them", and the `stateChangeHash` proves "this was
the output". Together they form a **proof of blockchain access** that is
independently verifiable and composable with other ZK proofs.

### W3C Verifiable Credentials (VC 2.0)

The qmtree execution proof maps naturally to the
[W3C VC Data Model 2.0](https://www.w3.org/TR/vc-data-model-2.0/) framework:

- **Credential subject**: the output of the `eth_call` (e.g. "balance of
  address A at block N is V")
- **Issuer**: the Ethereum node (identified by its node key or ENS name)
- **Proof**: the qmtree witness + SMT branch, expressed as a VC proof
- **Verifiable**: any party can verify the VC without trusting the issuer,
  by checking the Merkle paths against the block's committed qmtree root

This enables Ethereum state facts to be embedded in W3C VCs — for example,
a DeFi protocol could issue a VC proving "this wallet held >= 1 ETH at
block N", anchored to the qmtree root rather than requiring the verifier
to run a full node.

**Broader proof-of-authority applications** — the same mechanism applies to
any scenario where on-chain state confers authority or identity:

| Use case | eth_call target | VC claim |
|---|---|---|
| NFT-gated access | `ownerOf(tokenId)` | "address A owns token T at block N" |
| DAO voting power | `getVotes(address)` | "address A had N votes at block N" |
| Smart contract role | `hasRole(role, address)` | "address A holds ADMIN role at block N" |
| Token balance threshold | `balanceOf(address)` | "address A held ≥ X tokens at block N" |
| KYC/compliance | registry contract | "address A passed KYC check at block N" |
| Credential revocation | revocation registry | "credential C was not revoked at block N" |

In each case the proof is: a `qm_call` witness anchored to the qmtree root,
plus an MPT branch proving the specific return value. The verifier checks the
proof without contacting the node, without running a full node, and without
trusting the issuer — only the committed qmtree root need be available (e.g.
posted to a bulletin board or embedded in a VC metadata document).

### UCAN-enabled agent workflows (MCP)

The same authority model applies to AI agent workflows. [UCAN](https://github.com/ucan-wg)
(User Controlled Authorization Networks) is a capability-based delegation
system where authority is proved by a chain of signed tokens rather than
by trusting a central server.

qmtree proofs can serve as the **root of authority** in a UCAN chain,
anchored to on-chain state:

```
blockchain state (qmtree root)
  └── qm_call proof: "address A owns contract role R"
        └── UCAN token: "A delegates capability C to agent X"
              └── MCP server invocation: agent X exercises C
```

A concrete example: a DAO grants an AI agent permission to execute trades
up to a certain value. The agent presents a UCAN token whose root capability
is proved by a qmtree witness showing the DAO contract's `hasRole` return
value at the relevant block. Any MCP server that receives the agent's
requests can verify the authority chain without contacting the DAO or
trusting an intermediary — the qmtree root in the block header is the
only trusted anchor.

This enables **authorized agentic workflows** where:
- Authority is derived from on-chain state (not from a centralized auth server)
- Delegation is cryptographically verifiable and auditable
- The authority proof is time-stamped to a specific block (block N proves
  the agent's authority as of that block)
- Authority automatically expires when on-chain state changes (e.g. role revoked)

### Claude / MCP permission PoC

Claude Code's permission system already models tool grants and restrictions
as structured capability tokens — a tool is either allowed or denied per
session, with optional parameters. This maps directly onto the UCAN
capability model.

A concrete PoC path, building on existing infrastructure:

1. **On-chain authority registry**: deploy a simple Solidity contract on
   a devnet (or hoodi) that records `(agentId, toolId) → authorized` via
   a governance transaction.

2. **`qm_call` proof**: call `isAuthorized(agentId, toolId)` via `qm_call`,
   producing a provable witness anchored to the block's qmtree root.

3. **UCAN token**: wrap the `qm_call` witness as the root capability in a
   UCAN token. The token delegates specific Claude tool permissions
   (e.g. `Bash`, `Write`) to a session key held by the agent.

4. **MCP permission hook**: add a Claude Code permission hook
   (`settings.hooks.PreToolUse`) that verifies the UCAN token before
   each tool call. The hook checks:
   - UCAN signature chain is valid
   - Root capability links to a valid `qm_call` witness
   - Witness root matches the known qmtree root for that block
   - Tool being invoked is within the delegated capability scope

5. **Result**: tool invocations are authorized by on-chain state, auditable
   on the blockchain, and require no central auth server. Revocation is
   immediate — the next block after an on-chain revocation, the proof fails.

This PoC is achievable in stages: step 1-2 (on-chain registry + qm_call)
builds on the existing `qm_call` implementation task; step 3-4 (UCAN +
hook) is independent Go/JS work that can start in parallel.

---

## EIP Strategy (Rollup → Mainnet)

Two related EIPs, both prerequisites for mainnet adoption:

1. **EIP: Transaction Linearisation** — formalise the global transaction
   numbering (txnum) that Erigon already uses internally. Every transaction
   across all blocks gets a monotonically increasing serial number. This is
   the foundation the qmtree indexes on.

2. **EIP: QMTree State Commitment** — replace MPT root with qmtree root in
   block headers. Depends on EIP-1 for the serial numbering. Specifies leaf
   format, hash function, twig structure, proof format.

**Rollup-first** validates both EIPs in production before proposing for L1.

---

## Implementation Status

All core implementation steps are complete on the `qmtree` branch.

### Completed

| Step | Description | Files |
|------|-------------|-------|
| 0 | **DeriveSha hash format** — `preStateHash` and `stateChangeHash` use `DeriveSha` MPT roots (same format as tx/receipt roots). BAL-ordered (domain, key) sorting. | `execution/exec/qmtree_reader.go`, `qmtree_writer.go` |
| 1 | **KeyIndex** — sorted key commitment with inclusion and exclusion proofs. Tracks latest txNum per `keccak256(domain \|\| key)`. | `keyindex.go` |
| 2 | **LeafData persistence** — bounded LRU cache with twig-anchored reconstruction from entry files. | `tracker.go` (leafData cache, twigPrevLeaf) |
| 3 | **`qm_getProof` RPC** — inclusion proof via inverted history index → txnum → Merkle proof. | `rpc/jsonrpc/qm_api.go` |
| 4 | **`qm_getWitness` / `qm_getTxWitness` RPC** — block-level range witness and single-tx variant. | `rpc/jsonrpc/qm_api.go` |
| 5 | **Verification endpoints** — `qm_verifyProof`, `qm_verifyWitness` (stateless). | `rpc/jsonrpc/qm_api.go` |
| 6 | **`qm_call` / `qm_callProof`** — simulated call with witnesses, compact twig-grouped format. | `rpc/jsonrpc/qm_api.go` |
| 7 | **`qm_getAccountStateProof` / `qm_getTxStateProof` / `qm_getLatestStateProof`** — state values + qmtree witness. | `rpc/jsonrpc/qm_api.go` |
| 8 | **Execution hash** — per-opcode rolling keccak256 over EVM interpreter loop. Precompile synthetic records. Wired into EVM via `ExecHasher`. | `execution/vm/exec_hasher.go` |
| 9 | **Transition hash** — rolling keccak256 over all 11 spec-mandated record types (TX_CONTEXT through FEE_DISTRIBUTION). Embeds exec hash. Wired into `TransitionDb()`. | `execution/vm/transition_hasher.go`, `execution/protocol/state_transition.go` |
| 10 | **SharedDomains integration** — tracker attached via `SetAppendOnly`, per-tx `AppendLeaf`, periodic `Flush`. | `execution/stagedsync/exec3_serial.go` |
| 11 | **`stage_exec_replay`** — conflict-free historical replay with qmtree dataset building. | `cmd/integration/commands/stages.go` |
| 12 | **`qmtree-bench` CLI** — benchmark tool for tree creation and storage measurement. | `cmd/utils/app/qmtree_bench_cmd.go` |

### Remaining work

| Item | Priority | Description |
|------|----------|-------------|
| KeyIndex unwind | Medium | No incremental reorg support; affected keys require full replay |
| KeyIndex persistence | Medium | In-memory only; rebuilt on LoadFromDisk. See [plan](keyindex-persistence-plan.md) |
| RPC exclusion proofs | Low | `KeyIndex.GetProof()` works but not exposed via `qm_` RPC |
| Head pruning | Low | HPFile design ready; twig eviction logic not implemented |
| Pipelined execution | Low | Serial `tracker.Flush()` only; no prefetcher-updater-flusher pipeline |
| `eth_getProof` compat | Low | Native qmtree format only; no MPT compatibility adapter |
| Transition hash → SSZ | Future | Phase 6 (rolling keccak) is live; Phase 8 (SSZ field-level Merkle proofs) deferred |

---

## Implemented RPC API

All 10 methods are served under the `qm_` RPC namespace via `QMAPIImpl`
(`rpc/jsonrpc/qm_api.go`). The namespace is registered in `daemon.go` when the
`--experimental.qmtree` flag is set:

```go
qmImpl := NewQMAPI(base, db, qmTracker, cfg.Gascap, logger)
```

### Namespace and wiring

All methods are served under the `qm_` RPC namespace via `QMAPIImpl`
(`rpc/jsonrpc/qm_api.go`). The namespace is registered in `daemon.go` when the
`--experimental.qmtree` flag is set:

```go
qmImpl := NewQMAPI(base, db, qmTracker, cfg.Gascap, logger)
```

### `qm_getProof(address, blockNrOrHash)`

Returns an inclusion proof for the last transaction that modified `address` at
or before the given block. Uses the inverted history index to find the txnum.

```json
{
  "address": "0x...",
  "txNum": "0x30d4",
  "blockNumber": "0x1000",
  "leafData": {
    "preStateHash":     "0x...",
    "stateChangeHash":  "0x...",
    "transitionHash":   "0x...",
    "previousLeafHash": "0x..."
  },
  "leafHash":    "0x...",
  "merkleProof": "0x...",
  "qmtreeRoot":  "0x...",
  "verified":    true
}
```

`merkleProof` is the binary-encoded `ProofPath` (`ToBytes()` format:
`8B serialNum + 32B selfHash + 11×32B LeftOfTwig + U×32B UpperPath + 32B root`).

### `qm_getWitness(blockNrOrHash)` / `qm_getTxWitness(blockNrOrHash, txIndex)`

Range witness for a full block (first + last tx Merkle proofs; hash-chained
intermediate leaves). Single-tx variant takes a 0-based `txIndex`.

```json
{
  "blockNumber": "0x1000",
  "qmtreeRoot":  "0x...",
  "firstProof":  "0x...",
  "lastProof":   "0x...",
  "leaves": [
    {
      "serialNum": "0x0",
      "leafData": { "preStateHash": "0x...", ... },
      "leafHash":  "0x..."
    }
  ],
  "verified": true
}
```

### `qm_getAccountStateProof(address, storageKeys[], blockNrOrHash)`

State values (balance, nonce, codeHash, storage slots) plus the qmtree witness
for the txnum that last wrote the address. Useful for verifying current state is
committed in the tree.

### `qm_call(callArgs, blockNrOrHash)` — combined witness

Executes a simulated call against the historical state at the given block.
Tracks every state read via `readTracker` (a `state.StateReader` wrapper).
For each read, uses the inverted history index to find the last-writer txnum,
then fetches the qmtree witness for that txnum.

**Algorithm:**
1. `CreateHistoryStateReader(blockNum)` — state at start of `blockNum`
2. `DoCall(ctx, ..., readTracker)` — EVM execution with reads recorded
3. For each `(domain, key)` in `readTracker.reads`:
   - `IndexRange(historyIdx, key, -1, endTxNum, desc, -1)` → `lastWriteTxNum`
4. Deduplicate txNums, sort ascending
5. `tracker.GetWitness(txNum)` for each unique txNum

**Response:**

```json
{
  "returnData":   "0x...",
  "gasUsed":      "0x5208",
  "failed":       false,
  "revertReason": "0x",
  "accessed": [
    {
      "domain":         0,
      "key":            "0xabcd...ef",
      "value":          "0x...",
      "lastWriteTxNum": "0x30d4"
    }
  ],
  "witnesses": [
    {
      "txNum":       "0x30d4",
      "leafData":    { "preStateHash": "0x...", ... },
      "leafHash":    "0x...",
      "merkleProof": "0x...",
      "qmtreeRoot":  "0x...",
      "verified":    true
    }
  ]
}
```

`domain` values: 0 = accounts, 1 = storage, 2 = code.
`key`: 20-byte address for accounts/code; 52-byte `addr||slot` for storage.

### `qm_callProof(callArgs, blockNrOrHash)` — compact twig-grouped proof

Same execution as `qm_call`, but the witnesses are compressed by deduplicating
upper-tree peer hashes that are shared among all leaves belonging to the same
twig. This is valid because all leaves in the same twig (serial numbers
`[twigId*2048 .. twigId*2048+2047]`) share identical upper-path peer hashes.

**Twig-grouping savings (at U=10 upper levels, hoodi ~615 twigs — computed by `TestFormatSizes_CallProof`):**

| Pattern | M leaves, T twigs | Individual witnesses | Compact proof | Saving |
|---|---|---|---|---|
| All in same twig | 20 leaves, 1 twig | 14,880 B | 10,792 B | **27%** |
| Typical (4/twig) | 20 leaves, 5 twigs | 14,880 B | 12,072 B | **19%** |
| Worst case (all diff) | 20 leaves, 20 twigs | 14,880 B | 16,992 B | **−14% (larger)** |

Compact format is only beneficial when average twig occupancy > ~1.5 leaves/twig.
For maximally diverse accesses (one leaf per twig), prefer `qm_call`.

**Response:**

```json
{
  "returnData":   "0x...",
  "gasUsed":      "0x5208",
  "failed":       false,
  "revertReason": "0x",
  "accessed": [ ... ],
  "root": "0x...",
  "twigs": [
    {
      "twigId":          "0x0",
      "upperPeerHashes": ["0x...", "0x...", ...]
    }
  ],
  "leaves": [
    {
      "txNum":               "0x30d4",
      "twigIndex":           0,
      "leafData":            { "preStateHash": "0x...", ... },
      "selfHash":            "0x...",
      "intraTwigPeerHashes": ["0x...", ..., "0x..."],
      "verified":            true
    }
  ],
  "digest": "0x..."
}
```

`root` — single qmtree root shared by all leaves.
`twigs` — one entry per unique twig; `upperPeerHashes` are the U peer hashes at
levels `FIRST_LEVEL_ABOVE_TWIG` through `root_level − 1`.
`leaves` — one per unique last-writer txNum; `twigIndex` indexes into `twigs`;
`intraTwigPeerHashes` are the 11 intra-twig sibling hashes (levels 0–10).
`digest` — the 32-byte Merkle commitment over all proof fields (see below).

### Proof digest (`callProofDigest`)

`QMCallProof.Digest` is an SSZ-inspired `hash_tree_root` over all proof fields,
producing a compact 32-byte commitment. A verifier can store just the digest and
later confirm a received proof has not been tampered with.

**Encoding:**

```
digest = merkleize([
  keccak256(returnData),                       // chunk 0 — execution output
  uint64_chunk(gasUsed),                       // chunk 1 — gas
  bool_chunk(failed),                          // chunk 2 — success flag
  keccak256(revertReason),                     // chunk 3 — revert data
  mixInLength(merkleize(accessed_hashes), n),  // chunk 4 — accessed set
  qmtreeRoot,                                  // chunk 5 — qmtree commitment
  mixInLength(merkleize(twig_hashes), n),      // chunk 6 — twig set
  mixInLength(merkleize(leaf_hashes), n),      // chunk 7 — leaf set
])
```

- `merkleize(chunks)` — binary Merkle tree padded to next power of 2 with zero
  chunks (SSZ convention); uses keccak256 for internal nodes.
- `mixInLength(root, n)` — `keccak256(root || uint64_le(n))` to distinguish
  lists of different lengths with the same element prefix.
- Element hashes (all via keccak256 over little-endian binary encoding):
  - **accessed**: `keccak256(uint64_le(domain) || key || value || uint64_le(lastWriteTxNum))`
  - **twig**: `keccak256(uint64_le(twigId) || upperPeerHashes_concat)`
  - **leaf**: `keccak256(uint64_le(txNum) || preStateHash || stateChangeHash || transitionHash || previousLeafHash || selfHash || intraPeers_concat)`

The qmtree root (chunk 5) is embedded directly rather than re-hashed — it is
already a Merkle commitment (the qmtree root over all historical state
transitions) and embedding it preserves the two-level structure: the qmtree
root binds on-chain state; the digest binds the proof itself.

**SSZ compatibility note**: The encoding follows SSZ `hash_tree_root` conventions
(power-of-2 padding, length mixing) so that a future full SSZ encoding of
`QMCallProof` will produce the same digest.

### Verification steps for `qm_callProof`

A verifier receiving a `QMCallProof` can check:

1. **Digest integrity**: Recompute `callProofDigest(proof)` and confirm it
   matches `proof.Digest`. Detects any tampering after transmission.

2. **Leaf Merkle proofs**: For each leaf `lf` in `Leaves`:
   - Compute `leafHash = keccak256(leafData.preStateHash || ... || leafData.previousLeafHash)`
   - Confirm `lf.selfHash == leafHash`
   - Walk the 11 `IntraTwigPeerHashes` up to get the twig's left-subtree root
   - Walk the `UpperPeerHashes` from `Twigs[lf.TwigIndex]` up to the root
   - Confirm the recomputed root matches `proof.Root`

3. **Execution integrity**: Verify that the state values in `Accessed` are
   consistent with the `preStateHash` fields in the leaves. Since `preStateHash`
   is now a DeriveSha MPT root, the verifier can reconstruct the trie from the
   accessed set and confirm the root matches. Per-key trie branch proofs are
   also possible.

4. **Call binding**: Verify that the accessed state matches the call's execution
   trace via the committed `transitionHash`. The transition hash (rolling
   keccak256 over TX_CONTEXT through FEE_DISTRIBUTION records) is now computed
   live during execution. Full call binding requires the verifier to replay the
   EVM against the proved inputs — the transition hash confirms the replay
   matches the original execution.

### Verification endpoints

- `qm_verifyProof(proofBytes)` — deserializes a binary `ProofPath` and calls
  `ProofPath.Check(hasher, true)`. Stateless.
- `qm_verifyWitness(witnessBytes)` — deserializes a binary `Witness` and calls
  `Witness.Verify(hasher)`. Stateless.
