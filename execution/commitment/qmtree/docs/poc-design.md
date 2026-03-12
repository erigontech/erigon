# Proof of Execution: Design Document

## 1. Overview

Proof of execution extends the qmtree leaf hash to include a cryptographic
commitment to the EVM instruction trace of each transaction. Combined with
the existing state-change hash, this provides a verifiable proof that a
specific sequence of EVM operations was executed to produce a given state
transition.

```
leaf = keccak256(preStateHash || stateChangeHash || transitionHash || previousLeafHash)
```

### 1.1 Motivation

The four-layer leaf provides a complete cryptographic commitment to each
transaction's behavior:

- **preStateHash** proves *what was read* — the inputs to execution.
- **stateChangeHash** proves *what changed* — the outputs of execution.
- **transitionHash** proves *how it changed* — the spec-mandated process
  (embeds the EVM execution hash).
- **previousLeafHash** proves *where it fits* — ordering and inclusion.

Use cases:

- **Fraud proofs**: A verifier can re-execute and compare all four hash
  layers without trusting the proposer.
- **External proving**: A prover receives an execution bundle (tx +
  pre-state + block context), re-executes, and produces alternative
  proofs (ZK, optimistic, etc.).
- **Execution auditing**: The execution hash serves as a fingerprint of
  the exact code path taken.
- **Completeness**: End-to-end provability from pre-state through
  execution to post-state, with ordering guarantees.

### 1.2 Cross-Client Determinism

The fields captured per instruction -- program counter, opcode, gas remaining,
gas cost, stack inputs, stack outputs, and call depth -- are all specified by
the Ethereum Yellow Paper and subsequent EIPs. Any spec-compliant EVM
implementation must produce identical values at every instruction step for the
same transaction executed against the same pre-state. This makes the execution
hash deterministic across clients.

## 2. Per-Instruction Record Format

Each opcode execution produces a variable-length record:

```
┌─────────┬──────┬────┬──────┬──────┬───────────────────┬────────────────────┐
│ depth:2 │ pc:8 │ op │ gas:8│cost:8│ stack_in: P * 32  │ stack_out: Q * 32  │
└─────────┴──────┴────┴──────┴──────┴───────────────────┴────────────────────┘
```

| Field     | Bytes  | Encoding   | Description                              |
|-----------|--------|------------|------------------------------------------|
| depth     | 2      | uint16 BE  | EVM call depth (0 = top-level)           |
| pc        | 8      | uint64 BE  | Program counter before execution         |
| op        | 1      | byte       | Opcode (0x00-0xFF)                       |
| gas       | 8      | uint64 BE  | Gas remaining before this instruction    |
| cost      | 8      | uint64 BE  | Total gas cost (constant + dynamic)      |
| stack_in  | P * 32 | uint256 BE | Top P stack items consumed (P = numPop)  |
| stack_out | Q * 32 | uint256 BE | Top Q stack items produced (Q = numPush) |

**Encoding**: All integers are big-endian for consistency with Ethereum's
convention (RLP, ABI encoding, keccak inputs). Stack items are 32-byte
uint256 in big-endian, matching their in-memory representation.

**Typical record size**: Most opcodes consume 1-2 and produce 0-1 stack items.
A typical record is 27 + 64 = 91 bytes, or 27 + 96 = 123 bytes.

### 2.1 What Is Excluded and Why

| Excluded         | Reason                                                    |
|------------------|-----------------------------------------------------------|
| Memory contents  | Too large (up to 30+ MB). Captured indirectly via stack   |
|                  | args (offset, size) and final state changes.              |
| Return data      | Captured when caller reads it via RETURNDATASIZE/COPY.    |
| Full stack       | Reconstructable from inputs + outputs of each instruction.|
| Contract address | Implicit from the call frame; depth disambiguates frames. |
| Storage values   | Captured in the state-change hash.                        |

### 2.2 Precompile Calls

Precompiled contracts (ecrecover, sha256, etc.) bypass the EVM interpreter
loop. To maintain completeness, we emit a synthetic record when a precompile
is called:

```
┌─────────┬──────────────────────┬───────────────┬────────────────┬──────┐
│ depth:2 │ precompile_addr: 20  │ input_hash:32 │ output_hash:32 │gas:8 │
└─────────┴──────────────────────┴───────────────┴────────────────┴──────┘
```

The `op` byte is set to `0xFF` (SELFDESTRUCT, which cannot appear at a
precompile call site) as a discriminator, or we reserve a synthetic opcode
value (e.g., `0xFE` = INVALID) with a flag byte to distinguish from real
INVALID instructions. The exact encoding is an implementation detail to be
finalized.

### 2.3 Call Depth Handling

EVM transactions involve nested calls (CALL, DELEGATECALL, STATICCALL,
CALLCODE, CREATE, CREATE2). Each increments `evm.depth` and creates a new
`CallContext`.

The `ExecHasher` is attached to the EVM instance, not the CallContext. All
instructions across all call frames within a transaction feed into a single
rolling hash. The `depth` field in each record disambiguates which frame
produced each instruction.

**Reverted inner calls** are included in the execution hash. The proof captures
what was *executed*, not what was *committed*. Reverts are reflected in the
state-change hash (which excludes reverted mutations).

## 3. Architecture

### 3.1 Inline Hashing (Phase 1)

The simplest approach: hash each instruction record directly in the
interpreter loop.

```
    EVM Interpreter Loop
    ┌────────────────────────────────┐
    │  for each opcode:              │
    │    capture stack_in            │
    │    execute(...)                │
    │    encode record               │
    │    keccak.Write(record)  <──── │ inline, same goroutine
    │    ...                         │
    │  keccak.Sum() at tx end        │
    └────────────────────────────────┘
```

**Overhead estimate**: ~450-600 cycles per opcode (dominated by keccak.Write
at ~6 cycles/byte for 60-90 byte records).

For realistic contract execution dominated by storage operations (SLOAD at
1000-5000+ cycles, CALL at 10000+ cycles), this adds <10% overhead. For
synthetic tight loops of cheap opcodes (ADD, PUSH), overhead is higher but
such patterns are rare in production.

### 3.2 Async Ring Buffer (Phase 2, If Needed)

If inline hashing proves too costly, decouple encoding from hashing:

```
    EVM Interpreter Loop              Hasher Goroutine
    ┌──────────────────────┐          ┌─────────────────────┐
    │  encode record       │          │  drain ring buffer   │
    │  write to ring buf   │──SPSC──→ │  keccak.Write(batch) │
    │  atomic store wpos   │          │  keccak.Sum() at end │
    └──────────────────────┘          └─────────────────────┘
```

Single-producer single-consumer (SPSC) ring buffer, no locks. EVM pays only
~30-40 cycles per opcode (memcpy + atomic store). Hashing runs on a separate
core.

Each parallel execution worker already has its own EVM instance, so one ring
buffer + one hasher goroutine per worker with no sharing.

### 3.3 Decision Criteria

Measure on real mainnet blocks (not synthetic benchmarks):

| Metric                         | Inline OK | Need Async |
|--------------------------------|-----------|------------|
| Block processing slowdown      | < 15%     | > 15%      |
| Opcode throughput (M ops/sec)  | > 85% baseline | < 85% |

## 4. Integration Points

### 4.1 ExecHasher (new: execution/vm/exec_hasher.go)

```go
type ExecHasher struct {
    hasher    crypto.KeccakState
    buf       [256]byte        // scratch buffer for encoding
    savedIn   [512]byte        // captured stack inputs (up to 16 items)
    savedInN  int              // number of saved input items
}

func (h *ExecHasher) Reset()
func (h *ExecHasher) CaptureInputs(stack *Stack, numPop int)
func (h *ExecHasher) HashOp(depth int, pc uint64, op byte, gas, cost uint64, stack *Stack, numPush int)
func (h *ExecHasher) HashPrecompile(depth int, addr [20]byte, input, output []byte, gas uint64)
func (h *ExecHasher) Finalize() [32]byte
```

### 4.2 EVM Struct (execution/vm/evm.go)

```go
type EVM struct {
    // ... existing fields ...
    execHasher *ExecHasher  // nil = proof of execution disabled
}

func (evm *EVM) SetExecHasher(h *ExecHasher) { evm.execHasher = h }
func (evm *EVM) ExecHasher() *ExecHasher     { return evm.execHasher }
```

### 4.3 Interpreter Loop (execution/vm/interpreter.go)

Two insertion points in the main loop:

```go
// BEFORE execute: capture stack inputs
if evm.execHasher != nil {
    evm.execHasher.CaptureInputs(&callContext.Stack, operation.numPop)
}

// execute the operation
pc, res, err = operation.execute(pc, evm, callContext)

// AFTER execute: hash complete record (inputs + outputs)
if evm.execHasher != nil {
    evm.execHasher.HashOp(evm.depth, pcCopy, byte(op), gasCopy, cost,
        &callContext.Stack, operation.numPush)
}
```

The nil check compiles to a single conditional branch. When `execHasher` is
nil (normal operation), the branch predictor learns this immediately and the
cost is effectively zero.

### 4.4 Precompile Hook (execution/vm/evm.go)

In the `call()` method, after a precompile executes successfully:

```go
if evm.execHasher != nil && isPrecompile {
    evm.execHasher.HashPrecompile(evm.depth, addr, input, ret, gas-leftOverGas)
}
```

### 4.5 Transaction Boundaries

In the execution worker (`execution/exec/state.go` - `RunTxTaskNoLock`):

```go
// Before execution
hasher := execHasherPool.Get().(*ExecHasher)
hasher.Reset()
evm.SetExecHasher(hasher)

// After execution
execHash := hasher.Finalize()
evm.SetExecHasher(nil)
execHasherPool.Put(hasher)

// Store in task result
txTask.ExecHash = execHash
```

### 4.6 Leaf Hash (execution/commitment/qmtree/tools/state_entry.go)

```go
func NewStateEntry(txNum uint64, changes []StateChange, opts LeafHashInputs) *StateEntry {
    sortChanges(changes)
    stateChangeHash := hashChanges(changes)

    // Four-component leaf hash
    combined := crypto.NewKeccakState()
    combined.Write(opts.PreStateHash[:])
    combined.Write(stateChangeHash[:])
    combined.Write(opts.TransitionHash[:])
    combined.Write(opts.PreviousLeafHash[:])
    var leaf common.Hash
    combined.Read(leaf[:])

    return &StateEntry{txNum: txNum, hash: leaf, ...}
}
```

### 4.7 Where NOT to Integrate

- **Tracer hooks** (`execution/tracing/hooks.go`): Function pointer dispatch
  overhead, designed for debugging/analytics. Not suitable for production
  commitment paths.
- **TRACE_INSTRUCTION flag**: Printf-based debug output. Inspired our design
  but shares no code.

## 5. Leaf Structure and Proof Layers

### 5.1 Four-Layer Proof Architecture

The complete proof system has four layers, each proving a different aspect:

```
leaf = hash(preStateHash || stateChangeHash || transitionHash || previousLeafHash)
```

| Layer | What It Proves | Source |
|-------|---------------|--------|
| **preStateHash** | *What* was read (account balances, storage values, code) | Pre-state reads during execution |
| **stateChangeHash** | *What* changed (balance diffs, storage writes, etc.) | Existing commitment inputs |
| **transitionHash** | *How* it changed (application logic + EVM execution) | Transition hasher (embeds exec hash) |
| **previousLeafHash** | *Where* it fits in the chain (ordering, inclusion) | Previous leaf's hash |

### 5.2 Pre-State Hash (Phase 7)

The pre-state hash is a rolling keccak256 over the sorted set of state
reads that occurred during execution. It captures the *inputs* to the
transaction — the state values that the EVM and state transition logic
observed while producing the state changes.

```
preStateHash = keccak256(sorted_reads)
```

Each read record:
```
[1 byte: domain_id] [2 bytes: key_length BE] [key] [4 bytes: value_length BE] [value]
```

This uses the same encoding as `stateChangeHash` for consistency. Reads
are collected during execution and sorted by (domain, key) before hashing.

**Why pre-state matters**: Without pre-state, a verifier can check that
a transition hash matches the claimed execution, but cannot independently
verify that the *correct inputs* were used. The pre-state hash closes
this gap — a prover must demonstrate that the values read during execution
match the actual state at that point.

Pre-state data is NOT stored on disk. It is derivable on demand from
Erigon's existing temporal history (state-at-txnum queries). This keeps
the incremental storage cost to just the hash (32 bytes per leaf).

### 5.3 State Change Hash (Existing)

The state change hash is already computed as part of the commitment
calculation. It covers the actual mutations: balance changes, storage
writes, nonce increments, code deployments, etc. These are the *results*
of execution — the inputs to the state trie update.

### 5.4 Transition Hash (Phase 6)

The transition hash proves the *process* — the application-level operations
mandated by the Ethereum spec that surround the EVM execution. It captures:

| Step                    | Where                              | What It Records |
|-------------------------|------------------------------------|-----------------|
| Nonce check/increment   | state_transition.go preCheck()     | sender, nonces |
| Gas purchase            | state_transition.go buyGas()       | sender, gas_value, blob_gas_value |
| Intrinsic gas           | state_transition.go                | regular_gas, floor_gas |
| EIP-7702 authorizations | state_transition.go                | authorities, delegations |
| Access list warm-up     | state_transition.go Prepare()      | addresses, slots |
| Value transfer          | evm.Call() / evm.Create()          | from, to, value, depth |
| **EVM execution**       | **interpreter.go Run()**           | **Embedded exec hash** |
| Gas refund              | state_transition.go refundGas()    | gas_used, refund |
| Tip payment             | state_transition.go                | coinbase, tip_amount |
| Fee burn                | state_transition.go                | burnt_contract, burn_amount |

The transition hash embeds the execution hash (Phase 1-5), creating a
composition:

```
transitionHash = keccak256(
    TX_CONTEXT || GAS_PURCHASE || INTRINSIC_GAS ||
    AUTH_PROCESS* || NONCE_INCREMENT || ACCESS_WARM* || ACCESS_SLOT* ||
    EXEC_HASH(execHash) ||
    GAS_ACCOUNTING || FEE_DISTRIBUTION
)
```

The actual state mutations that result from these operations (the balance
diffs, etc.) are already proven by the state change hash. The transition
hash proves *why* those mutations are correct — that the spec-mandated
process was followed.

See [TRANSITION_DESIGN.md](TRANSITION_DESIGN.md) for the full canonical
record format specification.

### 5.5 Previous Leaf Hash (Chain Inclusion)

Including `previousLeafHash` in each leaf creates a hash chain:

```
leaf[0] = hash(preState[0] || stateChange[0] || transition[0] || genesisHash)
leaf[1] = hash(preState[1] || stateChange[1] || transition[1] || leaf[0])
leaf[2] = hash(preState[2] || stateChange[2] || transition[2] || leaf[1])
...
leaf[N] = hash(preState[N] || stateChange[N] || transition[N] || leaf[N-1])
```

This provides:

- **Proof of ordering**: Each leaf commits to its predecessor, so the
  sequence cannot be reordered without changing all subsequent hashes.
- **Proof of chain inclusion**: To verify leaf[N], you need leaf[N-1],
  which needs leaf[N-2], etc. — proving the entire history is consistent.
- **Tamper evidence**: Modifying any leaf invalidates all subsequent leaves.

### 5.6 Two Consumer Models

The proof system serves two distinct consumer types:

#### Verifier (Hash-Only)

A verifier checks correctness using only hashes and Merkle proofs:

1. Receive a Merkle inclusion proof for a leaf
2. Verify the 4-component leaf hash matches the tree
3. Verify `previousLeafHash` chains correctly
4. Trust that the hashes were computed correctly (or spot-check via prover)

**Data required**: Merkle path + 4 leaf hash components (128 bytes + tree path).
No re-execution needed.

#### Prover (Re-Execution Bundle)

A prover re-executes the transaction to independently verify or produce
an alternate proof:

1. Receive the execution bundle: transaction + pre-state + block context
2. Re-execute through the EVM with hashers attached
3. Compare resulting hashes against the claimed leaf components
4. Optionally produce alternative proofs (ZK, optimistic, etc.)

**Data required**: Transaction (Tier 2, already stored), block headers
(Tier 2, already stored), pre-state values (Tier 3, derivable from
temporal history on demand). No additional storage needed.

### 5.7 Storage Analysis

#### Incremental Cost per Transaction

| Component | Size | Storage |
|-----------|------|---------|
| preStateHash | 32 bytes | New (in leaf) |
| stateChangeHash | 32 bytes | New (in leaf) |
| transitionHash | 32 bytes | New (in leaf) |
| previousLeafHash | 32 bytes | New (in leaf) |
| Tree overhead | ~32 bytes | New (Merkle nodes) |
| **Total incremental** | **~160 bytes/tx** | |

#### Storage Tiers

| Tier | Data | Status | Size |
|------|------|--------|------|
| 1 | Leaf hashes (4 × 32 bytes + tree) | New | ~160 bytes/tx |
| 2 | Transactions, headers, post-state, code | Already stored | Existing |
| 3 | Pre-state values | Derivable from temporal history | On demand |

#### Comparison with MPT History

Full Merkle Patricia Trie (MPT) history on Ethereum mainnet requires
~5 TB of storage. The qmtree approach stores only leaf hashes (~160
bytes/tx), with pre-state derivable from existing temporal indices.
For mainnet's ~2.5 billion transactions, this is approximately:

- **qmtree leaves**: ~400 GB (160 bytes × 2.5B txs)
- **Full MPT history**: ~5 TB

This is a ~12x reduction, and the qmtree leaves provide stronger
guarantees (execution proof + transition proof + ordering proof) than
MPT history alone (which only proves state at a given root).

### 5.8 Future: Merkle Model for Witnesses

The hash chain (Section 5.5) proves ordering but requires sequential
verification. For witness composability — proving that a *subset* of
transactions were executed correctly without replaying the full chain —
a Merkle tree over the leaf chain is needed:

```
        root
       /    \
     h01     h23
    /   \   /   \
  L[0] L[1] L[2] L[3]
```

This enables:
- **Range proofs**: Prove blocks N..M were executed correctly with
  O(log N) proof size.
- **Selective verification**: A verifier can check any individual
  transaction's execution without processing the full chain.
- **Parallel verification**: Independent subtrees can be verified
  concurrently.

The four-layer leaf structure (`preStateHash || stateChangeHash ||
transitionHash || previousLeafHash`) is compatible with this — each leaf
is a self-contained commitment that can be placed in a Merkle tree.

#### qmtree vs MPT Proof Efficiency

In a traditional MPT, state changes from a single transaction scatter
across the trie — each modified account and storage slot requires its own
Merkle proof branch (O(accounts × storage keys) proof size per tx).

In the qmtree model, all state changes for a transaction are bundled into
a single leaf hash. A Merkle inclusion proof for one transaction is O(1)
proof branches through the qmtree, regardless of how many state keys
were modified. This makes per-transaction proofs fundamentally more
compact.

## 6. Implementation Phases

| Phase | Description | Status |
|-------|-------------|--------|
| 1 | ExecHasher core (inline keccak) | Done |
| 2 | EVM integration (interpreter + precompiles) | Done |
| 3 | Unit tests + benchmarks | Done |
| 4 | Integration tests (real EVM bytecode) | Done |
| 5 | Async ring buffer (SPSC) | Done |
| 5b | Historic re-execution validation (hoodi) | Done |
| 6a | TransitionHasher core + canonical record format | Done |
| 6b | Integration with StateTransition | Done |
| 6c | Historic re-execution with transition hashes | Done |
| 7 | Leaf structure (preState + stateChange + transition + prevLeaf) | Next |
| 8 | Merkle tree over leaves for witness composability | - |

## 6. Performance Considerations

### 6.1 Per-Opcode Cost Breakdown (Inline)

| Component          | Cycles  | Notes                            |
|--------------------|---------|----------------------------------|
| Stack peek (in)    | 10-20   | memcpy 1-2 uint256 values        |
| Encode record      | 20-30   | write fixed fields to buf        |
| keccak.Write       | 400-550 | ~60-90 bytes at ~6 cycles/byte   |
| Stack peek (out)   | 10-20   | memcpy 0-1 uint256 values        |
| Nil branch         | 0-1     | perfectly predicted               |
| **Total**          | **~450-600** |                             |

### 6.2 Baseline EVM Opcode Costs

| Category                          | Cycles      | Examples            |
|-----------------------------------|-------------|---------------------|
| Cheap (stack/arithmetic)          | 10-50       | ADD, PUSH, DUP, POP |
| Medium (memory, jumps)            | 50-200      | MLOAD, JUMP, SHA3   |
| Expensive (state access)          | 1,000-5,000+| SLOAD, BALANCE      |
| Very expensive (calls, creates)   | 10,000+     | CALL, CREATE         |

### 6.3 Expected Real-World Impact

Mainnet block execution is dominated by state access (SLOAD/SSTORE account
for 60-80% of gas and an even higher fraction of wall-clock time). The hashing
overhead on these expensive opcodes is negligible (<1%). The aggregate impact
on block processing time is estimated at 5-10% for inline hashing.

### 6.4 Memory Overhead

- `ExecHasher` struct: ~1KB (scratch buffers + keccak state)
- One per parallel worker (typically 4-16 workers): 4-16 KB total
- Pooled via `sync.Pool` to avoid allocation per transaction

## 7. Testing Strategy

### 7.1 Unit Tests (exec_hasher_test.go)

- **Determinism**: Same bytecode + same input = same execution hash, every time.
- **Known vectors**: Hand-computed hashes for simple programs:
  - `PUSH1 0x01 PUSH1 0x02 ADD STOP`
  - `PUSH1 0x00 SLOAD STOP` (with mocked state)
  - Nested CALL with revert
- **Empty execution**: No-code contract produces hash of empty byte sequence.

### 7.2 Integration Tests (poc/)

- **Round-trip**: Build tree with execution hashing, unwind, rebuild, verify
  roots match.
- **Consistency**: Process same blocks twice, verify identical execution hashes.
- **Proof validity**: Generate inclusion proof for leaf with execution hash,
  verify against root.

### 7.3 Benchmarks

- **Micro**: `BenchmarkExecHasher_HashOp` -- raw per-opcode encoding + hashing.
- **Macro**: qmtree-bench on hoodi/mainnet with `--exec-hash` flag, compared
  to baseline without.
