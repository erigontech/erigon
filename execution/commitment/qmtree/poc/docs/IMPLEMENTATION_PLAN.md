# Proof of Execution: Implementation Plan

## Phase 1: ExecHasher Core

Build the per-instruction hashing engine with inline (synchronous) hashing.
No concurrency, no integration with the execution pipeline yet.

### 1.1 Create `execution/vm/exec_hasher.go`

- `ExecHasher` struct:
  - `hasher crypto.KeccakState` -- rolling keccak256
  - `buf [256]byte` -- scratch buffer for encoding one record
  - `savedIn [512]byte` -- snapshot of stack inputs before execute
  - `savedInN int` -- number of uint256 items saved
- Methods:
  - `Reset()` -- reinitialize keccak state for a new transaction
  - `CaptureInputs(stack *Stack, numPop int)` -- copy top N items to savedIn
  - `HashOp(depth int, pc uint64, op byte, gas, cost uint64, stack *Stack, numPush int)`:
    1. Encode fixed header (depth, pc, op, gas, cost) into buf
    2. Write header to hasher
    3. Write savedIn (captured inputs) to hasher
    4. Peek top numPush items from stack, write to hasher
  - `HashPrecompile(depth int, addr [20]byte, input, output []byte, gas uint64)`:
    1. Hash input and output separately
    2. Encode synthetic record with addr + input_hash + output_hash + gas
    3. Write to hasher
  - `Finalize() [32]byte` -- read keccak digest
- `sync.Pool` for ExecHasher reuse

### 1.2 Unit Tests (`execution/vm/exec_hasher_test.go`)

- `TestExecHasher_Determinism` -- same ops produce same hash
- `TestExecHasher_DifferentOps` -- different ops produce different hash
- `TestExecHasher_StackInputOutput` -- varying numPop/numPush sizes
- `TestExecHasher_Precompile` -- synthetic precompile record
- `TestExecHasher_EmptyTx` -- Finalize without any HashOp calls
- `BenchmarkExecHasher_HashOp` -- single opcode throughput

### Deliverable

Standalone, testable hashing engine. No changes to EVM or interpreter yet.

---

## Phase 2: Interpreter Integration

Wire ExecHasher into the EVM interpreter loop.

### 2.1 Add field to EVM struct (`execution/vm/evm.go`)

```go
execHasher *ExecHasher
```

Plus getter/setter methods.

### 2.2 Instrument interpreter loop (`execution/vm/interpreter.go`)

Two insertion points in `EVM.Run()`:

1. **Before** `operation.execute()` -- call `CaptureInputs`
2. **After** `operation.execute()` -- call `HashOp`

Both guarded by `if evm.execHasher != nil`.

### 2.3 Precompile hook (`execution/vm/evm.go`)

In the `call()` method, after successful precompile execution, call
`HashPrecompile`.

### 2.4 Integration tests

- Execute known bytecode via `EVM.Run()` with ExecHasher set.
- Verify execution hash matches expected value.
- Test with nested calls (CALL, DELEGATECALL) and reverts.
- Test with precompile calls (ecrecover via CALL to address 0x01).

### Deliverable

EVM produces execution hashes when ExecHasher is set. Zero overhead when nil.

---

## Phase 3: PoC Pipeline Integration

Connect execution hashing to the qmtree PoC pipeline.

### 3.1 Update StateEntry (`poc/state_entry.go`)

- Add `execHash common.Hash` parameter to `NewStateEntry`
- Leaf hash becomes `keccak256(stateChangeHash || execHash)`
- Update all callers

### 3.2 Update Runner (`poc/poc.go`)

- In `processBlock`, for each txnum:
  1. Create/reset ExecHasher
  2. Re-execute the transaction with ExecHasher enabled
  3. Collect execution hash
  4. Pass to NewStateEntry alongside state changes
- Note: This requires actual tx re-execution, not just history reading.
  The PoC currently reads state diffs from history. For execution hashing,
  we need to actually run the EVM. This may mean the PoC operates in two
  modes:
  - **State-only mode** (current): reads diffs from history, no execution hash
  - **Full mode** (new): re-executes transactions, produces both hashes

### 3.3 Add `--exec-hash` flag to qmtree-bench

- When set, enables full mode with execution hashing
- Reports execution hash overhead in CSV output
- Add opcodes/sec metric to BlockResult

### 3.4 Update tests

- Existing PoC tests continue to work in state-only mode
- New tests for full mode with execution hashing

### Deliverable

qmtree-bench can produce trees with combined state + execution hashes.

---

## Phase 4: Benchmark and Measure

Run real-world benchmarks to determine if inline hashing is sufficient.

### 4.1 Baseline measurement

Run qmtree-bench on hoodi (or mainnet subset) without `--exec-hash`.
Record: blocks/sec, total time, storage size.

### 4.2 Execution hash measurement

Run same range with `--exec-hash`. Record same metrics plus:
- opcodes/sec
- hashing time per block
- hashing time as % of total

### 4.3 Profile

If overhead > 15%:
- `go tool pprof` to identify hotspots
- Is it keccak.Write? Encoding? Stack copies?
- Measure keccak throughput on the target hardware

### 4.4 Decision

| Overhead | Action                          |
|----------|---------------------------------|
| < 10%    | Ship inline, done               |
| 10-15%   | Optimize encoding, re-measure   |
| > 15%    | Proceed to Phase 5 (async)      |

### Deliverable

Benchmark report with concrete numbers. Go/no-go for async design.

---

## Phase 5: Async Ring Buffer (Conditional)

Only if Phase 4 shows >15% overhead on real chain data.

### 5.1 SPSC Ring Buffer (`execution/vm/exec_hasher_async.go`)

- Fixed-size ring buffer (64KB)
- Single-producer (EVM goroutine): writes encoded records, atomic store wpos
- Single-consumer (hasher goroutine): reads records, keccak.Write, updates rpos
- No locks, no channels in hot path

### 5.2 Lifecycle

- `Start()` -- launch hasher goroutine
- `SubmitOp(...)` -- encode and write to ring, return immediately
- `Finish() [32]byte` -- signal end-of-tx, block until hasher returns digest
- `Stop()` -- shut down goroutine (at worker shutdown)

### 5.3 Backpressure

If ring buffer is full (EVM producing faster than hashing), the EVM spins
briefly on wpos. This should be rare -- keccak is fast and the buffer is
large enough for ~700+ opcode records.

### 5.4 Re-benchmark

Verify overhead drops to <5% with async design.

### Deliverable

Async execution hasher with minimal impact on EVM throughput.

---

## Phase 6: Transition Hasher

Capture the application-level operations that surround the EVM — the
spec-mandated state transition logic in `StateTransition.TransitionDb()`.

The actual state changes (balance diffs, storage writes) are already
captured as inputs to the existing commitment calculation (stateChangeHash).
The transition hash proves the *process* was followed correctly.

### 6a: TransitionHasher Core

- Define canonical binary record format with typed tag bytes
- Implement `TransitionHasher` struct with rolling keccak256
- Methods for each operation: `HashNonceCheck`, `HashGasPurchase`,
  `HashIntrinsicGas`, `HashAuthProcess`, `HashTransfer`, `SetExecHash`,
  `HashGasRefund`, `HashTipPayment`, `HashFeeBurn`, etc.
- `sync.Pool` for reuse
- Unit tests for determinism and encoding correctness

### 6b: Integration with StateTransition

- Add `transitionHasher` field to `StateTransition`
- Insert hash calls at each operation point in `TransitionDb()`
- Transfer hook via EVM (for value transfers in CALL/CREATE)
- Nil-guarded: zero cost when disabled

### 6c: Historic Re-Execution Validation

- Extend `ExecRunner` to produce transition hashes alongside exec hashes
- Add `transition_hash` column to CSV output
- Verify determinism on hoodi data (run twice, compare)

### Deliverable

TransitionHasher producing deterministic hashes for the full state
transition process. See [TRANSITION_DESIGN.md](TRANSITION_DESIGN.md) for
the canonical record format specification.

---

## Phase 7: Leaf Structure (Four-Layer Proof)

Combine all four proof layers into the final leaf structure.

### 7.1 Leaf hash formula

```
leaf = hash(preStateHash || stateChangeHash || transitionHash || previousLeafHash)
```

- **preStateHash**: Keccak256 of sorted pre-state reads (what was read)
- **stateChangeHash**: Existing commitment inputs (what changed)
- **transitionHash**: Phase 6 output (how it changed, embeds exec hash)
- **previousLeafHash**: Hash of the previous leaf (ordering + inclusion)

### 7.2 PreStateHasher (`poc/pre_state_hasher.go`)

Rolling keccak256 over state reads collected during execution:

- `PreStateHasher` struct with rolling keccak and read collection
- `AddRead(domain, key, value)` -- record a state read
- `Finalize() common.Hash` -- sort reads, hash, return digest
- Same encoding as `hashChanges`: `[domain:1][keyLen:2][key][valLen:4][value]`
- Reads sorted by (domain, key) for determinism
- `sync.Pool` for reuse

The PreStateHasher collects reads that occur during EVM execution and
state transition logic (account lookups, storage reads, code fetches).
Pre-state values are NOT stored on disk — they are derivable from
Erigon's temporal history on demand.

### 7.3 Hash chain

Each leaf commits to its predecessor, creating a chain:

```
leaf[0] = hash(preState[0] || stateChange[0] || transition[0] || genesisHash)
leaf[N] = hash(preState[N] || stateChange[N] || transition[N] || leaf[N-1])
```

This provides proof of ordering (cannot reorder without invalidating
subsequent hashes) and proof of chain inclusion (each leaf proves the
entire history is consistent).

### 7.4 Update StateEntry (`poc/state_entry.go`)

- Add `LeafHashInputs` struct: `PreStateHash`, `TransitionHash`, `PreviousLeafHash`
- Update `NewStateEntry` to accept `LeafHashInputs`
- Leaf hash becomes `keccak256(preStateHash || stateChangeHash || transitionHash || previousLeafHash)`
- Update all callers

### 7.5 Wire into PoC pipeline (`poc/poc.go`)

- Thread `previousLeafHash` through block processing
- Each transaction's leaf hash becomes the next transaction's `previousLeafHash`
- Genesis hash (all zeros or configurable) for the first leaf

### 7.6 Tests

- Existing PoC tests updated for new `NewStateEntry` signature
- `TestStateEntry_FourComponentLeaf` -- verify 4-component formula
- `TestStateEntry_PreviousLeafChain` -- verify hash chain
- `TestPreStateHasher_Determinism` -- same reads produce same hash
- `TestPreStateHasher_DifferentReads` -- different reads produce different hash

### Deliverable

Leaf structure with four-layer proof and sequential chaining.
PreStateHasher producing deterministic hashes over state reads.

---

## Phase 8: Merkle Model for Witnesses (Future)

The hash chain from Phase 7 proves ordering but requires sequential
verification. For witness composability — proving a *subset* of
transactions were executed correctly — a Merkle tree over the leaf chain
is needed.

### 8.1 Merkle tree over leaves

- Build a binary Merkle tree where each leaf is a Phase 7 leaf hash
- Enables O(log N) inclusion proofs for arbitrary transaction ranges
- Supports selective and parallel verification

### 8.2 Witness format

- Define a witness structure containing:
  - Merkle proof (sibling hashes along the path)
  - Leaf data (stateChangeHash, transitionHash, previousLeafHash)
  - Optionally: the transition/exec records for re-verification
- Compact encoding for efficient transmission

### 8.3 Verifier

- Standalone verifier that checks:
  1. Merkle inclusion proof is valid
  2. Transition hash matches re-execution of the transaction
  3. State change hash matches the claimed state diff
  4. Previous leaf hash chains correctly

### Deliverable

Complete witness system with Merkle proofs for composable verification.

---

## Phase 9: Production Integration

Wire into the real block execution pipeline for production use.

### 9.1 Worker integration (`execution/exec/state.go`)

- In `RunTxTaskNoLock`: set up TransitionHasher + ExecHasher on EVM
- After execution: collect transition hash into TxTask result
- Pool hashers per worker
- Chain leaves with previousLeafHash

### 9.2 Commitment domain

- Store leaf hashes alongside state roots in the commitment domain
- Include in snapshot/segment files for historical access

### 9.3 Proof generation

- Merkle proofs from qmtree include the full leaf as part of the tree
- Verifier can check: re-execute tx, compare all three hash layers,
  verify Merkle inclusion

### Deliverable

Full production proof system in the commitment pipeline.

---

## Status Summary

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
| 7 | Leaf structure (preState + stateChange + transition + prevLeaf) | Done |
| 8 | Merkle model for witnesses | Done |
| 9.1 | Production integration: Worker + serial executor wiring | Done |
| 9.2 | Production integration: Disk-backed tree + leaf data storage | Done |
| 9.3 | Production integration: Witness/proof generation | Done |

## File Summary

| File                                    | Phase | Action  |
|-----------------------------------------|-------|---------|
| `execution/vm/exec_hasher.go`           | 1     | Done    |
| `execution/vm/exec_hasher_test.go`      | 1     | Done    |
| `execution/vm/evm.go`                   | 2     | Done    |
| `execution/vm/interpreter.go`           | 2     | Done    |
| `execution/vm/exec_hasher_async.go`     | 5     | Done    |
| `execution/vm/exec_hasher_async_test.go`| 5     | Done    |
| `execution/vm/exec_hasher_integration_test.go` | 4 | Done |
| `poc/exec_runner.go`                    | 5b    | Done    |
| `cmd/utils/app/exec_hash_cmd.go`        | 5b    | Done    |
| `execution/vm/transition_hasher.go`     | 6a    | Done    |
| `execution/vm/transition_hasher_test.go`| 6a    | Done    |
| `execution/protocol/state_transition.go`| 6b    | Done    |
| `execution/protocol/transition_integration_test.go` | 6b | Done |
| `execution/vm/evmtypes/evmtypes.go`    | 6b    | Done    |
| `poc/pre_state_hasher.go`               | 7     | Done    |
| `poc/pre_state_hasher_test.go`          | 7     | Done    |
| `poc/state_entry.go`                    | 7     | Done    |
| `execution/commitment/qmtree/witness.go` | 8    | Done    |
| `execution/commitment/qmtree/witness_test.go` | 8 | Done    |
| `execution/exec/state.go`               | 9.1   | Done    |
| `execution/stagedsync/exec3_qmtree.go` | 9.1   | Done    |
| `execution/stagedsync/exec3_serial.go`  | 9.1   | Done    |
| `cmd/integration/commands/flags.go`     | 9.1   | Done    |
| `cmd/integration/commands/stages.go`    | 9.1   | Done    |

## Dependencies

- Phase 1-5b: Complete
- Phase 6a: None (standalone, like Phase 1)
- Phase 6b: Phase 6a
- Phase 6c: Phase 6b + hoodi datadir
- Phase 7: Phase 6 + existing qmtree PoC
- Phase 8: Phase 7
- Phase 9: Phase 8 + production readiness review
