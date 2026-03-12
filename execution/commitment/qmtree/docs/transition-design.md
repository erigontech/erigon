# Proof of Transition: Design Document

## 1. Problem Statement

The proof of execution (Phase 1-5) covers the EVM interpreter loop — every
opcode executed within `interpreter.Run()`. But a transaction's state
transition involves significant application-level logic that runs *outside*
the EVM, in `StateTransition.TransitionDb()` and related functions.

These operations are **spec-mandated and deterministic** given the same
pre-state and transaction, but they are not EVM opcodes and therefore not
captured by the exec hasher. A complete proof of transition must cover them.

### What we need to capture

The operations *around* the EVM, not the state changes themselves. The
actual state changes (balance diffs, storage writes, nonce updates, code
deployments) are already captured as inputs to the existing commitment
calculation — they form the **stateChangeHash**. The transition hash proves
the *process* that produced those changes was correct.

### Leaf structure

The complete leaf combines all three proof layers:

```
leaf = hash(stateChangeHash || transitionHash || previousLeafHash)
```

- **stateChangeHash**: What changed (existing commitment inputs)
- **transitionHash**: How it changed (this document — embeds the exec hash)
- **previousLeafHash**: Chain ordering and inclusion (hash chain)

## 2. Operations Outside the EVM

These are the spec-mandated operations that happen in `state_transition.go`
and related code, ordered by execution sequence:

### Phase A: Pre-Execution

| # | Operation | Spec Reference | Data to Capture |
|---|-----------|---------------|-----------------|
| A1 | **Nonce validation** | Yellow Paper sec 6 | sender, expected_nonce, tx_nonce |
| A2 | **EOA check** (EIP-3607) | EIP-3607 | sender, code_hash (empty or delegated) |
| A3 | **Fee cap validation** (EIP-1559) | EIP-1559 | fee_cap, tip_cap, base_fee |
| A4 | **Blob fee validation** (EIP-4844) | EIP-4844 | max_fee_per_blob_gas, blob_base_fee, blob_gas |
| A5 | **Gas purchase** | Yellow Paper sec 6 | sender, gas_value, blob_gas_value |
| A6 | **Intrinsic gas** | Yellow Paper + EIPs | data_cost, auth_cost, access_list_cost, creation_cost, floor_cost |
| A7 | **EIP-7702 authorizations** | EIP-7702 | per authority: address, nonce, delegation_target, refund_added |
| A8 | **Nonce increment** | Yellow Paper sec 6 | sender, old_nonce -> new_nonce |
| A9 | **Access list warm-up** | EIP-2930 | sender, coinbase, destination, precompiles[], access_tuples[] |

### Phase B: EVM Execution

| # | Operation | Covered By |
|---|-----------|-----------|
| B1 | **Value transfer** (`Transfer()`) | New: transition record (not an opcode) |
| B2 | **EVM interpreter loop** | Existing: exec hash (Phase 1-5) — see [design.md §2.2](design.md#execution-hash-per-opcode-record-format-exechasher) for per-opcode record format |
| B3 | **Precompile calls** | Existing: exec hash precompile records — same section |

Note: `Transfer()` is called inside `evm.Call()` / `evm.Create()` but is
not an EVM opcode — it's application logic that moves balances. It needs
its own record type.

### Phase C: Post-Execution

| # | Operation | Spec Reference | Data to Capture |
|---|-----------|---------------|-----------------|
| C1 | **Gas refund calculation** | EIP-3529 | gas_used, state_refund, refund_quotient, effective_refund |
| C2 | **Floor gas** (EIP-7623/7778) | EIP-7623 | floor_gas_cost, block_gas_used |
| C3 | **Gas return** | Yellow Paper sec 6 | sender, remaining_gas, refund_value |
| C4 | **Tip payment** | EIP-1559 | coinbase, effective_tip, gas_used, tip_amount |
| C5 | **Fee burn** | EIP-1559 | burnt_contract, base_fee, gas_used, burn_amount |
| C6 | **PostApplyMessage** | Engine-specific | engine_type, effects |

## 3. Canonical Record Format

The full canonical record format specification is defined in
[TRANSITION_FORMAT.md](TRANSITION_FORMAT.md). That document specifies:

- 11 record types with fixed-width tagged binary encoding
- Primitive type encodings (uint8/16/64, uint256, address, hash, bool)
- Record sequence ordering and within-type sorting rules
- Cross-client determinism analysis
- Complete worked example

### 3.1 Encoding Strategy: Rolling Keccak -> SSZ

**Phase 6 (initial)**: Tagged binary records written to a rolling
keccak256. Simple, fast, validates the concept. The transition hash is:

```
transitionHash = keccak256(record[0] || record[1] || ... || record[N])
```

**Phase 8 (witnesses)**: Migrate to SSZ (Simple Serialize) container with
`hash_tree_root()`. This provides:

- **Field-level Merkle proofs**: Prove individual fields (e.g., `exec_hash`
  or `tip_amount`) without revealing the full record
- **CL alignment**: Same proof format as the Beacon Chain's `BeaconState`
- **Standardization**: Cross-client SSZ libraries exist in Go, Rust,
  Python, Java, TypeScript, C++
- **Forward compatibility**: New fields can be appended without breaking
  existing proofs

SSZ was chosen over RLP because RLP does not support Merkleization or
witness proofs. RLP is also being phased out of the EL (EIP-6404/6466).
The CL already uses SSZ for all state transition proofs.

The field set and semantics are identical between the rolling keccak and
SSZ approaches — only the encoding and hash computation change.

See [TRANSITION_FORMAT.md](TRANSITION_FORMAT.md) Section 9 for the full
SSZ container definition.

### 3.2 Record Sequence

Records are emitted in the order the Ethereum spec mandates:

```
TX_CONTEXT          transaction identity and fee parameters
INTRINSIC_GAS       computed intrinsic gas costs
GAS_PURCHASE        balance deducted from sender
NONCE_INCREMENT     sender nonce update
AUTH_RESULT*        EIP-7702 authorization outcomes (tx order)
ACCESS_WARM*        warmed addresses (sorted)
ACCESS_SLOT*        warmed storage slots (sorted by address, slot)
TRANSFER*           value transfers at all call depths (execution order)
EXEC_HASH           embedded EVM execution hash (Phase 1-5)
GAS_ACCOUNTING      refund calculation and gas return
FEE_DISTRIBUTION    tip, burn, and gas return to sender
```

## 4. Resolved Design Decisions

| Question | Decision | Rationale |
|----------|----------|-----------|
| Access list ordering | Sort by (address, slot) | Maps are unordered; sorting guarantees determinism |
| Transfer depth | Capture all depths | Inner transfers are not captured by exec hash; CALL opcode has value but Transfer() is application logic |
| Detail level | Record computed results, not inputs | Verifier re-derives from tx + block context; keeps records compact |
| Encoding format | Rolling keccak (Phase 6) -> SSZ (Phase 8) | Keccak validates concept; SSZ adds witness composability and CL alignment |
| Receipts | Excluded | Derivable from gas_used + execution result; stateChangeHash covers logs |
| System calls | Excluded | EIP-4788 etc. are block-level, not per-tx |
| PostApplyMessage | Excluded | Effects are in stateChangeHash |
| Zero-value transfers | Excluded | No balance movement occurs |
| Sender in records | Excluded | Derivable from tx_hash (ecrecover) |

## 5. Integration Points

### 5.1 In `state_transition.go`

The `TransitionHasher` is attached to `StateTransition` and called at
each operation point. See [TRANSITION_FORMAT.md](TRANSITION_FORMAT.md)
for exact record definitions.

### 5.2 Transfer Hook

Transfer happens inside `evm.Call()` BEFORE `interpreter.Run()`. The EVM
gets a `transitionHasher` field (like `execHasher`) and emits TRANSFER
records when `Transfer()` is called with non-zero value.

## 6. Implementation Plan

See [IMPLEMENTATION_PLAN.md](IMPLEMENTATION_PLAN.md) for the full
phase-by-phase plan. Phase 6a (TransitionHasher core) is next.
