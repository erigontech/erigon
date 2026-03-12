# Proof of Transition: Canonical Record Format Specification

Version: 0.1 (draft)

## 1. Overview

This document specifies the canonical binary encoding for the transition
hash — the cryptographic commitment to the application-level operations
that surround the EVM during transaction execution.

The transition hash is one of three components in the leaf:

```
leaf = hash(stateChangeHash || transitionHash || previousLeafHash)
```

The transition hash covers the **process** (how the state transition was
computed). The state change hash covers the **result** (what changed). The
previous leaf hash provides **ordering** (chain inclusion).

### 1.1 Relationship to Execution Hash

The transition hash **embeds** the execution hash (Phase 1-5) as one of
its records. The execution hash covers the EVM interpreter loop. The
transition hash covers everything outside the interpreter loop that the
Ethereum spec mandates.

```
transitionHash = keccak256(record[0] || record[1] || ... || record[N])
```

where one of the records is the embedded execution hash.

### 1.2 Design Principles

1. **Fixed-width fields**: Every field has a known size. No length-prefixed
   variable-length data in the hot path.
2. **Self-describing**: Each record starts with a 1-byte tag identifying
   its type. The tag determines the exact byte layout.
3. **Big-endian**: All multi-byte integers are big-endian, consistent with
   Ethereum conventions (RLP, ABI, keccak inputs, EVM stack).
4. **Deterministic ordering**: Records are emitted in the exact sequence
   the Ethereum spec mandates. Where the spec defines set semantics (e.g.,
   access lists), elements are sorted canonically.
5. **No framing**: Records are written directly to the rolling keccak256.
   No length prefixes or delimiters between records — the tag byte and
   fixed sizes provide unambiguous parsing.

## 2. Primitive Type Encoding

| Type | Size | Encoding |
|------|------|----------|
| `uint8` | 1 byte | Raw byte |
| `uint16` | 2 bytes | Big-endian |
| `uint64` | 8 bytes | Big-endian |
| `uint256` | 32 bytes | Big-endian, left-zero-padded |
| `address` | 20 bytes | Raw 20 bytes (no left-padding) |
| `hash` | 32 bytes | Raw 32 bytes |
| `bool` | 1 byte | `0x00` = false, `0x01` = true |

Addresses are always the raw 20-byte Ethereum address. For clients using
internal representations (e.g., interned handles), extract the raw 20
bytes before encoding.

## 3. Record Definitions

### 3.1 Record Sequence

Records MUST be emitted in the following order for each transaction:

```
TX_CONTEXT          (exactly 1)
INTRINSIC_GAS       (exactly 1)
GAS_PURCHASE        (exactly 1)
NONCE_INCREMENT     (exactly 1, for non-contract-creation only)
AUTH_RESULT          (0..N, one per EIP-7702 authorization, tx order)
ACCESS_WARM         (0..N, sorted by address)
ACCESS_SLOT         (0..N, sorted by (address, slot))
TRANSFER            (0..N, one per value transfer including nested calls)
EXEC_HASH           (exactly 1)
GAS_ACCOUNTING      (exactly 1)
FEE_DISTRIBUTION    (exactly 1)
```

### 3.2 TX_CONTEXT

Identifies the transaction and its validation parameters. Emitted once
at the start.

```
Tag: 0x01
Size: 117 bytes

┌──────┬─────────────┬───────┬──────────┬──────────┬──────────┬──────────┬──────────┐
│ 0x01 │ tx_hash: 32 │ ty: 1 │ nonce: 8 │ gas_lim:8│ fee_cap:32│ tip_cap:32│ value:32│
└──────┴─────────────┴───────┴──────────┴──────────┴──────────┴──────────┴──────────┘
  1    + 32           + 1     + 8        + 8        + 32       + 32       + 32  = 146
```

| Field | Type | Description |
|-------|------|-------------|
| tag | uint8 | `0x01` |
| tx_hash | hash | Transaction hash (keccak256 of signed tx RLP) |
| tx_type | uint8 | Transaction type (0=legacy, 1=access list, 2=EIP-1559, 3=blob, 4=EIP-7702) |
| nonce | uint64 | Transaction nonce (must match sender's state nonce) |
| gas_limit | uint64 | Transaction gas limit |
| fee_cap | uint256 | Max fee per gas (gasPrice for legacy/access-list txs) |
| tip_cap | uint256 | Max priority fee per gas (gasPrice for legacy txs) |
| value | uint256 | ETH value transferred |

**Rationale**: These fields identify the transaction and its fee parameters.
A verifier needs them to independently calculate intrinsic gas, gas
purchase amount, and fee distribution. The `tx_hash` provides a binding
to the signed transaction.

### 3.3 INTRINSIC_GAS

Records the result of the intrinsic gas calculation.

```
Tag: 0x02
Size: 17 bytes

┌──────┬────────────────┬────────────────┐
│ 0x02 │ regular_gas: 8 │ floor_gas: 8   │
└──────┴────────────────┴────────────────┘
```

| Field | Type | Description |
|-------|------|-------------|
| tag | uint8 | `0x02` |
| regular_gas | uint64 | Standard intrinsic gas (data cost + auth cost + access list cost + creation cost) |
| floor_gas | uint64 | Floor gas cost (EIP-7623). Zero if pre-Prague. |

**Rationale**: The intrinsic gas formula is specified exactly by the Yellow
Paper and subsequent EIPs. Recording both `regular_gas` and `floor_gas`
allows a verifier to check the calculation independently and verify that
the correct EIP rules were applied.

### 3.4 GAS_PURCHASE

Records the gas purchase — balance deducted from sender.

```
Tag: 0x03
Size: 65 bytes

┌──────┬──────────────────┬───────────────────┐
│ 0x03 │ gas_value: 32    │ blob_gas_value: 32 │
└──────┴──────────────────┴───────────────────┘
```

| Field | Type | Description |
|-------|------|-------------|
| tag | uint8 | `0x03` |
| gas_value | uint256 | `gas_limit * gas_price` — wei deducted for execution gas |
| blob_gas_value | uint256 | `blob_gas * blob_base_fee` — wei deducted for blob gas. Zero if no blobs. |

**Rationale**: The sender address is already in TX_CONTEXT (derivable from
tx_hash). Recording the computed values rather than the inputs allows a
verifier to check the multiplication was done correctly.

### 3.5 NONCE_INCREMENT

Records the sender nonce increment.

```
Tag: 0x04
Size: 9 bytes

┌──────┬────────────────┐
│ 0x04 │ new_nonce: 8   │
└──────┴────────────────┘
```

| Field | Type | Description |
|-------|------|-------------|
| tag | uint8 | `0x04` |
| new_nonce | uint64 | Nonce after increment (`old_nonce + 1`) |

**Rationale**: `old_nonce` is in TX_CONTEXT as `nonce`. The new nonce is
always `nonce + 1`. Recording just the result is sufficient — a verifier
checks `new_nonce == nonce + 1`.

**Note**: For contract creation transactions, the nonce increment happens
inside `evm.Create()` rather than in `TransitionDb()`. The NONCE_INCREMENT
record is still emitted at this point in the sequence for both call and
create transactions.

### 3.6 AUTH_RESULT

Records the result of processing one EIP-7702 authorization.

```
Tag: 0x05
Size: 50 bytes

┌──────┬───────────────┬──────────┬────────────────┬──────────┐
│ 0x05 │ authority: 20 │ nonce: 8 │ delegation: 20 │ accept:1 │
└──────┴───────────────┴──────────┴────────────────┴──────────┘
```

| Field | Type | Description |
|-------|------|-------------|
| tag | uint8 | `0x05` |
| authority | address | Recovered authority address |
| nonce | uint64 | Authority's nonce at time of processing |
| delegation | address | Delegation target address. Zero address if revoking. |
| accepted | bool | `0x01` if authorization was accepted, `0x00` if skipped |

**Ordering**: AUTH_RESULT records MUST appear in the same order as the
authorizations in the transaction. This is the transaction's original
order, which all clients process identically (it's an array, not a set).

**Rationale**: Each authorization may be accepted or rejected for various
reasons (wrong chain ID, nonce mismatch, non-empty code). Recording the
outcome per authorization lets a verifier check the processing logic.

### 3.7 ACCESS_WARM

Records an address added to the access list (warmed).

```
Tag: 0x06
Size: 21 bytes

┌──────┬──────────────┐
│ 0x06 │ address: 20  │
└──────┴──────────────┘
```

| Field | Type | Description |
|-------|------|-------------|
| tag | uint8 | `0x06` |
| address | address | Warmed address |

**Ordering**: ACCESS_WARM records MUST be sorted by address bytes
(lexicographic, unsigned). This is necessary because `Prepare()` adds
addresses from multiple sources (sender, destination, precompiles, access
list, coinbase, authorities) and the internal data structure is a map
(unordered in Go and most languages).

**Deduplication**: Each unique address appears exactly once, even if added
from multiple sources.

### 3.8 ACCESS_SLOT

Records a storage slot added to the access list (warmed).

```
Tag: 0x07
Size: 53 bytes

┌──────┬──────────────┬───────────┐
│ 0x07 │ address: 20  │ slot: 32  │
└──────┴──────────────┴───────────┘
```

| Field | Type | Description |
|-------|------|-------------|
| tag | uint8 | `0x07` |
| address | address | Account address |
| slot | hash | Storage slot key |

**Ordering**: ACCESS_SLOT records MUST be sorted by `(address, slot)` —
first by address bytes (lexicographic), then by slot bytes (lexicographic).

**Deduplication**: Each unique `(address, slot)` pair appears exactly once.

### 3.9 TRANSFER

Records a value transfer (balance movement).

```
Tag: 0x10
Size: 75 bytes

┌──────┬──────────┬──────────┬─────────┬───────────┐
│ 0x10 │ depth: 2 │ from: 20 │ to: 20  │ value: 32 │
└──────┴──────────┴──────────┴─────────┴───────────┘
```

| Field | Type | Description |
|-------|------|-------------|
| tag | uint8 | `0x10` |
| depth | uint16 | Call depth at which transfer occurs (0 = top-level) |
| from | address | Sender of the value |
| to | address | Recipient of the value |
| value | uint256 | Amount transferred in wei |

**When emitted**: A TRANSFER record is emitted every time the `Transfer()`
function is called — both for the top-level `evm.Call()`/`evm.Create()`
and for nested CALL/CREATE opcodes with non-zero value. This captures all
balance movements that are not EVM opcodes.

**Ordering**: TRANSFER records appear in execution order (the order
`Transfer()` is called as the EVM enters call frames). For a transaction
with nested calls, the sequence would be:

```
TRANSFER(depth=0, sender, dest, msg.value)     # top-level
TRANSFER(depth=1, dest, inner_addr, call_value) # nested CALL with value
```

**Zero-value transfers**: A TRANSFER record is NOT emitted when the value
is zero, since no balance movement occurs (the `Transfer()` function is
still called but it's a no-op for the state).

**Note**: The exec hash already captures the CALL opcode with its value
parameter in the stack. The TRANSFER record in the transition hash captures
the actual balance-movement operation that is outside the EVM interpreter.

### 3.10 EXEC_HASH

Embeds the execution hash from Phase 1-5.

```
Tag: 0x11
Size: 41 bytes

┌──────┬────────────────┬──────────┐
│ 0x11 │ exec_hash: 32  │ ops: 8   │
└──────┴────────────────┴──────────┘
```

| Field | Type | Description |
|-------|------|-------------|
| tag | uint8 | `0x11` |
| exec_hash | hash | Execution hash from `ExecHasher.Finalize()` |
| ops | uint64 | Number of opcodes executed |

**Rationale**: This creates a composition — the transition hash includes
the execution hash as a nested commitment. A verifier who wants to check
EVM correctness can re-execute the transaction and compare the exec_hash.
Including `ops` provides a quick sanity check and diagnostic.

### 3.11 GAS_ACCOUNTING

Records the post-execution gas accounting.

```
Tag: 0x20
Size: 41 bytes

┌──────┬────────────┬───────────────┬──────────────────┬─────────────────┬────────────────┐
│ 0x20 │ gas_used: 8│ state_refund:8│ effective_refund:8│ remaining_gas: 8│ block_gas_used:8│
└──────┴────────────┴───────────────┴──────────────────┴─────────────────┴────────────────┘
```

| Field | Type | Description |
|-------|------|-------------|
| tag | uint8 | `0x20` |
| gas_used | uint64 | Gas consumed by execution (before refunds) |
| state_refund | uint64 | Accumulated refund counter from SSTORE operations |
| effective_refund | uint64 | Actual refund applied: `min(gas_used/quotient, state_refund)` |
| remaining_gas | uint64 | Gas returned to sender: `initial_gas - gas_used + effective_refund` (adjusted for floor) |
| block_gas_used | uint64 | Gas charged against the block limit (may differ from receipt gas due to EIP-7778) |

**Rationale**: This single record captures the entire refund calculation.
A verifier can check: `effective_refund <= gas_used/5` (EIP-3529),
`remaining_gas = initial_gas - gas_used + effective_refund` (respecting
floor gas), and `block_gas_used` matches the correct formula for the
active hard fork.

### 3.12 FEE_DISTRIBUTION

Records how fees are distributed after execution.

```
Tag: 0x21
Size: 97 bytes

┌──────┬──────────────┬───────────────┬──────────────────────┬────────────────┬──────────────────┐
│ 0x21 │ coinbase: 20 │ tip_amount: 32│ burnt_contract: 20   │ burn_amount: 32│ gas_return_val: 32│
└──────┴──────────────┴───────────────┴──────────────────────┴────────────────┴──────────────────┘

 Note: burnt_contract is 20 bytes (zero address if no burn).
       gas_return_val is the wei value returned to sender.
```

| Field | Type | Description |
|-------|------|-------------|
| tag | uint8 | `0x21` |
| coinbase | address | Block producer address |
| tip_amount | uint256 | `gas_used * effective_tip` — wei sent to coinbase |
| burnt_contract | address | Burn address (chain-specific). Zero if no EIP-1559 burn. |
| burn_amount | uint256 | `gas_used * base_fee` — wei sent to burn address |
| gas_return_value | uint256 | `remaining_gas * gas_price` — wei returned to sender |

**Rationale**: Combining all fee distribution into one record keeps the
format compact and makes verification straightforward. A verifier checks:
- `tip = gas_used * min(tip_cap, fee_cap - base_fee)`
- `burn = gas_used * base_fee`
- `return = remaining_gas * gas_price`

## 4. Complete Example

A simple ETH transfer (type 2, EIP-1559) with no access list, no
authorizations, no contract creation:

```
TX_CONTEXT(tx_hash, type=2, nonce=42, gas_limit=21000,
           fee_cap=30gwei, tip_cap=2gwei, value=1eth)
INTRINSIC_GAS(regular=21000, floor=0)
GAS_PURCHASE(gas_value=630000gwei, blob_gas_value=0)
NONCE_INCREMENT(new_nonce=43)
ACCESS_WARM(coinbase)              # sorted
ACCESS_WARM(sender)                # sorted
ACCESS_WARM(destination)           # sorted
TRANSFER(depth=0, sender, dest, 1eth)
EXEC_HASH(empty_keccak, ops=0)    # no code to execute
GAS_ACCOUNTING(gas_used=21000, state_refund=0, effective_refund=0,
               remaining_gas=0, block_gas_used=21000)
FEE_DISTRIBUTION(coinbase, tip=42000gwei, burnt_contract, burn=588000gwei,
                  gas_return=0)
```

Total: 146 + 17 + 65 + 9 + 21*3 + 75 + 41 + 41 + 97 = **554 bytes**
hashed into keccak256.

A contract call with EIP-2930 access list of 2 addresses and 3 slots
would add 2 ACCESS_WARM (42 bytes) and 3 ACCESS_SLOT (159 bytes), plus
a non-trivial EXEC_HASH. Total remains under 1 KB for typical transactions.

## 5. Ordering Guarantees

### 5.1 Record Sequence (Fixed)

The record types MUST appear in the order specified in Section 3.1. This
order matches the Ethereum spec's execution sequence:

1. Validate and identify the transaction (TX_CONTEXT)
2. Calculate intrinsic gas (INTRINSIC_GAS)
3. Purchase gas from sender (GAS_PURCHASE)
4. Increment sender nonce (NONCE_INCREMENT)
5. Process authorizations (AUTH_RESULT, in tx order)
6. Warm access list (ACCESS_WARM + ACCESS_SLOT, sorted)
7. Execute with value transfer (TRANSFER + EXEC_HASH)
8. Account for gas (GAS_ACCOUNTING)
9. Distribute fees (FEE_DISTRIBUTION)

### 5.2 Within-Type Ordering

| Record Type | Ordering Rule |
|-------------|--------------|
| AUTH_RESULT | Transaction order (array index) |
| ACCESS_WARM | Sorted by address (lexicographic, 20 bytes unsigned) |
| ACCESS_SLOT | Sorted by (address, slot) — address first, then slot, both lexicographic |
| TRANSFER | Execution order (order in which Transfer() is called during EVM execution) |

### 5.3 Conditional Records

Some records are conditionally present:

| Record | Condition |
|--------|-----------|
| NONCE_INCREMENT | Always present (both call and create txs) |
| AUTH_RESULT | Only for type-4 (EIP-7702) transactions |
| ACCESS_SLOT | Only if transaction has storage keys in access list |
| TRANSFER | Only if value > 0 in any call frame |
| FEE_DISTRIBUTION.burn | `burnt_contract` is zero address and `burn_amount` is zero if no EIP-1559 burn |

## 6. Cross-Client Determinism

### 6.1 What Makes This Deterministic

Every field in every record is derived from one of:

1. **Transaction fields** (tx_hash, type, nonce, gas_limit, fee_cap,
   tip_cap, value) — identical for all clients processing the same tx.
2. **Block context** (base_fee, blob_base_fee, coinbase) — from the block
   header, identical for all clients.
3. **Computed values** (intrinsic gas, gas purchase, refunds, tips) —
   formulas specified exactly by the Yellow Paper and EIPs.
4. **Execution results** (exec_hash, gas_used, state_refund) — determined
   by the pre-state + transaction, identical for correct implementations.
5. **Sorted sets** (access list addresses/slots) — canonical ordering
   removes implementation-dependent iteration order.

### 6.2 What Is NOT in the Format

| Excluded | Why |
|----------|-----|
| Sender address | Derivable from tx_hash (ecrecover) |
| Destination address | Derivable from tx data or CREATE address formula |
| Pre/post state values | Covered by stateChangeHash |
| Error messages | Implementation-specific strings |
| Gas pool state | Block-level accounting, not per-tx |
| Tracer output | Debug infrastructure |
| Engine-specific hooks | PostApplyMessage effects are in stateChangeHash |

### 6.3 Version Evolution

If the format needs to change (new EIPs, new record types), the
`tx_type` field in TX_CONTEXT implicitly versions the expected record
sequence — different transaction types produce different record sets.

For hard-fork-level changes that affect all tx types (e.g., new gas
accounting rules), a format version byte can be prepended:

```
0x00 || TX_CONTEXT || ...
```

Tag `0x00` is reserved for this purpose. Version `0x00` means "no version
prefix" (current format). Future versions start with a non-zero byte.

## 7. Implementation Notes

### 7.1 Performance

The transition hasher adds ~554-1000 bytes of keccak hashing per
transaction. At ~6 cycles/byte for keccak, this is ~3,000-6,000 cycles
per transaction — negligible compared to the EVM execution (millions of
cycles for contract calls) or even the exec hasher (~500 cycles per
opcode * thousands of opcodes).

### 7.2 Memory

The `TransitionHasher` struct needs:
- Keccak state: ~200 bytes
- Scratch buffer: ~150 bytes (largest record is FEE_DISTRIBUTION at 97 bytes, but we want room)

Total: ~400 bytes per hasher. Pooled via `sync.Pool`.

### 7.3 Sorting Access Lists

The access list sort happens once per transaction during `Prepare()`.
Sorting is O(N log N) where N is typically small (< 50 entries). This
is negligible overhead.

Implementation approach: after `Prepare()` populates the access list map,
extract addresses and slots into sorted slices, then emit ACCESS_WARM
and ACCESS_SLOT records in order.

### 7.4 Transfer Tracking

The `Transfer()` function is called via a function pointer
(`TransferFunc`) set on the EVM block context. To capture transfers
without modifying the Transfer function signature, the EVM's
`transitionHasher` field can be checked:

```go
func (evm *EVM) transfer(from, to Address, value *uint256.Int) {
    evm.Context.Transfer(evm.intraBlockState, from, to, value, ...)
    if evm.transitionHasher != nil && !value.IsZero() {
        evm.transitionHasher.HashTransfer(evm.depth, from, to, value)
    }
}
```

## 8. Resolved Design Decisions

| Question | Decision | Rationale |
|----------|----------|-----------|
| Access list ordering | Sort by (address, slot) | Maps are unordered; sorting guarantees determinism |
| Transfer depth | Capture all depths | Inner transfers are not captured elsewhere; exec hash has the CALL opcode but not the balance movement |
| Detail level | Record computed results, not inputs | Verifier can re-derive from tx + block context; keeps records compact |
| Receipts | Excluded | Derivable from gas_used + execution result; stateChangeHash covers logs |
| System calls | Excluded | EIP-4788 etc. are block-level, not per-tx; they would need their own mechanism |
| PostApplyMessage | Excluded | Effects are in stateChangeHash; the transition hash proves spec operations only |
| Zero-value transfers | Excluded | No balance movement occurs; reduces noise |

## 9. Encoding Format Considerations

### 9.1 Current Approach: Custom Binary with Rolling Keccak

The format defined in this document uses fixed-width tagged records
written directly to a rolling keccak256 hash. This is simple and fast
but produces an opaque 32-byte commitment — to verify any part of the
transition, a verifier must re-execute the entire transaction and
re-hash everything.

### 9.2 Alternative: SSZ (Simple Serialize)

SSZ is the serialization format used by the Ethereum consensus layer
(Beacon Chain). It provides several properties relevant to our use case:

**SSZ advantages for transition proofs:**

1. **Built-in Merkleization**: Every SSZ container has a defined
   `hash_tree_root()` that produces a Merkle tree over its fields.
   This means a verifier can prove individual fields without the
   full record — e.g., prove "the tip_amount was X" without revealing
   gas accounting details.

2. **Witness composability**: SSZ Merkle proofs are standardized. A
   witness for a single field is O(log N) hashes where N is the number
   of fields in the container. This maps directly to Phase 8's witness
   requirement.

3. **CL alignment**: The consensus layer already uses SSZ for its state
   transition (`BeaconState` is an SSZ container). Using SSZ for the EL
   transition proof creates a unified proof format across both layers.

4. **Standardization**: SSZ is specified in the Ethereum consensus specs.
   Cross-client implementations already exist in Go, Rust, Python, Java,
   TypeScript, and C++.

5. **Forward compatibility**: New fields can be appended to containers
   without breaking existing Merkle proofs for earlier fields.

**SSZ considerations:**

1. **EL precedent**: The execution layer currently uses RLP. However,
   EIP-6404 and EIP-6466 propose migrating EL receipts and transactions
   to SSZ. Our transition proof is new infrastructure and doesn't need
   to maintain RLP compatibility.

2. **Performance**: SSZ's `hash_tree_root` computes a full Merkle tree,
   which is more expensive than a single rolling keccak. For a container
   with ~11 fields, this is ~10 additional hashes. Still negligible
   compared to EVM execution.

3. **Library availability**: Erigon already has SSZ encoding in the CL
   (`cl/ssz/` package). The EL has minimal SSZ via `protolambda/ztyp`.

**RLP** is also native to the EL but is being phased out and does not
support Merkleization or witness proofs.

### 9.3 Proposed Approach: SSZ Container

Define the transition record as an SSZ container:

```python
class TransitionRecord(Container):
    # Transaction identity
    tx_hash: Bytes32
    tx_type: uint8
    nonce: uint64
    gas_limit: uint64
    fee_cap: uint256          # Bytes32
    tip_cap: uint256          # Bytes32
    value: uint256            # Bytes32

    # Intrinsic gas
    regular_gas: uint64
    floor_gas: uint64

    # Gas purchase
    gas_value: uint256        # Bytes32
    blob_gas_value: uint256   # Bytes32

    # Nonce
    new_nonce: uint64

    # Authorizations (EIP-7702)
    authorizations: List[AuthResult, MAX_AUTHORIZATIONS]

    # Access list (sorted)
    warm_addresses: List[Bytes20, MAX_ACCESS_ADDRESSES]
    warm_slots: List[AccessSlot, MAX_ACCESS_SLOTS]

    # Transfers (execution order)
    transfers: List[Transfer, MAX_TRANSFERS]

    # Execution
    exec_hash: Bytes32
    exec_ops: uint64

    # Gas accounting
    gas_used: uint64
    state_refund: uint64
    effective_refund: uint64
    remaining_gas: uint64
    block_gas_used: uint64

    # Fee distribution
    coinbase: Bytes20
    tip_amount: uint256       # Bytes32
    burnt_contract: Bytes20
    burn_amount: uint256      # Bytes32
    gas_return_value: uint256 # Bytes32

class AuthResult(Container):
    authority: Bytes20
    nonce: uint64
    delegation: Bytes20
    accepted: boolean

class AccessSlot(Container):
    address: Bytes20
    slot: Bytes32

class Transfer(Container):
    depth: uint16
    from_addr: Bytes20        # "from" is reserved in Python
    to_addr: Bytes20
    value: uint256            # Bytes32
```

The `hash_tree_root(TransitionRecord)` becomes the `transitionHash`.

This gives us:
- **Field-level proofs**: Prove `exec_hash` or `tip_amount` independently
- **CL-compatible witness format**: Same proof structure as BeaconState
- **Future-proof**: Append new fields for new EIPs without breaking proofs

### 9.4 Decision

**Recommendation**: Use SSZ for the transition record container. Use the
existing tagged binary format (Sections 3-7) for initial implementation
and testing, then migrate to SSZ when integrating with the witness system
(Phase 8). The field set and semantics are identical — only the encoding
and hash computation change.

This two-step approach lets us:
1. Validate the concept quickly with the simple rolling keccak (Phase 6)
2. Upgrade to SSZ Merkleization when composability is needed (Phase 8)

The SSZ container definition above serves as the target specification.
