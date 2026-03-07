# Proof of Execution for QMDB Twig Merkle Tree

## Summary

This PR introduces **Proof of Execution** -- a mechanism that commits a
cryptographic hash of every EVM instruction executed during a transaction into
the qmtree leaf, alongside the existing state-change hash.

Today each qmtree leaf contains a hash of the **state diff** (account and
storage mutations) for a single transaction. This proves *what changed* but not
*how it changed*. Two different execution paths could theoretically produce the
same state diff. Proof of execution closes this gap: a verifier who re-executes
the transaction must produce an identical instruction-level hash, proving the
same code path was followed with the same inputs, outputs, and gas accounting
at every step.

### What Gets Hashed

For every opcode executed in the EVM interpreter loop, a compact record is fed
into a rolling keccak256:

```
[depth:2][pc:8][op:1][gas:8][cost:8][stack_inputs:N*32][stack_outputs:M*32]
```

These fields -- program counter, opcode, gas, cost, stack inputs/outputs, and
call depth -- are all defined by the Ethereum EVM specification. Any
spec-compliant client executing the same transaction against the same state
must produce the same values at every instruction, making the execution hash
deterministic across implementations.

The leaf hash becomes:

```
leaf = keccak256(stateChangeHash || executionHash)
```

For transactions with no EVM execution (simple ETH transfers), `executionHash`
is the hash of the empty byte sequence, maintaining a fixed-width structure.

### Proof of Transition (Future Work)

Proof of execution covers the EVM interpreter loop -- the core of smart
contract execution. A complete **proof of transition** would wrap this with the
full state transition context: gas purchase, nonce increment, value transfer,
gas refund, and receipt construction. These outer steps are mechanical
applications of the protocol rules and don't involve arbitrary code execution,
but including them would provide a complete cryptographic proof of the entire
`block.parentState + tx -> block.postState` transition. This is planned as a
follow-on extension once the EVM-level proof is validated.

### Approach

The implementation is built directly into the EVM interpreter loop (not the
tracer infrastructure) to minimize overhead. A nil-guarded `ExecHasher` field
on the EVM struct means zero cost when disabled. The PoC starts with inline
hashing (simplest) and only moves to an async ring-buffer design if benchmarks
show unacceptable overhead on real chain data.

### Key Design Decisions

- **Spec-level fields only**: All hashed fields (pc, op, gas, cost, stack
  values, depth) are defined by the EVM spec, ensuring cross-client determinism.
- **Stack inputs + outputs, not full snapshots**: `numPop` items before execute
  and `numPush` items after is sufficient to reconstruct the full trace.
- **Memory excluded**: Too large to hash per-instruction. Memory effects are
  captured indirectly through stack arguments and the state-change hash.
- **Reverted calls included**: The execution hash captures what was *executed*,
  not what was *committed*. Reverts are reflected in the state-change hash.
- **Precompiles**: Hashed as synthetic records (address + input/output hashes)
  since they bypass the EVM loop.

See the attached design document and implementation plan for full details.
