# Flashblocks: Incremental Block Updates via DAG Commits

## Problem

The execution module assumes each block number arrives exactly once via
`newPayload`. The block is executed, validated, and then `forkchoiceUpdated`
finalises it. This is the L1 model — one complete block per payload.

In a DAG-based consensus system (Narwhal/Bullshark), multiple Bullshark
commits occur within a single slot. Each commit appends an ordered batch
of transactions to the in-progress block. The same block number arrives
multiple times, each time extended with additional transactions:

```
Flashblock 1: Block N  txs [A, B, C]
Flashblock 2: Block N  txs [A, B, C, D, E]
Flashblock 3: Block N  txs [A, B, C, D, E, F]
Slot seal:    Block N  txs [A, B, C, D, E, F]  ← FCU fires here
```

The execution module must handle this without re-executing the entire
transaction list on each update, and without treating an updated block as
a reorg.

## Design

### SharedDomains carries VersionedIO

Today the parallel executor (`blockExecutor`) creates `VersionedIO` and
`VersionMap` per block (`newBlockExec` at `exec3_parallel.go:2119`). The
block assembler tracks per-tx IO via `ba.balIO` and `ibs.TxIO()`. Both
are scoped to a single execution pass and discarded afterward.

SharedDomains already carries state with lifetimes beyond a single
execution: `stateCache`, `branchCache`, `adaptivePinController`, and the
`parent` chain for read-through to uncommitted generations. VersionedIO
has the same cross-execution lifetime requirement in the flashblock model
— the read/write sets from flashblock N must survive to inform flashblock
N+1's execution.

**Change:** Add `versionedIO *state.VersionedIO` and
`versionMap *state.VersionMap` fields to `SharedDomains`. The executor
reads them from the SD at the start of execution. If nil (fresh block or
post-reset), the executor creates them in the SD. If non-nil (flashblock
continuation), the executor extends them.

The executor remains responsible for all execution decisions — when to
re-execute, when to extend, when to restart. The SD is the carrier, not
the decision-maker.

### newPayload update semantics

`ValidateChain` (the `newPayload` handler) currently rejects or reorgs
when it receives a block number it has already processed. For flashblocks,
receiving the same block number is the normal path — it is an update, not
a duplicate or fork.

**Change:** When `ValidateChain` receives a block with the same number as
the current in-progress block:

1. Compare the transaction list prefix against what was already executed
   (the VersionedIO in the SD knows the tx count and identities).
2. **Prefix matches** — the new block extends the previous version. Pass
   only the delta transactions to the executor. The executor appends to
   the existing VersionedIO/VersionMap without touching already-executed
   state.
3. **Prefix does not match** — ordering changed. The executor restarts
   from the beginning of the slot. SD rewinds domain state to the slot
   start. VersionedIO resets. Full re-execution with the corrected tx
   list.

### Executor restart, not SD lifecycle

The executor decides whether to extend or restart based on the
VersionedIO read/write sets. The SD provides the state surface; the
executor provides the judgement. On restart:

- The executor resets versionedIO/versionMap in the SD (sets nil or
  creates fresh)
- SD rewinds domain state to the slot-start snapshot
- Executor re-executes the full tx list from the updated block

On extend:

- Executor reads existing versionedIO from the SD
- Executes only the new transactions
- Updates versionedIO in the SD with the new read/write sets
- Parallel execution detects conflicts automatically — independent txs
  from different flashblock commits parallelise; conflicting txs
  serialise

### Slot boundary and reset

At slot end, the sealed block goes through `forkchoiceUpdated` as normal.
FCU is unchanged — it commits the SD (now containing the full slot's
execution state) and hands it to the background commit worker (the
generation chain from PR #21414).

The executor resets the versionedIO in the SD after the block seals. For
multi-block sync (catch-up), the executor resets between each block — the
versionedIO never leaks across block boundaries.

**Reset rule:** The executor resets versionedIO/versionMap at:
- Slot boundary (block sealed via FCU)
- Block boundary during sync (multiple full blocks in sequence)
- Executor restart (ordering conflict detected)

### Unified execution: parallel executor for assembly and validation

Today there are two separate execution flows:

1. **Block assembly** (`execution/builder/exec.go`) — serial
   `AddTransactions` loop, pulls batches of 50 from the txpool,
   tracks per-tx IO via `ba.balIO` / `ibs.TxIO()`.
2. **Block validation** (`execution/stagedsync/exec3_parallel.go`) —
   parallel executor with `blockExecutor`, `VersionedIO`, `VersionMap`,
   speculative execution and validation.

The builder already does multiple execution passes — its loop at
`exec.go:185` keeps pulling txpool batches until interrupted or dry.
This is structurally the same as flashblock updates: multiple tx batches
arriving for the same in-progress block.

**Change:** Use the parallel executor for block assembly, not just
validation. The builder feeds txpool batches (or DAG commit batches in
flashblock mode) into the same parallel executor that validation uses.

This consolidation:

- **Unifies the execution engine.** Both assembly and validation use
  the same VersionedIO/VersionMap model. Today `ba.balIO` in the
  builder and `blockIO` in `blockExecutor` are separate
  implementations of the same concept. With one executor there is one
  VersionedIO that covers both paths.

- **Makes flashblocks natural.** The builder loop (pull batch → execute
  → pull more) and the flashblock loop (receive commit → execute →
  receive more) are the same pattern. The parallel executor handles
  both — batches from the txpool in standard mode, batches from DAG
  commits in flashblock mode.

Assembly and validation remain separate steps — the builder still
produces a block, `newPayload` still validates it in its own SD. The
change is that both steps use the same executor, not that validation is
skipped.

**Potential future optimization:** Once both paths use the same executor
and produce identical VersionedIO state, carrying the SD forward from
assembly to avoid re-execution in `ValidateChain` becomes possible. This
is a further step that changes the trust model (assembly result trusted
without re-validation) and should only be evaluated once the unified
executor is stable and the flow disruption can be assessed.

### hasMore / partial completion

The builder's `AddTransactions` currently returns `stop=true` for two
reasons: gas exhausted or interrupt fired. Both are terminal — the block
is done.

**Change:** Add a third stop condition: **flashblock boundary**. The
batch of transactions from this DAG commit is exhausted, but the block
is not sealed. The response to `newPayload` signals "accepted, awaiting
more" rather than "valid" (final).

The existing `interrupt` mechanism in the builder extends naturally:
a flashblock-mode builder does not loop polling the txpool — it
processes exactly the transactions from the DAG commit, returns partial
completion, and waits for the next commit's `newPayload` to resume.

This does not require changes to the serial execution path. The serial
executor does not use VersionedIO and does not operate in flashblock
mode.

## Data flow

```
DAG commit N arrives
    │
    ▼
newPayload(Block N, txs=[A..E])
    │
    ├─ First time seeing Block N?
    │   └─ Create SD, init VersionedIO, execute [A..E]
    │
    ├─ Same block, prefix [A..C] matches prior execution?
    │   └─ Read VersionedIO from SD, execute delta [D, E] only
    │
    └─ Same block, prefix mismatch?
        └─ Reset VersionedIO, rewind SD, re-execute full [A..E]
    │
    ▼
Response: "accepted, awaiting more"
    │
    ... more flashblock commits ...
    │
    ▼
Slot boundary: final newPayload + forkchoiceUpdated
    │
    ▼
FCU commits SD via generation chain (PR #21414 model)
Executor resets VersionedIO
```

## Scope

This design applies to the parallel executor only. Serial execution is
not affected — it does not use VersionedIO and does not operate in
flashblock mode.

The SharedDomains rename to ExecutionContext is a separate change that
can happen before or after this work. The versionedIO field addition does
not depend on or conflict with the rename.

## Files affected

### SharedDomains (db/state/execctx/domain_shared.go)

- Add `versionedIO *state.VersionedIO` field
- Add `versionMap *state.VersionMap` field
- Add getters/setters; nil means fresh, executor populates on first use
- Reset method for slot/block boundary cleanup

### Parallel executor (execution/stagedsync/exec3_parallel.go)

- `blockExecutor` reads versionedIO/versionMap from SD instead of
  creating its own
- On flashblock update: compare tx prefix, decide extend vs restart
- On extend: execute delta txs, merge results into existing VersionedIO
- On restart: reset SD's versionedIO, re-execute full list

### ValidateChain (execution/execmodule/exec_module.go)

- Recognise same-block-number payload as update, not reorg
- Pass update signal to executor (extend vs restart decision)
- Return partial-acceptance status for non-final flashblocks

### Builder (execution/builder/)

- Replace serial `AddTransactions` loop with parallel executor
- Builder becomes tx selection policy; executor handles mechanics
- Flashblock mode: process exactly the DAG commit's txs, return
  partial completion
- Standard mode: feed txpool batches into same parallel executor
- Locally-assembled blocks: SD carries forward, no re-validation

### Engine API types

- New payload status: "accepted, awaiting more" (or equivalent signal
  that the block is not yet sealed)

## Staging

### Step 1 (this commit)

- VersionedIO/VersionMap fields in SharedDomains
- newPayload update semantics (same block number = update)
- hasMore / partial completion signalling
- Parallel executor used in both assembly and validation
- Executor owns extend/restart/reset decisions

### Step 2 (separate, depends on SD lifecycle analysis)

- SD carry-forward from assembly into validation (skip re-execution
  for locally-assembled blocks)
- Requires tracing SD lifecycle through ValidateChain →
  updateForkChoice → enqueueCommit → commitWorker to understand
  what assumptions are baked in
- Only viable if the exec module's generation chain can adopt an SD
  created during assembly without flow disruption

## Not in scope

- Multi-validator DAG transport (Phase 3)
- Commitment/trie updates mid-slot (commitment runs at seal)
- SharedDomains → ExecutionContext rename
- Serial execution changes
