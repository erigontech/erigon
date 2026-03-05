# Rationalize IntraBlockState to a 2-Cache Model

## Summary

We currently cache Ethereum execution state in **four separate places**, causing significant code duplication, a latent timing race between blocks, and an expensive round-trip in the hot path of parallel execution. The goal of this refactor is to reduce to **two authoritative caches**:

- **`VersionMap` / `VersionedIO`** — intra-block, per-transaction, revertable MVCC memory. Workers write here; snapshots and reverts operate here. This is the source of truth during a block's execution.
- **`SharedDomains`** — confirmed inter-block state. After a block is fully validated and ordered, its net writes are applied here. Workers for the next block read from this as their baseline.

Everything else — `stateObject`, `rs.accounts` / `StateV3Buffered`, and `BufferedWriter` — is redundant scaffolding that can be eliminated once the two-cache model is properly plumbed.

---

## Background: The Current 4-Cache Architecture

During parallel block execution, a state write touches up to four distinct stores:

| Cache | Type | Location | Role |
|---|---|---|---|
| `VersionedIO` / `WriteSet` | Field-granular MVCC map | `versionmap.go`, `versionedio.go` | Per-tx write set; conflict detection; BAL hash |
| `stateObject` | Per-address struct | `state_object.go` | EVM read/write buffer inside IBS; snapshot substrate |
| `StateV3Buffered.accounts` (`rs.accounts`) | Address-keyed in-memory map | `rw_v3.go` | Cross-tx/cross-block read cache; workaround for timing hole |
| `SharedDomains` | Persistent in-memory B-tree | `shared_domains.go` | Block-to-block state; written to MDBX on commit |

## The Dual-Path Round-Trip

The most acute form of duplication is in `finalize()` inside `exec3_parallel.go`. After a transaction is validated and ordered, the code reconstructs an `IntraBlockState` from `VersionedWrites`, then immediately tears it back down into `StateUpdates` for domain application:

```
VersionedWrites (field-granular)
  → ApplyVersionedWrites → new IBS (stateObjects rebuilt from scratch)
    → FinalizeTx / MakeWriteSet → StateUpdates (account-granular)
      → BufferedWriter → SharedDomains + rs.accounts
```

`VersionedWrites` and `StateUpdates` carry the same information in different formats. `VersionedWrites` has `BalancePath → uint256.Int`, `NoncePath → uint64`, `AddressPath → *accounts.Account`, etc. `StateUpdates` has `writeSet BTreeG[Address, *accountWrite]` with full `*accounts.Account` structs. The `AddressPath` entry in `VersionedWrites` already carries the full account, so `StateUpdates` can be derived directly without the IBS round-trip.

## The Timing Hole

In `execLoop()`, block N+1's workers are scheduled as soon as block N's `blockExecutor.result` is complete:

```go
blockExecutor.applyResults <- blockResult  // async
// block N+1 workers start HERE:
blockExecutor, ok = pe.blockExecutors[blockResult.BlockNum+1]
if ok {
    blockExecutor.scheduleExecution(ctx, pe)
}
```

Block N+1 workers immediately begin reading state. But `applyResults` is an async channel — the `exec()` goroutine may not have applied block N's writes to `SharedDomains` yet. `rs.accounts` (`StateV3Buffered.accounts`) patches this by being populated synchronously in `finalize()` / `nextResult()` before `applyResults` is sent. This is a workaround for the race, not a fix. Eliminating `rs.accounts` requires making the domain apply synchronous (in `execLoop()`, before scheduling N+1).

## `stateObject` Eliminability

`stateObject` currently serves three roles:

1. **Write buffer** — `data.Balance`, `data.Nonce`, `dirtyStorage`, `dirtyCode`, `selfdestructed`. Fully duplicated by `VersionedWrites` when `versionMap != nil`.
2. **Read cache** — `code []byte`, `originStorage` (pre-tx values for EIP-1283/2929), `blockOriginStorage`. Replaceable by a lightweight `codeCache map[Address][]byte` and `committedStorage map[Address]map[StorageKey]uint256.Int` on IBS.
3. **Snapshot/revert substrate** — journal entries already contain `prev` + `wasCommited` booleans sufficient to revert `VersionedWrites` independently of `stateObject`. The one exception is `resetObjectChange` (for `CreateAccount` overwriting an existing account), which currently snapshots the whole `*stateObject` and needs to be migrated to snapshot `map[AccountKey]*VersionedWrite`.

Since we are decommissioning the serial execution path, `versionMap` will always be non-nil in IBS. This makes the stateObject write-buffer role immediately redundant.

The `newlyCreated` flag (EIP-6780, SELFDESTRUCT semantics) is derivable from `VersionedWrites` by checking whether `IncarnationPath` is present with a write from the current transaction.

---

## Phased Refactoring Plan

### Phase 1 — Establish a Test Baseline

Before any structural changes, add tests that will catch regressions across all subsequent phases.

**New tests in `execution/state/intra_block_state_test.go`:**
- `TestVersionedWritesMatchStateObjects` — for a randomized sequence of EVM operations, assert that every field in every `stateObject` is reflected in `VersionedWrites` with the same value.
- `TestSnapshotRandomWithVersionMap` — extend the existing `TestSnapshotRandom` to always set `versionMap`, and verify that revert produces identical state in both stateObjects and VersionedWrites.
- `TestCommittedStateWithVersionMap` — verify that `GetCommittedState` returns pre-tx values (EIP-1283) correctly when reading through versionMap at `txIndex-1`.
- `TestStateUpdatesEquivalence` (initially skipped) — given the same tx sequence, assert that `StateUpdatesFromVersionedWrites()` (not yet implemented) produces identical `StateUpdates` to the existing `BufferedWriter` path. Unskip in Phase 2.

**New tests in `execution/stagedsync/exec3_parallel_test.go`:**
- `TestCrossBlockStateReadConsistency` — two sequential blocks; block N+1 workers must read block N's committed writes. Assert no stale reads.
- `TestCrossBlockTimingRace` — stress test: inject artificial delay in `exec()` goroutine's `ApplyTxState`; verify block N+1 workers still see correct state. This test should be designed to fail on the current code without `rs.accounts`, to demonstrate the hole.
- `TestDomainApplyFromVersionedWrites` — assert that applying `StateUpdates` derived from `VersionedWrites` to a test `SharedDomains` instance produces the same domain state as the existing path.

### Phase 2 — Derive `StateUpdates` Directly from `VersionedWrites`

**Goal**: Eliminate the `VersionedWrites → IBS → StateUpdates` round-trip in `finalize()`.

**Changes:**
- Add `StateUpdatesFromVersionedWrites(writes VersionedWrites, rules *chain.Rules) StateUpdates` in `versionedio.go` (or a new file). Iterate writes, reconstruct `*accounts.Account` from `AddressPath` (full account), `BalancePath`, `NoncePath`, `IncarnationPath`, `CodePath`, `StoragePath`, `SelfDestructPath`.
- In `finalize()`, replace `ApplyVersionedWrites → IBS → FinalizeTx/MakeWriteSet → BufferedWriter` with a direct call to `StateUpdatesFromVersionedWrites`.
- Keep `rs.accounts` update in place (still needed for timing hole workaround — removed in Phase 3).

**Tests to un-skip/add:**
- `TestStateUpdatesEquivalence` — un-skip; must pass.
- `TestStateUpdatesFromVersionedWritesSelfDestruct` — verify selfdestruct is represented correctly (account deletion vs. zeroing).
- `TestStateUpdatesFromVersionedWritesIncarnation` — verify that a recreated contract (new incarnation) produces correct domain writes.

### Phase 3 — Fix the Timing Hole; Eliminate `rs.accounts`

**Goal**: Make domain apply synchronous so `SharedDomains` is always up-to-date before block N+1 workers start.

**Changes:**
- In `execLoop()`, after `blockExecutor.result` is complete, call `applyBlockWritesToDomains(blockResult)` synchronously before `scheduleExecution(blockResult.BlockNum+1)`. This replaces the async `applyResults` channel send for the domain-apply portion (the channel may still be used for other coordination).
- Remove `StateV3Buffered.accounts` (the `rs.accounts` map) and `bufferedReader`. Workers now read from `SharedDomains` directly (via `ReaderV3`) for cross-block state.
- Remove `BufferedWriter`'s `rs.accounts` update side effect; it is now dead code.

**Tests:**
- `TestCrossBlockTimingRace` — must now pass without `rs.accounts`.
- `TestCrossBlockApplyOrdering` — assert that applying block N synchronously before scheduling N+1 means N+1 workers never see stale state, even under goroutine scheduling pressure.
- `TestDomainConsistencyAfterBlockApply` — after `applyBlockWritesToDomains`, assert the `SharedDomains` snapshot matches what `VersionedWrites` predicted.
- `TestNoRsAccountsRace` — run the race detector (`-race`) over a multi-block parallel execution scenario; confirm no data races on the eliminated `rs.accounts` map.

### Phase 4 — Journal Reverts via `VersionedWrites` Only; Migrate `resetObjectChange`

**Goal**: Decouple journal revert from `stateObject`, so Phase 5 can remove `stateObject` entirely.

**Changes:**
- In all journal entries (`balanceChange`, `nonceChange`, `storageChange`, `addSlotToAccessListChange`, etc.), remove the `stateObject` field mutations from the `revert()` method. The `VersionedWrites` revert path (already present in the `if s.versionMap != nil` block) is the only one that matters.
- Migrate `resetObjectChange`: instead of storing `*stateObject`, store `map[AccountKey]*VersionedWrite` (a snapshot of the pre-`CreateAccount` writes for that address). Revert restores those entries.
- Remove `balanceInc` map from IBS (only used in serial path).
- Verify that `newlyCreated` derivation from `VersionedWrites` is correct for EIP-6780.

**Tests:**
- `TestJournalRevertWithVersionMap` — revert every journal entry type and assert `VersionedWrites` contains the correct previous value.
- `TestResetObjectChangeMigrateWrites` — create an account, write to it, then `CreateAccount` over it, then revert; assert the pre-creation writes are fully restored in `VersionedWrites`.
- `TestSnapshotRandomWithVersionMapExtended` — extend Phase 1 test with more operations including `CreateAccount` over existing accounts, code sets, self-destructs. Must pass with journal reverts driven solely by `VersionedWrites`.

### Phase 5 — Remove `stateObject`

**Goal**: `IntraBlockState` holds no `stateObjects` map. All reads go through `versionMap`/storage reader; all writes go to `VersionedWrites`.

**Changes:**
- Add `codeCache map[Address][]byte` and `committedStorage map[Address]map[StorageKey]uint256.Int` to `IntraBlockState` for the read-cache role of `stateObject`.
- `GetCommittedState(addr, key)` reads from `committedStorage` if present; otherwise reads `versionMap` at `txIndex-1` (pre-tx value for EIP-1283 gas calculation); populates `committedStorage` on first access.
- `GetCode(addr)` reads from `codeCache`; populates from storage reader on miss.
- Remove `stateObjects`, `stateObjectsDirty`, `nilAccounts`, `dirtyStorage`, `originStorage`, `blockOriginStorage` from IBS.
- Remove `state_object.go` (or reduce to an empty shim for the compilation cycle).

**Tests:**
- `TestGetCommittedStateNoStateObject` — assert EIP-1283 original values are correct when reading through `committedStorage`/versionMap.
- `TestCreatedContractFlagFromVersionedWrites` — assert `newlyCreated` is correctly derived from presence of `IncarnationPath` write in current tx.
- `TestSSTOREGasCalcNoStateObject` — end-to-end SSTORE gas calculation for cold/warm/original value cases, with no `stateObject`.
- Full execution-spec-tests (`TestExecutionSpecBlockchain`) — run the full EEST fixture suite; all tests must pass.
- `TestDeleteRecreateSlots`, `TestDeleteCreateRevert` — existing chain-level tests; must continue to pass.

---

## Summary Table

| Phase | Goal | Key Change | Gate Tests |
|---|---|---|---|
| 1 | Baseline | Add tests that will catch regressions | `TestVersionedWritesMatchStateObjects`, `TestCrossBlockTimingRace`, `TestSnapshotRandomWithVersionMap` |
| 2 | Eliminate round-trip | `StateUpdatesFromVersionedWrites`, remove IBS reconstruction in `finalize()` | `TestStateUpdatesEquivalence`, `TestStateUpdatesFromVersionedWritesSelfDestruct` |
| 3 | Fix timing hole | Synchronous domain apply in `execLoop`, remove `rs.accounts` | `TestCrossBlockTimingRace` (now passing), `TestNoRsAccountsRace` |
| 4 | Decouple journal from stateObject | Revert only via `VersionedWrites`; migrate `resetObjectChange` | `TestJournalRevertWithVersionMap`, `TestResetObjectChangeMigrateWrites` |
| 5 | Remove `stateObject` | `codeCache`, `committedStorage` on IBS; delete `state_object.go` | `TestGetCommittedStateNoStateObject`, `TestSSTOREGasCalcNoStateObject`, EEST suite |

---

## Files Affected

- `execution/state/intra_block_state.go` — major (Phases 2–5)
- `execution/state/state_object.go` — removed in Phase 5
- `execution/state/journal.go` — Phase 4
- `execution/state/versionedio.go` — Phase 2 (`StateUpdatesFromVersionedWrites`)
- `execution/state/rw_v3.go` — Phase 3 (remove `StateV3Buffered.accounts`, `bufferedReader`, `BufferedWriter` side-effect)
- `execution/stagedsync/exec3_parallel.go` — Phases 2 and 3 (simplify `finalize()`, synchronize domain apply in `execLoop`)
- `execution/exec/state.go` — Phase 3 (worker state reader simplified)
