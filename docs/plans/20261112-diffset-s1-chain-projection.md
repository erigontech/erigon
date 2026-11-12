# Diffset S1: history and values projected from the mem-batch chains at flush

Research log: `~/org/mode/e/research/diffset-pipeline.org`, idea I2, next step N5. Base: `origin/main` `f9da00be8fb`.

## Problem

- Every put into `TemporalMemBatch` is held up to five times before flush:
  - the per-key chain (`putLatest`)
  - the history-values ETL collector, `history.go` `AddPrevValue`
  - the history-keys ETL collector, also in `AddPrevValue`
  - the values ETL collector (`domain.go` `addValue`)
  - the per-block `DomainDiff` (`db/kv/helpers.go` `DomainUpdate`)
- The chains already hold every version of every key in node runs:
  - `ERIGON_IN_MEM_HISTORY` defaults to true (`execution/execmodule/exec_module.go:63`)
  - parallel exec forces it on (`execution/stagedsync/exec3_parallel.go:395`)
- The three ETL collectors are fed on the exec path, inside the parallel apply loop. They duplicate what the chains hold.

## Goal

- The put path keeps the chain entry and the `DomainDiff` update only.
- `Flush` feeds the existing history and values writers from the chains, then flushes them as today.
- Every MDBX row and every `ChangeSets3` entry stays byte-identical to `f9da00be8fb` (T1's golden).
- Out of scope:
  - replacing the ETL collectors with one sorted pass (S2)
  - deriving `ChangeSets3` from the chains
  - any on-disk format change

## Invariants

- I-1: a same-txNum update replaces the chain entry's value and keeps its first prev. History and `DomainDiff` record the first prev (`temporal_mem_batch.go:224-235`).
- I-2: values rows are last-wins per (key, step). History rows are one per (key, txNum), with the prev exactly as passed to `TemporalMemBatch.DomainPut`/`DomainDel`, so history bytes are identical by construction. Rows are never derived from the previous chain entry.
- I-3: a discarded domain (`DiscardWrites`, `!d.Enabled`) or a history-disabled domain writes what it writes today. Feeding the writers at flush keeps their own `discard` checks.
- I-4: a chain entry owns its bytes (`temporal_mem_batch.go:240-242`). The prev is cloned unless it equals the previous entry's data; when it does, it shares that slice.

## Perf

- Arms: the branch head vs `f9da00be8fb`.
- Bench: M5 Max, `evm enginextest --run test_ether_transfers` and `test_storage`, stage time.
- Discipline: 8v8 ABBA under `lockf /Volumes/dev/tmp/m5-bench.lock`. Harness pattern: `~/org/mode/e/research/vm-bench-bal-on/snd/ab-et/run.sh`.
- First measured after T3 by the supervisor, and again after T4.
- Number to beat: the base arm's stage time on each fill.
- Stop line: if `ab-verdict` after T4 is not a stage win on `test_ether_transfers`, stop. The win then needs S2's sorted pass, and these tasks do not merge alone.
- RAM: before T2 merges, a heap profile at flush on `integration stage_exec` with and without the toggle, on the same batch. That covers the only runs where chains are off today.

## Tasks

### T1: flush golden oracle (test only)

- [x] `db/state/execctx/flush_golden_test.go`: a seeded write script through `SharedDomains`, flushed into a temporal MDBX tx. The dump holds every row of every table each domain owns, plus `ChangeSets3`, as sorted hex lines, compared against `testdata/flush_golden/<case>.txt`. A `-update` flag regenerates the file.
- [x] Use the domain set the existing `execctx` tests open; pick a step size small enough that the script crosses at least three steps.
- [x] Cases, one golden each:
  - [x] `forward`: Accounts, Storage, Code and Commitment, plus RCache if the test setup enables it. Several blocks, each with `SetChangesetAccumulator` / `SavePastChangesetAccumulator` as `exec3_serial.go:111-225` does. Include:
    - put, then put of the same key at a later txNum
    - put-put and del-put at the same txNum
    - delete of an existing key, and delete of an absent key
    - a key written in two steps
    - an account delete that cascades to storage (`DomainDel` on Accounts)
  - [x] `discard`: `forward` with `DiscardWrites` on Accounts/Storage/Code, as `db/state/squeeze.go:549` does.
  - [x] `unwind`: forward over blocks 1..6, then the in-memory `Unwind` to block 4's end, using the changeset `unwindDomsToBlock` builds (`execution/stagedsync/stage_execute.go:527`). Then blocks 5'..6' with different writes, then flush.
  - [x] `merge`: a receiver batch over blocks 1..3 and an extending-fork batch over blocks 4..5, merged as `ForkValidator.MergeExtendingFork` does (`execution/execmodule/fork_validator.go:143`), then flush. The fork batch reads through the receiver: build it the way the fork validator does.
- [x] `NOTES` answers: does the `unwind` golden hold history or values rows from the unwound blocks 5..6 that 5'..6' did not overwrite? Does `GetAsOf` on the merged batch, at a ts inside the receiver's range for a key both batches wrote, return the receiver's value?

NOTES:

- The unwind golden retains old-only rows at txNums 5..6: AccountVals 3, AccountHistoryVals 2, AccountHistoryKeys 2, StorageVals 3, StorageHistoryVals 2, StorageHistoryKeys 2, CodeVals 2, CodeHistoryVals 1, and CodeHistoryKeys 1. The old-only rows are from the pre-unwind writes and were not written by 5'..6'.
- The merged batch's `GetAsOf` for the shared account at ts 3 returns `(nil, false, nil)`, falling through to the unflushed transaction instead of returning the receiver value. This is not asserted because it is the behavior T3 is meant to change.
- The test copies the minimal unwind sequence from `execution/stagedsync/stage_execute.go:527` and the receiver/child merge sequence from `execution/execmodule/fork_validator.go:143`; it does not import `execution/...`.
- review fixes: Sequence table, multi-txNum blocks, Receipt domain

### T2: chains always complete

- [x] Delete the `inMemHistoryReads` toggle:
  - `TemporalMemBatch.SetInMemHistoryReads` / `InMemHistoryReads`, the `kv` interface method, `kv.ErrInMemHistoryDisabled`
  - `ERIGON_IN_MEM_HISTORY`
  - every setter: integration commands, `squeeze.go:552`, execmodule, `exec3_parallel.go:395`
  - the readers that tolerate `ErrInMemHistoryDisabled`
- [x] `putLatestLocked` keeps one entry per txNum in every run.
- [x] Tests that asserted the disabled behaviour are removed, not rewritten to pass. Name them under `NOTES`.
- [x] `NOTES`: how often `squeeze.go` (`openDomains`, `rebuildCommitmentShard`) writes one key at distinct txNums. That is its chain growth.
- [x] T1 goldens unchanged.

NOTES:

- Deleted `TestHistoryDisabledFallsThrough` in `db/kv/membatchwithdb/memory_mutation_test.go`. Removed the `pinned/history disabled` and `unpinned/history disabled` subcases from `TestEmbeddedRPCCacheViewHistoricalReads` in `db/state/execctx_test/statecache_rpc_integration_test.go`.
- `openDomains` only creates and configures the shared domains. Its caller reuses the same domains and write transaction for each block in a step; `finalizeBlock` sets the block txNum and calls `ComputeCommitment`, so a key touched in multiple blocks is written at each of those distinct txNums in one batch. `rebuildCommitmentShard` calls `ComputeCommitment` once at `cfg.TxnNumber` for a shard, and shard key ranges do not overlap, so each key is written once per shard batch.

### T3: history from the chains at flush

- [x] `dataWithTxNum` carries `prev []byte`, set per I-1 and I-4. `putLatestLocked` receives the prev.
- [x] `putHistory` no longer calls `PutWithPrev` / `DeleteWithPrev` / `PutWithPrevDiff` / `DeleteWithPrevDiff` for history. It keeps the `addValue` call (T4 moves it) and calls `diff.DomainUpdate(k, step, prev)` itself when the writer's diff, or `PutCommitmentBranchDiff`'s explicit diff, is set. A same-txNum update records nothing.
- [x] `Flush`, before `flushWriters`: for every chain entry of every domain, `domainWriters[d].h.AddPrevValue(key, txNum, prev)`.
- [x] `Merge` projects every unflushed receiver entry trimmed by the other's first txNum into the receiver's writers, then appends the other's chain unchanged.
- [x] Review: overlapping merges project trimmed receiver entries and append the other chain unchanged.
- [x] Review: discarded child entries are marked history/value flushed.
- [x] Review: storage same-txNum updates preserve the first prev.
- [x] Red first: a `GetAsOf` test on a merged batch, at a ts inside the receiver's range for a key both batches wrote. It fails on the base (whole-chain replace) and passes here.
- [x] Goldens: `forward`, `discard`, and `merge` stay byte-identical; `reflush` adds the storage same-txNum row; `unwind` keeps two `AccountVals`, two `StorageVals`, and one `CodeVals` row from full raw replay.
- [x] Direction:
  - Writers of the chains: `putLatestLocked` under the shard lock (exec loop, apply loop, commitment calculator), `Unwind` and `Merge` under every lock.
  - Writers of the history collectors after T3: `Flush`, plus direct commitment dump projection, under the existing domain locks.
  - `DomainDiff`: written at put time as today, swapped under `changesetMu`.
  - Before the first line: prove with the LSP whether production writes to or flushes a `TemporalMemBatch` again after `Flush` (callers of `SharedDomains.Flush` / `Commit` / `flushMem`, and the `o.mem` reuse at `domain_shared.go:359`). If one is, a per-entry flushed flag; a txNum watermark is wrong after an unwind. Answer under `NOTES`.

NOTES:

- Production reuses a batch after `Flush`: `SharedDomains.Flush` calls `flushMem` at `db/state/execctx/domain_shared.go:1101`, which calls `sd.mem.Flush` at `:1118`; `SharedDomains.Commit` calls `flushMem` at `:1200` and `:1245`. `NewSharedDomains` reuses `o.mem` at `domain_shared.go:359`, and direct tests flush one shared domain repeatedly at `db/state/execctx/domain_shared_test.go:498`, `:500`, `:504`, `:506`, `:510`, and `:512`. The per-entry flushed flag is required because an unwind can re-execute an old txNum.
- LSP found `TemporalMemBatch` as the only in-package caller of `PutWithPrev`, `PutWithPrevDiff`, `DeleteWithPrev`, and `DeleteWithPrevDiff`; `db/state/commitment_convert.go:489` calls `DomainBufferedWriter.PutWithPrev` directly and keeps its behavior.
- `unsafe.Sizeof(dataWithTxNum{})` is 32 bytes before T3 and 64 bytes after T3.
- The direct `dumpCommitmentStep` path projects the chains before collating history because it intentionally bypasses `Flush`.
- Overlapping merges project the receiver's trimmed history and per-step values before appending the other chain; discarded child entries are marked flushed so they do not emit rows. The storage same-txNum path keeps the first prev, like the sharded map path.

### T4: values from the chains at flush

- [x] `putHistory` drops `addValue`. `PutCommitmentBranchDiff` keeps only `putLatest` and the diff update.
- [x] `Flush`: for every chain, the last unflushed entry per step run goes to `domainWriters[d].addValue(key, data, step)`, before `flushWriters`.
- [x] Goldens: `forward`, `discard`, and `merge` stay byte-identical; `reflush` includes the storage same-txNum update; `unwind` keeps the five value-table rows restored by full raw replay.

NOTES:

- `dumpCommitmentStep` is called from the direct branch of `flushDomainsAndRebuild` at `db/state/squeeze.go:610`; it closes the domains and rolls back the write transaction immediately after, so no later `Flush` can collect its projected values twice. `dumpStepRangeToPath` also projects for `rebuildCommitmentShard` at `db/state/squeeze.go:1223` and commitment conversion at `db/state/commitment_convert.go:510`; both use a batch that is closed or discarded without a later `Flush`.
- LSP found `DomainBufferedWriter.addValue` callers outside `TemporalMemBatch` in `db/state/domain.go:588`, `db/state/domain.go:610`, and `db/state/domain_test.go:1001`; these retain their behavior.
- `unsafe.Sizeof(dataWithTxNum{})` is 64 bytes after splitting `flushed` into `histFlushed` and `valFlushed`.
- The T3 review fixes project trimmed overlap rows, preserve discarded-child rows as absent, and cover the storage same-txNum first-prev rule.
- Full raw replay restores these five value-table rows for unwound txNums: `AccountVals\t720000000000000000000000000000000000008d\tfffffffffffffffa`, `AccountVals\t730000000000000000000000000000000000008c\tfffffffffffffffa`, `StorageVals\t720000000000000000000000000000000000008d12000000000000000000000000000000000000000000000000000000000000ed\tfffffffffffffffa`, `StorageVals\t730000000000000000000000000000000000008c13000000000000000000000000000000000000000000000000000000000000ec\tfffffffffffffffa`, and `CodeVals\t730000000000000000000000000000000000008cfffffffffffffffa\t`. The final old-only counts are 2, 2, and 1 respectively; history counts remain zero.
- The unwind filter was removed because a raw `(key, step)` entry can come from an earlier committed batch and must be replayed even when the current in-memory chain has no flushed entry.
- Value projection selects the last unflushed entry in each step run so a discarded child's flushed marker cannot hide the receiver's value.

### T5 (dropped): Merge keeps no domain writers

- [ ] dropped: the other batch's domain writers move again because overlapping and flushed children retain collectors that the receiver must flush. `pastDomainWriters` remains, and the II writers (`pastIIWriters`, standalone indices via `IndexAdd`) stay.
- [ ] dropped: Goldens unchanged; item 4 changes `forward.txt` and `discard.txt`.

NOTES:

- LSP found the only production write into a `TemporalMemBatch` domain writer outside chain projection at `db/state/commitment_convert.go:489` through the writer initialized at `:424-425`; direct test writers are used at `db/state/domain_test.go:317`, `:328`, `:331`, and `:376`, plus `db/state/temporal_mem_batch_test.go:90`, while `db/state/squeeze.go:1257-1265` and `db/state/domain.go:913-915` only project and collate. The field references were checked with gopls.
- The parent `b16730c6c44` value rows for the overlap test are one `AccountVals` row per case: key `910000000000000000000000000000000000006e`, value `ffffffffffffffff09`.
- T5 is dropped because an overlapping grandchild leaves projected rows in its domain writers and a flushed child retains drained collectors. `Merge` moves current and past domain writers back to `pastDomainWriters`.
- `Merge` rejects explicit discarded writes on receiver or other writable domains before changing either batch. The permanently disabled RCache writer is excluded because its discard bit is configuration state, not `DiscardWrites`.
- `TestT3MergeKeepsChildDiscarded` and `TestReviewDiscardedChildDomains` now assert the discarded-write merge error, unchanged receiver chains, and receiver-only rows after `Flush`.
- `TestMergeOverlapAdoptsChildValues`: history rows match `f9da00be8fb`; value rows do not. For a (key, step) both batches wrote, the branch writes the child's value (`fffffffffffffffe15`, `fffffffffffffffe25`) and main writes the receiver's trimmed one (`fffffffffffffffe14`, a delete), so main's latest disagrees with its own in-memory read. Production does not merge into a non-empty receiver (research log F15).

## Checkpoints (supervisor)

- After T1: read the goldens and the `NOTES`. If the `unwind` golden holds rows from the unwound blocks, that is a bug on main: file it, and decide whether T3 fixes it (and the golden changes) or keeps the bytes.
- After T3: first ABBA.
- After T4: second ABBA, then the stop line.
