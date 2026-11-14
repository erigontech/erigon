# Read-ahead: execute future blocks to prefetch state and commitment

Branch `awskii/readahead-exec-prefetch`, base main `ce34587ba46`. This plan file is deleted before any PR.

## Problem

- `BlocksReadAhead` (`execution/exec/blocks_read_ahead.go:695`) runs during multi-block exec when
  `!initialCycle && isApplyingBlocks` (`execution/stagedsync/exec3.go:224-228`), 2 workers, block `bn+500`.
- `blocksReadAheadFunc` (`:742`) touches only sender, recipient, recipient code, access-list keys and the
  coinbase. Every key reached by contract code is cold when exec reaches it. TODO at `:795`:
  "exec txn and pre-fetch commitment keys".
- The initial cycle (catch-up over frozen blocks, the longest multi-block run) starts no read-ahead.
- Mainnet tip, research F49: page faults are about 20% of exec wall over 299 blocks; exec and commitment
  off-CPU is 99% page-fault IO (F40).

## Goal and stop line

- Metric: wall time for N fixed mainnet blocks on an offline archive arm (arb1-dev, base 26054550, frozen
  blocks to 26253000), cold page cache, ABBA rounds, fresh arm per run. Reported as blk/s and mgas/s,
  verdict by ab-verdict.
- Number to beat: arm `off` (main behaviour) on the same binary.
- First measured after Task 2 (arms off / static / exec), again after Task 3 (+ commitment).
- Stop line: no arm a winner over `off` after Task 3 means the change is dropped, not shipped.

## Design

- All new behaviour behind env knobs in `common/dbg/experiments.go`, next to the `READ_AHEAD` family,
  using `EnvBool`/`EnvInt`. Defaults keep main's behaviour exactly:
  - `READ_AHEAD_EXEC` (false): run the future block's transactions speculatively.
  - `READ_AHEAD_EXEC_COMMITMENT` (false): also prefetch commitment branches for the keys that run wrote.
  - `READ_AHEAD_INITIAL` (false): also start `BlocksReadAhead` in the initial cycle when applying blocks.
  - `READ_AHEAD_BLOCKS_WORKERS` (2): `BlocksReadAhead` workers in exec3 (today the literal `2`).
  - `READ_AHEAD_DISTANCE` (500): blocks ahead (today the const `readAheadBlocks`).
- Speculative run of block B, inside `blocksReadAheadFunc`, on the worker's existing RO temporal tx:
  - reader `state.NewReaderV3(execctx.NewTemporalTxStateGetter(ttx))`: reads latest committed state, fills
    no StateCache. Never use `readAheadGetter`/`cachePopulatingGetter` or `NewCachedTemporalTxStateGetter`
    here: stale state must never enter a cache.
  - one `IntraBlockState` per block over that reader, txs applied in order on it, so intra-block effects
    follow. Writer: a recording writer wrapping `state.NewNoopWriter()` that keeps the written account
    addresses and storage (address, slot) pairs. Nothing reaches the DB.
  - EVM from `protocol.NewEVMBlockContext(header, protocol.GetHashFn(...), engine, nil, chainConfig)` and
    `protocol.ApplyMessage` with nonce and balance checks off (as eth_call does: gas bailout on), so stale
    nonces and balances do not stop a tx before it touches state. No tracer. No engine system calls.
  - a tx that errors is skipped; a panic in the speculative run is recovered and counted, the block is
    dropped; context cancellation stops it.
  - late skip: a shared atomic holds the block exec is on; a target at or below it is skipped and counted.
- Commitment prefetch (Task 3): hash the recorded written keys with `commitment.KeyToHexNibbleHash`,
  sharing one helper with `balCommitmentWarmupKeys` (`bal_commitment_warmup.go:41`) instead of a copy, and
  pass them to `prefetchCommitment(ctx, db, hashedKeys)` (`:449`).
- `CustomTraceMapReduce` (`historical_trace_worker.go:514`) keeps today's behaviour: speculative exec off
  there; it replays history as-of, and latest state is the wrong state for it.
- Metrics (erigon `metrics.GetOrCreate*`): blocks by result (done, late, error), txs run, keys read and
  written by kind, commitment keys prefetched, seconds per block. They make the A/B readable: a read-ahead
  that is always late explains a null result.
- RO tx budget: `httpcfg.RoTxsLimit` reserves `execReadAheadTxs = 2`; it must follow
  `READ_AHEAD_BLOCKS_WORKERS`.

## Tasks

### Task 1: speculative execution in `blocksReadAheadFunc`

- [x] knobs `READ_AHEAD_EXEC`, `READ_AHEAD_EXEC_COMMITMENT` (declared, unused until Task 3)
- [x] `blocksReadAheadFunc` gains the chain config and an exec switch; exec3 passes `dbg.ReadAheadExec`,
      `CustomTraceMapReduce` passes false
- [x] speculative run as in Design, returning per-block stats (txs, accounts read, storage read, code read,
      accounts written, storage written) used by the metrics and by tests
- [x] late skip with the shared exec-block atomic, fed from the exec loop next to `readAhead <- blockNum`
- [x] metrics
- [x] test: a chain whose block calls a contract that reads storage slot S and writes slot T, with neither
      in an access list. With exec on, the stats show S read and T written; with exec off both are zero;
      the DB is byte-identical before and after. Guard: removing the tx loop turns it red at the assertion
      on S
- [x] test: a tx whose sender nonce is stale still runs and reads S

### Task 2: wire it into multi-block runs

- [x] `READ_AHEAD_INITIAL`, `READ_AHEAD_BLOCKS_WORKERS`, `READ_AHEAD_DISTANCE`; exec3 starts
      `BlocksReadAhead` on `isApplyingBlocks && (!initialCycle || dbg.ReadAheadInitial)` with the worker knob
- [x] `RoTxsLimit` reserves the worker knob instead of the constant 2
- [x] test: a multi-block ExecV3 run (pick the nearest existing driver, e.g.
      `exec3_ephemeral_multiblock_test.go` or `execmoduletester`) with every knob on gives the same state
      root and receipts as with every knob off, and the done-blocks metric (or stats hook) is above 0 with
      the knobs on. Guard: dropping the initial-cycle start turns the counter assertion red

### Task 3: commitment branch prefetch

- [x] shared hashed-key helper used by both `balCommitmentWarmupKeys` and the read-ahead
- [x] with `READ_AHEAD_EXEC_COMMITMENT`, `prefetchCommitment` runs on the written keys of each speculative
      block; its count goes to the metrics and stats
- [x] test: a block writing existing accounts and slots prefetches a non-zero number of commitment keys;
      off gives zero. Guard: removing the call turns it red

### Task 4: verification (supervisor)

- [ ] A/B on arb1-dev as in Goal; numbers into the research log; drop or keep per the stop line
