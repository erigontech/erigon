## Summary

- Improve forkchoice logging when parallel state flushing is enabled
- Refactor sender recovery stage to process individual transactions instead of blocks for better parallelism

## Changes

### Forkchoice Logging (`execution/execmodule/forkchoice.go`)

- When `stateFlushingInParallel` is true, log "head updated" immediately after sending early forkchoice receipt
- Extract `logHeadUpdated` method to consolidate logging logic
- Use debug level for the detailed log when parallel flushing is active
- Skip mgas/s metrics for early log (timing data incomplete)

### Sender Recovery Stage (`execution/stagedsync/stage_senders.go`)

- Change job granularity from block-level to transaction-level
- Each worker now processes individual transactions instead of entire blocks
- Transactions from the same block can be processed concurrently across workers
- Added `pendingBlock` tracking to aggregate results before writing to collector
- Added debug log at completion showing blocks processed and time taken

## Test plan

- [x] `go test ./execution/stagedsync/...` passes
- [x] `go build ./execution/...` succeeds
