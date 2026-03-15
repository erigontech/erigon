---
name: erigon-exec-from-0
description: Re exec all blocks from 0 to reproduce bugs
---

```
# remove all State files (not Block files)
go run ./cmd/erigon snapshots rm-all-state-snapshots --datadir ~/data/chiado34_regen

# clean database tables related to stage_exec
go run ./cmd/integration stage_exec --datadir ~/data/chiado34_regen --chain=chiado --reset

# exec blocks
COLLECT_TABLE_SIZES_FREQUENCY=3s NO_DEEP_MERGE_HISTORY=true EXEC_SKIP_RECEIPT_CHECK=true MDBX_NO_FSYNC=true go run -tags=debug ./cmd/integration stage_exec  --datadir /erigon-data/mainnet_stepsize4_regen --chain=mainnet --batchSize=128m --pprof --pprof.port=6160 --metrics --metrics.addr=0.0.0.0 --block=18_031_000

# flags for profiler
--pprof --pprof.port=6160

# flags for correctness
`ERIGON_ASSERT=true`
```

### tools for files correctness check

```
go run ./cmd/erigon seg integrity --datadir ~/data/chiado34_archive --sample=0.001 
```
