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
go run ./cmd/integration stage_exec --datadir ~/data/chiado34_regen --chain=chiado --batchSize=8m

# flags for profiler
--pprof --pprof.port=6160

# flags for correctness
`ERIGON_ASSERT=true`
```

### tools for files correctness check

```
go run ./cmd/erigon seg integrity --datadir ~/data/chiado34_archive --sample=0.001 
```