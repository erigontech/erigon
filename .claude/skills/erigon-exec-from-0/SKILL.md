---
name: erigon-exec-from-0
description: >
  Re-execute all Ethereum blocks from block 0 to reproduce bugs, validate state, or test
  correctness after code changes. Use this skill whenever the user wants to re-run execution
  from scratch, reset execution stage, reproduce a state mismatch, validate a fix by re-exec,
  or run blocks from the beginning on a specific chain/datadir.
---

# Re-execute all blocks from 0

Use this workflow to wipe execution state and re-run all blocks from block 0.
Common reasons: reproducing a state root mismatch, validating a bug fix, testing
new execution logic end-to-end.

## Prerequisites

You need:

- `--datadir` — path to the Erigon data directory (block snapshots must be present)
- `--chain` — chain name (e.g. `chiado`, `mainnet`, `gnosis`, `sepolia`)

## Step 1: Remove state snapshots (keep block snapshots)

```bash
go run ./cmd/erigon snapshots rm-all-state-snapshots --datadir <datadir>
```

This removes `.kv` state snapshot files but leaves `.seg` block snapshot files intact,
so you don't need to re-download block data.

## Step 2: Reset the execution stage

```bash
go run ./cmd/integration stage_exec --datadir <datadir> --chain=<chain> --reset
```

This clears the execution-stage progress in the database so the next run starts from block 0.

## Step 3: Run execution

```bash
go run ./cmd/integration stage_exec --datadir <datadir> --chain=<chain> --batchSize=8m
```

Adjust `--batchSize` to control how frequently state is flushed to disk. Larger = faster
but more RAM; `8m` is a reasonable default.

## Useful optional flags

**Profiling** (add when you want pprof):

```
--pprof --pprof.port=6160
```

**Correctness assertions** (add when hunting state bugs — slows execution):

```
ERIGON_ASSERT=true
```

**Stop at a specific block** (useful to reproduce a bug at block N):

```
--block=<N>
```

## Verifying snapshot integrity

If you suspect corrupted snapshot files before running:

```bash
go run ./cmd/erigon seg integrity --datadir <datadir> --sample=0.001
```
