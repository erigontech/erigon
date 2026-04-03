---
name: qmtree-replay
description: Run stage_exec_replay to generate qmtree dataset (entries, twigs, keyindex) from a synced datadir. Supports mainnet and hoodi. Long-running — manages start, monitor, resume, and validation.
user-invocable: true
allowed-tools: Bash, Read, Write, Edit, Glob, Grep
allowed-prompts:
  - tool: Bash
    prompt: run integration commands, manage erigon processes, read log files, check disk usage
---

# QMTree Replay — Dataset Generation

Generate a complete qmtree dataset (entries, twigs, keyindex) by replaying
historical blocks through `integration stage_exec_replay`. This is the
primary way to build the qmtree proof tree for testing and benchmarking.

## Prerequisites

1. **Synced datadir** with execution history (accounts, storage, code domains
   + history files). The replay reads from history, not from live state.
2. **Erigon + integration binaries** built from the `qmtree` branch.
3. **Disk space**: ~50 GB for mainnet qmtree files (entries + twigs + keyindex).

## Arguments

| Argument | Default | Description |
|----------|---------|-------------|
| `chain` | `mainnet` | Chain to replay (`mainnet`, `hoodi`) |
| `datadir` | auto-detect | Path to synced datadir |
| `to` | max | Block number to replay up to |
| `workers` | 4 | Parallel replay workers |
| `start` | — | Start/resume the replay |
| `monitor` | — | Show progress of running replay |
| `stop` | — | Stop a running replay |
| `status` | — | Check if replay is running and show output dir |

### Examples

```
/qmtree-replay start                    # Start mainnet replay (auto-detect datadir)
/qmtree-replay start chain=hoodi        # Start hoodi replay
/qmtree-replay start datadir=/erigon/qmdb-test-datadir to=1000000
/qmtree-replay monitor                  # Show live progress
/qmtree-replay stop                     # Stop replay
/qmtree-replay status                   # Check if running
```

## Available Datadirs

Check for synced datadirs with history:

```bash
# List datadirs with their sync state
for dir in /erigon/qmdb-test-datadir /erigon/hoodi-direct-test; do
  if [ -d "$dir/snapshots/history" ]; then
    latest=$(ls "$dir/snapshots/history/" 2>/dev/null | grep "v2.0-accounts" | sort -t. -k3 -n | tail -1)
    from_zero=$(ls "$dir/snapshots/history/" 2>/dev/null | grep "accounts.0-" | head -1)
    echo "$dir  latest=$latest  from_zero=${from_zero:-MISSING}"
  fi
done
```

**Important**: `stage_exec_replay` replays from block 0. The datadir MUST have
history files starting from step 0 (`v2.0-accounts.0-*.v`). If history starts
from a later step, the replay will fail or produce incomplete results.

## Procedure

### Phase 1: Validate Prerequisites

```bash
# 1. Check binaries exist
ls -la ./build/bin/integration 2>/dev/null || echo "Need: make integration"

# 2. Check datadir has history from genesis
ls $DATADIR/snapshots/history/v2.0-accounts.0-* 2>/dev/null || echo "No genesis history"

# 3. Check disk space (need ~50 GB for mainnet, ~5 GB for hoodi)
df -h $(dirname $DATADIR) | tail -1
```

If integration binary is missing, build it:
```bash
make erigon integration
```

### Phase 2: Start Replay

```bash
DATADIR=/erigon/qmdb-test-datadir  # or user-specified
CHAIN=mainnet                        # or hoodi
WORKERS=4
TO_BLOCK=""                          # empty = replay to max

# Build the command
CMD="./build/bin/integration stage_exec_replay \
  --datadir=$DATADIR \
  --chain=$CHAIN \
  --workers=$WORKERS"

# Add --block if specified
if [ -n "$TO_BLOCK" ]; then
  CMD="$CMD --block=$TO_BLOCK"
fi

# Run in background with output logging
LOGFILE=/tmp/qmtree-replay-$(date +%Y%m%d-%H%M%S).log
echo "Logging to: $LOGFILE"
nohup $CMD > $LOGFILE 2>&1 &
echo "PID: $!"
echo $! > /tmp/qmtree-replay.pid
echo $LOGFILE > /tmp/qmtree-replay.log
```

### Phase 3: Monitor Progress

```bash
# Check if running
REPLAY_PID=$(cat /tmp/qmtree-replay.pid 2>/dev/null)
if kill -0 $REPLAY_PID 2>/dev/null; then
  echo "Running (PID $REPLAY_PID)"
else
  echo "Not running"
fi

# Show recent progress
LOGFILE=$(cat /tmp/qmtree-replay.log 2>/dev/null)
grep -E "qmtree root|progress|step completed|finished" "$LOGFILE" | tail -20

# Show qmtree storage stats
du -sh $DATADIR/snapshots/qmtree/ 2>/dev/null
ls $DATADIR/snapshots/qmtree/keyindex/ 2>/dev/null | wc -l
```

### Phase 4: Stop

```bash
REPLAY_PID=$(cat /tmp/qmtree-replay.pid 2>/dev/null)
kill $REPLAY_PID 2>/dev/null
sleep 2
kill -0 $REPLAY_PID 2>/dev/null && kill -9 $REPLAY_PID
echo "Stopped"
```

### Phase 5: Validate Output

After replay completes (or for partial results):

```bash
QMDIR=$DATADIR/snapshots/qmtree

# Check entry files
echo "=== Entries ==="
du -sh $QMDIR/entries/ 2>/dev/null
ls $QMDIR/entries/ 2>/dev/null | wc -l

# Check twig files
echo "=== Twigs ==="
du -sh $QMDIR/twigs/ 2>/dev/null
ls $QMDIR/twigs/ 2>/dev/null | wc -l

# Check keyindex files
echo "=== KeyIndex ==="
du -sh $QMDIR/keyindex/ 2>/dev/null
ls $QMDIR/keyindex/ 2>/dev/null

# Show final root from log
grep "qmtree root" "$LOGFILE" | tail -1
grep "finished" "$LOGFILE" | tail -1
```

## Expected Performance

| Chain | Blocks/sec | Time to 1M blocks | Time to full |
|-------|-----------|-------------------|-------------|
| Hoodi | ~100-200 | ~2 hours | ~6-12 hours |
| Mainnet | ~40-80 | ~5 hours | ~7-14 days |

## Resume After Interruption

The replay currently does NOT support resume — it always starts from block 0.
After interruption:

1. The qmtree files (entries, twigs, keyindex) from the partial run are valid
   up to the last flushed block
2. To continue, you must restart from block 0 (the replay is idempotent —
   it overwrites existing files)
3. To avoid re-doing work, use `--block=N` to replay to a specific block

**Future**: add resume support by reading `tracker.NextSN` from the existing
entry file and skipping already-processed blocks.

## Troubleshooting

| Problem | Solution |
|---------|----------|
| `No genesis history` | Datadir needs history from step 0. Run a full archive sync first. |
| `OOM during replay` | Reduce `--workers` (default 4). Each worker holds state readers in memory. |
| `Slow replay` | Check I/O with `iostat`. History files on spinning disk will bottleneck. |
| `KeyIndex files missing` | Check `$DATADIR/snapshots/qmtree/keyindex/`. Flushed every 1000 blocks. |
| `Wrong roots after code change` | Delete `$DATADIR/snapshots/qmtree/` and re-run from scratch. |
