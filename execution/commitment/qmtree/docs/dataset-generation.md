# QMTree Dataset Generation

This document describes how to generate a qmtree dataset (entries, twigs,
keyindex) from an existing synced Erigon datadir.

## Overview

The `stage_exec_replay` integration command replays historical blocks from
genesis, computing proof-of-execution hashes per transaction and building
the qmtree on disk. It reads from Erigon's committed history files (accounts,
storage, code domains) and writes to `<datadir>/snapshots/qmtree/`.

## Prerequisites

1. **Synced Erigon datadir** with full execution history (not pruned).
   The history must start from step 0 (`v2.0-accounts.0-*.v`).

2. **Erigon + integration binaries** built from the `qmtree` branch:
   ```bash
   git checkout qmtree
   make erigon integration
   ```

3. **Disk space**: entries + twigs + keyindex grows roughly as:
   - ~96 bytes/tx (entries) + ~36 bytes/tx (twigs) + keyindex deltas
   - Mainnet (~2.5B txns): ~300 GB entries + ~90 GB twigs + ~12 GB keyindex
   - Hoodi (~350M txns): ~34 GB entries + ~13 GB twigs + ~2 GB keyindex

## Quick Start (Hoodi)

Hoodi is the fastest chain for testing. With a synced hoodi datadir:

```bash
./build/bin/integration stage_exec_replay \
  --datadir=/path/to/hoodi-datadir \
  --chain=hoodi \
  --workers=4
```

Or use the skill: `/qmtree-replay start chain=hoodi datadir=/path/to/hoodi`

## Mainnet

```bash
./build/bin/integration stage_exec_replay \
  --datadir=/path/to/mainnet-datadir \
  --chain=mainnet \
  --workers=4
```

Expected runtime: 7-14 days depending on hardware. Use `--block=N` to limit
to a specific block number for testing.

## Output Structure

```
<datadir>/snapshots/qmtree/
├── entries/          # HPFile segments: 3 × 32B hash components per entry
│   ├── 0000000000    # Segment files (one per step)
│   ├── 0000000001
│   └── ...
├── twigs/            # HPFile segments: intra-twig Merkle tree data
│   ├── 0000000000
│   └── ...
└── keyindex/         # RecSplit-indexed segments: keyHash → lastTxNum
    ├── v1-keyindex.0-1.kv    # Sorted (keyHash, txNum) pairs
    ├── v1-keyindex.0-1.kvi   # RecSplit perfect hash index
    └── ...
```

### File Sizes Per Step (mainnet, stepSize=1,562,500)

| Component | Size per step | Formula |
|-----------|--------------|---------|
| Entries | ~150 MB | stepSize × 96 bytes |
| Twigs | ~56 MB | ceil(stepSize/2048) × 73,708 bytes |
| KeyIndex delta | 1-50 MB | varies by unique keys per step |

## Monitoring Progress

The replay logs progress every 30 seconds and qmtree roots every 1000 blocks:

```
[stage_exec_replay] qmtree root  block=1000 root=0x... leaves=11851
[stage_exec_replay] progress     block=5000 elapsed=2m30s blk/s=33.3 leaves=52410
[stage_exec_replay] qmtree step completed  step=1 entries=1562500 ...
```

Monitor a running replay:
```bash
tail -f /tmp/qmtree-replay-*.log | grep -E "qmtree|progress|finished"
```

Check storage growth:
```bash
du -sh /path/to/datadir/snapshots/qmtree/{entries,twigs,keyindex}
```

## Validation

After generation (or for partial results), verify the dataset:

```bash
# Count entries
ENTRY_BYTES=$(du -sb /path/to/datadir/snapshots/qmtree/entries/ | cut -f1)
echo "Entries: $((ENTRY_BYTES / 96))"

# Count keyindex segments
ls /path/to/datadir/snapshots/qmtree/keyindex/*.kv 2>/dev/null | wc -l

# Run qmtree unit tests against the generated data
QMTREE_DATADIR=/path/to/datadir/snapshots/qmtree \
  go test -run TestBench ./execution/commitment/qmtree/... -v
```

## Re-generation After Code Changes

If the leaf hash format, entry format, or KeyIndex format changes:

1. Delete the existing qmtree directory:
   ```bash
   rm -rf /path/to/datadir/snapshots/qmtree/
   ```

2. Re-run `stage_exec_replay` from scratch.

The replay is idempotent — it always starts from block 0 and overwrites
existing files.

## Known Limitations

- **No resume**: the replay always starts from block 0. After interruption,
  partial files are valid but you must restart to continue.
- **History from genesis required**: the datadir must have history files
  starting from step 0. Pruned datadirs cannot be used.
- **Single-threaded qmtree writes**: while block replay uses parallel workers,
  the qmtree consumer (AppendLeaf) is serial. This is the bottleneck at
  high block rates.

## Syncing a Fresh Archive Node for Replay

If you don't have a synced mainnet archive node:

```bash
# Mainnet archive sync (full history, no pruning)
./build/bin/erigon \
  --datadir=/erigon/mainnet-archive \
  --chain=mainnet \
  --prune.mode=archive \
  --torrent.download.rate=1024mb

# This takes 3-7 days depending on network and disk speed.
# Once synced, run stage_exec_replay against this datadir.
```

For hoodi (faster, smaller):
```bash
./build/bin/erigon \
  --datadir=/erigon/hoodi-archive \
  --chain=hoodi \
  --prune.mode=archive \
  --torrent.download.rate=1024mb
```

Hoodi syncs in ~6-12 hours.
