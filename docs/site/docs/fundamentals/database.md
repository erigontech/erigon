---
title: "Database"
description: "How Erigon stores chain data — MDBX engine, datadir layout, snapshot files, and real mainnet sizing numbers."
sidebar_position: 3
---

# Database

Erigon stores all chain data under a single **datadir**. Understanding what lives where is useful when you size hardware, debug disk-usage issues, or split storage across fast and slow drives.

This page covers the *what* and *where* of Erigon's data. For the *why* — flat KV state, immutable snapshots — see [Architecture](architecture). For operational tuning (symlinks, multi-disk layout), see [Optimizing Storage](optimizing-storage).

## The datadir at a glance

```
datadir/
├── chaindata/        # Active state + recent blocks (MDBX). Small, hot, mutable.
├── snapshots/        # Historical data as immutable .seg files. Large, cold.
│   ├── domain/         # Latest value per domain (4 state domains + 2 receipt domains)
│   ├── history/        # Historical values per domain
│   ├── idx/            # Inverted indices — search/filter/intersect historical data
│   └── accessor/       # Random-access indices over history (point lookups only)
├── txpool/           # Pending transactions. Safe to delete; will repopulate from peers.
├── nodes/            # p2p peer cache. Safe to delete; will repopulate.
└── temp/             # External-sort buffers (~100 GB peak). Cleaned at startup.
```

The split between **`chaindata/`** (mutable) and **`snapshots/`** (immutable) is the central design decision. It is what makes Erigon's archive node 10× smaller than other clients while staying fast.

## Storage engine: MDBX

`chaindata/` is a single [MDBX](https://github.com/erigontech/mdbx-go) key-value store. MDBX is a B+ tree engine derived from LMDB, optimised for read-heavy workloads and predictable memory use.

Properties that matter operationally:

- **No background compaction.** Writes go directly into the B+ tree; there is no compaction thread that can spike CPU mid-RPC-call. This is why Erigon's RPC latency does not degrade under load the way LSM-based engines (LevelDB, RocksDB) can.
- **Memory-mapped reads.** The OS page cache is the hot data cache. There is no separate per-process cache to tune. Multiple Erigon services on one machine share the same page cache automatically.
- **Single-writer.** One process holds the write lock; readers are unlimited and lock-free. This is why splitting RPC Daemon out as a separate process for read scaling works cleanly — only one writer ever touches the file.

## Snapshots: immutable history

Older blocks and history are not stored in MDBX. They are written to **`.seg` files** in `snapshots/` and **never modified after creation**.

Once a snapshot file is finalised it is the same bytes on every Erigon node in the world, identified by content hash. This unlocks two things:

- **BitTorrent distribution.** New nodes fetch snapshots from peers in parallel rather than re-executing history from genesis. This is what OtterSync does.
- **Backup / disaster recovery costs ~10× less.** Most of your datadir is content-addressed and can be re-downloaded from any peer if a disk fails. You only need to back up `chaindata/`.

Snapshots are organised into several subdirectories. The main ones are:

| Directory | What it holds | Access pattern |
|---|---|---|
| `snapshots/domain/` | Latest value per (domain, key). 6 domains: the 4 state domains (`account`, `storage`, `code`, `commitment`) plus 2 receipt domains (`receipt`, `rcache`) | Sequential + point lookup |
| `snapshots/history/` | Every historical value per (domain, key, txn) | Point lookup keyed by transaction |
| `snapshots/idx/` | Inverted indices over history — answers "which transactions touched key X?" | Search / filter / set intersection |
| `snapshots/accessor/` | Pre-built random-access indices over history files | Random-touch point reads only |

**Per-transaction granularity.** Erigon 3 indexes history at the **transaction** level, not the block level. This means:

- You can replay a single historical transaction without re-executing its block.
- If an account changes V1 → V2 → V1 within one block, `debug_getModifiedAccountsByNumber` correctly returns it.
- Erigon stores compact per-transaction receipt *metadata* — cumulative gas used, blob gas used, log index — in a **required** receipt domain. Full receipts (with logs) live in a separate cache domain that is **persisted by default** for `full`, `minimal`, and `blocks` modes (toggled with `--persist.receipts`; `archive` nodes skip it by default and re-derive from full state history). When a full receipt isn't cached, it is reconstructed on demand, re-deriving logs by re-execution.

## What does it cost on disk?

Real numbers from a Nov 2024 mainnet archive node:

```sh
# eth-mainnet — archive — prune.mode=archive
chaindata           15 GB
snapshots/accessor 120 GB
snapshots/domain   300 GB
snapshots/history  280 GB
snapshots/idx      430 GB
snapshots TOTAL    2.3 TB
```

The breakdown above lists the state/history snapshot subdirectories. The remaining ~1.2 TB is mostly block/transaction `.seg` data, which is not broken out separately here.

For up-to-date totals across all networks and pruning modes, see [Hardware Requirements](../get-started/hardware-requirements).

## Why `chaindata/` stays so small

In Erigon 3, `chaindata/` only holds:

- The very latest state values (post-snapshot tip)
- Recent blocks not yet folded into snapshots
- Live txpool, peer state, sync stage progress

Most of the bulk that other clients keep in the active database — historical state, ancient blocks, receipt logs — is in immutable snapshot files instead. This is why `chaindata/` rarely exceeds 20 GB even on archive nodes.

Deleting `chaindata/` is **recoverable but not free**: it discards the latest mutable state and any recent blocks not yet folded into snapshots. On restart Erigon re-derives state from the immutable snapshots (re-downloading them if needed) and resyncs the post-snapshot tip forward from the consensus layer over the Engine API — blocks are no longer pulled from peers over devp2p. Treat it as a resync of the chain tip, not a quick rebuild, and keep a backup if fast recovery matters.

## Tuning knobs

- **`--batchSize`** — size of the Execution stage's in-memory buffer before it is flushed to MDBX. Default: `512M`. Raising it (for example `--batchSize 1G` or higher) can speed up execution-heavy sync at the cost of more RAM.
- **`--db.size.limit`** — caps the MDBX file size. Useful when running multiple Erigon instances on one disk to prevent one from starving the others.
- **`--db.read.concurrency`** — number of concurrent MDBX read transactions. Increase when you run a high-throughput RPC Daemon against the same datadir.
- **Symlinks for tiered storage.** Place `chaindata/` and `snapshots/domain/` on fast NVMe, leave `snapshots/idx/` and `snapshots/history/` on cheaper SATA. See [Optimizing Storage](optimizing-storage) for the recipe.

## Safe-to-delete subdirectories

If you need to reclaim space without resyncing from scratch:

| Directory | Effect of deletion |
|---|---|
| `txpool/` | Pending transactions lost; pool refills from peers within minutes |
| `nodes/` | Peer cache lost; reconnects on restart |
| `temp/` | Cleaned automatically at startup anyway |
| `chaindata/` | Recoverable, but triggers a resync of the post-snapshot tip from the consensus layer — not instant; keep a backup for fast recovery |
| `snapshots/` | **Do not delete** — would force a full resync |

## Where to go next

- [Architecture](architecture) — how this storage model fits into staged sync and the flat-KV state design
- [Optimizing Storage](optimizing-storage) — concrete recipes for splitting the datadir across multiple disks
- [Hardware Requirements](../get-started/hardware-requirements) — disk-size numbers for each `--prune.mode`
- [Pruning Modes](pruning-modes) — choosing what history to keep
