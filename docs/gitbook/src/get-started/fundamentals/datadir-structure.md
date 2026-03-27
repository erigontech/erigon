---
description: Layout of the Erigon data directory and the role of each subdirectory.
---

# Datadir Structure

When Erigon runs, it stores all its data under a single root directory (`--datadir`). Understanding the layout helps you manage disk space, set up symlinks for multi-disk configurations, and diagnose issues.

## Directory Layout

```
datadir/
    chaindata/      # "Recently-updated Latest State", "Recent History", "Recent Blocks"
    snapshots/      # Immutable historical data stored as .seg files
        domain/     # Latest State (4 domains: account, storage, code, commitment)
        history/    # Historical values of each domain
        idx/        # Inverted indices — used for search/filter/union/intersect queries
                    # (e.g. eth_getLogs, trace_transaction)
        accessor/   # Additional generated indices with random-touch read pattern
                    # Only serves Get requests (no search/filter)
    txpool/         # Pending transactions. Safe to delete.
    nodes/          # P2P peer data. Safe to delete.
    temp/           # Temporary sort buffers for data larger than RAM.
                    # Can grow up to ~100 GB. Cleaned automatically at startup.
```

## Typical Disk Usage (Ethereum mainnet, Archive, Nov 2024)

```sh
du -hsc /erigon/chaindata
15G     /erigon/chaindata

du -hsc /erigon/snapshots/*
120G    /erigon/snapshots/accessor
300G    /erigon/snapshots/domain
280G    /erigon/snapshots/history
430G    /erigon/snapshots/idx
2.3T    /erigon/snapshots
```

For current disk usage figures by network and sync mode, see [Hardware Requirements](../hardware-requirements.md).

## History on Cheap Disk

If you cannot afford a single large NVMe for the full datadir, you can split fast-disk and slow-disk usage with symlinks. Keep latency-sensitive folders on the fast disk and move sequential-IO folders to a cheaper drive.

```sh
# Place (or ln -s) the datadir root on a slow/large disk.
# Then symlink the latency-sensitive subfolders to a fast (low-latency) NVMe.

# Minimum for fast execution:
datadir/
    chaindata   → symlink to fast disk
    snapshots/
        domain  → symlink to fast disk
        history
        idx
        accessor
    temp        # sequential-buffered I/O — slow-disk-friendly

# If execution speed is still not sufficient, progressively move more to fast disk:
#   Step 1: accessor  → fast disk
#   Step 2: idx       → fast disk (if step 1 not enough)
#   Step 3: history   → fast disk (if step 2 not enough)
```

{% hint style="success" %}
See [Optimizing Storage](optimizing-storage.md) for further strategies on managing disk usage.
{% endhint %}
