---
description: 'Optimizing Erigon Performance: Sync Speed, Cloud Drives, and Memory Tuning'
---

# Performance Tricks

These instructions are designed to improve the performance of Erigon 3, particularly for synchronization and memory management, on cloud drives and other systems with specific performance characteristics.

## Increase Sync Speed

* Set `--sync.loop.block.limit=10_000` and `--batchSize=2g` to speed up the synchronization process.

```bash
--sync.loop.block.limit=10_000 --batchSize=2g
```

* Increase download speed with flag `--torrent.download.rate=[value]` setting your max speed (default value is 128MB). For example:

```bash
--torrent.download.rate=512mb
```

## Optimize for Cloud Drives

* Set `SNAPSHOT_MADV_RND=false` to enable the operating system's cache prefetching for better performance on cloud drives with good throughput but bad latency.

```bash
SNAPSHOT_MADV_RND=false
```

## Lock Latest State in RAM

* Use `vmtouch -vdlw /mnt/erigon/snapshots/domain/*bt` to lock the latest state in RAM, preventing it from being evicted due to high historical RPC traffic.

```bash
vmtouch -vdlw /mnt/erigon/snapshots/domain/*bt
```

* Run `ls /mnt/erigon/snapshots/domain/*.kv | parallel vmtouch -vdlw` to apply the same locking to all relevant files.

## Handle Memory Allocation Issues

* If you encounter issues with memory allocation, run the following to flush any pending write operations and free up memory:

```bash
sync && sudo sysctl vm.drop_caches=3
```

* Alternatively, set:

```bash
echo 1 > /proc/sys/vm/compact_memory
```

to help with memory allocation.
