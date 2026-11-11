---
title: "Performance Tricks"
description: "OS-level tuning, memory settings, and tips to maximize sync throughput."
sidebar_position: 13
---

# Performance Tricks

These instructions are designed to improve the performance of Erigon 3, particularly for synchronization and memory management, on cloud drives and other systems with specific performance characteristics.

## Increase Sync Speed

* Set `--sync.loop.block.limit=10_000` and `--batchSize=2g` to speed up the synchronization process.

```bash
--sync.loop.block.limit=10_000 --batchSize=2g
```

* Adjust the download speed with the `--torrent.download.rate=[value]` flag setting your max speed (default value is `512mb`). For example:

```bash
--torrent.download.rate=1024mb
```

## Optimize for Cloud Drives

* Set `ERIGON_SNAPSHOT_MADV_RND=false` to enable the operating system's cache prefetching for better performance on cloud drives with good throughput but bad latency.

```bash
export ERIGON_SNAPSHOT_MADV_RND=false
```

## Lock Latest State in RAM

* Use `vmtouch -vdlw /mnt/erigon/snapshots/domain/*bt` to lock the latest state in RAM, preventing it from being evicted due to high historical RPC traffic.

```bash
vmtouch -vdlw /mnt/erigon/snapshots/domain/*bt
```

* Run `ls /mnt/erigon/snapshots/domain/*.kv | parallel vmtouch -vdlw` to apply the same locking to all relevant files.

## Swap and execution throughput

Erigon relies on memory-mapped state files and can become significantly slower
when memory pages are moved to swap. On Linux, check whether swap is configured
**and whether it is in use**:

```bash
free -h
swapon --show
```

Erigon logs a warning when swap has pages in use. For a dedicated,
well-provisioned node, aim for enough physical memory that active swapping is
unnecessary. To disable swap temporarily *after verifying sufficient available
RAM*:

```bash
sudo swapoff -a
```

Avoid disabling swap on machines already short of memory: doing so may cause
out-of-memory process termination. Also note that `swapoff -a` is not
persistent; configure swap activation at boot according to your Linux
distribution if you decide to disable it permanently.

For a memory-constrained node, a starting point to **limit Go heap usage** is:

```bash
GOGC=50 GOMEMLIMIT=10GiB ./build/bin/erigon --batchSize=120M
```

This is an example, not a universal recommendation. Leave headroom for the
operating system, memory-mapped files, and other processes. More aggressive
garbage collection can use additional CPU and reduce throughput; test against
your actual memory limit before relying on this configuration.

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
