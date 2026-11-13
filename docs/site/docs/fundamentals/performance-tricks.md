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

Erigon uses file-backed memory mappings for MDBX and snapshot files. Linux can
evict those pages and read them back from their backing files; it does not swap
them out. Under memory pressure, **anonymous memory**, such as the Go heap and
in-process caches, can be moved to swap and slow down execution.

On Linux, check whether swap is configured **and whether it is in use**:

```bash
free -h
swapon --show
```

Erigon logs host-wide swap usage as informational context at startup. This
number can include unrelated processes and pages that were swapped out before
Erigon started; it does not prove that Erigon itself is swapping or losing
throughput. For a dedicated, well-provisioned node, aim for enough physical
memory that active swapping is unnecessary. To disable swap temporarily *after
verifying sufficient available RAM*:

```bash
sudo swapoff -a
```

Avoid disabling swap on machines already short of memory: doing so may cause
out-of-memory process termination. Also note that `swapoff -a` is not
persistent; configure swap activation at boot according to your Linux
distribution if you decide to disable it permanently.

For memory-constrained nodes, see the
[out-of-memory troubleshooting guide](/help-center/common-errors-and-solutions#out-of-memory-oom-or-unexpected-process-termination)
for the existing `GOMEMLIMIT`, `GOGC`, and `--batchSize` guidance. Erigon
also automatically sets a memory limit by default, so tune these settings only
when your workload requires it.

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
