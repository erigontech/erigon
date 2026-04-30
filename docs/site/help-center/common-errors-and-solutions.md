---
title: "Common Errors and Solutions"
description: "Error messages decoded — causes explained and actionable fixes provided."
sidebar_position: 6
---

# Common Errors and Solutions

This section details common error messages and provides clear, actionable steps to resolve them.

## Sync and Performance

### Stalling during snapshot sync (0 B/s download rate)

* **Error Description:** The sync process appears to be stuck, showing a download rate of 0B/s, even with peers.
* **Cause:** The [RPC daemon](/fundamentals/modules/rpc-daemon) or a connected consensus client may be "spamming" the Erigon node with requests, which interferes with the snapshot sync. This issue is still present in recent versions.
* **Solution:** Temporarily disable the RPC by removing flags like `--http` and `--ws`, or stop your consensus client until the initial snapshot sync stage is complete.

### Sync is extremely slow or the node is constantly falling behind

* **Error Description:** The node is not keeping up with the blockchain tip despite a fast internet connection.
* **Cause:** This is almost always a disk-related problem. The read/write speed and latency of your storage device are the most significant performance bottlenecks for Erigon.
* **Solution:** Upgrade your storage to a high-end NVMe SSD. Avoid using HDDs, network drives, or slow consumer-grade SSDs. See [Hardware Requirements](/get-started/hardware-requirements) and [Performance Tricks](/fundamentals/performance-tricks).

## Memory and Resources

### Out of Memory (OOM) or unexpected process termination

* **Error Description:** The Erigon process is abruptly terminated by the operating system, often with an OOM-kill event in the system logs.
* **Cause:** This can be a genuine memory leak or, more commonly, a symptom of a disk I/O bottleneck. When the disk can't keep up with processing, memory usage can balloon as the system tries to buffer data.
* **Solution:** Ensure your system meets the recommended RAM requirements in [Hardware Requirements](/get-started/hardware-requirements). A clean shutdown and restart can often resolve the issue. If the problem persists, check your dmesg logs and consider upgrading your disk.

## Database

### Database corruption after an unexpected shutdown

* **Error Description:** The Erigon process fails to start or crashes immediately after a power outage or a forced kill.
* **Cause:** Erigon's database can be corrupted if it is not shut down gracefully, which prevents the final writes from being committed.
* **Solution:** The most reliable solution is to delete the corrupted datadir and re-sync from scratch. This is often faster than attempting to repair the database.

## Permissions and Access

### Permission denied or Access Denied errors on startup

* **Error Description:** The process fails to access the datadir, logs, or other files.
* **Cause:** The user or service account running Erigon does not have the correct file permissions for the data directory.
* **Solution:** Use the `chown` and `chmod` commands to ensure the correct user account has ownership and full read/write access to the datadir. See [Security](/fundamentals/security) for service account best practices.

### Permission denied inside Docker (UID/GID mismatch)

* **Error Description:** When running the official Docker image, Erigon fails to read or write files in the mounted datadir with a `permission denied` error.
* **Cause:** The container runs the Erigon process as UID/GID `1000`. If the host directory is owned by a different user, the process cannot access it.
* **Solution:** On the host, change ownership of the datadir to UID/GID 1000: `sudo chown -R 1000:1000 /your/datadir`. Alternatively, pass `--user $(id -u):$(id -g)` to `docker run` to run the container with your host user's identity. See [Docker Compose](/fundamentals/docker-compose).

## Network and Configuration

### Connect: connection refused or dial tcp... failures

* **Error Description:** The node cannot connect to an external service, such as a local or remote Heimdall instance.
* **Cause:** This is a configuration error. The dependent service is either not running, or the command-line flag is pointing to an incorrect address.
* **Solution:** Confirm that the required services are running and that the command-line flags (e.g., `--bor.heimdall.url`) are correctly set. See [Configuring Erigon](/fundamentals/configuring-erigon) for all available flags.

## Chain-Specific Issues

### Bad block / Invalid Merkle on Polygon network

* **Error Description:** The node stops importing new blocks, and the logs show errors related to bad blocks.
* **Cause:** This is a Polygon-specific issue that occurs when the Heimdall and Bor layers are out of sync.
* **Solution:** Verify that your Heimdall and REST servers are running. Restarting the Bor and Heimdall services on both the sentry and validator nodes should resolve the issue by bringing the layers back into sync. See the [Polygon Node](/get-started/easy-nodes/how-to-run-a-polygon-node) guide.

## Build and Installation

### `libsilkworm_capi.so`: missing shared library

* **Error Description:** Erigon fails to start with a dynamic linker error about a missing `libsilkworm_capi.so` shared library.
* **Cause:** The binary was built with Silkworm support but the shared library is not present in the system's library path or alongside the binary.
* **Solution:** Ensure the `libsilkworm_capi.so` file is located in the same directory as the `erigon` binary, or add its location to `LD_LIBRARY_PATH`. If you built from source, run `make erigon` again to confirm the library was compiled and placed correctly. See [Installation](/get-started/installation) for build-from-source instructions. Official Docker images bundle the library automatically.
