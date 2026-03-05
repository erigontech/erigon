---
description: Specific error messages and the steps required to resolve them.
---

# Common Errors and Solutions

This section details common error messages and provides clear, actionable steps to resolve them.

1. **Stalling during snapshot sync (0B/s download rate):**
   * **Error Description:** The sync process appears to be stuck, showing a download rate of 0B/s, even with peers.
   * **Cause:** The RPC daemon or a connected consensus client may be "spamming" the Erigon node with requests, which interferes with the snapshot sync. This issue is still present in recent versions.
   * **Solution:** Temporarily disable the RPC by removing flags like `--http` and `--ws`, or stop your consensus client until the initial snapshot sync stage is complete.
2. **Out of Memory (OOM) or unexpected process termination:**
   * **Error Description:** The Erigon process is abruptly terminated by the operating system, often with an OOM-kill event in the system logs.
   * **Cause:** This can be a genuine memory leak or, more commonly, a symptom of a disk I/O bottleneck. When the disk can't keep up with processing, memory usage can balloon as the system tries to buffer data.
   * **Solution:** Ensure your system meets the recommended RAM requirements. A clean shutdown and restart can often resolve the issue. If the problem persists, check your dmesg logs and consider upgrading your disk.
3. **Sync is extremely slow or the node is constantly falling behind:**
   * **Error Description:** The node is not keeping up with the blockchain tip despite a fast internet connection.
   * **Cause:** This is almost always a disk-related p roblem. The read/write speed and latency of your storage device are the most significant performance bottlenecks for Erigon.
   * **Solution:** Upgrade your storage to a high-end NVMe SSD. Avoid using HDDs, network drives, or slow consumer-grade SSDs.
4. **Database corruption after an unexpected shutdown:**
   * **Error Description:** The Erigon process fails to start or crashes immediately after a power outage or a forced kill.
   * **Cause:** Erigon's database can be corrupted if it is not shut down gracefully, which prevents the final writes from being committed.
   * **Solution:** The most reliable solution is to delete the corrupted datadir and re-sync from scratch. This is often faster than attempting to repair the database.
5. **Permission denied or Access Denied errors on startup:**
   * **Error Description:** The process fails to access the datadir, logs, or other files.
   * **Cause:** The user or service account running Erigon does not have the correct file permissions for the data directory.
   * **Solution:** Use the chown and chmod commands to ensure the correct user account has ownership and full read/write access to the datadir.
6. **Bad block/Invalid Merkle on Polygon network:**
   * **Error Description:** The node stops importing new blocks, and the logs show errors related to bad blocks.
   * **Cause:** This is a Polygon-specific issue that occurs when the Heimdall and Bor layers are out of sync.
   * **Solution:** Verify that your Heimdall and REST servers are running. Restarting the Bor and Heimdall services on both the sentry and validator nodes should resolve the issue by bringing the layers back into sync.
7. **Connect: connection refused or dial tcp... failures:**
   * **Error Description:** The node cannot connect to an external service, such as a local or remote Heimdall instance.
   * **Cause:** This is a configuration error. The dependent service is either not running, or the command-line flag is pointing to an incorrect address.
   * **Solution:** Confirm that the required services are running and that the command-line flags (e.g., `--bor.heimdall.url`) are correctly set.
