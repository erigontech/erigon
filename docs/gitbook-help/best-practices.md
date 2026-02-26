---
description: >-
  Recommendations for optimizing hardware, configuration, and operation for peak
  performance and stability.
---

# Best Practices

These integrated practices focus on maximizing the reliability, efficiency, and security of your Erigon node setup.

**1. Hardware and Data Integrity**

| **Practice**                        | **Details**                                                                                                                                                                     | **Rationale**                                                                                                                                                                           |
| ----------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Use a High-End NVMe SSD             | This is a non-negotiable prerequisite. NVMe SSDs are essential for reliable, high-performance node operation.                                                                   | Erigon is highly disk I/O intensive. Fast storage prevents bottlenecks and sync issues.                                                                                                 |
| Integrate Hardware Reliability      | Use ECC memory and consider a disk RAID setup. Do regular backups of your chain data.                                                                                           | While Erigon's database is fully-transactional and resilient to graceful shutdowns, crashes, and power outages, it is not protected against hardware failures (disk or RAM corruption). |
| Symlink Data Directories (Advanced) | For systems with multiple disks, consider symlinking `snapshots` and `temp` directories to a cheaper, high-capacity drive, while keeping the main `chaindata` on the fast NVMe. | Optimizes cost and capacity while retaining performance for critical files.                                                                                                             |
| Maintain Sufficient RAM             | Ensure your system has at least the recommended RAM for your chosen network and sync mode.                                                                                      | Insufficient RAM, even with Erigon's memory efficiency, will lead to OOM-kill events (Out-of-Memory).                                                                                   |

**2. Node Configuration and Startup**

| **Practice**                               | **Details**                                                                                                                                                                         | **Rationale**                                                                                                                                                           |
| ------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Make a Mindful Pruning Mode Decision       | The `--prune.mode` flag is irreversible. Choose `full` for most cases, `minimal` for validators with limited space, and `archive` only if all historical data is absolutely needed. | An incorrect choice can lead to a necessary resync. `Full` is a balance of performance and space.                                                                       |
| Employ a Process Manager                   | Always run Erigon as a systemd or supervisor service.                                                                                                                               | Ensures a graceful shutdown and allows for automatic restarts, contributing to stability.                                                                               |
| Use a Dedicated User Account               | Run the Erigon service with a non-root, dedicated user account.                                                                                                                     | Follows security best practices and prevents potential permission-related errors.                                                                                       |
| Avoid Graceful Shutdown (Unless Necessary) | While a process manager ensures a graceful shutdown, know that Erigon is safe against abrupt halts like process kills (`kill -9`) or a power outage.                                | Erigon's database is fully-transactional, meaning writes are atomic ("all-or-nothing"), preventing "partial writes" and database corruption from non-hardware failures. |
| Tune Sync Speed with Flags                 | Use flags like `--sync.loop.block.limit` and `--batchSize` to tune the synchronization speed, especially during the initial sync.                                                   | Allows you to optimize the sync process based on your system's hardware capabilities.                                                                                   |
| Lock the Latest State in RAM (Advanced)    | On systems with high historical RPC traffic, a tool like vmtouch can be used to lock the latest state files in RAM.                                                                 | Improves RPC response times for common queries by keeping the latest data in the fastest memory.                                                                        |

**3. Security and Monitoring**

| **Practice**                | **Details**                                                                                                                                            | **Rationale**                                                                                               |
| --------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------ | ----------------------------------------------------------------------------------------------------------- |
| Configure Firewall Rules    | Configure your firewall to allow only the necessary ports for P2P and RPC communication.                                                               | Standard security practice to limit attack surface.                                                         |
| Never Expose RPC Ports      | Never expose the RPC ports to the public internet without a reverse proxy.                                                                             | RPC (Remote Procedure Call) ports grant access to node functions; public exposure is a major security risk. |
| Monitor Your Node's Health  | Implement comprehensive monitoring using tools like Prometheus and Grafana to track key metrics like disk I/O, CPU load, and sync status in real time. | Essential for proactively identifying performance bottlenecks and stability issues.                         |
| Enable JSON-RPC Compression | Use the `--http.compression` flag.                                                                                                                     | Reduces network bandwidth usage for RPC requests.                                                           |
| Tune RPC Batch Concurrency  | Adjust the `--rpc.batch.concurrency` flag.                                                                                                             | Limits the number of parallel database reads to prevent a single batch request from overloading the server. |

**4. Maintenance and Development**

| **Practice**                   | **Details**                                                                                                         | **Rationale**                                                                                       |
| ------------------------------ | ------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------- |
| Keep Your Binary Updated       | Regularly update your binary. Erigon is in active development.                                                      | New releases often contain critical bug fixes and performance improvements essential for stability. |
| Use Official Docker Images     | If using Docker, rely on the official images provided by the Erigon team.                                           | Best compatibility and stability for containerized deployment.                                      |
| Run a Test Sync on a Testnet   | Before running on Mainnet, consider performing a sync on a testnet (e.g., Sepolia).                                 | Familiarizes you with the process and your system's performance characteristics.                    |
| Report Issues with Diagnostics | When a bug is encountered, provide a detailed report on GitHub with a stack trace and other diagnostic information. | Helps the development team quickly identify and fix issues.                                         |

**5. Validator-Specific Practices**

| **Practice**                      | **Details**                                                                            | **Rationale**                                                           |
| --------------------------------- | -------------------------------------------------------------------------------------- | ----------------------------------------------------------------------- |
| Check Validator Health            | For validator nodes, a daily check of the node's logs and service status is essential. | Confirms that the node is signing attestations and performing its duty. |
| Maintain a Sufficient ETH Balance | Validators must maintain an adequate ETH balance on their signer address.              | Required to cover transaction fees for checkpoint submissions.          |
