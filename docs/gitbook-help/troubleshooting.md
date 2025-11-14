---
description: >-
  Step-by-step guidance for diagnosing and fixing unexpected problems with your
  node.
---

# Troubleshooting

**🛡️ Resilience and Data Integrity**

Erigon is highly resilient and uses a fully-transactional database. This design makes it safe against hard termination (`kill -9`) and power outages. The database ensures users never see "partial writes," meaning all data changes are atomic (all-or-nothing), and all RPC methods operate within Read-Only Transactions, guaranteeing a consistent data view.

**Protect Against Hardware Failure**: True data corruption is typically only caused by hardware failures (like disk or RAM failure). We strongly recommend using ECC memory, disk RAID, and performing regular backups to mitigate these risks.

When an issue arises, follow these steps to methodically diagnose and resolve the problem.

1. **Check Hardware Requirements:** The most common cause of issues is insufficient disk or RAM. Ensure your system meets the recommended specifications. Note that Erigon is very adaptive—adding more RAM to the server will make Erigon faster without requiring any setting changes.
2. **Inspect Erigon Logs:** The logs are your best friend. Use `tail -f erigon.log` or `journalctl` to see real-time output and identify error messages or warnings.
3. **Verify Sync Status:** Use `curl localhost:8545 \-X POST \-H "Content-Type: application/json" \--data '{"jsonrpc":"2.0","method":"eth\_syncing","params":\[\],"id":1}'` to check if the node is actively syncing.
4. **Monitor System Resources:** Use `htop`, `top`, or `iostat` to monitor CPU, RAM, and disk I/O. This can help you identify a performance bottleneck.
5. **Look for OOM-kill events:** After an unexpected crash, always check your system logs for an "Out of Memory" killer event. This confirms if a memory issue caused the crash.
6. **Perform a Simple Restart**: If the node is stalled, simply restart the service using `systemctl restart erigon` . Erigon's transactional database is designed to handle interruption gracefully.
7. **Disable RPC/CL during Initial Sync**: If you are stalling during snapshot sync, try restarting without the RPC daemon or consensus client to reduce concurrent disk access, which is a bottleneck during the slowest stage (Blocks Execution)
8. **Check for Disk Space:** Regularly check your disk usage. A full disk will cause performance degradation and can lead to a node crash.
9. **Verify Network Time:** Ensure your system's clock is synchronized. Incorrect time can cause issues with block propagation.
10. **Check P2P Peer Connections:** Use `net\_peerCount` or similar RPC methods to check if you have a healthy number of peers. A low count may indicate a network problem.
11. **Review Firewall Rules:** Confirm that your firewall is not blocking inbound or outbound traffic on the required P2P and RPC ports.
12. **Double-Check Configuration Flags:** Review all your command-line flags for typos or incorrect values. A single misplaced character can cause a cryptic error.
13. **Check for Snapshot File Issues:** For version upgrades, a known issue with snapshot filenames can cause problems. Use snapshot [upgrade](https://erigon.gitbook.io/docs/summary/getting-started/installation/upgrading#snapshots-upgrade-options) and [repair](https://erigon.gitbook.io/docs/summary/getting-started/installation/upgrading#managing-your-data) options.
14. **Correct File Ownership:** If using a dedicated user or Docker, confirm that the user has full read/write access to the datadir.
15. **Adjust RPC Timeouts:** If specific RPC requests are timing out, try increasing the timeout values to allow more time for heavy requests to complete.
16. **Check for `DB.read.concurrency` issues:** If you have high RPC traffic and low TPS, try reducing the `--DB.read.concurrency` flag.
17. **Report a Bug:** If all else fails, open a detailed bug report on GitHub with logs, version info, and a clear description of the problem.
18. **Engage with the Community:** The Erigon Discord server is an invaluable resource for seeking help from core developers and experienced users.
