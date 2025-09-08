# Troubleshooting

When an issue arises, follow these steps to methodically diagnose and resolve the problem.

1. **Check Hardware Requirements:** The most common cause of issues is insufficient disk or RAM. Ensure your system meets the recommended specifications.  
2. **Inspect Erigon Logs:** The logs are your best friend. Use `tail -f erigon.log` or `journalctl` to see real-time output and identify error messages or warnings.  
3. **Verify Sync Status:** Use `curl localhost:8545 \-X POST \-H "Content-Type: application/json" \--data '{"jsonrpc":"2.0","method":"eth\_syncing","params":\[\],"id":1}'` to check if the node is actively syncing.  
4. **Monitor System Resources:** Use `htop`, `top`, or `iostat` to monitor CPU, RAM, and disk I/O. This can help you identify a performance bottleneck.  
5. **Look for OOM-kill events:** After an unexpected crash, always check your system logs for an "Out of Memory" killer event. This confirms if a memory issue caused the crash.  
6. **Perform a Graceful Restart:** If the node is stalled, restart the service using `systemctl restart erigon` instead of a hard kill to prevent database corruption.  
7. **Disable RPC/CL during Initial Sync:** If you are stalling during snapshot sync, try restarting without the RPC daemon or consensus client to resolve conflicts.  
8. **Check for Disk Space:** Regularly check your disk usage. A full disk will cause performance degradation and can lead to a node crash.  
9. **Clear System Caches:** If memory usage is high, run `sync && sudo sysctl vm.drop\_caches=3` to flush the OS page cache and free up memory.  
10. **Verify Network Time:** Ensure your system's clock is synchronized. Incorrect time can cause issues with block propagation.  
11. **Check P2P Peer Connections:** Use `net\_peerCount` or similar RPC methods to check if you have a healthy number of peers. A low count may indicate a network problem.  
12. **Review Firewall Rules:** Confirm that your firewall is not blocking inbound or outbound traffic on the required P2P and RPC ports.  
13. **Double-Check Configuration Flags:** Review all your command-line flags for typos or incorrect values. A single misplaced character can cause a cryptic error.  
14. **Check for Snapshot File Issues:** For version upgrades, a known issue with snapshot filenames can cause problems. Use snapshot upgrade and repair options.  
15. **Correct File Ownership:** If using a dedicated user or Docker, confirm that the user has full read/write access to the datadir.  
16. **Adjust RPC Timeouts:** If specific RPC requests are timing out, try increasing the timeout values to allow more time for heavy requests to complete.  
17. **Run a Diagnostic Report:** For complex issues, use the `support` command to connect the Erigon instance to a diagnostics system for a detailed bug report.  
18. **Check for `DB.read.concurrency` issues:** If you have high RPC traffic and low TPS, try reducing the `--DB.read.concurrency` flag.  
19. **Report a Bug:** If all else fails, open a detailed bug report on GitHub with logs, version info, and a clear description of the problem.  
20. **Engage with the Community:** The Erigon Discord server is an invaluable resource for seeking help from core developers and experienced users.
