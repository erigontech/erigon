# Best Practices

These practices will help you operate your Erigon node with maximum reliability and efficiency, preventing many of the common issues before they occur.

1. **Use a High-End NVMe SSD:** This is the single most important choice you can make. It is a non-negotiable prerequisite for a reliable and high-performance node.  
2. **Make a Mindful Pruning Mode Decision:** The `--prune.mode` flag is irreversible. Choose full for most cases, minimal for validators with limited space, and archive only if you absolutely need all historical data.  
3. **Employ a Process Manager:** **Always** run Erigon as a `systemd` or `supervisor` service. This ensures a graceful shutdown, preventing database corruption and allowing for automatic restarts.  
4. **Use a Dedicated User Account:** Run the Erigon service with a non-root, dedicated user account to follow security best practices and prevent permission-related errors.  
5. **Configure Firewall Rules:** Configure your firewall to allow only the necessary ports for P2P and RPC communication. Never expose the RPC ports to the public internet without a reverse proxy.  
6. **Maintain Sufficient RAM:** Ensure your system has at least the recommended RAM for your chosen network and node type. While Erigon is memory-efficient, insufficient RAM will lead to OOM-kill events.  
7. **Keep Your Binary Updated:** Erigon is in active development, and new releases often contain critical bug fixes and performance improvements. Regularly update your binary for stability.  
8. **Symlink Data Directories (Advanced):** For systems with multiple disks, consider symlinking snapshots and temp directories to a cheaper, high-capacity drive, while keeping the main chaindata on a fast NVMe.  
9. **Monitor Your Node's Health:** Implement comprehensive monitoring using tools like Prometheus and Grafana to track key metrics like disk I/O, CPU load, and sync status in real time.  
10. **Enable JSON-RPC Compression:** Use the `--http.compression` flag to reduce network bandwidth usage for RPC requests.  
11. **Avoid kill \-9:** Never use kill \-9 to stop the process unless absolutely necessary, as it can corrupt the database. Always prefer a graceful shutdown.  
12. **Lock the Latest State in RAM:** On systems with high historical RPC traffic, a tool like `vmtouch` can be used to lock the latest state files in RAM, improving response times.  
13. **Tune Sync Speed with Flags:** Use flags like `--sync.loop.block.limit` and `--batchSize` to tune the synchronization speed, especially during the initial sync.  
14. **Check Validator Health (for stakers):** For validator nodes, a daily check of the node's logs and service status is essential to confirm that it is signing attestations.  
15. **Maintain a Sufficient ETH Balance:** Validators must maintain an adequate ETH balance on their signer address to cover transaction fees for checkpoint submissions.  
16. **Tune RPC Batch Concurrency:** Adjust the `--rpc.batch.concurrency` flag to limit the number of parallel database reads, which can prevent a single batch request from overloading the server.  
17. **Use Official Docker Images:** If you are using Docker, rely on the official images provided by the Erigon team for the best compatibility and stability.  
18. **Report Issues with Diagnostics:** When a bug is encountered, provide a detailed report on GitHub with a stack trace and other diagnostic information to help the development team.  
19. **Run a Test Sync on a Testnet:** Before running on Mainnet, consider performing a sync on a testnet (e.g., Sepolia) to familiarize yourself with the process and your system's performance.  
20. **Use a Separate CL Client:** While Caplin offers a built-in beacon API, consider also a dedicated, separate consensus client (e.g., Lighthouse, Nimbus) for a production validator setup.
