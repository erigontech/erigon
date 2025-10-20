# Migrating from Geth

This guide provides a smooth and quick transition from **Geth** to **Erigon**. To begin, ensure you have sufficient disk space. The most secure method involves syncing an Erigon node on the same machine along with your existing Geth node. This allows for verification of the Erigon node's proper synchronization, functional verification, and minimal node downtime. This approach is highly recommended for public JSON-RPC providers and validators.

* **If you have enough disk space**, [Option 1](migrating-from-geth.md#option-1-sync-erigon-alongside-geth) is the recommended choice.
* **If disk space is limited and downtime is not an option**, we recommend extending the disk or, if not possible, syncing Erigon on a separate machine. Once synced, move all validator-related files to the new machine and decommission the old one.
* **If downtime of 12 hours or more is acceptable**, see [Option 2](migrating-from-geth.md#option-2-remove-geth-and-sync-erigon).

### Option 1: Sync Erigon Alongside Geth

First, install Erigon and a consensus client of your choice.

**Important considerations:**

* If you use the same consensus client for Erigon that's already paired with Geth, confirm their settings (e.g., data directories) do not conflict.
* Ensure the network ports for the Erigon-paired consensus client and the Geth-paired consensus client do not conflict.
* Verify that Erigon's JSON-RPC, Engine API, and P2P networking ports are different from Geth's. These ports are configured via command-line options: `--http.port <port>`, `--authrpc.port <port>`, and `--p2p.listen-addr <IP:port>`.

Once the above requirements are met, you can start syncing Erigon. To monitor the sync status, use the `eth_syncing` JSON-RPC method. When it returns `false`, Erigon is fully synced and ready to function as a validator. Alternatively, you can use a health check to monitor the sync.

After Erigon is fully synced, shut down both Geth and Erigon, along with their respective consensus clients. Then, restart Erigon using the same ports and JWT secret that Geth previously used. Check the logs for any warnings or errors and confirm that Erigon is following the chain correctly. If everything is in order, you can then safely remove Geth, its associated consensus client, and their data.

### Option 2: Remove Geth and Sync Erigon

This is the simplest option as it requires no configuration adjustments. However, the node will be down until Erigon finishes syncing.

1. Shut down and remove Geth and its data.
2. Install Erigon.
3. Ensure Erigon uses the same network ports and JWT secret that Geth previously used. Otherwise, you must reconfigure the consensus client to match Erigon's new settings.

Once these requirements are met, start syncing Erigon. While it syncs, ensure no errors appear in the logs of either Erigon or the consensus client. Note that the sync time can vary depending on the chain. You can periodically check the `eth_syncing` JSON-RPC method or the health check to monitor the progress.
