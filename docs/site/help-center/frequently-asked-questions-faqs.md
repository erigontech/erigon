---
title: "Frequently Asked Questions (FAQs)"
description: "Answers to the most common questions about installing, syncing, and operating Erigon."
sidebar_position: 2
---

# Frequently Asked Questions (FAQs)

This list addresses the most common inquiries from the Erigon community, offering quick and direct answers to a variety of topics.

1. **What is the difference between Erigon and Geth?** Erigon originated as a fork of Geth but has been entirely rewritten with a focus on disk space and sync speed. It uses a flat, key-value database (MDBX) instead of a Merkle Patricia Trie, resulting in a much smaller disk footprint and faster synchronization times.
2. **Is Erigon a good choice for client diversity?** Yes. The codebases have diverged so significantly from Geth that they are now considered separate clients, making Erigon an excellent choice to support Ethereum's client diversity goals.
3. **What are the minimum hardware requirements?** The most critical component is a high-end NVMe or SSD with very low latency. While the exact requirements vary by network, 16GB of RAM and a fast SSD are generally the minimum for a full node. See [Hardware Requirements](/get-started/hardware-requirements) for detailed specs per network and pruning mode.
4. **How long does it take to sync a node?** Initial synchronization time is highly dependent on hardware and bandwidth. A fast system with a high-end NVMe can sync a full node in as little as few hours, while on slower hardware, it can take several days.
5. **How much disk space is required?** Disk space needs are constantly changing as the blockchain grows. As of September 2025, an Ethereum Mainnet full node requires about 920GB, while an archive node is significantly larger at around 1.77TB. See [Hardware Requirements](/get-started/hardware-requirements) for up-to-date figures.
6. **Can I run Erigon on an HDD?** It is not recommended to run Erigon on an HDD. The client's performance is critically dependent on high-speed disk I/O, and an HDD will almost certainly cause the node to fall behind the blockchain tip.
7. **What is the `--prune.mode` flag, and which mode should I use?** This flag determines which historical data is discarded. The `full` mode (the default for Erigon 3) is suitable for most users. The `minimal` mode is for validators with limited disk space. The `archive` mode is for those who need a full historical record for all past states. See [Pruning Modes](/fundamentals/pruning-modes) for a full comparison.
8. **Can I change my pruning mode after starting the node?** No. The pruning mode is a permanent choice made at the first sync. Changing it requires deleting the `datadir` directory and a full re-sync from scratch.
9. **Do I need a separate consensus client?** No, you don't need a separate consensus client. In many cases, it's fine to use [Caplin](/staking/caplin), the embedded consensus client that runs by default within Erigon. However, some users, especially validators, prefer to run a [separate consensus client](/staking/external-consensus-client-as-validator) for enhanced reliability.
10. **How do I upgrade my Erigon binary?** The process involves gracefully shutting down the node, replacing the old binary with the new one, and restarting. See the [Upgrading](/get-started/installation/upgrading) guide for step-by-step instructions. It is also recommended to back up your datadir before any major upgrade.
11. **How do I gracefully shut down Erigon?** The safest way to shut down is by using a process manager like `systemd` or `supervisor`. Alternatively, you can press `Ctrl+C` in the terminal to allow the database to close cleanly.
12. **What is Erigon's RPC daemon?** The [RPC daemon](/fundamentals/modules/rpc-daemon) is a separate process that handles JSON RPC API requests. It can run on a different machine from the core Erigon client to enhance security and scalability.
13. **Is it possible to recover from a database corruption?** While Erigon's database is robust, an ungraceful shutdown can cause corruption. The most reliable solution is to delete the corrupted datadir and perform a full re-sync, but it is also worth using repair tools.
14. **What are the required ports?** Erigon requires specific ports for P2P networking (default `30303` TCP/UDP) and RPC access (`8545` HTTP, `8546` WS, `8551` Engine API). See [Default Ports](/fundamentals/default-ports) for the complete list.
15. **What is a "snapshot sync"?** Snapshot sync is a stage of the synchronization process where the node downloads pre-made snapshots of the blockchain state. This significantly accelerates the initial sync, reducing the time required to catch up with the network. See [Pruning Modes](/fundamentals/pruning-modes) for details.
16. **How do I monitor my node's sync progress?** There are three complementary ways:

    * **Read the live log line** — every sync log line is prefixed with the current stage and its position in the pipeline, e.g. `[4/8 Bodies]`. The prefix advances through the stages until `[8/8 Finish]`, at which point the node is at the chain tip. See [Logs → Stage Definitions](/fundamentals/logs#stage-definitions) for the full ordered list.
    * **Query the JSON-RPC** — call `eth_syncing` against the RPC daemon (default port `8545`):

      ```bash
      curl -s -X POST -H "Content-Type: application/json" \
        --data '{"jsonrpc":"2.0","method":"eth_syncing","params":[],"id":1}' \
        http://localhost:8545
      ```

      While syncing the result is an object with `currentBlock`, `highestBlock`, and a `stages` array (per-stage progress, matching the log prefix); once fully synced it is simply `false`. (`startingBlock` is also returned but is hardcoded to `"0x0"`.)
    * **Ask an AI assistant via MCP** — Erigon's built-in [MCP server](/fundamentals/mcp) (default `127.0.0.1:8553`) lets a connected assistant like Claude Desktop answer questions like *"Is my node synced? How many blocks behind am I?"* in plain language by querying the same data the JSON-RPC exposes. Useful when you'd rather not parse logs or `eth_syncing` output by hand.
17. **Can I use Erigon with Docker?** Yes, Erigon provides official Docker images on Dockerhub. See the [Installation](/get-started/installation) guide for the Docker setup instructions.
18. **Does Erigon support other chains besides Ethereum?** Yes, Erigon is an EVM-compatible client and supports other networks like Gnosis. You can specify the chain with the `--chain` flag. See [Supported Networks](/fundamentals/supported-networks) and the [Easy Nodes](/get-started/easy-nodes) guides for chain-specific setup.
19. **Where can I find official support?** The primary channels for support and community discussions are the official Erigon Discord and GitHub repository.
20. **What is Caplin?** [Caplin](/staking/caplin) is Erigon's built-in beacon API server. It allows Erigon to function as both a consensus and execution client.
21. **What is OtterSync?** OtterSync is a syncing algorithm that further enhances performance by shifting 98% of the computation to network bandwidth, reducing synchronization times and improving chain tip performance, disk footprint, and decentralization.
22. **Why can't I query blocks before a certain height?** In `full` and `minimal` pruning modes, Erigon discards historical state data that is older than the pruning window. If you need to query early blocks, you must run an archive node (`--prune.mode=archive`). See [Pruning Modes](/fundamentals/pruning-modes) for more on pruning. Note that the pruning mode is set permanently at the first sync and cannot be changed without a full re-sync.
23. **Can I run Erigon on a Raspberry Pi or low-power ARM device?** It is not recommended for Mainnet. Erigon requires a high-end NVMe SSD and sufficient RAM (16 GB minimum) for reliable operation. Low-power ARM devices typically lack the disk I/O performance and memory needed to keep up with the chain tip. See [Hardware Requirements](/get-started/hardware-requirements). A testnet like Sepolia may be feasible on higher-end ARM hardware, but expect limited performance.
24. **What is the MCP server?** The MCP (Model Context Protocol) server is a built-in component that exposes Erigon's blockchain data to AI assistants like Claude Desktop. It is enabled by default and listens on `127.0.0.1:8553`. To disable it, pass `--mcp.disable` at startup.
25. **How do I connect Claude Desktop to my Erigon node?** Build the standalone `mcp` binary with `make mcp`, then add it to your Claude Desktop configuration at `~/.config/claude-desktop/config.json` pointing to your node's JSON-RPC endpoint or datadir. See the [MCP Server](/fundamentals/mcp) page for the full configuration example.

