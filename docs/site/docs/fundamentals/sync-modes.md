---
title: "Sync Modes"
description: "Full, minimal, blocks, and archive sync explained — choose the right mode for your use case."
sidebar_position: 2
---


# Sync Modes

Erigon 3 supports four prune modes that control how much chain history your node retains. Choose based on your use case — most users should run a Full Node.

| **Prune Mode**                                                        | **Flag**               | **Data Retained**                                                                                   | **Primary Use Case**                                                                     |
| --------------------------------------------------------------------- | ---------------------- | --------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------- |
| <p>* <a href="#full-node">Full Node</a><br />(Default)</p> | `--prune.mode=full`    | State and block data within the EIP-8252 window (last 262,144 blocks, ~36 days)                     | General users, DApp interaction, fastest sync.                                           |
| \* [Minimal Node](#minimal-node)                         | `--prune.mode=minimal` | State and block data within the last 100,000 blocks (~14 days)                                      | Solo staking, users with constrained hardware, maximum privacy for sending transactions. |
| Historical Blocks                                                     | `--prune.mode=blocks`  | All block/transaction history, plus state within the EIP-8252 window                                | Users needing historical block data for research or indexing.                            |
| [Archive Node](#archive-node)                            | `--prune.mode=archive` | All historical state and all blocks                                                                 | Developers, researchers, and RPC providers requiring full historical state access.       |

By **default**, Erigon run as a [full node](#full-node), to change its behavior use the flag `--prune.mode <value>`.

In order to switch type of node, you must first delete the `/chaindata` folder in the chosen `--datadir` directory and re-sync from scratch.

:::tip
**\* Persisting receipts**, which are pre-calculated receipts, increase the requests-per-second (RPS) and improve the latency and throughput of all receipts and logs-related RPC calls.

They are enabled by default for **Minimal** and **Full Node.** They can be activated or deactivated with the flag `--persist.receipts <value>` .
:::

## Archive node

Ethereum's state refers to account balances, contracts, and consensus data. Archive nodes retain all historical state and require more **disk space.** However, Erigon 3 has consistently reduced the [disk space](../get-started/hardware-requirements.mdx#disk-size-and-ram-requirements) requirements for running an archive node, rendering it more affordable and accessible to a broader range of users.

Archive are ideal for extensive research on the blockchain, developers, researchers, and RPC providers requiring a complete history of the state.

## Full node

The default configuration in Erigon 3 is a Full Node. This setup is designed to offer significantly **faster sync times and reduced resource consumption** for daily operations compared to other clients. It maintains state and block data within the **EIP-8252 reorg-retention window** — the last 262,144 blocks (~36.4 days), the inactivity-leak-bounded non-finality window across which an execution-layer client must be able to reconstruct state to handle any reorg without external sync. Older blocks, receipts, and state history are pruned. See [EIP-8252](https://github.com/ethereum/EIPs/pull/11601) for the rationale behind the constant.

We strongly recommend running a Full Node whenever possible, as its reduced disk space requirements make it suitable for the majority of users. By running a Full Node, you directly support the network's **decentralization, resilience, and robustness**, aligning with Ethereum's distributed ethos.

## Minimal node

The Minimal Node configuration (`--prune.mode=minimal`) is the smallest possible setup. It keeps only recent blocks and the **latest state** — it does not retain state history, so historical state queries are not supported. This makes it perfectly suited for **solo staking** and users seeking maximum **privacy** when interacting with the EVM, such as sending transactions directly through their node. This mode is the most suitable for users with severely constrained hardware.
