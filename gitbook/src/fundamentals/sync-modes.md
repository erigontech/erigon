# Sync Modes

Erigon 3 introduces a flexible approach to node configuration, offering three distinct types to suit various user needs. Depending on your need, you can choose from three different node types.

| **Prune Mode**      | **Flag**               | **Data Retained**                                                | **Primary Use Case**                                                                     |
| ------------------- | ---------------------- | ---------------------------------------------------------------- | ---------------------------------------------------------------------------------------- |
| Full Node (Default) | (No flag needed)       | Blocks and Receipts after The Merge (EIP-4444 enabled)           | General users, DApp interaction, fastest sync.                                           |
| Minimal Node        | `--prune.mode=minimal` | Only recent blocks                                               | Solo staking, users with constrained hardware, maximum privacy for sending transactions. |
| Historical Blocks   | `--prune.mode=blocks`  | Blocks and Receipts after The Merge _plus_ all historical blocks | Users needing historical block data for research or indexing.                            |
| Archive Node        | `--prune.mode=archive` | All historical state                                             | Developers, researchers, and RPC providers requiring full historical state access.       |

By **default**, Erigon run as a [full node](sync-modes.md#full-node), to change its behavior use the flag `--prune.mode <value>`.

In order to switch type of node, you must first delete the `/chaindata` folder in the chosen `--datadir` directory and re-sync from scratch.

{% hint style="success" %}
**\* Persisting receipts**, which are pre-calculated receipts, increase the requests-per-second (RPS) and improve the latency and throughput of all receipts and logs-related RPC calls.

They are enabled by default for **Minimal** and **Full Node.** can be activated or deactivated with the flag `--persist.receipts <value>` .
{% endhint %}

## Archive node

Ethereum's state refers to account balances, contracts, and consensus data. Archive nodes retain all historical state and require more [**disk space**](../getting-started/hardware-requirements.md#archive-node-requirements). They are typically used for block explorers or deep analytical queries. They provide comprehensive historical data, making them optimal for conducting extensive research on the chain, ranging from searching for old states of the EVM to implementing advanced block explorers, such as [Otterscan](../tools/otterscan.md), and undertaking development activities.

Erigon 3 has consistently reduced the disk space requirements for running an archive node, rendering it more affordable and accessible to a broader range of users. To run an archive node use the flag `--prune.mode=archive`.

## Full node

The default configuration in Erigon 3 is a Full Node. This setup is designed to offer significantly **faster sync times and reduced resource consumption** for daily operations compared to other clients. It achieves this by maintaining all essential data while intelligently pruning old, unnecessary historical data (blocks and receipts prior to The Merge, in line with [EIP-4444](https://eips.ethereum.org/EIPS/eip-4444)).

We strongly recommend running a Full Node whenever possible, as its reduced disk space requirements make it suitable for the majority of users. By running a Full Node, you directly support the network's **decentralization, resilience, and robustness**, aligning with Ethereum's distributed ethos.

## Minimal node

The Minimal Node configuration (`--prune.mode=minimal`) is the smallest possible setup. By keeping only recent blocks, it is perfectly suited for **solo staking** and users seeking maximum **privacy** when interacting with the EVM, such as sending transactions directly through their node. This mode is the most suitable for users with severely constrained hardware.
