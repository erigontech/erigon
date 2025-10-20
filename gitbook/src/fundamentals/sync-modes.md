# Sync Modes

Erigon 3 introduces a flexible approach to node configuration, offering three distinct types to suit various user needs. Depending on your need, you can choose from three different node types.

| Usage        | Minimal Node | Full Node | Archive Node |
|--------------|--------------|-----------|--------------|
| Privacy, RPC |    **Yes**   |   **Yes** |    **Yes**   |
| Contribute to network | No  |   **Yes** |    **Yes**   |
| Research     |    No        |    No     |    **Yes**   |
| Staking      |    **Yes**   |  **Yes**  |    **Yes**   |

By **default**, Erigon run as a [full node](#full-node).

In order to switch type of node, you must first delete the ```/chaindata``` folder in the chosen ```--datadir``` directory and re-sync from scratch.

> **Persisting receipts**, which are pre-calculated receipts, are enabled by default for minimal and full node. To disable them use the flag `--persist.receipts=false`.

## Archive node

Ethereum's state refers to account balances, contracts, and consensus data. Archive nodes retain all historical state and require  more **[disk space](../getting-started/hardware-requirements.md#archive-node-requirements)**. They are typically used for block explorers or deep analytical queries. They provide comprehensive historical data, making them optimal for conducting extensive research on the chain, ranging from searching for old states of the EVM to implementing advanced block explorers, such as [Otterscan](../tools/otterscan.md), and undertaking development activities.

Erigon 3 has consistently reduced the disk space requirements for running an archive node, rendering it more affordable and accessible to a broader range of users. To run an archive node use the flag `--prune.mode=archive`.

## Full node

Erigon 3 is full node by **default**. This configuration delivers **faster sync times** and reduced resource consumption for everyday operation, maintaining essential data while **reducing storage requirements**. We recommend running a full node whenever possible, as it supports the network's decentralization, resilience, and robustness, aligning with Ethereum's trustless and distributed ethos. Given the reduced **[disk space](../getting-started/hardware-requirements.md#full-node-requirements)** requirements of Erigon 3, the full node configuration is suitable for the majority of users. 


## Minimal node

Erigon 3 implements support for [EIP-4444](https://eips.ethereum.org/EIPS/eip-4444) through its minimal node configuration, enabled by the flag `--prune.mode=minimal`. For example:

```bash
./build/bin/erigon --prune.mode=minimal
```
Minimal node is suitable for users with constrained [hardware](../getting-started/hardware-requirements.md#minimal-node-requirements) who wants to achieve more **privacy** during their interaction with EVM, like for example sending transactions with your node.

Minimal node is also suitable for **staking**.

