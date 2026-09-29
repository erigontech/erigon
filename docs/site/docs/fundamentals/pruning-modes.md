---
title: "Pruning Modes"
description: "Full, minimal, blocks, and archive pruning modes explained — choose the right mode for your use case."
sidebar_position: 4
---


# Pruning Modes

Erigon 3 supports four pruning modes that control how much chain history your node retains. Choose based on your use case — most users should run a Full Node.

| **Pruning Mode**                                                        | **Flag**               | **Data Retained**                                                                                   | **Primary Use Case**                                                                     |
| --------------------------------------------------------------------- | ---------------------- | --------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------- |
| <p><a href="#full-node">Full Node</a><br />(Default)</p> | `--prune.mode=full`    | State and block data within the EIP-8252 window (last 262,144 blocks, ~36 days)                     | General users, DApp interaction, fastest sync.                                           |
| [Minimal Node](#minimal-node)                         | `--prune.mode=minimal` | State and block data within the last 100,000 blocks (~14 days)                                      | Solo staking, users with constrained hardware, maximum privacy for sending transactions. |
| [Historical Blocks](#blocks-node)                                     | `--prune.mode=blocks`  | All block/transaction history, plus state within the EIP-8252 window                                | Users needing historical block data for research or indexing.                            |
| [Archive Node](#archive-node)                            | `--prune.mode=archive` | All historical state and all blocks                                                                 | Developers, researchers, and RPC providers requiring full historical state access.       |

By **default**, Erigon run as a [full node](#full-node), to change its behavior use the flag `--prune.mode <value>`.

In order to switch type of node, you must first delete the `/chaindata` folder in the chosen `--datadir` directory and re-sync from scratch. Pruning is not reversible — data a mode has already discarded can only be recovered by re-syncing — so pick the mode that covers the history you need before you start.

:::tip
**Persisting receipts**, which are pre-calculated receipts, increase the requests-per-second (RPS) and improve the latency and throughput of all receipts and logs-related RPC calls.

They are enabled by default in every pruning mode on **fresh datadirs**; turn them off with
`--prune.include-receipts=false` (the former `--persist.receipts` still works as an alias). An **existing datadir keeps the setting it was created with**: if the
flag disagrees with the stored value, Erigon logs a warning and uses the stored value — changing it requires a fresh
datadir. Without the cache, receipts and logs are re-derived on demand from state history, so the related RPC calls keep
working within the node's state-history window, just with higher latency.

`--prune.include-receipts` on its own does **not** extend receipts and logs back to genesis: the receipt cache follows
the node's state-history window. On an [Archive node](#archive-node) that window is unbounded, so the cache covers the
whole chain. On a [Full](#full-node), [Minimal](#minimal-node) or [Historical Blocks](#blocks-node) node it is the
mode's state-history window (262,144 blocks for full and blocks, 100,000 for minimal). To keep the cache in full
regardless of the state-history window, add `--prune.receipts.distance=keep-all`; a finite
`--prune.receipts.distance=N` keeps it for the latest `N` blocks instead. Either form requires
`--prune.include-receipts`. Note that this retains the receipt *cache* only — it does not keep the
log address and topic indexes a filtered `eth_getLogs` needs. See
[Historical Blocks Node](#blocks-node).
:::

:::note[New in v3.6]
**Pruned nodes now reclaim disk from old snapshot files.** Frozen state-history (`.v`) and inverted-index (`.ef`) files
that fall entirely below the retention cutoff of the active `--prune.mode` are now deleted (a fresh sync already
skipped downloading them, in v3.5 too). Before v3.6, files already on disk were retained indefinitely regardless of
`--prune.mode`, so a long-running `full` or `minimal` node kept growing. Deletion is deferred until no reader still holds the retired
files. The commitment-history and receipt-cache domains do not behave the same way here. Commitment history is retired
against its own window, `--prune.commitment-history.distance`; left unset, nothing is retired. The receipt cache
instead follows the general state-history window by default, and is only retired against an independent window when
`--prune.receipts.distance` is set explicitly (with `keep-all` retiring nothing).
:::

## Archive node

Ethereum's state refers to account balances, contracts, and consensus data. Archive nodes retain all historical state and require more **disk space.** However, Erigon 3 has consistently reduced the [disk space](../get-started/hardware-requirements.mdx#disk-size-and-ram-requirements) requirements for running an archive node, rendering it more affordable and accessible to a broader range of users.

Archive are ideal for extensive research on the blockchain, developers, researchers, and RPC providers requiring a complete history of the state.

## Full node

The default configuration in Erigon 3 is a Full Node. This setup is designed to offer significantly **faster sync times and reduced resource consumption** for daily operations compared to other clients. It maintains state and block data within the **EIP-8252 reorg-retention window** — the last 262,144 blocks (~36.4 days), the inactivity-leak-bounded non-finality window across which an execution-layer client must be able to reconstruct state to handle any reorg without external sync. Older blocks, receipts, and state history are pruned, so a Full Node cannot serve queries against them. For block and transaction history reaching further back, run a [Blocks Node](#blocks-node); receipts need their own flags on top, as that section explains. See [EIP-8252](https://github.com/ethereum/EIPs/pull/11601) for the rationale behind the constant.

To keep every post-merge block instead of only the window — pruning pre-merge block data alone — set `--prune.distance.blocks=keep-post-merge`. Decide before the first start: pruning is not reversible, and block data already discarded comes back only by re-syncing.

We strongly recommend running a Full Node whenever possible, as its reduced disk space requirements make it suitable for the majority of users. By running a Full Node, you directly support the network's **decentralization, resilience, and robustness**, aligning with Ethereum's distributed ethos.

## Minimal node

The Minimal Node configuration (`--prune.mode=minimal`) is the smallest possible setup. It keeps only recent blocks and the **latest state** — it does not retain state history, so historical state queries are not supported. This makes it perfectly suited for **solo staking** and users seeking maximum **privacy** when interacting with the EVM, such as sending transactions directly through their node. This mode is the most suitable for users with severely constrained hardware.

## Blocks node

The Blocks Node configuration (`--prune.mode=blocks`) keeps the **full block and transaction history** — every block
back to genesis — while pruning **state history**. It retains state only within the EIP-8252 window (the last 262,144
blocks), the same state-retention as a Full Node, but unlike a Full Node it never prunes older blocks. This suits users
who need complete historical **block and transaction data** — for research, indexing, or block explorers — without
paying the disk cost of an archive node's full historical **state**. For full-range **receipts** by block
(`eth_getBlockReceipts` back to genesis), add
`--prune.include-receipts --prune.receipts.distance=keep-all`; with `--prune.include-receipts` alone the receipt cache
follows the state-history window (the last 262,144 blocks), and without it receipts are re-derived from state history
within that same window.

`keep-all` does **not** extend filtered `eth_getLogs` back to genesis. It retains the receipt-cache domain only; the
log address and topic indexes that a filtered query needs are standalone inverted indexes, retired against the general
state-history cutoff (`AggregatorRoTx.Retire` applies `RetireCutoffs.Default` to them, and only `RCacheDomain` carries
the `keep-all` override). An address- or topic-filtered `eth_getLogs` can therefore miss matches older than 262,144
blocks even with the cache retained. Note that dropping the filters does not merely widen the query — it changes the
read path: with neither `address` nor `topics` set, `applyFiltersV3` consults no bitmap and falls back to a plain
`stream.Range` over the retained cache, so an unfiltered range query is unaffected. For those older ranges, either query
by block with `eth_getBlockReceipts` and filter client-side, or use an [Archive node](#archive-node), whose unbounded
state-history window keeps the log indexes too.
