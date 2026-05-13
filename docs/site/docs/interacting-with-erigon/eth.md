---
title: "eth"
description: "Standard Ethereum JSON-RPC methods: blocks, transactions, state, logs, and filters."
sidebar_position: 1
---


# eth

The `eth` namespace is the foundational and most commonly used API set in Ethereum's JSON-RPC interface. It provides core functionality for interacting with the Ethereum blockchain, enabling users and applications to read blockchain state and submit transactions.

Key methods within this namespace allow you to check an account's balance (`eth_getBalance`), get the current block number (`eth_blockNumber`), retrieve transaction details (`eth_getTransactionByHash`), and send signed transactions to the network (`eth_sendRawTransaction`, `eth_sendRawTransactionSync`). Essentially, the `eth` namespace contains all the fundamental tools needed to observe and participate in the life of the chain.

### API usage

For API usage refer to the below official resources:

* [https://ethereum.org/en/developers/docs/apis/json-rpc/](https://ethereum.org/en/developers/docs/apis/json-rpc/)
* [https://ethereum.github.io/execution-apis/](https://ethereum.github.io/execution-apis/)

### eth\_getProof

`eth_getProof` returns Merkle proofs for account state and storage slots, as defined in [EIP-1186](https://eips.ethereum.org/EIPS/eip-1186). It is stable and production-ready as of Erigon v3.4.

To enable historical proof support, activate commitment history storage at startup:

```
--prune.include-commitment-history=true
```

:::warning
**RAM requirement:** Historical `eth_getProof` requires at least **+32 GB RAM** to operate efficiently. Running without sufficient memory will severely degrade node performance.
:::

This enables faster retrieval of Merkle proofs for any executed block.

To bound commitment-history disk usage, combine with `--prune.commitment-history.distance.blocks=N` to keep only the latest `N` blocks of commitment history. The value must be `≤ --prune.distance` because `eth_getProof` also needs the underlying state history. Retention may be tightened between runs (extra snapshot files are removed on the next prune cycle); widening requires resyncing the chaindata.

### eth\_getStorageValues

`eth_getStorageValues` is an Erigon extension that retrieves multiple storage slots for a given account in a single call, reducing round-trips compared to multiple `eth_getStorageAt` calls.

**Parameters**

| Parameter    | Type             | Description                                          |
| ------------ | ---------------- | ---------------------------------------------------- |
| address      | STRING           | The account address to query storage for             |
| storageKeys  | ARRAY of STRING  | List of 32-byte storage slot keys (hex-encoded)      |
| blockNumber  | STRING or NUMBER | Block number or tag (`"latest"`, `"earliest"`, etc.) |

**Example**

```bash
curl --data '{"jsonrpc":"2.0","method":"eth_getStorageValues","params":["0xAddress","["0x0000000000000000000000000000000000000000000000000000000000000001"],"latest"],"id":1}' -H "Content-Type: application/json" -X POST http://localhost:8545
```


**Returns**

An object mapping each requested storage key to its 32-byte value.
