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

### Pending state

Erigon does not support the `pending` block tag for `eth_call`, `eth_createAccessList`, `eth_getProof`, `eth_getWitness`, `eth_getTxWitness`, or `eth_simulateV1`. These methods need a block header and state from the same view, and Erigon cannot currently acquire a matching pending-state view. They return `pending state is not supported` instead of executing against a different block. Other `eth` methods keep their existing pending behavior.

### eth\_getProof

`eth_getProof` returns Merkle proofs for account state and storage slots, as defined in [EIP-1186](https://eips.ethereum.org/EIPS/eip-1186). It is stable and production-ready as of Erigon v3.4.

To enable historical proof support, activate commitment history storage at startup:

```text
--prune.include-commitment-history=true
```

:::warning
**RAM requirement:** Historical `eth_getProof` requires at least **+32 GB RAM** to operate efficiently. Running without sufficient memory will severely degrade node performance.
:::

This enables faster retrieval of Merkle proofs for any executed block.

### eth\_getStorageValues

`eth_getStorageValues` reads several storage slots for one or more accounts in a single call, reducing round-trips compared to multiple `eth_getStorageAt` calls. It is part of the [Ethereum execution APIs](https://github.com/ethereum/execution-apis/blob/main/src/eth/state.yaml) and takes two parameters.

**Parameters**

| Parameter   | Type             | Description                                                                                       |
| ----------- | ---------------- | ------------------------------------------------------------------------------------------------- |
| requests    | OBJECT           | Maps each account address to an array of 32-byte storage slot keys (hex-encoded)                   |
| blockNumber | STRING or NUMBER | Optional. Block number, block hash or tag (`"latest"`, `"earliest"`, etc.). Defaults to `"latest"` |

**Example**

```bash
curl --data '{"jsonrpc":"2.0","method":"eth_getStorageValues","params":[{"0xdAC17F958D2ee523a2206206994597C13D831ec7":["0x0000000000000000000000000000000000000000000000000000000000000000","0x0000000000000000000000000000000000000000000000000000000000000002"],"0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48":["0x0000000000000000000000000000000000000000000000000000000000000002"]},"latest"],"id":1}' -H "Content-Type: application/json" -X POST http://localhost:8545
```

**Returns**

An object mapping each requested address to an array of 32-byte values, in the same order as the slot keys requested for that address. Slots that were never written return zero.

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": {
    "0xdac17f958d2ee523a2206206994597c13d831ec7": [
      "0x000000000000000000000000c6cde7c39eb2f0f0095f41570af89efc2c1ea828",
      "0x0000000000000000000000000000000000000000000000000000000000000000"
    ],
    "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48": [
      "0x0000000000000000000000000a06be16275b95a7d2567fbdae118b36c7da78f9"
    ]
  }
}
```

**Limits**

* A single call may request at most 1024 slots in total, counting all addresses together. Larger requests are rejected.
* An empty `requests` object returns error code `-32602` with the message `empty request`.
* When `blockNumber` is a block hash, the block must be canonical.
