---
description: Core Methods for Interacting with the Ethereum Blockchain
metaLinks:
  alternates:
    - >-
      https://app.gitbook.com/s/3DGBf2RdbfoitX1XMgq0/interacting-with-erigon/interacting-with-erigon/eth
---

# eth

The `eth` namespace is the foundational and most commonly used API set in Ethereum's JSON-RPC interface. It provides core functionality for interacting with the Ethereum blockchain, enabling users and applications to read blockchain state and submit transactions.

Key methods within this namespace allow you to check an account's balance (`eth_getBalance`), get the current block number (`eth_blockNumber`), retrieve transaction details (`eth_getTransactionByHash`), and send signed transactions to the network (`eth_sendRawTransaction`, `eth_sendRawTransactionSync`). Essentially, the `eth` namespace contains all the fundamental tools needed to observe and participate in the life of the chain.

### API usage

For API usage refer to the below official resources:

{% embed url="https://ethereum.org/en/developers/docs/apis/json-rpc/" %}

{% embed url="https://ethereum.github.io/execution-apis/" %}

### eth\_getProof

`eth_getProof` returns Merkle proofs for account state and storage slots, as defined in [EIP-1186](https://eips.ethereum.org/EIPS/eip-1186). It is stable and production-ready as of Erigon v3.4.

To enable historical proof support, activate commitment history storage at startup:

```
--prune.include-commitment-history=true
```

{% hint style="warning" %}
**RAM requirement:** Historical `eth_getProof` requires at least **+32 GB RAM** to operate efficiently. Running without sufficient memory will severely degrade node performance.
{% endhint %}

This enables blazing fast retrieval of Merkle proofs for any executed block.

### eth\_getStorageValues

`eth_getStorageValues` is an Erigon extension that retrieves multiple storage slots for a given account in a single call, reducing round-trips compared to multiple `eth_getStorageAt` calls.

**Parameters**

| Parameter    | Type             | Description                                          |
| ------------ | ---------------- | ---------------------------------------------------- |
| address      | STRING           | The account address to query storage for             |
| storageKeys  | ARRAY of STRING  | List of 32-byte storage slot keys (hex-encoded)      |
| blockNumber  | STRING or NUMBER | Block number or tag (`"latest"`, `"earliest"`, etc.) |

**Example**

{% code overflow="wrap" %}
```bash
curl --data '{"jsonrpc":"2.0","method":"eth_getStorageValues","params":["0xAddress","["0x0000000000000000000000000000000000000000000000000000000000000001"],"latest"],"id":1}' -H "Content-Type: application/json" -X POST http://localhost:8545
```
{% endcode %}

**Returns**

An object mapping each requested storage key to its 32-byte value.
