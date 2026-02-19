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

To enable the `eth_getProof` JSON-RPC method, you must explicitly activate the storage of commitment history. Add the following option to your Erigon instance:

```
--prune.include-commitment-history=true
```

This will allows for blazing fast retrieval of Merkle proofs for executed blocks.
