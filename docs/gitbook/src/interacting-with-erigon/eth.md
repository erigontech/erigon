---
description: Core Methods for Interacting with the Ethereum Blockchain
---

# eth

The `eth` namespace is the foundational and most commonly used API set in Ethereum's JSON-RPC interface. It provides core functionality for interacting with the Ethereum blockchain, enabling users and applications to read blockchain state and submit transactions.&#x20;

Key methods within this namespace allow you to check an account's balance (`eth_getBalance`), get the current block number (`eth_blockNumber`), retrieve transaction details (`eth_getTransactionByHash`), and send signed transactions to the network (`eth_sendRawTransaction`). Essentially, the `eth` namespace contains all the fundamental tools needed to observe and participate in the life of the chain.

For API usage refer to the below official resources:

{% embed url="https://ethereum.org/en/developers/docs/apis/json-rpc/" %}

{% embed url="https://ethereum.github.io/execution-apis/api-documentation/" %}
