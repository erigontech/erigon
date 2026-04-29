---
sidebar_position: 1
---

# eth

The `eth` namespace is the foundational and most commonly used API set in Ethereum's JSON-RPC interface. It provides core functionality for interacting with the Ethereum blockchain, enabling users and applications to read blockchain state and submit transactions. Key methods within this namespace allow you to check an account's balance (`eth_getBalance`), get the current block number (`eth_blockNumber`), retrieve transaction details (`eth_getTransactionByHash`), and send signed transactions to the network (`eth_sendRawTransaction`). Essentially, the `eth` namespace contains all the fundamental tools needed to observe and participate in the life of the chain.

## API Documentation

For comprehensive API details, refer to two official sources: the general Ethereum JSON-RPC documentation on ethereum.org (covering `eth`, `net`, and `web3` namespaces):

[https://ethereum.org/developers/docs/apis/json-rpc/](https://ethereum.org/developers/docs/apis/json-rpc/)

and the formal Execution APIs documentation (for detailed specifications of `debug`, `engine`, and advanced `eth` methods like `eth_getProof`).

[https://ethereum.github.io/execution-apis/api-documentation/](https://ethereum.github.io/execution-apis/api-documentation/)
