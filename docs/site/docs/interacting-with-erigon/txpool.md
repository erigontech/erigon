---
title: "txpool"
description: "txpool_ namespace: inspect pending and queued transactions in the mempool."
sidebar_position: 8
---

# txpool

The `txpool` namespace provides methods for inspecting and managing the transaction pool (mempool) in Erigon. These methods allow you to view pending and queued transactions, providing insight into the current state of unconfirmed transactions. In Erigon, the `txpool` namespace is implemented through the `TxPoolAPI` interface and `TxPoolAPIImpl` struct.

The TxPool namespace must be explicitly enabled using the `--http.api` flag when starting the RPC Daemon. These methods are particularly useful for monitoring transaction pool status and debugging transaction submission issues.

### Transaction Pool Architecture

* Erigon's transaction pool is organized into three sub-pools: pending (executable), baseFee (insufficient base fee), and queued (nonce gaps)
* The `txpool_content`, `txpool_contentFrom`, `txpool_inspect` and `txpool_status` methods expose only the `pending` and `queued` categories, as defined by the `ethereum/execution-apis` specification. Transactions in the internal baseFee sub-pool are nonce-ready and otherwise valid, but are not executable at the current base fee: they wait for the fee condition to change. They are reported as pending, matching Geth
* Blob transactions (type 3) are reported by `txpool_content`, `txpool_contentFrom` and `txpool_inspect`, as the `ethereum/execution-apis` specification defines and as Reth does. Geth and Nethermind leave them out
* The pool can run either integrated within the main Erigon process or as a separate service for scalability
* Transaction pool methods communicate via gRPC with the TxPool service when running in external mode

### Implementation Details

* The `TxPoolAPIImpl` uses a `proto_txpool.TxpoolClient` to communicate with the transaction pool service
* All transactions are decoded from Recursive Length Prefix (RLP) format and converted to `ethapi.RPCTransaction` objects for JSON-RPC responses
* The implementation handles transaction categorization based on the `TxnType` field from the pool service

### External vs Internal Mode

* **Internal Mode**: The transaction pool runs within the main Erigon process (default configuration)
* **External Mode**: The transaction pool runs as a separate service, requiring explicit configuration with `--txpool.api.addr`. External mode requires an external Sentry service and provides better resource isolation

### Usage in Development and Testing

* These methods are commonly used for monitoring transaction submission and pool state
* The `txpool_status` method provides quick insight into pool health and congestion
* Transaction pool content methods help debug why transactions may not be getting mined

### Configuration and Limits

* Transaction pool limits can be configured via flags like `--txpool.globalslots`, `--txpool.globalbasefeeslots`, and `--txpool.globalqueue`
* The pool supports various transaction types including blob transactions with separate limits
* Pool configuration is managed through the `txpoolcfg.Config` structure

### Availability

* The `txpool` namespace is available when included in the `--http.api` flag
* Methods are marked as "remote" in the documentation, indicating they communicate with external services
* All `txpool` methods are available on both HTTP and WebSocket connections

***

## **txpool\_content**

Returns the content of the transaction pool, organized by sender address and categorized into pending and queued transactions.

**Parameters**

None

**Example**

```bash
curl -s --data '{"jsonrpc":"2.0","method":"txpool_content","params":[],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545
```


**Returns**

| Type    | Description                                         |
| ------- | --------------------------------------------------- |
| Object  | Transaction pool content organized by sub-pool type |
| pending | Object                                              |
| queued  | Object                                              |

***

## **txpool\_contentFrom**

Returns the content of the transaction pool for a specific sender address, categorized into pending and queued transactions. The same rules as `txpool_content` apply.

**Parameters**

| Parameter | Type           | Description                                  |
| --------- | -------------- | -------------------------------------------- |
| address   | DATA, 20 BYTES | The sender address to query transactions for |

**Example**

```bash
curl -s --data '{"jsonrpc":"2.0","method":"txpool_contentFrom","params":["0xb60e8dd61c5d32be8058bb8eb970870f07233155"],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545
```


**Returns**

| Type    | Description                                        |
| ------- | -------------------------------------------------- |
| Object  | Transaction pool content for the specified address |
| pending | Object                                             |
| queued  | Object                                             |

***

## **txpool\_inspect**

Returns a text summary of every transaction in the pool, organized like `txpool_content` by sub-pool, sender address and nonce, for quick inspection without the full transaction objects.

Each entry has the form `<to>: <value> wei + <gas limit> gas × <gas price> wei`, or `contract creation: ...` when the transaction has no recipient. For EIP-1559 and later transaction types the price shown is the fee cap (`maxFeePerGas`), as Geth reports it.

**Parameters**

None

**Example**

```bash
curl -s --data '{"jsonrpc":"2.0","method":"txpool_inspect","params":[],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545
```

**Returns**

| Type    | Description                                                        |
| ------- | ------------------------------------------------------------------ |
| Object  | Transaction summaries organized by sub-pool, sender and nonce      |
| pending | Object: sender address → nonce → summary string                    |
| queued  | Object: sender address → nonce → summary string                    |

***

## **txpool\_status**

Returns the current status of the transaction pool, with the count of pending and queued transactions.

**Parameters**

None

**Example**

```bash
curl -s --data '{"jsonrpc":"2.0","method":"txpool_status","params":[],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545
```


**Returns**

| Type    | Description                    |
| ------- | ------------------------------ |
| Object  | Transaction pool status counts |
| pending | QUANTITY                       |
| queued  | QUANTITY                       |
