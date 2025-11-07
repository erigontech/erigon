---
description: Inspecting Unconfirmed Transactions in Erigon
---

# txpool

The `txpool` namespace provides methods for inspecting and managing the transaction pool (mempool) in Erigon. These methods allow you to view pending, queued, and base fee transactions, providing insight into the current state of unconfirmed transactions. In Erigon, the `txpool` namespace is implemented through the `TxPoolAPI` interface and `TxPoolAPIImpl` struct.

The txpool namespace must be explicitly enabled using the `--http.api` flag when starting the RPC daemon. These methods are particularly useful for monitoring transaction pool status and debugging transaction submission issues.

### Transaction Pool Architecture

* Erigon's transaction pool is organized into three sub-pools: pending (executable), baseFee (insufficient base fee), and queued (nonce gaps)
* The pool can run either integrated within the main Erigon process or as a separate service for scalability
* Transaction pool methods communicate via gRPC with the txpool service when running in external mode

### Implementation Details

* The `TxPoolAPIImpl` uses a `proto_txpool.TxpoolClient` to communicate with the transaction pool service
* All transactions are decoded from RLP format and converted to `ethapi.RPCTransaction` objects for JSON-RPC responses
* The implementation handles transaction categorization based on the `TxnType` field from the pool service

### External vs Internal Mode

* **Internal Mode**: Transaction pool runs within the main Erigon process (default configuration)
* **External Mode**: Transaction pool runs as a separate service, requiring explicit configuration with `--txpool.api.addr`. External mode requires an external sentry service and provides better resource isolation

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

Returns the content of the transaction pool, organized by sender address and categorized into pending, queued, and baseFee sub-pools.

**Parameters**

None

**Example**

{% code overflow="wrap" %}
```bash
curl -s --data '{"jsonrpc":"2.0","method":"txpool_content","params":[],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545
```
{% endcode %}

**Returns**

| Type    | Description                                         |
| ------- | --------------------------------------------------- |
| Object  | Transaction pool content organized by sub-pool type |
| pending | Object                                              |
| baseFee | Object                                              |
| queued  | Object                                              |

***

## **txpool\_contentFrom**

Returns the content of the transaction pool for a specific sender address, showing all transactions from that address across all sub-pools.

**Parameters**

| Parameter | Type           | Description                                  |
| --------- | -------------- | -------------------------------------------- |
| address   | DATA, 20 BYTES | The sender address to query transactions for |

**Example**

{% code overflow="wrap" %}
```bash
curl -s --data '{"jsonrpc":"2.0","method":"txpool_contentFrom","params":["0xb60e8dd61c5d32be8058bb8eb970870f07233155"],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545
```
{% endcode %}

**Returns**

| Type    | Description                                        |
| ------- | -------------------------------------------------- |
| Object  | Transaction pool content for the specified address |
| pending | Object                                             |
| baseFee | Object                                             |
| queued  | Object                                             |

***

## **txpool\_status**

Returns the current status of the transaction pool, including the count of transactions in each sub-pool.

**Parameters**

None

**Example**

{% code overflow="wrap" %}
```bash
curl -s --data '{"jsonrpc":"2.0","method":"txpool_status","params":[],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545
```
{% endcode %}

**Returns**

| Type    | Description                    |
| ------- | ------------------------------ |
| Object  | Transaction pool status counts |
| pending | QUANTITY                       |
| baseFee | QUANTITY                       |
| queued  | QUANTITY                       |
