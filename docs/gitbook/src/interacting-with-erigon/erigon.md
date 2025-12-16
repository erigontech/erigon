---
description: Erigon-Specific Methods for Optimized Data Access
---

# erigon

Erigon provides several specialized RPC namespaces that extend beyond the standard Ethereum JSON-RPC API. These namespaces offer optimized access to blockchain data and expose Erigon-specific functionality that takes advantage of the node's unique architecture and data structures. The primary Erigon-specific namespace is **`erigon_`** which offer extended blockchain data access methods.

These methods must be explicitly enabled using the `--http.api` flag when starting the RPC daemon.

### Namespace Availability

* The `erigon_` namespace is enabled by default in the RPC daemon and must be explicitly included in the `--http.api` flag if customizing enabled namespaces.

### Performance Considerations

* Erigon-specific methods are optimized for Erigon's architecture and often provide better performance than standard equivalents
* Methods like `erigon_getHeaderByNumber` and `erigon_getHeaderByHash` can be faster as they skip transaction and uncle data
* The `erigon_getLatestLogs` method includes advanced pagination to handle large result sets efficiently

### Enhanced Features

* `erigon_getLatestLogs` supports `ignoreTopicsOrder` for flexible topic matching
* `erigon_getLogs` returns enhanced ErigonLog objects with additional metadata like timestamps
* `erigon_getBlockByTimestamp` uses binary search for efficient timestamp-based block lookup

See more details [here](https://github.com/erigontech/erigon/blob/main/cmd/rpcdaemon/README.md#rpc-implementation-status) about implementation status.

***

## **erigon\_forks**

Returns the genesis block hash and a sorted list of all fork block numbers for the current chain configuration.

**Parameters**

None

**Example**

{% code overflow="wrap" %}
```bash
curl -s --data '{"jsonrpc":"2.0","method":"erigon_forks","params":[],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545
```
{% endcode %}

**Returns**

| Type        | Description                                                   |
| ----------- | ------------------------------------------------------------- |
| Object      | Contains genesis hash and fork information                    |
| genesis     | DATA, 32 BYTES - The genesis block hash                       |
| heightForks | ARRAY - Array of block numbers where height-based forks occur |
| timeForks   | ARRAY - Array of timestamps where time-based forks occur      |

***

## **erigon\_blockNumber**

Returns the latest executed block number. Unlike `eth_blockNumber`, this method can accept a specific block number parameter and returns the latest executed block rather than the fork choice head after the merge.

**Parameters**

| Parameter   | Type                | Description                                                             |
| ----------- | ------------------- | ----------------------------------------------------------------------- |
| blockNumber | QUANTITY (optional) | Block number to query. If omitted, returns latest executed block number |

**Example**

{% code overflow="wrap" %}
```bash
curl -s --data '{"jsonrpc":"2.0","method":"erigon_blockNumber","params":[],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545
```
{% endcode %}

**Returns**

| Type     | Description                     |
| -------- | ------------------------------- |
| QUANTITY | The block number as hexadecimal |

***

## **erigon\_getHeaderByNumber**

Returns a block header by block number, ignoring transaction and uncle data for potentially faster response times.

**Parameters**

| Parameter   | Type          | Description                                     |
| ----------- | ------------- | ----------------------------------------------- |
| blockNumber | QUANTITY\|TAG | Block number or "latest", "earliest", "pending" |

**Example**

{% code overflow="wrap" %}
```bash
curl -s --data '{"jsonrpc":"2.0","method":"erigon_getHeaderByNumber","params":["0x1b4"],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545
```
{% endcode %}

**Returns**

| Type   | Description                                |
| ------ | ------------------------------------------ |
| Object | Block header object with all header fields |

***

## **erigon\_getHeaderByHash**

Returns a block header by block hash, ignoring transaction and uncle data for potentially faster response times.

**Parameters**

| Parameter | Type           | Description       |
| --------- | -------------- | ----------------- |
| blockHash | DATA, 32 BYTES | Hash of the block |

**Example**

{% code overflow="wrap" %}
```bash
curl -s --data '{"jsonrpc":"2.0","method":"erigon_getHeaderByHash","params":["0x1d59ff54b1eb26b013ce3cb5fc9dab3705b415a67127a003c3e61eb445bb8df2"],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545
```
{% endcode %}

**Returns**

| Type   | Description                                |
| ------ | ------------------------------------------ |
| Object | Block header object with all header fields |

***

## **erigon\_getBlockByTimestamp**

Returns a block by timestamp using binary search to find the closest block to the specified timestamp.

**Parameters**

| Parameter | Type     | Description                                                                  |
| --------- | -------- | ---------------------------------------------------------------------------- |
| timestamp | QUANTITY | Unix timestamp                                                               |
| fullTx    | Boolean  | If true, include full transaction objects; if false, only transaction hashes |

**Example**

{% code overflow="wrap" %}
```bash
curl -s --data '{"jsonrpc":"2.0","method":"erigon_getBlockByTimestamp","params":["0x5ddf2094", false],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545
```
{% endcode %}

**Returns**

| Type   | Description                                    |
| ------ | ---------------------------------------------- |
| Object | Block object matching closest to the timestamp |

***

## **erigon\_getBalanceChangesInBlock**

Returns all account balance changes that occurred within a specific block.

**Parameters**

| Parameter     | Type                | Description                      |
| ------------- | ------------------- | -------------------------------- |
| blockNrOrHash | QUANTITY\|TAG\|HASH | Block number, tag, or block hash |

**Example**

{% code overflow="wrap" %}
```bash
curl -s --data '{"jsonrpc":"2.0","method":"erigon_getBalanceChangesInBlock","params":["0x1b4"],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545
```
{% endcode %}

**Returns**

| Type   | Description                                      |
| ------ | ------------------------------------------------ |
| Object | Mapping of addresses to their new balance values |

***

## **erigon\_getLogsByHash**

Returns an array of arrays of logs generated by transactions in a block given by block hash.

**Parameters**

| Parameter | Type           | Description       |
| --------- | -------------- | ----------------- |
| blockHash | DATA, 32 BYTES | Hash of the block |

**Example**

{% code overflow="wrap" %}
```bash
curl -s --data '{"jsonrpc":"2.0","method":"erigon_getLogsByHash","params":["0x1d59ff54b1eb26b013ce3cb5fc9dab3705b415a67127a003c3e61eb445bb8df2"],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545
```
{% endcode %}

**Returns**

| Type  | Description                                               |
| ----- | --------------------------------------------------------- |
| Array | Array of arrays of log objects, one array per transaction |

***

## **erigon\_getLogs**

Returns an array of logs matching a given filter object with enhanced filtering capabilities.

**Parameters**

| Parameter | Type   | Description                                                                  |
| --------- | ------ | ---------------------------------------------------------------------------- |
| filter    | Object | Filter criteria including fromBlock, toBlock, address, topics, and blockHash |

**Example**

{% code overflow="wrap" %}
```bash
curl -s --data '{"jsonrpc":"2.0","method":"erigon_getLogs","params":[{"fromBlock":"0x1","toBlock":"0x2","address":"0x8888f1f195afa192cfee860698584c030f4c9db1"}],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545
```
{% endcode %}

**Returns**

| Type  | Description                                       |
| ----- | ------------------------------------------------- |
| Array | Array of ErigonLog objects with enhanced metadata |

***

## **erigon\_getLatestLogs**

Returns the latest logs matching a filter criteria in descending order with advanced pagination and topic matching options.

**Parameters**

| Parameter  | Type   | Description                                                   |
| ---------- | ------ | ------------------------------------------------------------- |
| filter     | Object | Filter criteria object                                        |
| logOptions | Object | Options including logCount, blockCount, and ignoreTopicsOrder |

**Example**

{% code overflow="wrap" %}
```bash
curl -s --data '{"jsonrpc":"2.0","method":"erigon_getLatestLogs","params":[{"address":"0x8888f1f195afa192cfee860698584c030f4c9db1"},{"logCount":100,"ignoreTopicsOrder":true}],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545
```
{% endcode %}

**Returns**

| Type  | Description                                                  |
| ----- | ------------------------------------------------------------ |
| Array | Array of ErigonLog objects in descending chronological order |

***

## **erigon\_getBlockReceiptsByBlockHash**

Returns all transaction receipts for a canonical block by block hash.

**Parameters**

| Parameter | Type           | Description                 |
| --------- | -------------- | --------------------------- |
| blockHash | DATA, 32 BYTES | Hash of the canonical block |

**Example**

{% code overflow="wrap" %}
```bash
curl -s --data '{"jsonrpc":"2.0","method":"erigon_getBlockReceiptsByBlockHash","params":["0x1d59ff54b1eb26b013ce3cb5fc9dab3705b415a67127a003c3e61eb445bb8df2"],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545
```
{% endcode %}

**Returns**

| Type  | Description                                                |
| ----- | ---------------------------------------------------------- |
| Array | Array of receipt objects for all transactions in the block |

***

## **erigon\_nodeInfo**

Returns a collection of metadata known about the host node and connected peers.

**Parameters**

None

**Example**

{% code overflow="wrap" %}
```bash
curl -s --data '{"jsonrpc":"2.0","method":"erigon_nodeInfo","params":[],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545
```
{% endcode %}

**Returns**

| Type  | Description                                                 |
| ----- | ----------------------------------------------------------- |
| Array | Array of NodeInfo objects containing peer and node metadata |

***
