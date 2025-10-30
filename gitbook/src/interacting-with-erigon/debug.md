---
description: 'Erigon RPC debug Namespace: Deep Diagnostics and State Introspection'
---

# debug

The `debug` namespace provides debugging and diagnostic methods for Erigon node operators and developers. These methods offer deep introspection into blockchain state, transaction execution, and node performance. The debug namespace is implemented through the `PrivateDebugAPI` interface and `DebugAPIImpl` struct.

The debug namespace must be explicitly enabled using the `--http.api` flag when starting the RPC daemon. For security reasons, these methods are considered private and should not be exposed on public RPC endpoints.

### Security and Access Control

* Debug methods are considered private and should not be exposed on public RPC endpoints
* These methods can consume significant resources and should be used carefully in production environments
* Access should be restricted to trusted operators and developers only

### Performance Considerations

* Tracing methods (`debug_traceTransaction`, `debug_traceBlockByHash`, etc.) support streaming to handle large results efficiently
* The `AccountRangeMaxResults` constant limits account range queries to 8192 results, or 256 when storage is included
* Memory and GC control methods allow fine-tuning of node performance

### Integration with Erigon Architecture

* Debug methods leverage Erigon's temporal database for historical state access
* The implementation uses `kv.TemporalRoDB` for efficient historical queries
* Tracing functionality integrates with Erigon's execution engine and EVM implementation

### Usage in Development and Testing

* These methods are essential for debugging transaction execution issues
* Storage range methods help analyze contract state changes
* Memory management methods assist in performance optimization and resource monitoring

***

## **debug\_storageRangeAt**

Returns information about a range of storage locations for a given address at a specific block and transaction index.

**Parameters**

| Parameter       | Type           | Description                                          |
| --------------- | -------------- | ---------------------------------------------------- |
| blockHash       | DATA, 32 BYTES | Hash of block at which to retrieve data              |
| txIndex         | QUANTITY       | Transaction index in the given block                 |
| contractAddress | DATA, 20 BYTES | Contract address from which to retrieve storage data |
| keyStart        | DATA, 32 BYTES | Storage key to start retrieval from                  |
| maxResult       | QUANTITY       | The maximum number of storage entries to retrieve    |

**Example**

```bash
curl -s --data '{"jsonrpc":"2.0","method":"debug_storageRangeAt","params":["0xd3f1853788b02e31067f2c6e65cb0ae56729e23e3c92e2393af9396fa182701d",1,"0xb734c74ff4087493373a27834074f80acbd32827","0x00",2],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545
```

**Returns**

| Type   | Description                                                              |
| ------ | ------------------------------------------------------------------------ |
| Object | StorageRangeResult object containing storage key-value pairs and nextKey |

***

## **debug\_accountRange**

Returns a range of accounts involved in the given block range with optional filtering and data inclusion controls.

**Parameters**

| Parameter     | Type                | Description                                     |
| ------------- | ------------------- | ----------------------------------------------- |
| blockNrOrHash | QUANTITY\|TAG\|HASH | Block number, tag, or block hash                |
| start         | DATA                | Starting point for account iteration            |
| maxResults    | QUANTITY            | Maximum number of accounts to retrieve          |
| nocode        | Boolean             | If true, exclude contract bytecode from results |
| nostorage     | Boolean             | If true, exclude account storage from results   |
| nopreimages   | Boolean             | If true, exclude missing preimages from results |

**Example**

```bash
curl -s --data '{"jsonrpc":"2.0","method":"debug_accountRange","params":["latest","0x00",100,true,true,true],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545
```

**Returns**

| Type   | Description                                                            |
| ------ | ---------------------------------------------------------------------- |
| Object | IteratorDump object containing account information and iteration state |

***

## **debug\_getModifiedAccountsByNumber**

Returns a list of accounts modified in the given block range specified by block numbers.

**Parameters**

| Parameter | Type          | Description                                                 |
| --------- | ------------- | ----------------------------------------------------------- |
| startNum  | QUANTITY\|TAG | Starting block number or tag                                |
| endNum    | QUANTITY\|TAG | Ending block number or tag (optional, defaults to startNum) |

**Example**

```bash
curl -s --data '{"jsonrpc":"2.0","method":"debug_getModifiedAccountsByNumber","params":["0x1b4","0x1b5"],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545
```

**Returns**

| Type  | Description                                          |
| ----- | ---------------------------------------------------- |
| Array | Array of addresses modified in the given block range |

***

## **debug\_getModifiedAccountsByHash**

Returns a list of accounts modified in the given block range specified by block hashes.

**Parameters**

| Parameter | Type           | Description                                         |
| --------- | -------------- | --------------------------------------------------- |
| startHash | DATA, 32 BYTES | Starting block hash                                 |
| endHash   | DATA, 32 BYTES | Ending block hash (optional, defaults to startHash) |

**Example**

```bash
curl -s --data '{"jsonrpc":"2.0","method":"debug_getModifiedAccountsByHash","params":["0x2a1af018e33bcbd5015c96a356117a5251fcccf94a9c7c8f0148e25fdee37aec","0x4e3d3e7eee350df0ee6e94a44471ee2d22cfb174db89bbf8e6c5f6aef7b360c5"],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545
```

**Returns**

| Type  | Description                                          |
| ----- | ---------------------------------------------------- |
| Array | Array of addresses modified in the given block range |

***

## **debug\_traceTransaction**

Returns Geth-style transaction traces for detailed execution analysis of a specific transaction.

**Parameters**

| Parameter | Type           | Description                             |
| --------- | -------------- | --------------------------------------- |
| hash      | DATA, 32 BYTES | Hash of transaction to trace            |
| config    | Object         | (optional) Tracer configuration options |

**Example**

```bash
curl -s --data '{"jsonrpc":"2.0","method":"debug_traceTransaction","params":["0x893c428fed019404f704cf4d9be977ed9ca01050ed93dccdd6c169422155586f"],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545
```

**Returns**

| Type   | Description                                     |
| ------ | ----------------------------------------------- |
| Object | Stack trace array with detailed execution steps |

***

## **debug\_traceBlockByHash**

Returns transaction traces for all transactions in a block specified by block hash.

**Parameters**

| Parameter | Type           | Description                             |
| --------- | -------------- | --------------------------------------- |
| hash      | DATA, 32 BYTES | Hash of the block to trace              |
| config    | Object         | (optional) Tracer configuration options |

**Example**

```bash
curl -s --data '{"jsonrpc":"2.0","method":"debug_traceBlockByHash","params":["0x1d59ff54b1eb26b013ce3cb5fc9dab3705b415a67127a003c3e61eb445bb8df2"],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545
```

**Returns**

| Type  | Description                                                          |
| ----- | -------------------------------------------------------------------- |
| Array | Array of transaction trace objects for all transactions in the block |

***

## **debug\_traceBlockByNumber**

Returns transaction traces for all transactions in a block specified by block number.

**Parameters**

| Parameter | Type          | Description                                     |
| --------- | ------------- | ----------------------------------------------- |
| number    | QUANTITY\|TAG | Block number or "latest", "earliest", "pending" |
| config    | Object        | (optional) Tracer configuration options         |

**Example**

```bash
curl -s --data '{"jsonrpc":"2.0","method":"debug_traceBlockByNumber","params":["0x1b4"],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545
```

**Returns**

| Type  | Description                                                          |
| ----- | -------------------------------------------------------------------- |
| Array | Array of transaction trace objects for all transactions in the block |

***

## **debug\_traceCall**

Executes a call and returns detailed execution traces without modifying the blockchain state.

**Parameters**

| Parameter     | Type                | Description                                      |
| ------------- | ------------------- | ------------------------------------------------ |
| call          | Object              | Call arguments (similar to eth\_call)            |
| blockNrOrHash | QUANTITY\|TAG\|HASH | Block number, tag, or hash for execution context |
| config        | Object              | (optional) Tracer configuration options          |

**Example**

```bash
curl -s --data '{"jsonrpc":"2.0","method":"debug_traceCall","params":[{"to":"0xd46e8dd67c5d32be8058bb8eb970870f07244567","data":"0x"},"latest"],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545
```

**Returns**

| Type   | Description                                           |
| ------ | ----------------------------------------------------- |
| Object | Execution trace object with detailed call information |

***

## **debug\_getRawReceipts**

Returns the raw receipt data for all transactions in a block.

**Parameters**

| Parameter     | Type                | Description                      |
| ------------- | ------------------- | -------------------------------- |
| blockNrOrHash | QUANTITY\|TAG\|HASH | Block number, tag, or block hash |

**Example**

```bash
curl -s --data '{"jsonrpc":"2.0","method":"debug_getRawReceipts","params":["0x1b4"],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545
```

**Returns**

| Type  | Description                                    |
| ----- | ---------------------------------------------- |
| Array | Array of raw receipt data as hexadecimal bytes |

***

## **debug\_memStats**

Returns detailed runtime memory statistics for the Erigon process.

**Parameters**

None

**Example**

```bash
curl -s --data '{"jsonrpc":"2.0","method":"debug_memStats","params":[],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545
```

**Returns**

| Type   | Description                                                             |
| ------ | ----------------------------------------------------------------------- |
| Object | Runtime memory statistics object with detailed memory usage information |

***

## **debug\_gcStats**

Returns garbage collection statistics for the Erigon process.

**Parameters**

None

**Example**

```bash
curl -s --data '{"jsonrpc":"2.0","method":"debug_gcStats","params":[],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545
```

**Returns**

| Type   | Description                          |
| ------ | ------------------------------------ |
| Object | Garbage collection statistics object |

***

## **debug\_freeOSMemory**

Forces a garbage collection to free OS memory.

**Parameters**

None

**Example**

```bash
curl -s --data '{"jsonrpc":"2.0","method":"debug_freeOSMemory","params":[],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545
```

**Returns**

| Type | Description     |
| ---- | --------------- |
| null | No return value |

***

## **debug\_setGCPercent**

Sets the garbage collection target percentage and returns the previous setting.

**Parameters**

| Parameter | Type     | Description                                       |
| --------- | -------- | ------------------------------------------------- |
| percent   | QUANTITY | GC target percentage (negative value disables GC) |

**Example**

```bash
curl -s --data '{"jsonrpc":"2.0","method":"debug_setGCPercent","params":[100],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545
```

**Returns**

| Type     | Description                        |
| -------- | ---------------------------------- |
| QUANTITY | The previous GC percentage setting |

***

## **debug\_setMemoryLimit**

Sets the GOMEMLIMIT for the process and returns the previous limit.

**Parameters**

| Parameter | Type     | Description           |
| --------- | -------- | --------------------- |
| limit     | QUANTITY | Memory limit in bytes |

**Example**

```bash
curl -s --data '{"jsonrpc":"2.0","method":"debug_setMemoryLimit","params":[1073741824],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545
```

**Returns**

| Type     | Description                       |
| -------- | --------------------------------- |
| QUANTITY | The previous memory limit setting |

***
