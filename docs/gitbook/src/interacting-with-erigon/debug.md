# debug

The `debug` namespace provides debugging and diagnostic methods for Erigon node operators and developers. These methods offer deep introspection into blockchain state, transaction execution, and node performance.

The debug namespace must be explicitly enabled using the `--http.api=debug` flag when starting the RPC daemon.

{% hint style="warning" %}
The `debug` namespace is intended for debugging and development purposes, not for production use.
{% endhint %}

#### Security and Access Control

* Debug methods are considered private and should not be exposed on public RPC endpoints;
* These methods can consume significant resources and should be used carefully in production environments;
* Access should be restricted to trusted operators and developers only.

#### Performance Considerations

* Tracing methods (`debug_traceBlockByHash`, `debug_traceBlockByNumber`, `debug_traceTransaction`, `debug_traceCall`, `debug_traceCallMany`) support streaming for large results to reduce memory usage
* The `AccountRangeMaxResults` constant limits account range queries to 8192 results, or 256 when storage is included;
* Memory and GC control methods allow fine-tuning of node performance.
* Some methods like `debug_accountRange` have compatibility layers for both Geth and legacy Erigon parameter formats `debug_api`

#### Integration with Erigon Architecture

* Debug methods leverage Erigon's temporal database for historical state access;
* The implementation uses `kv.TemporalRoDB` for efficient historical queries;
* Tracing functionality integrates with Erigon's execution engine and EVM implementation.

#### Usage in Development and Testing

* These methods are essential for debugging transaction execution issues;
* Storage range methods help analyze contract state changes;
* Memory management methods assist in performance optimization and resource monitoring.

***

## JSON-RPC Specification

### debug\_getRawReceipts

Returns an array of EIP-2718 binary-encoded receipts from a single block.

#### Parameters

| Parameter     | Type                    | Description                                                        |
| ------------- | ----------------------- | ------------------------------------------------------------------ |
| blockNrOrHash | QUANTITY \| TAG \| DATA | Block number, tag ("earliest", "latest", "pending"), or block hash |

#### Example

{% code overflow="wrap" %}

```bash
curl -s --data '{"jsonrpc":"2.0","method":"debug_getRawReceipts","params":["0x123456"],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545
```

{% endcode %}

#### Returns

| Type          | Description                                  |
| ------------- | -------------------------------------------- |
| Array of DATA | Array of binary-encoded transaction receipts |

### debug\_accountRange

Returns a range of accounts involved in the given block range.

#### Parameters

| Parameter      | Type            | Description                                                 |
| -------------- | --------------- | ----------------------------------------------------------- |
| blockNrOrHash  | QUANTITY \| TAG | Block number or tag                                         |
| start          | DATAARRAY       | Array of prefixes to match account addresses                |
| maxResults     | QUANTITY        | Maximum number of accounts to retrieve                      |
| excludeCode    | BOOLEAN         | If true, exclude byte code from results                     |
| excludeStorage | BOOLEAN         | If true, exclude storage from results                       |
| incompletes    | BOOLEAN         | If true, return missing preimages (not supported when true) |

#### Example

{% code overflow="wrap" %}

```bash
curl -s --data '{"jsonrpc":"2.0","method":"debug_accountRange","params":["0xaaaaa",[1],1,true,true,true],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545
```

{% endcode %}

#### Returns

| Type   | Description                                        |
| ------ | -------------------------------------------------- |
| Object | IteratorDump object containing account information |

### debug\_accountAt

Returns account information at a specific block and transaction index.

#### Parameters

| Parameter | Type           | Description                        |
| --------- | -------------- | ---------------------------------- |
| blockHash | DATA, 32 Bytes | Hash of the block                  |
| txIndex   | QUANTITY       | Transaction index within the block |
| address   | DATA, 20 Bytes | Account address                    |

#### Example

{% code overflow="wrap" %}

```bash
curl -s --data '{"jsonrpc":"2.0","method":"debug_accountAt","params":["0x123456...",1,"0x123456..."],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545
```

{% endcode %}

#### Returns

| Type   | Description                                           |
| ------ | ----------------------------------------------------- |
| Object | AccountResult with balance, nonce, code, and codeHash |

### debug\_getModifiedAccountsByNumber

Returns a list of accounts modified in the given block range by number.

#### Parameters

| Parameter   | Type            | Description                        |
| ----------- | --------------- | ---------------------------------- |
| startNumber | QUANTITY \| TAG | Start block number or tag          |
| endNumber   | QUANTITY \| TAG | End block number or tag (optional) |

#### Example

{% code overflow="wrap" %}

```bash
curl -s --data '{"jsonrpc":"2.0","method":"debug_getModifiedAccountsByNumber","params":["0xccccd","0xcccce"],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545
```

{% endcode %}

#### Returns

| Type                    | Description                         |
| ----------------------- | ----------------------------------- |
| Array of DATA, 20 Bytes | Array of modified account addresses |

### debug\_getModifiedAccountsByHash

Returns a list of accounts modified in the given block range by hash.

#### Parameters

| Parameter | Type           | Description                      |
| --------- | -------------- | -------------------------------- |
| startHash | DATA, 32 Bytes | Hash of the start block          |
| endHash   | DATA, 32 Bytes | Hash of the end block (optional) |

#### Example

{% code overflow="wrap" %}

```bash
curl -s --data '{"jsonrpc":"2.0","method":"debug_getModifiedAccountsByHash","params":["0x2a1af0...","0x4e3d3e..."],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545
```

{% endcode %}

#### Returns

| Type                    | Description                         |
| ----------------------- | ----------------------------------- |
| Array of DATA, 20 Bytes | Array of modified account addresses |

### debug\_storageRangeAt

Returns information about a range of storage locations for a contract address.

#### Parameters

| Parameter       | Type              | Description                             |
| --------------- | ----------------- | --------------------------------------- |
| blockHash       | DATA, 32 Bytes    | Hash of block at which to retrieve data |
| txIndex         | QUANTITY, 8 Bytes | Transaction index in the block          |
| contractAddress | DATA, 20 Bytes    | Contract address                        |
| keyStart        | DATA, 32 Bytes    | Storage key to start from               |
| maxResult       | QUANTITY, 8 Bytes | Maximum number of values to retrieve    |

#### Example

{% code overflow="wrap" %}

```bash
curl -s --data '{"jsonrpc":"2.0","method":"debug_storageRangeAt","params":["0xd3f185...",1,"0xb734c7...","0x00",2],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545
```

{% endcode %}

#### Returns

| Type   | Description                                         |
| ------ | --------------------------------------------------- |
| Object | StorageRangeResult with key/value pairs and nextKey |

### debug\_traceBlockByHash

Returns Geth style transaction traces for a block by hash.

#### Parameters

| Parameter | Type              | Description                 |
| --------- | ----------------- | --------------------------- |
| hash      | DATA, 32 Bytes    | Hash of block to trace      |
| config    | Object (optional) | Trace configuration options |

#### Example

{% code overflow="wrap" %}

```bash
curl -s --data '{"jsonrpc":"2.0","method":"debug_traceBlockByHash","params":["0x123456...",{}],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545
```

{% endcode %}

#### Returns

| Type  | Description                        |
| ----- | ---------------------------------- |
| Array | Array of transaction trace objects |

### debug\_traceBlockByNumber

Returns Geth style transaction traces for a block by number.

#### Parameters

| Parameter   | Type              | Description                 |
| ----------- | ----------------- | --------------------------- |
| blockNumber | QUANTITY \| TAG   | Block number or tag         |
| config      | Object (optional) | Trace configuration options |

#### Example

{% code overflow="wrap" %}

```bash
curl -s --data '{"jsonrpc":"2.0","method":"debug_traceBlockByNumber","params":["0x123456",{}],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545
```

{% endcode %}

#### Returns

| Type  | Description                        |
| ----- | ---------------------------------- |
| Array | Array of transaction trace objects |

### debug\_traceTransaction

Returns Geth style transaction trace.

#### Parameters

| Parameter | Type              | Description                  |
| --------- | ----------------- | ---------------------------- |
| hash      | DATA, 32 Bytes    | Hash of transaction to trace |
| config    | Object (optional) | Trace configuration options  |

#### Example

{% code overflow="wrap" %}

```bash
curl -s --data '{"jsonrpc":"2.0","method":"debug_traceTransaction","params":["0x123456...",{}],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545
```

{% endcode %}

#### Returns

| Type   | Description              |
| ------ | ------------------------ |
| Object | Transaction trace object |

### debug\_traceCall

Returns Geth style call trace.

#### Parameters

| Parameter     | Type                    | Description                                           |
| ------------- | ----------------------- | ----------------------------------------------------- |
| args          | Object                  | Call arguments (to, from, gas, gasPrice, value, data) |
| blockNrOrHash | QUANTITY \| TAG \| DATA | Block number, tag, or hash                            |
| config        | Object (optional)       | Trace configuration options                           |

#### Example

{% code overflow="wrap" %}

```bash
curl -s --data '{"jsonrpc":"2.0","method":"debug_traceCall","params":[{"to":"0x123456..."},"latest",{}],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545
```

{% endcode %}

#### Returns

| Type   | Description       |
| ------ | ----------------- |
| Object | Call trace object |

### debug\_traceCallMany

Returns Geth style traces for multiple call bundles.

#### Parameters

| Parameter       | Type              | Description                                        |
| --------------- | ----------------- | -------------------------------------------------- |
| bundles         | Array             | Array of transaction bundles to trace              |
| simulateContext | Object            | Simulation context (blockNumber, transactionIndex) |
| config          | Object (optional) | Trace configuration options                        |

#### Example

{% code overflow="wrap" %}

```bash
curl -s --data '{"jsonrpc":"2.0","method":"debug_traceCallMany","params":[[{"transactions":[...]}],{"blockNumber":"latest"},{}],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545
```

{% endcode %}

#### Returns

| Type  | Description                            |
| ----- | -------------------------------------- |
| Array | Array of trace results for each bundle |

### debug\_setMemoryLimit

Sets the GOMEMLIMIT for the process.

#### Parameters

| Parameter | Type     | Description           |
| --------- | -------- | --------------------- |
| limit     | QUANTITY | Memory limit in bytes |

#### Example

{% code overflow="wrap" %}

```bash
curl -s --data '{"jsonrpc":"2.0","method":"debug_setMemoryLimit","params":[8589934592],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545
```

{% endcode %}

#### Returns

| Type     | Description           |
| -------- | --------------------- |
| QUANTITY | Previous memory limit |

### debug\_setGCPercent

Sets the garbage collection target percentage.

#### Parameters

| Parameter | Type     | Description                                |
| --------- | -------- | ------------------------------------------ |
| v         | QUANTITY | GC percentage (negative value disables GC) |

#### Example

{% code overflow="wrap" %}

```bash
curl -s --data '{"jsonrpc":"2.0","method":"debug_setGCPercent","params":[100],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545
```

{% endcode %}

#### Returns

| Type     | Description                    |
| -------- | ------------------------------ |
| QUANTITY | Previous GC percentage setting |

### debug\_freeOSMemory

Forces a garbage collection to free OS memory.

#### **Parameters**

None

#### Example

{% code overflow="wrap" %}

```bash
curl -s --data '{"jsonrpc":"2.0","method":"debug_freeOSMemory","params":[],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545
```

{% endcode %}

#### Returns

| Type | Description     |
| ---- | --------------- |
| null | No return value |

### debug\_gcStats

Returns garbage collection statistics.

#### **Parameters**

None

#### Example

{% code overflow="wrap" %}

```bash
curl -s --data '{"jsonrpc":"2.0","method":"debug_gcStats","params":[],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545
```

{% endcode %}

#### Returns

| Type   | Description          |
| ------ | -------------------- |
| Object | GC statistics object |

### debug\_memStats

Returns detailed runtime memory statistics.

#### **Parameters**

None

#### Example

{% code overflow="wrap" %}

```bash
curl -s --data '{"jsonrpc":"2.0","method":"debug_memStats","params":[],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545
```

{% endcode %}

#### Returns

| Type   | Description                      |
| ------ | -------------------------------- |
| Object | Runtime memory statistics object |