---
title: "trace"
description: "trace_ namespace: OpenEthereum-compatible transaction and block traces."
sidebar_position: 7
---


# trace

The `trace` module is for getting a deeper insight into transaction processing. It includes two sets of calls the transaction trace filtering API and the ad-hoc tracing API.

In order to use the Transaction-Trace Filtering API, Erigon must be fully synced with the argument `trace` in `http.api` flag.

```bash
./build/bin/erigon --http.api: eth,erigon,trace
```

As for the Ad-hoc Tracing API, as long the blocks have not yet been pruned, the RPC calls will work.

## The Ad-hoc Tracing API

The ad-hoc tracing API allows you to perform diagnostics on calls or transactions—whether they are historical ones from the chain or hypothetical ones not yet mined. As long as the blocks have not yet been pruned, the RPC calls will work.

The diagnostics are requested by providing a configuration object that specifies the tracer type to be executed, allowing for deep insight into EVM execution:

* `trace`: Provides a Transaction Trace, showing the sequence of all external calls, internal messages, and value transfers that occurred during execution.
* `vmTrace`: Provides a Virtual Machine Execution Trace, delivering a full step-by-step trace of the EVM's state throughout the transaction, including all opcodes and gas usage.
* `stateDiff`: Provides a State Difference, detailing all altered portions of the Ethereum state (e.g., storage, balances, nonces) resulting from the transaction's execution.

#### Flat Tracers

As of v3.4, Erigon supports **flat tracers**, which produce OpenEthereum-compatible call traces in a flat (non-nested) list format. Each call frame is emitted as a separate object with an explicit `subtraces` count, matching the standard output format of `trace_transaction` and `trace_block`. Flat tracers are available for all ad-hoc tracing methods by including `"trace"` in the trace type array.

#### Providing a Transaction to Trace

There are three primary ways to specify the transaction to be traced:

1. Hypothetical Call: Providing the transaction information (like sender, recipient, and data) as if making a call using `eth_call` (see `trace_call`).
2. Raw Transaction: Providing raw, signed transaction data, as when using `eth_sendRawTransaction` (see `trace_rawTransaction`).
3. Mined Transaction: Providing a transaction hash for a previously mined transaction (see `trace_replayTransaction`).

:::info
**Note**: For replaying mined transactions, your node must be in archive mode, or the transaction must be within the most recent 1000 blocks.
:::

## The Transaction-Trace Filtering API

These APIs allow you to get a full _externality_ trace on any transaction executed throughout the Erigon chain. Unlike the log filtering API, you are able to search and filter based only upon address information. Information returned includes the execution of all `CREATE`s, `SUICIDE`s and all variants of `CALL` together with input data, output data, gas usage, amount transferred and the success status of each individual action.

### `traceAddress` field

The `traceAddress` field of all returned traces, gives the exact location in the call trace \[index in root, index in first `CALL`, index in second `CALL`, ...].

i.e. if the trace is:

```
A
  CALLs B
    CALLs G
  CALLs C
    CALLs G
```

then it should look something like:

`[ {A: []}, {B: [0]}, {G: [0, 0]}, {C: [1]}, {G: [1, 0]} ]`

## JSON-RPC methods

#### Ad-hoc Tracing

* [trace\_call](#trace_call)
* [trace\_callMany](#trace_callmany)
* [trace\_rawTransaction](#trace_rawtransaction)
* [trace\_replayBlockTransactions](#trace_replayblocktransactions)
* [trace\_replayTransaction](#trace_replaytransaction)

#### Transaction-Trace Filtering

* [trace\_block](#trace_block)
* [trace\_filter](#trace_filter)
* [trace\_get](#trace_get)
* [trace\_transaction](#trace_transaction)

## Response Fields Reference

All `trace_*` methods return objects built from the same set of fields. Each method's "Returns" section below indicates which top-level shape it produces; the field semantics are defined once here.

### Top-level shapes

| Method | Top-level shape |
| --- | --- |
| `trace_call`, `trace_rawTransaction`, `trace_replayTransaction` | `Object` with `output`, `stateDiff`, `trace[]`, `vmTrace` |
| `trace_callMany` | `Array<Object>` — one per input call |
| `trace_replayBlockTransactions` | `Array<Object>` — one per transaction; each entry adds `transactionHash` |
| `trace_block`, `trace_filter`, `trace_transaction` | `Array<TraceEntry>` (flat list across all call frames) |
| `trace_get` | `TraceEntry` (single call frame at the requested position) |

### Top-level (ad-hoc tracing) fields

| Field | Type | Description |
| --- | --- | --- |
| `output` | DATA | Return data of the top-level call (`0x` if no data was returned). |
| `stateDiff` | Object \| null | Set when `"stateDiff"` is requested in the trace types array. Maps each touched account address to an object describing changes to `balance`, `nonce`, `code`, and per-key `storage` entries. `null` if not requested. |
| `trace` | Array of TraceEntry | Set when `"trace"` is requested. Flat list of call frames executed during the transaction. See **TraceEntry fields** below. |
| `vmTrace` | Object \| null | Set when `"vmTrace"` is requested. Step-by-step EVM trace including `code`, per-step `ops` (with `pc`, `cost`, `ex` execution result, and `sub` for nested calls). `null` if not requested. |
| `transactionHash` | DATA, 32 BYTES | (Only in `trace_replayBlockTransactions` entries) Hash of the transaction this trace belongs to. |

### TraceEntry fields

A `TraceEntry` represents a single call frame (root call, internal call, contract creation, or self-destruct).

| Field | Type | Description |
| --- | --- | --- |
| `action` | Object | The action that initiated this call frame. Shape depends on `type` (see **Action variants**). |
| `result` | Object \| null | The outcome of the action. `null` if the call frame errored. See **Result variants**. |
| `error` | String | (Optional) Present when the call frame errored. `"Reverted"` (title-cased) is the only special-cased value; all other errors are the verbatim Go error string, e.g. `"out of gas"`, `"invalid opcode: ..."`. For `"Reverted"`, `result` is still populated with `gasUsed` and `output` (or `code`/`address` for a `create` frame); for other errors, `result` is `null`. |
| `subtraces` | QUANTITY | Number of direct child call frames produced by this frame. Used together with `traceAddress` to reconstruct the call tree from a flat list. |
| `traceAddress` | Array of QUANTITY | Path to this frame inside the call tree. Empty array `[]` for the root call; `[0]` is the first child of the root; `[1, 0]` is the first child of the second child of the root, etc. |
| `type` | String | One of `"call"`, `"create"`, `"suicide"` (self-destruct), `"reward"` (block/uncle reward — appears in `trace_block` and in `trace_filter` results when the filter matches block coinbases or uncle authors). |
| `blockHash` | DATA, 32 BYTES | (Only in block-level methods: `trace_block`, `trace_filter`, `trace_transaction`, `trace_get`) Hash of the block containing the transaction. |
| `blockNumber` | QUANTITY | (Block-level methods only) Number of the block containing the transaction. |
| `transactionHash` | DATA, 32 BYTES | (Block-level methods only) Hash of the transaction containing this call frame. Absent for `"reward"` entries. |
| `transactionPosition` | QUANTITY | (Block-level methods only) Index of the transaction within the block. Absent for `"reward"` entries. |

### Action variants

The `action` object's shape depends on `type`:

**`type: "call"`**

| Field | Type | Description |
| --- | --- | --- |
| `callType` | String | `"call"`, `"callcode"`, `"delegatecall"`, or `"staticcall"`. |
| `from` | DATA, 20 BYTES | Sender address. |
| `to` | DATA, 20 BYTES | Recipient address. |
| `value` | QUANTITY | Wei value transferred. |
| `gas` | QUANTITY | Gas provided to this call frame. |
| `input` | DATA | Call data (selector + arguments). |

**`type: "create"`**

| Field | Type | Description |
| --- | --- | --- |
| `from` | DATA, 20 BYTES | Address that initiated the creation. |
| `creationMethod` | String | How the contract was created: `"create"` or `"create2"`. |
| `value` | QUANTITY | Wei value sent to the new contract. |
| `gas` | QUANTITY | Gas provided to the creation. |
| `init` | DATA | Init bytecode (constructor + runtime). |

**`type: "suicide"`** (self-destruct)

| Field | Type | Description |
| --- | --- | --- |
| `address` | DATA, 20 BYTES | Address of the self-destructing contract. |
| `refundAddress` | DATA, 20 BYTES | Address that received the contract's remaining balance. |
| `balance` | QUANTITY | Wei amount transferred to `refundAddress`. |

**`type: "reward"`** (block/uncle reward, in `trace_block` and `trace_filter`)

| Field | Type | Description |
| --- | --- | --- |
| `author` | DATA, 20 BYTES | Address receiving the reward (miner / uncle author). |
| `value` | QUANTITY | Wei amount of the reward. |
| `rewardType` | String | `"block"` or `"uncle"`. |

### Result variants

The `result` object's shape depends on `type`:

**`type: "call"`**

| Field | Type | Description |
| --- | --- | --- |
| `gasUsed` | QUANTITY | Gas consumed by this call frame (excluding subcalls' charges). |
| `output` | DATA | Return data of this call frame (`0x` if no data was returned). |

**`type: "create"`**

| Field | Type | Description |
| --- | --- | --- |
| `gasUsed` | QUANTITY | Gas consumed by the creation. |
| `code` | DATA | Deployed runtime bytecode of the new contract. |
| `address` | DATA, 20 BYTES | Address of the newly deployed contract. |

**`type: "suicide"` and `type: "reward"`**

`result` is `null` — these actions have no return value.

***

## JSON-RPC API Reference

### trace\_call

Executes the given call and returns a number of possible traces for it.

#### Parameters

1. `Object` - \[Transaction object] where `from` field is optional and `nonce` field is omitted.
2. `Array` - Type of trace, one or more of: `"vmTrace"`, `"trace"`, `"stateDiff"`.
3. `Quantity` or `Tag` - (optional) Integer of a block number, or the string `'earliest'`, `'latest'` or `'pending'`.

#### Returns

`Object` containing `output`, `stateDiff`, `trace[]`, `vmTrace`. Each requested trace type is populated; the others are `null`. See [Response Fields Reference](#response-fields-reference) for full field semantics.

#### Example

Request

```bash
curl --data '{"method":"trace_call","params":[{ ... },["trace"]],"id":1,"jsonrpc":"2.0"}' -H "Content-Type: application/json" -X POST localhost:8545
```


Response

```js
{
  "id": 1,
  "jsonrpc": "2.0",
  "result": {
    "output": "0x",
    "stateDiff": null,
    "trace": [{
      "action": { ... },
      "result": {
        "gasUsed": "0x0",
        "output": "0x"
      },
      "subtraces": 0,
      "traceAddress": [],
      "type": "call"
    }],
    "vmTrace": null
  }
}
```

***

### trace\_callMany

Performs multiple call traces on top of the same block. i.e. transaction `n` will be executed on top of a pending block with all `n-1` transactions applied (traced) first. Allows to trace dependent transactions.

#### Parameters

1. `Array` - List of trace calls with the type of trace, one or more of: `"vmTrace"`, `"trace"`, `"stateDiff"`.
2. `Quantity` or `Tag` - (optional) integer block number, or the string `'latest'`, `'earliest'` or `'pending'` (default block parameter).

```js
params: [
  [
    [
      {
        "from": "0x407d73d8a49eeb85d32cf465507dd71d507100c1",
        "to": "0xa94f5374fce5edbc8e2a8697c15331677e6ebf0b",
        "value": "0x186a0"
      },
      ["trace"]
    ],
    [
      {
        "from": "0x407d73d8a49eeb85d32cf465507dd71d507100c1",
        "to": "0xa94f5374fce5edbc8e2a8697c15331677e6ebf0b",
        "value": "0x186a0"
      },
      ["trace"]
    ]
  ],
  "latest"
]
```

#### Returns

`Array<Object>` — one entry per input call, each shaped like the `trace_call` response (`output`, `stateDiff`, `trace[]`, `vmTrace`). See [Response Fields Reference](#response-fields-reference).

#### Example

Request

```bash
curl --data '{"method":"trace_callMany","params":[[[{"from":"0x407d73d8a49eeb85d32cf465507dd71d507100c1","to":"0xa94f5374fce5edbc8e2a8697c15331677e6ebf0b","value":"0x186a0"},["trace"]],[{"from":"0x407d73d8a49eeb85d32cf465507dd71d507100c1","to":"0xa94f5374fce5edbc8e2a8697c15331677e6ebf0b","value":"0x186a0"},["trace"]]],"latest"],"id":1,"jsonrpc":"2.0"}' -H "Content-Type: application/json" -X POST localhost:8545
```


Response

```js
{
  "id": 1,
  "jsonrpc": "2.0",
  "result": [
    {
      "output": "0x",
      "stateDiff": null,
      "trace": [{
        "action": {
          "callType": "call",
          "from": "0x407d73d8a49eeb85d32cf465507dd71d507100c1",
          "gas": "0x1dcd12f8",
          "input": "0x",
          "to": "0xa94f5374fce5edbc8e2a8697c15331677e6ebf0b",
          "value": "0x186a0"
        },
        "result": {
          "gasUsed": "0x0",
          "output": "0x"
        },
        "subtraces": 0,
        "traceAddress": [],
        "type": "call"
      }],
      "vmTrace": null
    },
    {
      "output": "0x",
      "stateDiff": null,
      "trace": [{
        "action": {
          "callType": "call",
          "from": "0x407d73d8a49eeb85d32cf465507dd71d507100c1",
          "gas": "0x1dcd12f8",
          "input": "0x",
          "to": "0xa94f5374fce5edbc8e2a8697c15331677e6ebf0b",
          "value": "0x186a0"
        },
        "result": {
          "gasUsed": "0x0",
          "output": "0x"
        },
        "subtraces": 0,
        "traceAddress": [],
        "type": "call"
      }],
      "vmTrace": null
    }
  ]
}
```

***

### trace\_rawTransaction

Traces a call to `eth_sendRawTransaction` without making the call, returning the traces

#### Parameters

1. `Data` - Raw transaction data.
2. `Array` - Type of trace, one or more of: `"vmTrace"`, `"trace"`, `"stateDiff"`.

```js
params: [
  "0xd46e8dd67c5d32be8d46e8dd67c5d32be8058bb8eb970870f072445675058bb8eb970870f072445675",
  ["trace"]
]
```


#### Returns

`Object` shaped like the `trace_call` response (`output`, `stateDiff`, `trace[]`, `vmTrace`). See [Response Fields Reference](#response-fields-reference).

#### Example

Request

```bash
curl --data '{"method":"trace_rawTransaction","params":["0xd46e8dd67c5d32be8d46e8dd67c5d32be8058bb8eb970870f072445675058bb8eb970870f072445675",["trace"]],"id":1,"jsonrpc":"2.0"}' -H "Content-Type: application/json" -X POST localhost:8545
```


Response

```js
{
  "id": 1,
  "jsonrpc": "2.0",
  "result": {
    "output": "0x",
    "stateDiff": null,
    "trace": [{
      "action": { ... },
      "result": {
        "gasUsed": "0x0",
        "output": "0x"
      },
      "subtraces": 0,
      "traceAddress": [],
      "type": "call"
    }],
    "vmTrace": null
  }
}
```

***

### trace\_replayBlockTransactions

Replays all transactions in a block returning the requested traces for each transaction.

#### Parameters

1. `Quantity` or `Tag` - Integer of a block number, or the string `'earliest'`, `'latest'` or `'pending'`.
2. `Array` - Type of trace, one or more of: `"vmTrace"`, `"trace"`, `"stateDiff"`.

```js
params: [
  "0x2ed119",
  ["trace"]
]
```

#### Returns

`Array<Object>` — one entry per transaction in the block. Each entry is shaped like the `trace_call` response plus a `transactionHash` field identifying the transaction. See [Response Fields Reference](#response-fields-reference).

#### Example

Request

```bash
curl --data '{"method":"trace_replayBlockTransactions","params":["0x2ed119",["trace"]],"id":1,"jsonrpc":"2.0"}' -H "Content-Type: application/json" -X POST localhost:8545
```


Response

```js
{
  "id": 1,
  "jsonrpc": "2.0",
  "result": [
    {
      "output": "0x",
      "stateDiff": null,
      "trace": [{
        "action": { ... },
        "result": {
          "gasUsed": "0x0",
          "output": "0x"
        },
        "subtraces": 0,
        "traceAddress": [],
        "type": "call"
      }],
      "transactionHash": "0x...",
      "vmTrace": null
    },
    { ... }
  ]
}
```

***

### trace\_replayTransaction

Replays a transaction, returning the traces.

#### Parameters

1. `Hash` - Transaction hash.
2. `Array` - Type of trace, one or more of: `"vmTrace"`, `"trace"`, `"stateDiff"`.

```js
params: [
  "0x02d4a872e096445e80d05276ee756cefef7f3b376bcec14246469c0cd97dad8f",
  ["trace"]
]
```

#### Returns

`Object` shaped like the `trace_call` response (`output`, `stateDiff`, `trace[]`, `vmTrace`). See [Response Fields Reference](#response-fields-reference).

#### Example

Request

```bash
curl --data '{"method":"trace_replayTransaction","params":["0x02d4a872e096445e80d05276ee756cefef7f3b376bcec14246469c0cd97dad8f",["trace"]],"id":1,"jsonrpc":"2.0"}' -H "Content-Type: application/json" -X POST localhost:8545
```


Response

```js
{
  "id": 1,
  "jsonrpc": "2.0",
  "result": {
    "output": "0x",
    "stateDiff": null,
    "trace": [{
      "action": { ... },
      "result": {
        "gasUsed": "0x0",
        "output": "0x"
      },
      "subtraces": 0,
      "traceAddress": [],
      "type": "call"
    }],
    "vmTrace": null
  }
}
```

***

### trace\_block

Returns traces created at given block.

#### Parameters

1. `Quantity` or `Tag` - Integer of a block number, or the string `'earliest'`, `'latest'` or `'pending'`.

```js
params: [
  "0x2ed119" // 3068185
]
```

#### Returns

`Array<TraceEntry>` — flat list of all call frames in the block, including any `"reward"` entries for block/uncle rewards. Each entry includes block-level fields (`blockHash`, `blockNumber`, `transactionHash`, `transactionPosition`). See [Response Fields Reference](#response-fields-reference).

#### Example

Request

```bash
curl --data '{"method":"trace_block","params":["0x2ed119"],"id":1,"jsonrpc":"2.0"}' -H "Content-Type: application/json" -X POST localhost:8545
```


Response

```js
{
  "id": 1,
  "jsonrpc": "2.0",
  "result": [
    {
      "action": {
        "callType": "call",
        "from": "0xaa7b131dc60b80d3cf5e59b5a21a666aa039c951",
        "gas": "0x0",
        "input": "0x",
        "to": "0xd40aba8166a212d6892125f079c33e6f5ca19814",
        "value": "0x4768d7effc3fbe"
      },
      "blockHash": "0x7eb25504e4c202cf3d62fd585d3e238f592c780cca82dacb2ed3cb5b38883add",
      "blockNumber": 3068185,
      "result": {
        "gasUsed": "0x0",
        "output": "0x"
      },
      "subtraces": 0,
      "traceAddress": [],
      "transactionHash": "0x07da28d752aba3b9dd7060005e554719c6205c8a3aea358599fc9b245c52f1f6",
      "transactionPosition": 0,
      "type": "call"
    },
    ...
  ]
}
```

***

### trace\_filter

Returns traces matching given filter

#### Parameters

1. `Object` - The filter object
   * `fromBlock`: `Quantity` or `Tag` - (optional) From this block.
   * `toBlock`: `Quantity` or `Tag` - (optional) To this block.
   * `fromAddress`: `Array` - (optional) Sent from these addresses.
   * `toAddress`: `Address` - (optional) Sent to these addresses.
   * `after`: `Quantity` - (optional) The offset trace number
   * `count`: `Quantity` - (optional) Integer number of traces to display in a batch.
   * `mode`: `String` - (optional) Default is `"union"`, meaning traces matching either address filter are returned. Set to `"intersection"` to only return traces that satisfy both `fromAddress` and `toAddress` filters simultaneously.


```js
params: [{
  "fromBlock": "0x2ed0c4", // 3068100
  "toBlock": "0x2ed128", // 3068200
  "toAddress": ["0x8bbB73BCB5d553B5A556358d27625323Fd781D37"],
  "after": 1000,
  "count": 100
}]
```

#### Returns

`Array<TraceEntry>` — flat list of call frames that match the filter, including any `"reward"` entries when the filter matches block coinbases or uncle authors. Each entry has block-level fields (`blockHash`, `blockNumber`, `transactionHash`, `transactionPosition`). See [Response Fields Reference](#response-fields-reference).

#### Example

Request

```bash
curl --data '{"method":"trace_filter","params":[{"fromBlock":"0x2ed0c4","toBlock":"0x2ed128","toAddress":["0x8bbB73BCB5d553B5A556358d27625323Fd781D37"],"after":1000,"count":100}],"id":1,"jsonrpc":"2.0"}' -H "Content-Type: application/json" -X POST localhost:8545
```


Response

```js
{
  "id": 1,
  "jsonrpc": "2.0",
  "result": [
    {
      "action": {
        "callType": "call",
        "from": "0x32be343b94f860124dc4fee278fdcbd38c102d88",
        "gas": "0x4c40d",
        "input": "0x",
        "to": "0x8bbb73bcb5d553b5a556358d27625323fd781d37",
        "value": "0x3f0650ec47fd240000"
      },
      "blockHash": "0x86df301bcdd8248d982dbf039f09faf792684e1aeee99d5b58b77d620008b80f",
      "blockNumber": 3068183,
      "result": {
        "gasUsed": "0x0",
        "output": "0x"
      },
      "subtraces": 0,
      "traceAddress": [],
      "transactionHash": "0x3321a7708b1083130bd78da0d62ead9f6683033231617c9d268e2c7e3fa6c104",
      "transactionPosition": 3,
      "type": "call"
    },
    ...
  ]
}
```

***

### trace\_get

Returns trace at given position.

#### Parameters

1. `Hash` - Transaction hash.
2. `Array` - Index positions of the traces.

```js
params: [
  "0x17104ac9d3312d8c136b7f44d4b8b47852618065ebfa534bd2d3b5ef218ca1f3",
  ["0x0"]
]
```

#### Returns

A single `TraceEntry` corresponding to the call frame at the requested position, with block-level fields (`blockHash`, `blockNumber`, `transactionHash`, `transactionPosition`). See [Response Fields Reference](#response-fields-reference).

#### Example

Request

```bash
curl --data '{"method":"trace_get","params":["0x17104ac9d3312d8c136b7f44d4b8b47852618065ebfa534bd2d3b5ef218ca1f3",["0x0"]],"id":1,"jsonrpc":"2.0"}' -H "Content-Type: application/json" -X POST localhost:8545
```


Response

```js
{
  "id": 1,
  "jsonrpc": "2.0",
  "result": {
    "action": {
      "callType": "call",
      "from": "0x1c39ba39e4735cb65978d4db400ddd70a72dc750",
      "gas": "0x13e99",
      "input": "0x16c72721",
      "to": "0x2bd2326c993dfaef84f696526064ff22eba5b362",
      "value": "0x0"
    },
    "blockHash": "0x7eb25504e4c202cf3d62fd585d3e238f592c780cca82dacb2ed3cb5b38883add",
    "blockNumber": 3068185,
    "result": {
      "gasUsed": "0x183",
      "output": "0x0000000000000000000000000000000000000000000000000000000000000001"
    },
    "subtraces": 0,
    "traceAddress": [
      0
    ],
    "transactionHash": "0x17104ac9d3312d8c136b7f44d4b8b47852618065ebfa534bd2d3b5ef218ca1f3",
    "transactionPosition": 2,
    "type": "call"
  }
}
```

***

### trace\_transaction

Returns all traces of given transaction

#### Parameters

1. `Hash` - Transaction hash

```js
params: ["0x17104ac9d3312d8c136b7f44d4b8b47852618065ebfa534bd2d3b5ef218ca1f3"]
```


#### Returns

`Array<TraceEntry>` — flat list of all call frames in the transaction, with block-level fields (`blockHash`, `blockNumber`, `transactionHash`, `transactionPosition`) on each entry. See [Response Fields Reference](#response-fields-reference).

#### Example

Request

```bash
curl --data '{"method":"trace_transaction","params":["0x17104ac9d3312d8c136b7f44d4b8b47852618065ebfa534bd2d3b5ef218ca1f3"],"id":1,"jsonrpc":"2.0"}' -H "Content-Type: application/json" -X POST localhost:8545
```


Response

```js
{
  "id": 1,
  "jsonrpc": "2.0",
  "result": [
    {
      "action": {
        "callType": "call",
        "from": "0x1c39ba39e4735cb65978d4db400ddd70a72dc750",
        "gas": "0x13e99",
        "input": "0x16c72721",
        "to": "0x2bd2326c993dfaef84f696526064ff22eba5b362",
        "value": "0x0"
      },
      "blockHash": "0x7eb25504e4c202cf3d62fd585d3e238f592c780cca82dacb2ed3cb5b38883add",
      "blockNumber": 3068185,
      "result": {
        "gasUsed": "0x183",
        "output": "0x0000000000000000000000000000000000000000000000000000000000000001"
      },
      "subtraces": 0,
      "traceAddress": [
        0
      ],
      "transactionHash": "0x17104ac9d3312d8c136b7f44d4b8b47852618065ebfa534bd2d3b5ef218ca1f3",
      "transactionPosition": 2,
      "type": "call"
    },
    ...
  ]
}
```
