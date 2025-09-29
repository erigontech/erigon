# `trace` RPC Namespace

The `trace` module is for getting a deeper insight into transaction processing.
It includes two sets of calls the transaction trace filtering API and the ad-hoc tracing API.

In order to use the Transaction-Trace Filtering API, Erigon must be fully synced with the argument `trace` in `http.api` flag.

```bash
./build/bin/erigon --http.api: eth,erigon,trace
```

As for the Ad-hoc Tracing API, as long the blocks have not yet been pruned, the RPC calls will work.

## The Ad-hoc Tracing API

The ad-hoc tracing API allows you to perform a number of different diagnostics on calls or transactions,
either historical ones from the chain or hypothetical ones not yet mined. The diagnostics include:

- `trace` **Transaction trace**. An equivalent trace to that in the previous section.
- `vmTrace` **Virtual Machine execution trace**. Provides a full trace of the VM's state throughout the execution of the transaction, including for any subcalls.
- `stateDiff` **State difference**. Provides information detailing all altered portions of the Ethereum state made due to the execution of the transaction.

There are three means of providing a transaction to execute; either providing the same information as when making
a call using `eth_call` (see `trace_call`), through providing raw, signed, transaction data as when using
`eth_sendRawTransaction` (see `trace_rawTransaction`) or simply a transaction hash for a previously mined
transaction (see `trace_replayTransaction`). In the latter case, your node must be in archive mode or the
transaction should be within the most recent 1000 blocks.

## The Transaction-Trace Filtering API

These APIs allow you to get a full *externality* trace on any transaction executed throughout the Erigon chain.
Unlike the log filtering API, you are able to search and filter based only upon address information.
Information returned includes the execution of all `CREATE`s, `SUICIDE`s and all variants of `CALL` together
with input data, output data, gas usage, amount transferred and the success status of each individual action.

### `traceAddress` field

The `traceAddress` field of all returned traces, gives the exact location in the call trace [index in root, index in first `CALL`, index in second `CALL`, ...].

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
- [trace_call](#trace_call)
- [trace_callMany](#trace_callmany)
- [trace_rawTransaction](#trace_rawtransaction)
- [trace_replayBlockTransactions](#trace_replayblocktransactions)
- [trace_replayTransaction](#trace_replaytransaction)

#### Transaction-Trace Filtering
- [trace_block](#trace_block)
- [trace_filter](#trace_filter)
- [trace_get](#trace_get)
- [trace_transaction](#trace_transaction)

## JSON-RPC API Reference

### trace_call

Executes the given call and returns a number of possible traces for it.

#### Parameters

0. `Object` - [Transaction object] where `from` field is optional and `nonce` field is omitted.
0. `Array` - Type of trace, one or more of: `"vmTrace"`, `"trace"`, `"stateDiff"`.
0. `Quantity` or `Tag` - (optional) Integer of a block number, or the string `'earliest'`, `'latest'` or `'pending'`.

#### Returns

- `Array` - Block traces

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

### trace_callMany

Performs multiple call traces on top of the same block. i.e. transaction `n` will be executed on top of a pending block with all `n-1` transactions applied (traced) first. Allows to trace dependent transactions.

#### Parameters

0. `Array` - List of trace calls with the type of trace, one or more of: `"vmTrace"`, `"trace"`, `"stateDiff"`.
0. `Quantity` or `Tag` - (optional) integer block number, or the string `'latest'`, `'earliest'` or `'pending'`, see the [default block parameter](#the-default-block-parameter).

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

- `Array` - Array of the given transactions' traces

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

### trace_rawTransaction

Traces a call to `eth_sendRawTransaction` without making the call, returning the traces

#### Parameters

0. `Data` - Raw transaction data.
0. `Array` - Type of trace, one or more of: `"vmTrace"`, `"trace"`, `"stateDiff"`.

```js
params: [
  "0xd46e8dd67c5d32be8d46e8dd67c5d32be8058bb8eb970870f072445675058bb8eb970870f072445675",
  ["trace"]
]
```

#### Returns

- `Object` - Block traces.

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

### trace_replayBlockTransactions

Replays all transactions in a block returning the requested traces for each transaction.

#### Parameters

0. `Quantity` or `Tag` - Integer of a block number, or the string `'earliest'`, `'latest'` or `'pending'`.
0. `Array` - Type of trace, one or more of: `"vmTrace"`, `"trace"`, `"stateDiff"`.

```js
params: [
  "0x2ed119",
  ["trace"]
]
```

#### Returns

- `Array` - Block transactions traces.

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

### trace_replayTransaction

Replays a transaction, returning the traces.

#### Parameters

0. `Hash` - Transaction hash.
0. `Array` - Type of trace, one or more of: `"vmTrace"`, `"trace"`, `"stateDiff"`.

```js
params: [
  "0x02d4a872e096445e80d05276ee756cefef7f3b376bcec14246469c0cd97dad8f",
  ["trace"]
]
```

#### Returns

- `Object` - Block traces.

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

### trace_block

Returns traces created at given block.

#### Parameters

0. `Quantity` or `Tag` - Integer of a block number, or the string `'earliest'`, `'latest'` or `'pending'`.

```js
params: [
  "0x2ed119" // 3068185
]
```

#### Returns

- `Array` - Block traces.

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

### trace_filter

Returns traces matching given filter

#### Parameters

0. `Object` - The filter object
    - `fromBlock`:   `Quantity` or `Tag` - (optional) From this block.
    - `toBlock`:   `Quantity` or `Tag` - (optional) To this block.
    - `fromAddress`:   `Array` - (optional) Sent from these addresses.
    - `toAddress`:   `Address` - (optional) Sent to these addresses.
    - `after`:   `Quantity` - (optional) The offset trace number
    - `count`:   `Quantity` - (optional) Integer number of traces to display in a batch.

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

- `Array` - Traces matching given filter

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

### trace_get

Returns trace at given position.

#### Parameters

0. `Hash` - Transaction hash.
0. `Array` - Index positions of the traces.

```js
params: [
  "0x17104ac9d3312d8c136b7f44d4b8b47852618065ebfa534bd2d3b5ef218ca1f3",
  ["0x0"]
]
```

#### Returns

- `Object` - Trace object

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

### trace_transaction

Returns all traces of given transaction

#### Parameters

0. `Hash` - Transaction hash

```js
params: ["0x17104ac9d3312d8c136b7f44d4b8b47852618065ebfa534bd2d3b5ef218ca1f3"]
```

#### Returns

- `Array` - Traces of given transaction

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