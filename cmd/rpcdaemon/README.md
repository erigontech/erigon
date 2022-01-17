- [Introduction](#introduction)
- [Getting Started](#getting-started)
    * [Running locally](#running-locally)
    * [Running remotely](#running-remotely)
    * [Testing](#testing)
- [FAQ](#faq)
    * [RPC Implementation Status](#rpc-implementation-status)
    * [Clients getting timeout, but server load is low](#clients-getting-timeout--but-server-load-is-low)
    * [Server load too high](#server-load-too-high)
    * [Faster Batch requests](#faster-batch-requests)

## Introduction

TurboBor's `rpcdaemon` runs in its own seperate process.

This brings many benefits including easier development, the ability to run multiple daemons at once, and the ability to
run the daemon remotely. It is possible to run the daemon locally as well (read-only) if both processes have access to
the data folder.

## Getting Started

The `rpcdaemon` gets built as part of the main `turbo` build process, but you can build it directly with this command:

```[bash]
make rpcdaemon
```

### Running locally

This is only possible if RPC daemon runs on the same computer as TurboBor. This mode uses shared memory access to the
database of TurboBor, which has better performance than accessing via TPC socket.
Provide both `--datadir` and `--private.api.addr` options:

```sh
./build/bin/rpcdaemon --datadir=<your_data_dir> --private.api.addr=localhost:9090 --http.api=eth,web3,net,debug,trace,txpool,bor
```

### Running remotely

This works regardless of whether RPC daemon is on the same computer with TurboBor, or on a different one. They use TPC
socket connection to pass data between them. 

To use this mode, you have to give `--private.api.addr=<private_ip>:9090` while starting TurboBor where `private_ip` is the IP Address of system in which the TurboBor is running.

Run TurboBor in one terminal window

```sh
turbo-bor --chain=mumbai --bor.heimdall=https://heimdall.api.matic.today --datadir=<your_data_dir> --private.api.addr=<private_ip>:9090
```
On other Terminal, run

```sh
./build/bin/rpcdaemon --private.api.addr=<turbo_bor_ip>:9090 --http.api=eth,web3,net,debug,trace,txpool,bor
```

The daemon should respond with something like:

`INFO [date-time] HTTP endpoint opened url=localhost:8545...`


### Testing

By default, the `rpcdaemon` serves data from `localhost:8545`. You may send `curl` commands to see if things are
working.

Try `eth_blockNumber` for example. In a third terminal window enter this command:

```[bash]
curl -X POST -H "Content-Type: application/json" --data '{"jsonrpc": "2.0", "method": "eth_blockNumber", "params": [], "id":1}' localhost:8545
```

This should return something along the lines of this (depending on how far your node has synced):

```[bash]
{
    "jsonrpc": "2.0",
    "id": 1,
    "result":" 0xa5b9ba"
}
```

You can also use similar command to test the `bor` namespace methods. Methods in this namespace provides bor consensus specific info like snapshot, signers, validator info, etc. For example:
```[bash]
curl -X POST -H "Content-Type: application/json" --data '{"jsonrpc": "2.0", "method": "bor_getSnapshot", "params": ["0x400"], "id":1}' localhost:8545
```
This should return something like this:
```[bash]
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": {
    "number": 1024,
    "hash": "0xb976c3964fc461faa40745cb0a2d560bd8db5793e22fd1b63dc7e32c7455c21d",
    "validatorSet": {
      "validators": [
        {
          "ID": 0,
          "signer": "0x928ed6a3e94437bbd316ccad78479f1d163a6a8c",
          "power": 10000,
          "accum": -30000
        },
        ...
      ],
      "proposer": {
        "ID": 0,
        "signer": "0xbe188d6641e8b680743a4815dfa0f6208038960f",
        "power": 10000,
        "accum": -30000
      }
    },
    "recents": {
      "961": "0x928ed6a3e94437bbd316ccad78479f1d163a6a8c",
      "962": "0x928ed6a3e94437bbd316ccad78479f1d163a6a8c",
      ...
      "1023": "0x928ed6a3e94437bbd316ccad78479f1d163a6a8c",
      "1024": "0xbe188d6641e8b680743a4815dfa0f6208038960f"
    }
  }
}
```

## FAQ

### RPC Implementation Status

Label "remote" means: `--private.api.addr` flag is required.

The following table shows the current implementation status of TurboBor's RPC daemon.

| Command                                    | Avail   | Notes                                      |
| ------------------------------------------ | ------- | ------------------------------------------ |
| web3_clientVersion                         | Yes     |                                            |
| web3_sha3                                  | Yes     |                                            |
|                                            |         |                                            |
| net_listening                              | HC      | (`remote` hard coded returns true)         |
| net_peerCount                              | Limited | internal sentries only                     |
| net_version                                | Yes     | `remote`.                                  |
|                                            |         |                                            |
| eth_blockNumber                            | Yes     |                                            |
| eth_chainID/eth_chainId                    | Yes     |                                            |
| eth_protocolVersion                        | Yes     |                                            |
| eth_syncing                                | Yes     |                                            |
| eth_gasPrice                               | Yes     |                                            |
| eth_maxPriorityFeePerGas                   | Yes     |                                            |
| eth_feeHistory                             | Yes     |                                            |
| eth_forks                                  | Yes     |                                            |
|                                            |         |                                            |
| eth_getBlockByHash                         | Yes     |                                            |
| eth_getBlockByNumber                       | Yes     |                                            |
| eth_getBlockTransactionCountByHash         | Yes     |                                            |
| eth_getBlockTransactionCountByNumber       | Yes     |                                            |
| eth_getHeaderByHash                        | Yes     |                                            |
| eth_getHeaderByNumber                      | Yes     |                                            |
| eth_getUncleByBlockHashAndIndex            | Yes     |                                            |
| eth_getUncleByBlockNumberAndIndex          | Yes     |                                            |
| eth_getUncleCountByBlockHash               | Yes     |                                            |
| eth_getUncleCountByBlockNumber             | Yes     |                                            |
|                                            |         |                                            |
| eth_getTransactionByHash                   | Yes     |                                            |
| eth_getRawTransactionByHash                | Yes     |                                            |
| eth_getTransactionByBlockHashAndIndex      | Yes     |                                            |
| eth_retRawTransactionByBlockHashAndIndex   | Yes     |                                            |
| eth_getTransactionByBlockNumberAndIndex    | Yes     |                                            |
| eth_retRawTransactionByBlockNumberAndIndex | Yes     |                                            |
| eth_getTransactionReceipt                  | Yes     |                                            |
| eth_getBlockReceipts                       | Yes     |                                            |
|                                            |         |                                            |
| eth_estimateGas                            | Yes     |                                            |
| eth_getBalance                             | Yes     |                                            |
| eth_getCode                                | Yes     |                                            |
| eth_getTransactionCount                    | Yes     |                                            |
| eth_getStorageAt                           | Yes     |                                            |
| eth_call                                   | Yes     |                                            |
| eth_callBundle                             | Yes     |                                            |
|                                            |         |                                            |
| eth_newFilter                              | -       | not yet implemented                        |
| eth_newBlockFilter                         | -       | not yet implemented                        |
| eth_newPendingTransactionFilter            | -       | not yet implemented                        |
| eth_getFilterChanges                       | -       | not yet implemented                        |
| eth_uninstallFilter                        | -       | not yet implemented                        |
| eth_getLogs                                | Yes     |                                            |
|                                            |         |                                            |
| eth_accounts                               | No      | deprecated                                 |
| eth_sendRawTransaction                     | Yes     | `remote`.                                  |
| eth_sendTransaction                        | -       | not yet implemented                        |
| eth_sign                                   | No      | deprecated                                 |
| eth_signTransaction                        | -       | not yet implemented                        |
| eth_signTypedData                          | -       | ????                                       |
|                                            |         |                                            |
| eth_getProof                               | -       | not yet implemented                        |
|                                            |         |                                            |
| eth_mining                                 | Yes     | returns true if --mine flag provided       |
| eth_coinbase                               | Yes     |                                            |
| eth_hashrate                               | Yes     |                                            |
| eth_submitHashrate                         | Yes     |                                            |
| eth_getWork                                | Yes     |                                            |
| eth_submitWork                             | Yes     |                                            |
|                                            |         |                                            |
| eth_issuance                               | Yes     |                                            |
|                                            |         |                                            |
| eth_subscribe                              | Limited | Websock Only - newHeads,                   |
|                                            |         | newPendingTransaction                      |
| eth_unsubscribe                            | Yes     | Websock Only                               |
|                                            |         |                                            |
| debug_accountRange                         | Yes     | Private Erigon debug module                |
| debug_accountAt                            | Yes     | Private Erigon debug module                |
| debug_getModifiedAccountsByNumber          | Yes     |                                            |
| debug_getModifiedAccountsByHash            | Yes     |                                            |
| debug_storageRangeAt                       | Yes     |                                            |
| debug_traceTransaction                     | Yes     | Streaming (can handle huge results)        |
| debug_traceCall                            | Yes     | Streaming (can handle huge results)        |
|                                            |         |                                            |
| trace_call                                 | Yes     |                                            |
| trace_callMany                             | Yes     |                                            |
| trace_rawTransaction                       | -       | not yet implemented (come help!)           |
| trace_replayBlockTransactions              | yes     | stateDiff only (come help!)                |
| trace_replayTransaction                    | yes     | stateDiff only (come help!)                |
| trace_block                                | Yes     |                                            |
| trace_filter                               | Yes     | no pagination, but streaming               |
| trace_get                                  | Yes     |                                            |
| trace_transaction                          | Yes     |                                            |
|                                            |         |                                            |
| txpool_content                             | Yes     | `remote`                                   |
| txpool_status                              | Yes     | `remote`                                   |
|                                            |         |                                            |
| eth_getCompilers                           | No      | deprecated                                 |
| eth_compileLLL                             | No      | deprecated                                 |
| eth_compileSolidity                        | No      | deprecated                                 |
| eth_compileSerpent                         | No      | deprecated                                 |
|                                            |         |                                            |
| db_putString                               | No      | deprecated                                 |
| db_getString                               | No      | deprecated                                 |
| db_putHex                                  | No      | deprecated                                 |
| db_getHex                                  | No      | deprecated                                 |
|                                            |         |                                            |
| erigon_getLogsByHash                       | No      | Subset of `eth_getLogs`                    |
|                                            |         |                                            |
| bor_getSnapshot                            | Yes     | Bor only                                   |
| bor_getAuthor                              | Yes     | Bor only                                   |
| bor_getSnapshotAtHash                      | Yes     | Bor only                                   |
| bor_getSigners                             | Yes     | Bor only                                   |
| bor_getSignersAtHash                       | Yes     | Bor only                                   |
| bor_getCurrentProposer                     | Yes     | Bor only                                   |
| bor_getCurrentValidators                   | Yes     | Bor only                                   |
| bor_getRootHash                            | Yes     | Bor only                                   |

This table is constantly updated. Please visit again.

### Clients getting timeout, but server load is low

In this case: increase default rate-limit - amount of requests server handle simultaneously - requests over this limit
will wait. Increase it - if your 'hot data' is small or have much RAM or see "request timeout" while server load is low.

```
./build/bin/turbo --private.api.addr=localhost:9090 --private.api.ratelimit=1024
```

### Server load too high

Reduce `--private.api.ratelimit`

### Faster Batch requests

Currently batch requests are spawn multiple goroutines and process all sub-requests in parallel. To limit impact of 1
huge batch to other users - added flag `--rpc.batch.concurrency` (default: 2). Increase it to process large batches
faster.

Known Issue: if at least 1 request is "stremable" (has parameter of type *jsoniter.Stream) - then whole batch will
processed sequentially (on 1 goroutine).
