- [Introduction](#introduction)
- [Getting Started](#getting-started)
  - [Running locally](#running-locally)
  - [Running remotely](#running-remotely)
  - [Healthcheck](#healthcheck)
  - [Testing](#testing)
- [FAQ](#faq)
  - [Relations between prune options and rpc methods](#relations-between-prune-options-and-rpc-method)
  - [RPC Implementation Status](#rpc-implementation-status)
  - [Securing the communication between RPC daemon and Erigon instance via TLS and authentication](#securing-the-communication-between-rpc-daemon-and-erigon-instance-via-tls-and-authentication)
  - [Ethstats](#ethstats)
  - [Allowing only specific methods (Allowlist)](#allowing-only-specific-methods--allowlist-)
  - [Trace transactions progress](#trace-transactions-progress)
  - [Clients getting timeout, but server load is low](#clients-getting-timeout--but-server-load-is-low)
  - [Server load too high](#server-load-too-high)
  - [Faster Batch requests](#faster-batch-requests)
- [For Developers](#for-developers)
  - [Code generation](#code-generation)

## Introduction

Erigon's `rpcdaemon` runs in its own seperate process.

This brings many benefits including easier development, the ability to run multiple daemons at once, and the ability to
run the daemon remotely. It is possible to run the daemon locally as well (read-only) if both processes have access to
the data folder.

## Getting Started

The `rpcdaemon` gets built as part of the main `erigon` build process, but you can build it directly with this command:

```[bash]
make rpcdaemon
```

### Running locally

Run `rpcdaemon` on same computer with Erigon. It's default option because it using Shared Memory access to Erigon's db -
it's much faster than TCP access. Provide both `--datadir` and `--private.api.addr` flags:

```[bash]
make erigon
./build/bin/erigon --datadir=<your_data_dir> --private.api.addr=localhost:9090
make rpcdaemon
./build/bin/rpcdaemon --datadir=<your_data_dir> --txpool.api.addr=localhost:9090 --private.api.addr=localhost:9090 --http.api=eth,erigon,web3,net,debug,trace,txpool
```

Note that we've also specified which RPC namespaces to enable in the above command by `--http.api` flag.

### Running remotely

To start the daemon remotely - just don't set `--datadir` flag:

```[bash]
make erigon
./build/bin/erigon --datadir=<your_data_dir> --private.api.addr=0.0.0.0:9090
make rpcdaemon
./build/bin/rpcdaemon --private.api.addr=<erigon_ip>:9090 --txpool.api.addr=localhost:9090 --http.api=eth,erigon,web3,net,debug,trace,txpool
```

The daemon should respond with something like:

```[bash]
INFO [date-time] HTTP endpoint opened url=localhost:8545...
```

When RPC daemon runs remotely, by default it maintains a state cache, which is updated every time when Erigon imports a
new block. When state cache is reasonably warm, it allows such remote RPC daemon to execute queries related to `latest`
block (i.e. to current state) with comparable performance to a local RPC daemon
(around 2x slower vs 10x slower without state cache). Since there can be multiple such RPC daemons per one Erigon node,
it may scale well for some workloads that are heavy on the current state queries.

### Healthcheck

Running the daemon also opens an endpoint `/health` that provides a basic health check.

If the health check is successful it returns 200 OK.

If the health check fails it returns 500 Internal Server Error.

Configuration of the health check is sent as POST body of the method.

```
{
   "min_peer_count": <minimal number of the node peers>,
   "known_block": <number_of_block_that_node_should_know>
}
```

Not adding a check disables that.

**`min_peer_count`** -- checks for mimimum of healthy node peers. Requires
`net` namespace to be listed in `http.api`.

**`known_block`** -- sets up the block that node has to know about. Requires
`eth` namespace to be listed in `http.api`.

Example request
`http POST http://localhost:8545/health --raw '{"min_peer_count": 3, "known_block": "0x1F"}'`
Example response

```
{
    "check_block": "HEALTHY",
    "healthcheck_query": "HEALTHY",
    "min_peer_count": "HEALTHY"
}
```

### Testing

By default, the `rpcdaemon` serves data from `localhost:8545`. You may send `curl` commands to see if things are
working.

Try `eth_blockNumber` for example. In a third terminal window enter this command:

```[bash]
curl -X POST -H "Content-Type: application/json" --data '{"jsonrpc": "2.0", "method": "eth_blockNumber", "params": [], "id":1}' localhost:8545
```

This should return something along the lines of this (depending on how far your Erigon node has synced):

```[bash]
{
    "jsonrpc": "2.0",
    "id": 1,
    "result":" 0xa5b9ba"
}
```

Also, there
are [extensive instructions for using Postman](https://github.com/ledgerwatch/erigon/wiki/Using-Postman-to-Test-TurboGeth-RPC)
to test the RPC.

## FAQ

### Relations between prune options and RPC methods

Next options available (by `--prune` flag):

```
* h - prune history (ChangeSets, HistoryIndices - used to access historical state)
* r - prune receipts (Receipts, Logs, LogTopicIndex, LogAddressIndex - used by eth_getLogs and similar RPC methods)
* t - prune tx lookup (used to get transaction by hash)
* c - prune call traces (used by trace_* methods)
```

By default data pruned after 90K blocks, can change it by flags like `--prune.history.after=100_000`

Some methods, if not found historical data in DB, can fallback to old blocks re-execution - but it require `h`.

### RPC Implementation Status

Label "remote" means: `--private.api.addr` flag is required.

The following table shows the current implementation status of Erigon's RPC daemon.

| Command                                    | Avail   | Notes                                |
| ------------------------------------------ | ------- | ------------------------------------ |
| web3_clientVersion                         | Yes     |                                      |
| web3_sha3                                  | Yes     |                                      |
|                                            |         |                                      |
| net_listening                              | HC      | (`remote` hard coded returns true)   |
| net_peerCount                              | Limited | internal sentries only               |
| net_version                                | Yes     | `remote`.                            |
|                                            |         |                                      |
| eth_blockNumber                            | Yes     |                                      |
| eth_chainID/eth_chainId                    | Yes     |                                      |
| eth_protocolVersion                        | Yes     |                                      |
| eth_syncing                                | Yes     |                                      |
| eth_gasPrice                               | Yes     |                                      |
| eth_maxPriorityFeePerGas                   | Yes     |                                      |
| eth_feeHistory                             | Yes     |                                      |
| eth_forks                                  | Yes     |                                      |
|                                            |         |                                      |
| eth_getBlockByHash                         | Yes     |                                      |
| eth_getBlockByNumber                       | Yes     |                                      |
| eth_getBlockTransactionCountByHash         | Yes     |                                      |
| eth_getBlockTransactionCountByNumber       | Yes     |                                      |
| eth_getHeaderByHash                        | Yes     |                                      |
| eth_getHeaderByNumber                      | Yes     |                                      |
| eth_getUncleByBlockHashAndIndex            | Yes     |                                      |
| eth_getUncleByBlockNumberAndIndex          | Yes     |                                      |
| eth_getUncleCountByBlockHash               | Yes     |                                      |
| eth_getUncleCountByBlockNumber             | Yes     |                                      |
|                                            |         |                                      |
| eth_getTransactionByHash                   | Yes     |                                      |
| eth_getRawTransactionByHash                | Yes     |                                      |
| eth_getTransactionByBlockHashAndIndex      | Yes     |                                      |
| eth_retRawTransactionByBlockHashAndIndex   | Yes     |                                      |
| eth_getTransactionByBlockNumberAndIndex    | Yes     |                                      |
| eth_retRawTransactionByBlockNumberAndIndex | Yes     |                                      |
| eth_getTransactionReceipt                  | Yes     |                                      |
| eth_getBlockReceipts                       | Yes     |                                      |
|                                            |         |                                      |
| eth_estimateGas                            | Yes     |                                      |
| eth_getBalance                             | Yes     |                                      |
| eth_getCode                                | Yes     |                                      |
| eth_getTransactionCount                    | Yes     |                                      |
| eth_getStorageAt                           | Yes     |                                      |
| eth_call                                   | Yes     |                                      |
| eth_callBundle                             | Yes     |                                      |
| eth_createAccessList                       | Yes     |
|                                            |         |                                      |
| eth_newFilter                              | -       | not yet implemented                  |
| eth_newBlockFilter                         | -       | not yet implemented                  |
| eth_newPendingTransactionFilter            | -       | not yet implemented                  |
| eth_getFilterChanges                       | -       | not yet implemented                  |
| eth_uninstallFilter                        | -       | not yet implemented                  |
| eth_getLogs                                | Yes     |                                      |
|                                            |         |                                      |
| eth_accounts                               | No      | deprecated                           |
| eth_sendRawTransaction                     | Yes     | `remote`.                            |
| eth_sendTransaction                        | -       | not yet implemented                  |
| eth_sign                                   | No      | deprecated                           |
| eth_signTransaction                        | -       | not yet implemented                  |
| eth_signTypedData                          | -       | ????                                 |
|                                            |         |                                      |
| eth_getProof                               | -       | not yet implemented                  |
|                                            |         |                                      |
| eth_mining                                 | Yes     | returns true if --mine flag provided |
| eth_coinbase                               | Yes     |                                      |
| eth_hashrate                               | Yes     |                                      |
| eth_submitHashrate                         | Yes     |                                      |
| eth_getWork                                | Yes     |                                      |
| eth_submitWork                             | Yes     |                                      |
|                                            |         |                                      |
| eth_issuance                               | Yes     |                                      |
|                                            |         |                                      |
| eth_subscribe                              | Limited | Websock Only - newHeads,             |
|                                            |         | newPendingTransaction                |
| eth_unsubscribe                            | Yes     | Websock Only                         |
|                                            |         |                                      |
| debug_accountRange                         | Yes     | Private Erigon debug module          |
| debug_accountAt                            | Yes     | Private Erigon debug module          |
| debug_getModifiedAccountsByNumber          | Yes     |                                      |
| debug_getModifiedAccountsByHash            | Yes     |                                      |
| debug_storageRangeAt                       | Yes     |                                      |
| debug_traceTransaction                     | Yes     | Streaming (can handle huge results)  |
| debug_traceCall                            | Yes     | Streaming (can handle huge results)  |
|                                            |         |                                      |
| trace_call                                 | Yes     |                                      |
| trace_callMany                             | Yes     |                                      |
| trace_rawTransaction                       | -       | not yet implemented (come help!)     |
| trace_replayBlockTransactions              | yes     | stateDiff only (come help!)          |
| trace_replayTransaction                    | yes     | stateDiff only (come help!)          |
| trace_block                                | Yes     |                                      |
| trace_filter                               | Yes     | no pagination, but streaming         |
| trace_get                                  | Yes     |                                      |
| trace_transaction                          | Yes     |                                      |
|                                            |         |                                      |
| txpool_content                             | Yes     | `remote`                             |
| txpool_status                              | Yes     | `remote`                             |
|                                            |         |                                      |
| eth_getCompilers                           | No      | deprecated                           |
| eth_compileLLL                             | No      | deprecated                           |
| eth_compileSolidity                        | No      | deprecated                           |
| eth_compileSerpent                         | No      | deprecated                           |
|                                            |         |                                      |
| db_putString                               | No      | deprecated                           |
| db_getString                               | No      | deprecated                           |
| db_putHex                                  | No      | deprecated                           |
| db_getHex                                  | No      | deprecated                           |
|                                            |         |                                      |
| erigon_getHeaderByHash                     | Yes     | Erigon only                          |
| erigon_getHeaderByNumber                   | Yes     | Erigon only                          |
| erigon_getLogsByHash                       | Yes     | Erigon only                          |
| erigon_forks                               | Yes     | Erigon only                          |
| erigon_issuance                            | Yes     | Erigon only                          |
|                                            |         |                                      |
| starknet_call                              | Yes     | Starknet only                        |
| erigon_getLogsByHash                       | No      | Subset of `eth_getLogs`              |
|                                            |         |                                      |
| bor_getSnapshot                            | Yes     | Bor only                             |
| bor_getAuthor                              | Yes     | Bor only                             |
| bor_getSnapshotAtHash                      | Yes     | Bor only                             |
| bor_getSigners                             | Yes     | Bor only                             |
| bor_getSignersAtHash                       | Yes     | Bor only                             |
| bor_getCurrentProposer                     | Yes     | Bor only                             |
| bor_getCurrentValidators                   | Yes     | Bor only                             |
| bor_getRootHash                            | Yes     | Bor only                             |

This table is constantly updated. Please visit again.

### Clients getting timeout, but server load is low

In this case: increase default rate-limit - amount of requests server handle simultaneously - requests over this limit
will wait. Increase it - if your 'hot data' is small or have much RAM or see "request timeout" while server load is low.

```
turbo-bor --private.api.addr=localhost:9090 --private.api.ratelimit=1024
```

### Server load too high

Reduce `--private.api.ratelimit`

### Read DB directly without Json-RPC/Graphql

[./../../docs/programmers_guide/db_faq.md](./../../docs/programmers_guide/db_faq.md)

### Faster Batch requests

Currently batch requests are spawn multiple goroutines and process all sub-requests in parallel. To limit impact of 1
huge batch to other users - added flag `--rpc.batch.concurrency` (default: 2). Increase it to process large batches
faster.

Known Issue: if at least 1 request is "stremable" (has parameter of type \*jsoniter.Stream) - then whole batch will
processed sequentially (on 1 goroutine).
