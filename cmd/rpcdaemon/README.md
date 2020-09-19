## Introduction

turbo-geth's `rpcdaemon` runs in its own seperate process.

This brings many benefits including easier development, the ability to run multiple daemons at once, and the ability to run the daemon remotely. It is possible to run the daemon locally as well (read-only) if both processes have access to the data folder.

## Getting Started

The `rpcdaemon` gets built as part of the main `turbo-geth` build process, but you can build it directly with this command:

```[bash]
make rpcdaemon
```

### Running locally

If you have direct access to turbo-geth's database folder, you may run the `rpcdaemon` locally. This may provide faster results.

After building, run this command to start the daemon locally:

```[bash]
./build/bin/rpcdaemon --chaindata ~/Library/TurboGeth/tg/chaindata --http.api=eth,debug,net,web3
```

Note that we've also specified which RPC commands to enable in the above command.

### Running remotely

To start the daemon remotely, build it as described above, then run `turbo-geth` in one terminal window:

```[bash]
./build/bin/tg --private.api.addr=localhost:9090
```

In another terminal window, start the daemon with the same `--private-api` setting:

```[bash]
./build/bin/rpcdaemon --private.api.addr=localhost:9090
```

The daemon should respond with something like:

```[bash]
INFO [date-time] HTTP endpoint opened url=localhost:8545...
```

## Testing

By default, the `rpcdaemon` serves data from `localhost:8545`. You may send `curl` commands to see if things are working.

Try `eth_blockNumber` for example. In a third terminal window enter this command:

```[bash]
curl -X POST -H "Content-Type: application/json" --data '{"jsonrpc": "2.0", "method": "eth_blockNumber", "params": [], "id":1}' localhost:8545
```

This should return something along the lines of this (depending on how far your turbo-geth node has synced):

```[bash]
{
    "jsonrpc": "2.0",
    "id": 1,
    "result":" 0xa5b9ba"
}
```

## RPC Implementation Status

The following table shows the current implementation status of turbo-geth's RPC daemon.

| Command                                 | Avail | Notes                                      |
| --------------------------------------- | ----- | ------------------------------------------ |
| web3_clientVersion                      | Yes   |                                            |
| web3_sha3                               | Yes   |                                            |
|                                         |       |                                            |
| net_listening                           | HC    | (hard coded returns true)                  |
| net_peerCount                           | HC    | (hard coded 25 - work continues on Sentry) |
| net_version                             | Yes   |                                            |
|                                         |       |                                            |
| eth_blockNumber                         | Yes   |                                            |
| eth_chainID                             | Yes   |                                            |
| eth_protocolVersion                     | Yes   |                                            |
| eth_syncing                             | Yes   |                                            |
| eth_gasPrice                            | -     |                                            |
|                                         |       |                                            |
| eth_getBlockByHash                      | Yes   |                                            |
| eth_getBlockByNumber                    | Yes   |                                            |
| eth_getBlockTransactionCountByHash      | Yes   |                                            |
| eth_getBlockTransactionCountByNumber    | Yes   |                                            |
| eth_getHeaderByHash                     | Yes   | turbo-geth only                            |
| eth_getHeaderByNumber                   | Yes   | turbo-geth only                            |
| eth_getUncleByBlockHashAndIndex         | Yes   |                                            |
| eth_getUncleByBlockNumberAndIndex       | Yes   |                                            |
| eth_getUncleCountByBlockHash            | Yes   |                                            |
| eth_getUncleCountByBlockNumber          | Yes   |                                            |
|                                         |       |                                            |
| eth_getTransactionByHash                | Yes   |                                            |
| eth_getTransactionByBlockHashAndIndex   | Yes   |                                            |
| eth_getTransactionByBlockNumberAndIndex | Yes   |                                            |
| eth_getTransactionReceipt               | Yes   |                                            |
| eth_getLogsByHash                       | Yes   | turbo-geth only (all logs in block)        |
|                                         |       |                                            |
| eth_estimateGas                         | Yes   |                                            |
| eth_getBalance                          | Yes   |                                            |
| eth_getCode                             | Yes   |                                            |
| eth_getTransactionCount                 | Yes   |                                            |
| eth_getStorageAt                        | Yes   |                                            |
| eth_call                                | Yes   |                                            |
|                                         |       |                                            |
| eth_newFilter                           | -     |                                            |
| eth_newBlockFilter                      | -     |                                            |
| eth_newPendingTransactionFilter         | -     |                                            |
| eth_getFilterChanges                    | -     |                                            |
| eth_getFilterLogs                       | -     |                                            |
| eth_uninstallFilter                     | -     |                                            |
| eth_getLogs                             | Yes   |                                            |
|                                         |       |                                            |
| eth_accounts                            | -     |                                            |
| eth_sendRawTransaction                  | Yes   |                                            |
| eth_sendTransaction                     | -     |                                            |
| eth_sign                                | -     |                                            |
| eth_signTransaction                     | -     |                                            |
| eth_signTypedData                       | -     |                                            |
|                                         |       |                                            |
| eth_getProof                            | -     |                                            |
|                                         |       |                                            |
| eth_mining                              | -     |                                            |
| eth_coinbase                            | Yes   |                                            |
| eth_hashrate                            | -     |                                            |
| eth_submitHashrate                      | -     |                                            |
| eth_getWork                             | -     |                                            |
| eth_submitWork                          | -     |                                            |
|                                         |       |                                            |
| debug_accountRange                      | Yes   | Private turbo-geth debug module            |
| debug_getModifiedAccountsByNumber       | Yes   |                                            |
| debug_getModifiedAccountsByHash         | Yes   |                                            |
| debug_storageRangeAt                    | Yes   |                                            |
| debug_traceTransaction                  | Yes   |                                            |
|                                         |       |                                            |
| trace_call                              | -     | From OpenEthereum trace module             |
| trace_callMany                          | -     |                                            |
| trace_rawTransaction                    | -     |                                            |
| trace_replayBlockTransactions           | -     |                                            |
| trace_replayTransaction                 | -     |                                            |
| trace_block                             | -     |                                            |
| trace_filter                            | Yes   |                                            |
| trace_get                               | -     |                                            |
| trace_transaction                       | -     |                                            |
|                                         |       |                                            |
| eth_getCompilers                        | No    | retired                                    |
| eth_compileLLL                          | No    | retired                                    |
| eth_compileSolidity                     | No    | retired                                    |
| eth_compileSerpent                      | No    | retired                                    |
|                                         |       |                                            |
| db_putString                            | No    | retired                                    |
| db_getString                            | No    | retired                                    |
| db_putHex                               | No    | retired                                    |
| db_getHex                               | No    | retired                                    |
|                                         |       |                                            |
| shh_post                                | No    | retired                                    |
| shh_version                             | No    | retired                                    |
| shh_newIdentity                         | No    | retired                                    |
| shh_hasIdentity                         | No    | retired                                    |
| shh_newGroup                            | No    | retired                                    |
| shh_addToGroup                          | No    | retired                                    |
| shh_newFilter                           | No    | retired                                    |
| shh_uninstallFilter                     | No    | retired                                    |
| shh_getFilterChanges                    | No    | retired                                    |
| shh_getMessages                         | No    | retired                                    |

This table is constantly updated. Please visit again.

## For Developers

### Code generation

`go.mod` stores right version of generators, use `make grpc` to install it and generate code.

Recommended `protoc` version is 3.x. [Installation instructions](https://grpc.io/docs/protoc-installation/)
