In turbo-geth RPC calls are extracted out of the main binary into a separate daemon.
This daemon can use both local or remote DBs. That means, that this RPC daemon
doesn't have to be running on the same machine as the main turbo-geth binary or
it can run from a snapshot of a database for read-only calls. [Docs](./cmd/rpcdaemon/Readme.md)

### Get started
**For local DB**

```
> make rpcdaemon
> ./build/bin/rpcdaemon --chaindata ~/Library/TurboGeth/tg/chaindata --http.api=eth,debug,net
```
**For remote DB**

Run turbo-geth in one terminal window

```
> ./build/bin/tg --private.api.addr=localhost:9090
```

Run RPC daemon
```
> ./build/bin/rpcdaemon --private.api.addr=localhost:9090
```

### Test

Try `eth_blockNumber` call. In another console/tab, use `curl` to make RPC call:

```
curl -X POST -H "Content-Type: application/json" --data '{"jsonrpc":"2.0","method":"eth_blockNumber", "params": [], "id":1}' localhost:8545
```

It should return something like this (depending on how far your turbo-geth node has synced):

```
{"jsonrpc":"2.0","id":1,"result":823909}
```

### For Developers

**Code generation**: `go.mod` stores right version of generators, use `mage grpc` to install it and generate code.

`protoc` version not managed but recommended version is 3.x, [install instruction](https://grpc.io/docs/protoc-installation/)

### RPC Implementation Status

eth_getBalance
eth_getCode
eth_getTransactionCount
eth_getStorageAt
eth_call
When requests are made that act on the state of ethereum, the last default block parameter determines the height of the block.

The following options are possible for the defaultBlock parameter:

HEX String - an integer block number
String "earliest" for the earliest/genesis block
String "latest" - for the latest mined block
String "pending" - for the pending state/transactions
Curl Examples Explained
The curl options below might return a response where the node complains about the content type, this is because the --data option sets the content type to application/x-www-form-urlencoded . If your node does complain, manually set the header by placing -H “Content-Type: application/json” at the start of the call.

The examples also do not include the URL/IP & port combination which must be the last argument given to curl e.x. 127.0.0.1:8545

| Command                                       | Available | Notes                                             |
| --------------------------------------------- | --------- | ------------------------------------------------- |
| _**------------ Web3 ------------**_          |           |
| web3_clientVersion                            | Y         |
| web3_sha3                                     | Y         |
|                                               |           |
| _**------------ Net ------------**_           |           |
| net_listening                                 | -         |
| net_peerCount\*                               | Y         | Returns a count of 25 as work continues on Sentry |
| net_version                                   | Y         |
|                                               |           |
| _**------------ System ------------**_        |           |
| eth_blockNumber                               | Y         |
| eth_chainID                                   | -         |
| eth_protocolVersion                           | -         |
| eth_syncing                                   | Y         |
| eth_estimateGas                               | Y         |
| eth_gasPrice                                  | -         |
|                                               |           |
| _**------------ Blocks/Uncles ------------**_ |           |
| eth_getBlockByHash                            | Y         |
| eth_getBlockByNumber                          | Y         |
| eth_getBlockTransactionCountByHash            | Y         |
| eth_getBlockTransactionCountByNumber          | Y         |
| eth_getUncleByBlockHashAndIndex               | Y         |
| eth_getUncleByBlockNumberAndIndex             | Y         |
| eth_getUncleCountByBlockHash                  | Y         |
| eth_getUncleCountByBlockNumber                | Y         |
|                                               |           |
| _**------------ Transactions ------------**_  |           |
| eth_getTransactionByHash                      | Y         |
| eth_getTransactionByBlockHashAndIndex         | Y         |
| eth_getTransactionByBlockNumberAndIndex       | Y         |
| eth_getTransactionReceipt                     | Y         |
| eth_getLogs                                   | Y         |
|                                               |           |
| _**------------ State ------------**_         |           |
| eth_getBalance                                | Y         |
| eth_getCode                                   | Y         |
| eth_getStorageAt                              | Y         |
|                                               |           |
| _**------------ Filters ------------**_       |           |
| eth_newFilter                                 | -         |
| eth_newBlockFilter                            | -         |
| eth_newPendingTransactionFilter               | -         |
| eth_getFilterChanges                          | -         |
| eth_getFilterLogs                             | -         |
| eth_uninstallFilter                           | -         |
|                                               |           |
| _**------------ Accounts ------------**_      |           |
| eth_accounts                                  |           |
| eth_call                                      | Y         |
| eth_getTransactionCount                       | Y         |
| eth_sendRawTransaction                        | Y         |
| eth_sendTransaction                           | -         |
| eth_sign                                      | -         |
| eth_signTransaction                           | -         |
| eth_signTypedData                             | -         |
|                                               |           |
| _**------------ ????? ------------**_         |           |
| eth_getProof                                  | -         |
|                                               |           |
| _**------------ Mining ------------**_        |           |
| eth_mining                                    | -         |
| eth_coinbase                                  | Y         |
| eth_hashrate                                  | -         |
| eth_submitHashrate                            | -         |
| eth_getWork                                   | -         |
| eth_submitWork                                | -         |
|                                               |           |
| _**------------ Debug ------------**_         |           |
| debug_accountRange                            | Y         |
| debug_getModifiedAccountsByNumber             | Y         |
| debug_getModifiedAccountsByHash               | Y         |
| debug_storageRangeAt                          | Y         |
| debug_traceTransaction                        | Y         |
|                                               |           |
| _\*\*------------ trace_ ------------\*\*\_   |           |
| trace_filter                                  | Y         |
|                                               |           |
| _\*\*------------ Retired ------------\*\*\_  |           |
| eth_getCompilers                              | N         |
| eth_compileLLL                                | N         |
| eth_compileSolidity                           | N         |
| eth_compileSerpent                            | N         |
|                                               |           |
| db_putString                                  | N         |
| db_getString                                  | N         |
| db_putHex                                     | N         |
| db_getHex                                     | N         |
|                                               |           |
| shh_post                                      | N         |
| shh_version                                   | N         |
| shh_newIdentity                               | N         |
| shh_hasIdentity                               | N         |
| shh_newGroup                                  | N         |
| shh_addToGroup                                | N         |
| shh_newFilter                                 | N         |
| shh_uninstallFilter                           | N         |
| shh_getFilterChanges                          | N         |
| shh_getMessages                               | N         |
