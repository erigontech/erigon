- [Introduction](#introduction)
- [Getting Started](#getting-started)
    - [Running locally](#running-locally)
    - [Running remotely](#running-remotely)
    - [Healthcheck](#healthcheck)
    - [Testing](#testing)
- [FAQ](#faq)
    - [Relations between prune options and rpc methods](#relations-between-prune-options-and-rpc-methods)
    - [RPC Implementation Status](#rpc-implementation-status)
    - [Securing the communication between RPC daemon and Erigon instance via TLS and authentication](#securing-the-communication-between-rpc-daemon-and-erigon-instance-via-tls-and-authentication)
    - [Ethstats](#ethstats)
    - [Allowing only specific methods (Allowlist)](#allowing-only-specific-methods-allowlist)
    - [Server load too high](#server-load-too-high)
    - [Faster Batch requests](#faster-batch-requests)
- [For Developers](#for-developers)
    - [Code generation](#code-generation)

## Introduction

Erigon's `rpcdaemon` runs in its own separate process.

This brings many benefits including easier development, the ability to run multiple daemons at once, and the ability to
run the daemon remotely. It is possible to run the daemon locally as well (read-only) if both processes have access to
the data folder.

## Getting Started

The `rpcdaemon` gets built as part of the main `erigon` build process, but you can build it directly with this command:

```[bash]
make rpcdaemon
```

### Running locally

Run `rpcdaemon` on the same computer with Erigon. This is the default option because it uses Shared Memory access to
Erigon's db -
it's much faster than TCP access. Provide both `--datadir` and `--private.api.addr` flags:

```[bash]
make erigon
./build/bin/erigon --datadir=<your_data_dir> --private.api.addr=localhost:9090
make rpcdaemon
./build/bin/rpcdaemon --datadir=<your_data_dir> --txpool.api.addr=localhost:9090 --private.api.addr=localhost:9090 --http.api=eth,erigon,web3,net,debug,trace,txpool
```

Note that we've also specified which RPC namespaces to enable in the above command by `--http.api` flag.

### Running remotely

To start the daemon remotely - just don't set the `--datadir` flag:

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

There are 2 options for running healtchecks: POST request or a GET request with custom headers. Both options are
available at the `/health` endpoint.

#### POST request

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

**`min_peer_count`** -- checks for minimum of healthy node peers. Requires
`net` namespace to be listed in `http.api`.

**`known_block`** -- sets up the block that node has to know about. Requires
`eth` namespace to be listed in `http.api`.

Example request
`http POST http://localhost:8545/health --raw '{"min_peer_count": 3, "known_block": "0x1F"}'`
Example response

```json
{
  "check_block": "HEALTHY",
  "healthcheck_query": "HEALTHY",
  "min_peer_count": "HEALTHY"
}
```

#### GET with headers

If the healthcheck is successful it will return a 200 status code.

If the healthcheck fails for any reason a status 500 will be returned. This is true if one of the criteria requested
fails its check.

You can set any number of values on the `X-ERIGON-HEALTHCHECK` header. Ones that are not included are skipped in the
checks.

Available Options:

- `synced` - will check if the node has completed syncing
- `min_peer_count<count>` - will check that the node has at least `<count>` many peers
- `check_block<block>` - will check that the node is at least ahead of the `<block>` specified
- `max_seconds_behind<seconds>` - will check that the node is no more than `<seconds>` behind from its latest block

Example Request

```bash
curl --location --request GET 'http://localhost:8545/health' \
--header 'X-ERIGON-HEALTHCHECK: min_peer_count1' \
--header 'X-ERIGON-HEALTHCHECK: synced' \
--header 'X-ERIGON-HEALTHCHECK: max_seconds_behind600'
```

Example Response

```json
{
  "check_block": "DISABLED",
  "max_seconds_behind": "HEALTHY",
  "min_peer_count": "HEALTHY",
  "synced": "HEALTHY"
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
are [extensive instructions for using Postman](https://github.com/erigontech/erigon/wiki/Using-Postman-to-Test-TurboGeth-RPC)
to test the RPC.

### Debugging

To print more detailed logs for 1 request - add `--rpc.dbg.single=true` flag. Then can send HTTP header `"dbg: true"`:

```
curl -X POST -H "dbg: true" -H "Content-Type: application/json" --data '{"jsonrpc": "2.0", "method": "eth_blockNumber", "params": [], "id":1}' localhost:8545
```

## FAQ

### Relations between prune options and RPC methods

Next options available (by `--prune` flag):

```
* h - prune history (ChangeSets, HistoryIndices - used to access historical state, like eth_getStorageAt, eth_getBalanceAt, debug_traceTransaction, trace_block, trace_transaction, etc.)
* r - prune receipts (Receipts, Logs, LogTopicIndex, LogAddressIndex - used by eth_getLogs and similar RPC methods)
* t - prune txn lookup (used to get transaction by hash)
* c - prune call traces (used by trace_filter method)
```

By default data pruned after 90K blocks, can change it by flags like `--prune.history.after=100_000`

Some methods, if not found historical data in DB, can fallback to old blocks re-execution - but it requires `h`.

### The --http.url flag

the `--http.url` flag is an optional flag which allows one to bind the HTTP server to a socket, for
example, `tcp6://:8545` or `unix:///erigon_http.socket`

If the `--http.url` flag is set, then `--http.addr` and `--http.port` with both be ignored.

note that this is NOT geth-style IPC. for that, read the next section, IPC endpoint(geth-compatible)

### HTTPS, HTTP2, and H2C

Erigon supports HTTPS, HTTP2, and H2C out of the box. H2C is served by the default HTTP handler.

To enable the HTTPS+HTTP2 server, add flag `--https.enabled`, along with providing flags `--https.cert="/path/to.cert"`
and `--https.key=/path/to.key`

By default, the HTTPS server will run on the HTTP port + 363. use flag `--https.port` to set the port

The HTTPS server will inherit all other configuration parameters from http, for instance, enabling the websocket server,
cors domains, or enabled namespaces

If the `--https.url` flag is set, then `--https.addr` and `--https.port` with both be ignored.

### IPC endpoint (geth compatible)

erigon supports the geth-style unix socket IPC. you can enable this with `--socket.enabled` flag,
and setting the `--socket.url` flag. For instance, if you wanted the socket to exist at `/var/run/erigon.ipc`,
you would do `--socket.url=unix:///var/run/erigon.ipc`

you can also use `--socket.url=tcp://<addr>:<port>` to serve the raw jsonrpc2 protocol over tcp

the socket will inherit the namespaces from `http.api`

### RPC Implementation Status

Label "remote" means: `--private.api.addr` flag is required.

The following table shows the current implementation status of Erigon's RPC daemon.

| Command                                    | Avail   | Notes                                                 |
|--------------------------------------------|---------|-------------------------------------------------------|
| admin_nodeInfo                             | Yes     |                                                       |
| admin_peers                                | Yes     |                                                       |
| admin_addPeer                              | Yes     |                                                       |
| admin_removePeer                           | Yes     |                                                       |
|                                            |         |                                                       |
| web3_clientVersion                         | Yes     |                                                       |
| web3_sha3                                  | Yes     |                                                       |
|                                            |         |                                                       |
| net_listening                              | HC      | (`remote` hard coded returns true)                    |
| net_peerCount                              | Limited | only internal sentries counted                        |
| net_version                                | Yes     | `remote`.                                             |
|                                            |         |                                                       |
| eth_blockNumber                            | Yes     |                                                       |
| eth_chainID/eth_chainId                    | Yes     |                                                       |
| eth_protocolVersion                        | Yes     |                                                       |
| eth_syncing                                | Yes     |                                                       |
| eth_gasPrice                               | Yes     |                                                       |
| eth_maxPriorityFeePerGas                   | Yes     |                                                       |
| eth_feeHistory                             | Yes     |                                                       |
|                                            |         |                                                       |
| eth_getBlockByHash                         | Yes     |                                                       |
| eth_getBlockByNumber                       | Yes     |                                                       |
| eth_getBlockTransactionCountByHash         | Yes     |                                                       |
| eth_getBlockTransactionCountByNumber       | Yes     |                                                       |
| eth_getUncleByBlockHashAndIndex            | Yes     |                                                       |
| eth_getUncleByBlockNumberAndIndex          | Yes     |                                                       |
| eth_getUncleCountByBlockHash               | Yes     |                                                       |
| eth_getUncleCountByBlockNumber             | Yes     |                                                       |
|                                            |         |                                                       |
| eth_getTransactionByHash                   | Yes     |                                                       |
| eth_getRawTransactionByHash                | Yes     |                                                       |
| eth_getTransactionByBlockHashAndIndex      | Yes     |                                                       |
| eth_retRawTransactionByBlockHashAndIndex   | Yes     |                                                       |
| eth_getTransactionByBlockNumberAndIndex    | Yes     |                                                       |
| eth_retRawTransactionByBlockNumberAndIndex | Yes     |                                                       |
| eth_getTransactionReceipt                  | Yes     |                                                       |
| eth_getBlockReceipts                       | Yes     |                                                       |
|                                            |         |                                                       |
| eth_estimateGas                            | Yes     |                                                       |
| eth_getBalance                             | Yes     |                                                       |
| eth_getCode                                | Yes     |                                                       |
| eth_getTransactionCount                    | Yes     |                                                       |
| eth_getStorageAt                           | Yes     |                                                       |
| eth_call                                   | Yes     |                                                       |
| eth_callMany                               | Yes     | Erigon Method PR#4567                                 |
| eth_callBundle                             | Yes     |                                                       |
| eth_createAccessList                       | Yes     |                                                       |
|                                            |         |                                                       |
| eth_newFilter                              | Yes     | Added by PR#4253                                      |
| eth_newBlockFilter                         | Yes     |                                                       |
| eth_newPendingTransactionFilter            | Yes     |                                                       |
| eth_getFilterLogs                          | Yes     | Added by PR#6514                                      |
| eth_getFilterChanges                       | Yes     |                                                       |
| eth_uninstallFilter                        | Yes     |                                                       |
| eth_getLogs                                | Yes     |                                                       |
|                                            |         |                                                       |
| eth_accounts                               | No      | deprecated                                            |
| eth_sendRawTransaction                     | Yes     | `remote`.                                             |
| eth_sendTransaction                        | -       | not yet implemented                                   |
| eth_sign                                   | No      | deprecated                                            |
| eth_signTransaction                        | -       | not yet implemented                                   |
| eth_signTypedData                          | -       | ????                                                  |
|                                            |         |                                                       |
| eth_getProof                               | Yes     | Limited to last 100000 blocks                         |
|                                            |         |                                                       |
| eth_mining                                 | Yes     | returns true if --mine flag provided                  |
| eth_coinbase                               | Yes     |                                                       |
| eth_hashrate                               | Yes     |                                                       |
| eth_submitHashrate                         | Yes     |                                                       |
| eth_getWork                                | Yes     |                                                       |
| eth_submitWork                             | Yes     |                                                       |
|                                            |         |                                                       |
| eth_subscribe                              | Limited | Websock Only - newHeads,                              |
|                                            |         | newPendingTransactionsWithBody,                       |
|                                            |         | newPendingTransactions,                               |
|                                            |         | newPendingBlock                                       |
|                                            |         | logs                                                  |
| eth_unsubscribe                            | Yes     | Websock Only                                          |
|                                            |         |                                                       |
| engine_newPayloadV1                        | Yes     |                                                       |
| engine_newPayloadV2                        | Yes     |                                                       |
| engine_newPayloadV3                        | Yes     |                                                       |
| engine_newPayloadV4                        | Yes     | Added in Pectra                                       |
| engine_forkchoiceUpdatedV1                 | Yes     |                                                       |
| engine_forkchoiceUpdatedV2                 | Yes     |                                                       |
| engine_forkchoiceUpdatedV3                 | Yes     |                                                       |
| engine_getPayloadV1                        | Yes     |                                                       |
| engine_getPayloadV2                        | Yes     |                                                       |
| engine_getPayloadV3                        | Yes     |                                                       |
| engine_getPayloadV4                        | Yes     | Added in Pectra                                       |
| engine_getPayloadBodiesByHashV1            | Yes     |                                                       |
| engine_getPayloadBodiesByRangeV1           | Yes     |                                                       |
| engine_getClientVersionV1                  | Yes     |                                                       |
| engine_getBlobsV1                          | Yes     |                                                       |
|                                            |         |                                                       |
| debug_getRawReceipts                       | Yes     | `debug_` expected to be private                       |
| debug_accountRange                         | Yes     |                                                       |
| debug_accountAt                            | Yes     |                                                       |
| debug_getModifiedAccountsByNumber          | Yes     |                                                       |
| debug_getModifiedAccountsByHash            | Yes     |                                                       |
| debug_storageRangeAt                       | Yes     | see https://github.com/erigontech/erigon/issues/14186 |
| debug_traceBlockByHash                     | Yes     | Streaming (can handle huge results)                   |
| debug_traceBlockByNumber                   | Yes     | Streaming (can handle huge results)                   |
| debug_traceTransaction                     | Yes     | Streaming (can handle huge results)                   |
| debug_traceCall                            | Yes     | Streaming (can handle huge results)                   |
| debug_traceCallMany                        | Yes     | Erigon Method PR#4567.                                |
| debug_setMemoryLimit                       | Yes     |                                                       |
| debug_setGCPercent                         | Yes     |                                                       |
| debug_freeOSMemory                         | Yes     |                                                       |
| debug_gcStats                              | Yes     |                                                       |
| debug_memStats                             | Yes     |                                                       |
|                                            |         |                                                       |
| trace_call                                 | Yes     |                                                       |
| trace_callMany                             | Yes     |                                                       |
| trace_rawTransaction                       | -       | not yet implemented (come help!)                      |
| trace_replayBlockTransactions              | yes     | stateDiff only (come help!)                           |
| trace_replayTransaction                    | yes     | stateDiff only (come help!)                           |
| trace_block                                | Yes     |                                                       |
| trace_filter                               | Yes     | no pagination, but streaming                          |
| trace_get                                  | Yes     |                                                       |
| trace_transaction                          | Yes     |                                                       |
|                                            |         |                                                       |
| txpool_content                             | Yes     | `remote`                                              |
| txpool_contentFrom                         | Yes     | `remote`                                              |
| txpool_status                              | Yes     | `remote`                                              |
|                                            |         |                                                       |
| eth_getCompilers                           | No      | deprecated                                            |
| eth_compileLLL                             | No      | deprecated                                            |
| eth_compileSolidity                        | No      | deprecated                                            |
| eth_compileSerpent                         | No      | deprecated                                            |
|                                            |         |                                                       |
| db_putString                               | No      | deprecated                                            |
| db_getString                               | No      | deprecated                                            |
| db_putHex                                  | No      | deprecated                                            |
| db_getHex                                  | No      | deprecated                                            |
|                                            |         |                                                       |
| erigon_getHeaderByHash                     | Yes     | Erigon only                                           |
| erigon_getBlockReceiptsByBlockHash         | Yes     | Erigon only                                           |
| erigon_getHeaderByNumber                   | Yes     | Erigon only                                           |
| erigon_getLogsByHash                       | Yes     | Erigon only                                           |
| erigon_forks                               | Yes     | Erigon only                                           |
| erigon_getBlockByTimestamp                 | Yes     | Erigon only                                           |
| erigon_BlockNumber                         | Yes     | Erigon only                                           |
| erigon_getLatestLogs                       | Yes     | Erigon only                                           |
|                                            |         |                                                       |
| bor_getSnapshot                            | Yes     | Bor only                                              |
| bor_getAuthor                              | Yes     | Bor only                                              |
| bor_getSnapshotAtHash                      | Yes     | Bor only                                              |
| bor_getSigners                             | Yes     | Bor only                                              |
| bor_getSignersAtHash                       | Yes     | Bor only                                              |
| bor_getCurrentProposer                     | Yes     | Bor only                                              |
| bor_getCurrentValidators                   | Yes     | Bor only                                              |
| bor_getSnapshotProposerSequence            | Yes     | Bor only                                              |
| bor_getRootHash                            | Yes     | Bor only                                              |
| bor_getVoteOnHash                          | Yes     | Bor only                                              |

### GraphQL

| Command         | Avail | Notes |
|-----------------|-------|-------|
| GetBlockDetails | Yes   |       |
| GetChainID      | Yes   |       |

This table is constantly updated. Please visit again.

### Securing the communication between RPC daemon and Erigon instance via TLS and authentication

In some cases, it is useful to run Erigon nodes in a different network (for example, in a Public cloud), but RPC daemon
locally. To ensure the integrity of communication and access control to the Erigon node, TLS authentication can be
enabled. On the high level, the process consists of these steps (this process needs to be done for any "cluster" of
Erigon and RPC daemon nodes that are supposed to work together):

1. Generate key pair for the Certificate Authority (CA). The private key of CA will be used to authorise new Erigon
   instances as well as new RPC daemon instances, so that they can mutually authenticate.
2. Create CA certificate file that needs to be deployed on any Erigon instance and any RPC daemon. This CA cert file is
   used as a "root of trust", whatever is in it, will be trusted by the participants when they authenticate their
   counterparts.
3. For each Erigon instance and each RPC daemon instance, generate a key pair. If you are lazy, you can generate one
   pair for all Erigon nodes, and one pair for all RPC daemons, and copy these keys around.
4. Using the CA private key, create certificate file for each public key generated on the previous step. This
   effectively "inducts" these keys into the "cluster of trust".
5. On each instance, deploy 3 files - CA certificate, instance key, and certificate signed by CA for this instance key.

Following is the detailed description of how it can be done using `openssl` suite of tools.

Generate CA key pair using Elliptic Curve (as opposed to RSA). The generated CA key will be in the file `CA-key.pem`.
Access to this file will allow anyone to later include any new instance key pair into the "cluster of trust", so keep it
secure.

```
openssl ecparam -name prime256v1 -genkey -noout -out CA-key.pem
```

Create CA self-signed certificate (this command will ask questions, answers aren't important for now). The file created
by this command is `CA-cert.pem`

```
openssl req -x509 -new -nodes -key CA-key.pem -sha256 -days 3650 -out CA-cert.pem
```

For Erigon node, generate a key pair:

```
openssl ecparam -name prime256v1 -genkey -noout -out erigon-key.pem
```

Also, generate one for the RPC daemon:

```
openssl ecparam -name prime256v1 -genkey -noout -out RPC-key.pem
```

Now create certificate signing request for Erigon key pair:

```
openssl req -new -key erigon-key.pem -out erigon.csr
```

And from this request, produce the certificate (signed by CA), proving that this key is now part of the "cluster of
trust"

```
openssl x509 -req -in erigon.csr -CA CA-cert.pem -CAkey CA-key.pem -CAcreateserial -out erigon.crt -days 3650 -sha256
```

Then, produce the certificate signing request for RPC daemon key pair:

```
openssl req -new -key RPC-key.pem -out RPC.csr
```

And from this request, produce the certificate (signed by CA), proving that this key is now part of the "cluster of
trust"

```
openssl x509 -req -in RPC.csr -CA CA-cert.pem -CAkey CA-key.pem -CAcreateserial -out RPC.crt -days 3650 -sha256
```

When this is all done, these three files need to be placed on the machine where Erigon is running: `CA-cert.pem`
, `erigon-key.pem`, `erigon.crt`. And Erigon needs to be run with these extra options:

```
--tls --tls.cacert CA-cert.pem --tls.key erigon-key.pem --tls.cert erigon.crt
```

On the RPC daemon machine, these three files need to be placed: `CA-cert.pem`, `RPC-key.pem`, and `RPC.crt`. And RPC
daemon needs to be started with these extra options:

```
--tls.key RPC-key.pem --tls.cacert CA-cert.pem --tls.cert RPC.crt
```

**WARNING** Normally, the "client side" (which in our case is RPC daemon), verifies that the host name of the server
matches the "Common Name" attribute of the "server" certificate. At this stage, this verification is turned off, and it
will be turned on again once we have updated the instruction above on how to properly generate certificates with "Common
Name".

When running Erigon instance in the Google Cloud, for example, you need to specify the **Internal IP** in
the `--private.api.addr` option. And, you will need to open the firewall on the port you are using, so that connection
to the Erigon instances can be made.

### Ethstats

This version of the RPC daemon is compatible with [ethstats-client](https://github.com/goerli/ethstats-client).

To run ethstats, run the RPC daemon remotely and open some of the APIs.

`./build/bin/rpcdaemon --private.api.addr=localhost:9090 --http.api=net,eth,web3`

Then update your `app.json` for ethstats-client like that:

```json
[
  {
    "name": "ethstats",
    "script": "app.js",
    "log_date_format": "YYYY-MM-DD HH:mm Z",
    "merge_logs": false,
    "watch": false,
    "max_restarts": 10,
    "exec_interpreter": "node",
    "exec_mode": "fork_mode",
    "env": {
      "NODE_ENV": "production",
      "RPC_HOST": "localhost",
      "RPC_PORT": "8545",
      "LISTENING_PORT": "30303",
      "INSTANCE_NAME": "Erigon node",
      "CONTACT_DETAILS": "<your twitter handle>",
      "WS_SERVER": "wss://ethstats.net/api",
      "WS_SECRET": "<put your secret key here>",
      "VERBOSITY": 2
    }
  }
]
```

Run ethstats-client through pm2 as usual.

You will see these warnings in the RPC daemon output, but they are expected

```
WARN [11-05|09:03:47.911] Served                                   conn=127.0.0.1:59753 method=eth_newBlockFilter reqid=5 t="21.194µs" err="the method eth_newBlockFilter does not exist/is not available"
WARN [11-05|09:03:47.911] Served                                   conn=127.0.0.1:59754 method=eth_newPendingTransactionFilter reqid=6 t="9.053µs"  err="the method eth_newPendingTransactionFilter does not exist/is not available"
```

### Allowing only specific methods (Allowlist)

In some cases you might want to only allow certain methods in the namespaces and hide others. That is possible
with `rpc.accessList` flag.

1. Create a file, say, `rules.json`

2. Add the following content

```json
{
  "allow": ["net_version", "web3_eth_getBlockByHash"]
}
```

3. Provide this file to the rpcdaemon using `--rpc.accessList` flag

```
> rpcdaemon --private.api.addr=localhost:9090 --http.api=eth,debug,net,web3 --rpc.accessList=rules.json
```

Now only these two methods are available.

### Clients getting timeout, but server load is low

In this case: increase default rate-limit - amount of requests server handle simultaneously - requests over this limit
will wait. Increase it - if your 'hot data' is small or have much RAM or see "request timeout" while server load is low.

```
./build/bin/erigon --private.api.addr=localhost:9090 --private.api.ratelimit=1024
```

### Server load too high

Reduce `--private.api.ratelimit`

### Read DB directly without Json-RPC/Graphql

[./../../docs/programmers_guide/db_faq.md](./../../docs/programmers_guide/db_faq.md)

### Faster Batch requests

Currently batch requests are spawn multiple goroutines and process all sub-requests in parallel. To limit impact of 1
huge batch to other users - added flag `--rpc.batch.concurrency` (default: 2). Increase it to process large batches
faster.

Known Issue: if at least 1 request is "streamable" (has parameter of type \*jsoniter.Stream) - then whole batch will
processed sequentially (on 1 goroutine).

## For Developers

### Code generation

`go.mod` stores right version of generators, use `make grpc` to install it and generate code (it also installs protoc
into ./build/bin folder).
