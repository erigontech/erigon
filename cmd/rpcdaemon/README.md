- [Introduction](#introduction)
- [Getting Started](#getting-started)
    * [Running locally](#running-locally)
    * [Running remotely](#running-remotely)
    * [Running in dual mode](#running-in-dual-mode)
    * [Testing](#testing)
- [FAQ](#faq)
    * [RPC Implementation Status](#rpc-implementation-status)
    * [Securing the communication between RPC daemon and TG instance via TLS and authentication](#securing-the-communication-between-rpc-daemon-and-tg-instance-via-tls-and-authentication)
    * [Ethstats](#ethstats)
    * [Allowing only specific methods (Allowlist)](#allowing-only-specific-methods--allowlist-)
    * [Trace transactions progress](#trace-transactions-progress)
    * [Clients getting timeout, but server load is low](#clients-getting-timeout--but-server-load-is-low)
    * [Server load too high](#server-load-too-high)
- [For Developers](#for-developers)
    * [Code generation](#code-generation)

## Introduction

turbo-geth's `rpcdaemon` runs in its own seperate process.

This brings many benefits including easier development, the ability to run multiple daemons at once, and the ability to
run the daemon remotely. It is possible to run the daemon locally as well (read-only) if both processes have access to
the data folder.

## Getting Started

The `rpcdaemon` gets built as part of the main `turbo-geth` build process, but you can build it directly with this
command:

```[bash]
make rpcdaemon
```

### Running locally

If you have direct access to turbo-geth's database folder, you may run the `rpcdaemon` locally. This may provide faster
results.

After building, run this command to start the daemon locally:

```[bash]
./build/bin/rpcdaemon --chaindata ~/Library/TurboGeth/tg/chaindata --http.api=eth,debug,net,web3
```

This mode is mostly convenient for debugging purposes, because we know that the database does not change as we are
sending requests to the RPC daemon.

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

### Running in dual mode

If both `--chaindata` and `--private.api.addr` options are used for RPC daemon, it works in a "dual" mode. This only
works when RPC daemon is on the same computer as turbo-geth. In this mode, most data transfer from turbo-geth to RPC
daemon happens via shared memory, only certain things (like new header notifications) happen via TPC socket.

### Testing

By default, the `rpcdaemon` serves data from `localhost:8545`. You may send `curl` commands to see if things are
working.

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

Also, there
are [extensive instructions for using Postman](https://github.com/ledgerwatch/turbo-geth/wiki/Using-Postman-to-Test-TurboGeth-RPC)
to test the RPC.

## FAQ

### RPC Implementation Status

The following table shows the current implementation status of turbo-geth's RPC daemon.

| Command                                 | Avail   | Notes                                      |
| --------------------------------------- | ------- | ------------------------------------------ |
| web3_clientVersion                      | Yes     |                                            |
| web3_sha3                               | Yes     |                                            |
|                                         |         |                                            |
| net_listening                           | HC      | (remote only hard coded returns true)      |
| net_peerCount                           | HC      | (hard coded 25 - work continues on Sentry) |
| net_version                             | Yes     | remote only                                |
|                                         |         |                                            |
| eth_blockNumber                         | Yes     |                                            |
| eth_chainID                             | Yes     |                                            |
| eth_protocolVersion                     | Yes     |                                            |
| eth_syncing                             | Yes     |                                            |
| eth_gasPrice                            | Yes     |                                            |
|                                         |         |                                            |
| eth_getBlockByHash                      | Yes     |                                            |
| eth_getBlockByNumber                    | Yes     |                                            |
| eth_getBlockTransactionCountByHash      | Yes     |                                            |
| eth_getBlockTransactionCountByNumber    | Yes     |                                            |
| eth_getUncleByBlockHashAndIndex         | Yes     |                                            |
| eth_getUncleByBlockNumberAndIndex       | Yes     |                                            |
| eth_getUncleCountByBlockHash            | Yes     |                                            |
| eth_getUncleCountByBlockNumber          | Yes     |                                            |
|                                         |         |                                            |
| eth_getTransactionByHash                | Yes     |                                            |
| eth_getTransactionByBlockHashAndIndex   | Yes     |                                            |
| eth_getTransactionByBlockNumberAndIndex | Yes     |                                            |
| eth_getTransactionReceipt               | Yes     |                                            |
|                                         |         |                                            |
| eth_estimateGas                         | Yes     |                                            |
| eth_getBalance                          | Yes     |                                            |
| eth_getCode                             | Yes     |                                            |
| eth_getTransactionCount                 | Yes     |                                            |
| eth_getStorageAt                        | Yes     |                                            |
| eth_call                                | Yes     |                                            |
|                                         |         |                                            |
| eth_newFilter                           | -       | not yet implemented                        |
| eth_newBlockFilter                      | -       | not yet implemented                        |
| eth_newPendingTransactionFilter         | -       | not yet implemented                        |
| eth_getFilterChanges                    | -       | not yet implemented                        |
| eth_uninstallFilter                     | -       | not yet implemented                        |
| eth_getLogs                             | Yes     |                                            |
|                                         |         |                                            |
| eth_accounts                            | No      | deprecated                                 |
| eth_sendRawTransaction                  | Yes     | remote only                                |
| eth_sendTransaction                     | -       | not yet implemented                        |
| eth_sign                                | No      | deprecated                                 |
| eth_signTransaction                     | -       | not yet implemented                        |
| eth_signTypedData                       | -       | ????                                       |
|                                         |         |                                            |
| eth_getProof                            | -       | not yet implemented                        |
|                                         |         |                                            |
| eth_mining                              | Yes     | returns true if --mine flag provided       |
| eth_coinbase                            | Yes     |                                            |
| eth_hashrate                            | Yes     |                                            |
| eth_submitHashrate                      | Yes     |                                            |
| eth_getWork                             | Yes     |                                            |
| eth_submitWork                          | Yes     |                                            |
|                                         |         |                                            |
| eth_subscribe                           | Limited | Websock Only - newHeads,                   |
|                                         |         | newPendingTransaction                      |
| eth_unsubscribe                         | Yes     | Websock Only                               |
|                                         |         |                                            |
| debug_accountRange                      | Yes     | Private turbo-geth debug module            |
| debug_accountAt                         | Yes     | Private turbo-geth debug module            |
| debug_getModifiedAccountsByNumber       | Yes     |                                            |
| debug_getModifiedAccountsByHash         | Yes     |                                            |
| debug_storageRangeAt                    | Yes     |                                            |
| debug_traceTransaction                  | Yes     |                                            |
| debug_traceCall                         | Yes     |                                            |
|                                         |         |                                            |
| trace_call                              | Yes     |                                            |
| trace_callMany                          | Yes     |                                            |
| trace_rawTransaction                    | -       | not yet implemented (come help!)           |
| trace_replayBlockTransactions           | -       | not yet implemented (come help!)           |
| trace_replayTransaction                 | -       | not yet implemented (come help!)           |
| trace_block                             | Limited | working - has known issues                 |
| trace_filter                            | Limited | working - has known issues                 |
| trace_get                               | Limited | working - has known issues                 |
| trace_transaction                       | Limited | working - has known issues                 |
|                                         |         |                                            |
| eth_getCompilers                        | No      | deprecated                                 |
| eth_compileLLL                          | No      | deprecated                                 |
| eth_compileSolidity                     | No      | deprecated                                 |
| eth_compileSerpent                      | No      | deprecated                                 |
|                                         |         |                                            |
| db_putString                            | No      | deprecated                                 |
| db_getString                            | No      | deprecated                                 |
| db_putHex                               | No      | deprecated                                 |
| db_getHex                               | No      | deprecated                                 |
|                                         |         |                                            |
| shh_post                                | No      | deprecated                                 |
| shh_version                             | No      | deprecated                                 |
| shh_newIdentity                         | No      | deprecated                                 |
| shh_hasIdentity                         | No      | deprecated                                 |
| shh_newGroup                            | No      | deprecated                                 |
| shh_addToGroup                          | No      | deprecated                                 |
| shh_newFilter                           | No      | deprecated                                 |
| shh_uninstallFilter                     | No      | deprecated                                 |
| shh_getFilterChanges                    | No      | deprecated                                 |
| shh_getMessages                         | No      | deprecated                                 |
|                                         |         |                                            |
| tg_getHeaderByHash                      | Yes     | turbo-geth only                            |
| tg_getHeaderByNumber                    | Yes     | turbo-geth only                            |
| tg_getLogsByHash                        | Yes     | turbo-geth only                            |
| tg_forks                                | Yes     | turbo-geth only                            |
| tg_issuance                             | Yes     | turbo-geth only                            |

This table is constantly updated. Please visit again.

### Securing the communication between RPC daemon and TG instance via TLS and authentication

In some cases, it is useful to run Turbo-Geth nodes in a different network (for example, in a Public cloud), but RPC
daemon locally. To ensure the integrity of communication and access control to the Turbo-Geth node, TLS authentication
can be enabled. On the high level, the process consists of these steps (this process needs to be done for any "cluster"
of turbo-geth and RPC daemon nodes that are supposed to work together):

1. Generate key pair for the Certificate Authority (CA). The private key of CA will be used to authorise new turbo-geth
   instances as well as new RPC daemon instances, so that they can mutually authenticate.
2. Create CA certificate file that needs to be deployed on any turbo-geth instance and any RPC daemon. This CA cerf file
   is used as a "root of trust", whatever is in it, will be trusted by the participants when they authenticate their
   counterparts.
3. For each turbo-geth instance and each RPC daemon instance, generate a key pair. If you are lazy, you can generate one
   pair for all turbo-geth nodes, and one pair for all RPC daemons, and copy these keys around.
4. Using the CA private key, create cerificate file for each public key generated on the previous step. This
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

For turbo-geth node, generate a key pair:

```
openssl ecparam -name prime256v1 -genkey -noout -out TG-key.pem
```

Also, generate one for the RPC daemon:

```
openssl ecparam -name prime256v1 -genkey -noout -out RPC-key.pem
```

Now create cerificate signing request for turbo-geth key pair:

```
openssl req -new -key TG-key.pem -out TG.csr
```

And from this request, produce the certificate (signed by CA), proving that this key is now part of the "cluster of
trust"

```
openssl x509 -req -in TG.csr -CA CA-cert.pem -CAkey CA-key.pem -CAcreateserial -out TG.crt -days 3650 -sha256
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

When this is all done, these three files need to be placed on the machine where turbo-geth is running: `CA-cert.pem`
, `TG-key.pem`, `TG.crt`. And turbo-geth needs to be run with these extra options:

```
--tls --tls.cacert CA-cert.pem --tls.key TG-key.pem --tls.cert TG.crt
```

On the RPC daemon machine, these three files need to be placed: `CA-cert.pem`, `RPC-key.pem`, and `RPC.crt`. And RPC
daemon needs to be started with these extra options:

```
--tls.key RPC-key.pem --tls.cacert CA-cert.pem --tls.cert RPC.crt
```

**WARNING** Normally, the "client side" (which in our case is RPC daemon), verifies that the host name of the server
matches the "Common Name" attribute of the "server" cerificate. At this stage, this verification is turned off, and it
will be turned on again once we have updated the instruction above on how to properly generate cerificates with "Common
Name".

When running turbo-geth instance in the Google Cloud, for example, you need to specify the **Internal IP** in
the `--private.api.addr` option. And, you will need to open the firewall on the port you are using, to that connection
to the turbo-geth instances can be made.

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
      "INSTANCE_NAME": "turbo-geth node",
      "CONTACT_DETAILS": <your
      twitter
      handle>,
      "WS_SERVER": "wss://ethstats.net/api",
      "WS_SECRET": <put
      your
      secret
      key
      there>,
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
  "allow": [
    "net_version",
    "web3_eth_getBlockByHash"
  ]
}
```

3. Provide this file to the rpcdaemon using `--rpc.accessList` flag

```
> rpcdaemon --private.api.addr=localhost:9090 --http.api=eth,debug,net,web3 --rpc.accessList=rules.json
```

Now only these two methods are available.

### Trace transactions progress

There are still many open issues with the TurboGeth tracing routines. Please
see [this issue](https://github.com/ledgerwatch/turbo-geth/issues/1119#issuecomment-699028019) for the current open /
known issues related to tracing.

### Clients getting timeout, but server load is low

In this case: increase default rate-limit - amount of requests server handle simultaneously - requests over this limit
will wait. Increase it - if your 'hot data' is small or have much RAM or see "request timeout" while server load is low.

```
./build/bin/tg --private.api.addr=localhost:9090 --private.api.ratelimit=1024
```

### Server load too high

Reduce `--private.api.ratelimit`

## For Developers

### Code generation

`go.mod` stores right version of generators, use `make grpc` to install it and generate code (it also installs protoc
into ./build/bin folder).
