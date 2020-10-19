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

Runing RPC daemon locally (with `--chaindata` option) can only be used when turbo-geth node is not running. This mode is mostly convenient for debugging purposes, because we know that the database does not change as we are sending requests to the RPC daemon.

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

## Open / Known Issues

There are still many open issues with the TurboGeth tracing routines. Please see [this issue](https://github.com/ledgerwatch/turbo-geth/issues/1119#issuecomment-699028019) for the current open / known issues related to tracing.

## RPC Implementation Status

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
| eth_gasPrice                            | -       |                                            |
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
| eth_newFilter                           | -       |                                            |
| eth_newBlockFilter                      | -       |                                            |
| eth_newPendingTransactionFilter         | -       |                                            |
| eth_getFilterChanges                    | -       |                                            |
| eth_getFilterLogs                       | -       |                                            |
| eth_uninstallFilter                     | -       |                                            |
| eth_getLogs                             | Yes     |                                            |
|                                         |         |                                            |
| eth_accounts                            | -       |                                            |
| eth_sendRawTransaction                  | Yes     | remote only                                |
| eth_sendTransaction                     | -       |                                            |
| eth_sign                                | -       |                                            |
| eth_signTransaction                     | -       |                                            |
| eth_signTypedData                       | -       |                                            |
|                                         |         |                                            |
| eth_getProof                            | -       |                                            |
|                                         |         |                                            |
| eth_mining                              | -       |                                            |
| eth_coinbase                            | Yes     |                                            |
| eth_hashrate                            | -       |                                            |
| eth_submitHashrate                      | -       |                                            |
| eth_getWork                             | -       |                                            |
| eth_submitWork                          | -       |                                            |
|                                         |         |                                            |
| debug_accountRange                      | Yes     | Private turbo-geth debug module            |
| debug_getModifiedAccountsByNumber       | Yes     |                                            |
| debug_getModifiedAccountsByHash         | Yes     |                                            |
| debug_storageRangeAt                    | Yes     |                                            |
| debug_traceTransaction                  | Yes     |                                            |
|                                         |         |                                            |
| trace_call                              | -       | not yet implemented (come help!)           |
| trace_callMany                          | -       | not yet implemented (come help!)           |
| trace_rawTransaction                    | -       | not yet implemented (come help!)           |
| trace_replayBlockTransactions           | -       | not yet implemented (come help!)           |
| trace_replayTransaction                 | -       | not yet implemented (come help!)           |
| trace_block                             | Limited | working - has known issues                 |
| trace_filter                            | Limited | working - has known issues                 |
| trace_get                               | Limited | working - has known issues                 |
| trace_transaction                       | Limited | working - has known issues                 |
|                                         |         |                                            |
| eth_getCompilers                        | No      | depreciated                                |
| eth_compileLLL                          | No      | depreciated                                |
| eth_compileSolidity                     | No      | depreciated                                |
| eth_compileSerpent                      | No      | depreciated                                |
|                                         |         |                                            |
| db_putString                            | No      | depreciated                                |
| db_getString                            | No      | depreciated                                |
| db_putHex                               | No      | depreciated                                |
| db_getHex                               | No      | depreciated                                |
|                                         |         |                                            |
| shh_post                                | No      | depreciated                                |
| shh_version                             | No      | depreciated                                |
| shh_newIdentity                         | No      | depreciated                                |
| shh_hasIdentity                         | No      | depreciated                                |
| shh_newGroup                            | No      | depreciated                                |
| shh_addToGroup                          | No      | depreciated                                |
| shh_newFilter                           | No      | depreciated                                |
| shh_uninstallFilter                     | No      | depreciated                                |
| shh_getFilterChanges                    | No      | depreciated                                |
| shh_getMessages                         | No      | depreciated                                |
|                                         |         |                                            |
| tg_getHeaderByHash                      | Yes     | turbo-geth only                            |
| tg_getHeaderByNumber                    | Yes     | turbo-geth only                            |
|                                         |         |                                            |
| tg_getLogsByHash                        | Yes     | turbo-geth only                            |
|                                         |         |                                            |
| tg_forks                                | Yes     | turbo-geth only                            |


This table is constantly updated. Please visit again.

## Securing the communication between RPC daemon and TG instance via TLS and authentication

In some cases, it is useful to run Turbo-Geth nodes in a different network (for example, in a Public cloud), but RPC daemon locally. To ensure
the integrity of communication and access control to the Turbo-Geth node, TLS authentication can be enabled.
On the high level, the process consists of these steps (this process needs to be done for any "cluster" of turbo-geth and RPC daemon nodes that are
supposed to work together):

1. Generate key pair for the Certificate Authority (CA). The private key of CA will be used to authorise new turbo-geth instances as well as new RPC daemon instances, so that they can mutually authenticate.
2. Create CA certificate file that needs to be deployed on any turbo-geth instance and any RPC daemon. This CA cerf file is used as a "root of trust", whatever is in it, will be trusted by the participants when they authenticate their counterparts.
3. For each turbo-geth instance and each RPC daemon instance, generate a key pair. If you are lazy, you can generate one pair for all turbo-geth nodes, and one pair for all RPC daemons, and copy these keys around.
4. Using the CA private key, create cerificate file for each public key generated on the previous step. This effectively "inducts" these keys into the "cluster of trust".
5. On each instance, deploy 3 files - CA certificate, instance key, and certificate signed by CA for this instance key.

Following is the detailed description of how it can be done using `openssl` suite of tools.

Generate CA key pair using Elliptic Curve (as opposed to RSA). The generated CA key will be in the file `CA-key.pem`. Access to this file will allow anyone to later include any new instance key pair into the "cluster of trust", so keep it secure.

```
openssl ecparam -name prime256v1 -genkey -noout -out CA-key.pem
```

Create CA self-signed certificate (this command will ask questions, answers aren't important for now). The file created by this command is `CA-cert.pem`

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

And from this request, produce the certificate (signed by CA), proving that this key is now part of the "cluster of trust"

```
openssl x509 -req -in TG.csr -CA CA-cert.pem -CAkey CA-key.pem -CAcreateserial -out TG.crt -days 3650 -sha256
```

Then, produce the certificate signing request for RPC daemon key pair:

```
openssl req -new -key RPC-key.pem -out RPC.csr
```

And from this request, produce the certificate (signed by CA), proving that this key is now part of the "cluster of trust"

```
openssl x509 -req -in RPC.csr -CA CA-cert.pem -CAkey CA-key.pem -CAcreateserial -out RPC.crt -days 3650 -sha256
```

When this is all done, these three files need to be placed on the machine where turbo-geth is running: `CA-cert.pem`, `TG-key.pem`, `TG.crt`. And turbo-geth needs to be run with these extra options:

```
--tls --tls.cacert CA-cert.pem --tls.key TG-key.pem --tls.cert TG.crt
```

On the RPC daemon machine, these three files need to be placed: `CA-cert.pem`, `RPC-key.pem`, and `RPC.crt`. And RPC daemon needs to be started with these extra options:

```
--tls.key RPC-key.pem --tls.cacert CA-cert.pem --tls.cert RPC.crt
```

**WARNING** Normally, the "client side" (which in our case is RPC daemon), verifies that the host name of the server matches the "Common Name" attribute of the "server" cerificate. At this stage, this verification is turned off, and it will be turned on again once we have updated the instruction above on how to properly generate cerificates with "Common Name".

When running turbo-geth instance in the Google Cloud, for example, you need to specify the **Internal IP** in the `--private.api.addr` option. And, you will need to open the firewall on the port you are using, to that connection to the turbo-geth instances can be made.

## For Developers

### Code generation

`go.mod` stores right version of generators, use `make grpc` to install it and generate code.

Recommended `protoc` version is 3.x. [Installation instructions](https://grpc.io/docs/protoc-installation/)
