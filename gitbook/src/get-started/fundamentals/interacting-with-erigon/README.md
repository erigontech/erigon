---
description: >-
  The Erigon RPC Service: Enabling JSON-RPC, Transports (HTTP/WS/gRPC), and API
  Namespaces
---

# Interacting with Erigon

The Erigon RPC Service, managed by Erigon's modular [RPC daemon](../modules/rpc-daemon.md), supports various API namespaces, which can be enabled or disabled using the `--http.api` flag. The available namespaces include:

* [`eth`](eth.md): Standard Ethereum API.
* [`erigon`](erigon.md): Erigon-specific extensions.
* [`engine`](engine.md): The JSON-RPC Interface for Execution and Consensus Layer Communication.
* [`web3`](web3.md): Web3 compatibility API.
* [`net`](net.md): Network information API.
* [`debug`](debug.md): Debugging and tracing API.
* [`trace`](trace.md): Transaction tracing API.
* [`txpool`](txpool.md): Transaction pool API.
* [`admin`](admin.md): Node administration API
* [`bor`](bor.md): Polygon Bor-specific API (when running on Polygon)
* [`ots`](ots.md): These methods are specifically tailored for use with Otterscan, an open-source, fast block explorer.
* [`internal`](internal.md): Erigon specific API for development and debugging purposes.
* [`gRPC`](grpc.md): API for lower-level data access.

{% include "../../../.gitbook/includes/warning-admin_-and-debug_-....md" %}

For a complete reference on the standard Ethereum JSON-RPC methods, especially those in the `eth`, `net`, and `web3` namespaces, it is recommended to consult the general documentation on [ethereum.org's JSON-RPC API page](https://ethereum.org/en/developers/docs/apis/json-rpc/). Additionally, for the formal specification of the `debug`, `engine`, and `eth` namespaces, including unique, detailed descriptions for methods like `eth_getProof` and `eth_simulateV1`, refer to the [Execution APIs documentation](https://ethereum.github.io/execution-apis/api-documentation/).

{% embed url="https://ethereum.org/en/developers/docs/apis/json-rpc/" %}

{% embed url="https://ethereum.github.io/execution-apis/api-documentation/" %}

## Erigon RPC Transports

Erigon supports [HTTP](./#http), [HTTPS](./#https), [WebSockets](./#websockets), [IPC](./#ipc), [gRPC](./#grpc) and [GraphQL](./#graphql) through its RPC daemon.

### HTTP

Using the HTTP transport, clients send a request to the server and immediately get a response back. The connection is closed after the response for a given request is sent.

Because HTTP is unidirectional, subscriptions are not supported.

To start an HTTP server, you can either run Erigon with built-in RPC or use the separate `rpcdaemon`:

```bash
# Built-in RPC (default)
erigon --http

# Or separate RPC daemon
rpcdaemon --http.enabled
```

The default port is `8545`, and the default listen address is localhost. _node/nodecfg/defaults.go:30-31_

You can configure the listen address and port using `--http.addr` and `--http.port` respectively:

```bash
erigon --http --http.addr 127.0.0.1 --http.port 12345
# Or with rpcdaemon
rpcdaemon --http.addr 127.0.0.1 --http.port 12345
```

To enable JSON-RPC namespaces on the HTTP server, pass each namespace separated by a comma to `--http.api`:

```bash
erigon --http --http.api eth,net,debug,trace
# Or with rpcdaemon
rpcdaemon --http.api eth,net,debug,trace
```

The default APIs enabled are `eth` and `erigon`. Available namespaces include: `admin`, `debug`, `eth`, `erigon`, `net`, `trace`, `txpool`, `web3`, `bor` (Polygon only), and `internal`.

You can also restrict who can access the HTTP server by specifying domains for Cross-Origin requests using `--http.corsdomain`:

```bash
erigon --http --http.corsdomain https://mycoolapp.com
```

Alternatively, if you want to allow any domain, you can pass `*`:

```bash
erigon --http --http.corsdomain "*"
```

### HTTPS

Erigon supports HTTPS and HTTP/2 out of the box:

```bash
rpcdaemon --https.enabled --https.cert /path/to/cert.pem --https.key /path/to/key.pem
```

### WebSockets

WebSockets is a bidirectional transport protocol. Most modern browsers support WebSockets.

A WebSocket connection is maintained until it is explicitly terminated by either the client or the node.

Because WebSockets are bidirectional, nodes can push events to clients, which enables clients to subscribe to specific events, such as new transactions in the transaction pool, and new logs for smart contracts.

The configuration of the WebSocket server follows the same pattern as the HTTP server:

* Enable it using `--ws`
* Configure the server port by passing `--ws.port` (default `8546`) _node/nodecfg/defaults.go:34_
* Configure cross-origin requests using `--ws.origins` (though this maps to `--http.corsdomain` in Erigon)
* WebSocket APIs inherit from the HTTP API configuration

```bash
erigon --http --ws --http.api eth,net,debug,trace
```

### IPC

IPC is a simpler transport protocol for use in local environments where the node and the client exist on the same machine.

The IPC transport can be enabled using `--socket.enabled` and configured with `--socket.url`:

```bash
erigon --socket.enabled --socket.url unix:///var/run/erigon.ipc
```

On Linux and macOS, Erigon uses UNIX sockets. On Windows, IPC is provided using named pipes. The socket inherits the namespaces from `--http.api`.

### gRPC

Erigon also supports gRPC for high-performance access to blockchain data:

```bash
rpcdaemon --grpc --grpc.addr localhost --grpc.port 9090
```

### GraphQL

Erigon uses the standard GraphQL documented by Geth at [https://geth.ethereum.org/docs/interacting-with-geth/rpc/graphql](https://geth.ethereum.org/docs/interacting-with-geth/rpc/graphql).

## Interacting with the RPC

You can easily interact with these APIs using `curl`, a programming language with a low-level library, or tools like [Foundry](https://getfoundry.sh/) to interact with the chain at the exposed HTTP or WebSocket port.

To enable all APIs using an HTTP transport:

```bash
erigon --http --http.api "admin,debug,eth,erigon,net,trace,txpool,web3"
```

This allows you to then call:

{% code overflow="wrap" %}
```bash
curl -X POST -H "Content-Type: application/json" --data '{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}' localhost:8545

# With cast (if using Foundry)
cast block-number
cast rpc admin_nodeInfo  
cast rpc debug_traceTransaction <tx_hash>
cast rpc erigon_forks
```
{% endcode %}

