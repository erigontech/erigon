---
description: Ports used by Erigon
---

# Default ports

Erigon use the following default port for each service:

| Component | Port    | Protocol  | Purpose                     | Should Expose |
| --------- | ------- | --------- | --------------------------- | ------------- |
| engine    | `9090`  | TCP       | gRPC Server                 | Private       |
| engine    | `42069` | TCP & UDP | Snap sync (Bittorrent)      | Public        |
| engine    | `8551`  | TCP       | Engine API (JWT auth)       | Private       |
| sentry    | `30303` | TCP & UDP | eth/68 peering              | Public        |
| sentry    | `30304` | TCP & UDP | eth/67 peering              | Public        |
| sentry    | `9091`  | TCP       | incoming gRPC Connections   | Private       |
| rpcdaemon | `8545`  | TCP       | HTTP & WebSockets & GraphQL | Private       |
| shutter   | `23102` | TCP       | Peering                     | Public        |

Typically, `30303` and `30304` are exposed to the internet to allow incoming peering connections. `9090` is exposed only internally for rpcdaemon or other connections, (e.g. rpcdaemon -> erigon). Port `8551` (JWT authenticated) is exposed only internally for Engine API JSON-RPC queries from the Consensus Layer node.

To ensure proper P2P functionality for both the Execution and Consensus layers use a minimal configuration without exposing unnecessary services:

* Avoid exposing other ports unless necessary for specific use cases (e.g., JSON-RPC or WebSocket);
* Regularly audit your firewall rules to ensure they are aligned with your infrastructure needs;
* Use monitoring tools like Prometheus or Grafana to track P2P communication metrics.

## Command-Line Switches for Network and Port Configuration

Here is an extensive list of port-related options from the [options](configuring-erigon.md#options) list:

### Engine

* `--private.api.addr [value]`: Erigon's internal gRPC API, empty string means not to start the listener (default: `127.0.0.1:9090`)
* `--txpool.api.addr [value]`: TxPool api network address, (default: use value of `--private.api.addr`)
* `--torrent.port [value]`: Port to listen and serve BitTorrent protocol (default: `42069`)
* `--authrpc.port [value]`: HTTP-RPC server listening port for the Engine API (default: `8551`)

### Sentry

* `--port [value]`: Network listening port (default: `30303`)
* `--p2p.allowed-ports [value]`: Allowed ports to pick for different eth p2p protocol versions (default: `30303`, `30304`, `30305`, `30306`, `30307`)
* `--sentry.api.addr [value]`: Comma separated sentry addresses `<host>:<port>,<host>:<port>` (default `127.0.0.1:9091`)

### RPCdaemon

* `--ws.port [value]`: WS-RPC server listening port (default: `8546`)
* `--http.port [value]`: HTTP-RPC server listening port (default: `8545`)

### Caplin

* `--caplin.discovery.port [value]`: Port for Caplin DISCV5 protocol (default: `4000`)
* `--caplin.discovery.tcpport [value]`: TCP Port for Caplin DISCV5 protocol (default: `4001`)

### BeaconAPI

* `--beacon.api.port [value]`: Sets the port to listen for beacon api requests (default: `5555`)

### Diagnostics

* `--diagnostics.endpoint.port [value]`: Diagnostics HTTP server listening port (default: `6062`)

## Shutter Network Default Ports

The default peering port for Shutter is `23102` (TCP), to change it use `--shutter.p2p.listen.port <value>`.

Bootstrap nodes are used to help new nodes discover other nodes in the network. By default, the embedded configuration values are used, but these can be overridden with `--shutter.p2p.bootstrap.nodes <value>`.

### Shared ports

* `--pprof.port [value]`: pprof HTTP server listening port (default: `6060`)
* `--metrics.port [value]`: Metrics HTTP server listening port (default: `6061`)
* `--downloader.api.addr [value]`: Downloader address `<host>:<port>`
