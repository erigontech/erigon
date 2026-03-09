---
name: erigon-network-ports
user-invocable: false
description: Reference for all Erigon network ports. Use this when running multiple Erigon instances to avoid port conflicts. Lists every CLI flag that binds a port, its default value, and the protocol used.
allowed-tools: Bash
---

# Erigon Port Reference

When running multiple Erigon instances on the same machine, every listening port must be unique per instance. Erigon will fail at startup if any port is already in use, reporting `bind: address already in use`.

## All Ports That Must Be Changed

| CLI Flag | Default | Protocol | Description |
|----------|---------|----------|-------------|
| `--private.api.addr` | `127.0.0.1:9090` | TCP (gRPC) | Internal gRPC API for component communication (txpool, rpcdaemon, sentry, downloader) |
| `--http.port` | `8545` | TCP | JSON-RPC HTTP server |
| `--authrpc.port` | `8551` | TCP | Engine API (consensus layer auth RPC) |
| `--ws.port` | `8546` | TCP | WebSocket RPC server |
| `--torrent.port` | `42069` | TCP+UDP | BitTorrent protocol for snapshot downloads |
| `--port` | `30303` | TCP+UDP | DevP2P network listening port |
| `--p2p.allowed-ports` | `30303,30304,30305,30306,30307` | TCP+UDP | Additional ports for different eth p2p protocol versions |
| `--caplin.discovery.port` | `4000` | UDP | Caplin consensus DISCV5 discovery |
| `--caplin.discovery.tcpport` | `4001` | TCP | Caplin consensus DISCV5 TCP |
| `--sentinel.port` | `7777` | TCP | Sentinel (consensus p2p service) |
| `--beacon.api.port` | `5555` | TCP | Beacon Chain REST API |
| `--mcp.port` | `8553` | TCP | MCP (Model Context Protocol) RPC server |

### Conditional Ports (only if enabled)

| CLI Flag | Default | Protocol | Description |
|----------|---------|----------|-------------|
| `--pprof.port` | `6060` | TCP | Go pprof profiling HTTP server (requires `--pprof`) |
| `--metrics.port` | `6061` | TCP | Prometheus metrics HTTP server (requires `--metrics`) |

## Example: Two Instances

Instance 1 (using defaults):
```bash
./build/bin/erigon --datadir /path/to/datadir1
```

Instance 2 (all ports offset by +100):
```bash
./build/bin/erigon --datadir /path/to/datadir2 \
  --private.api.addr=127.0.0.1:9190 \
  --http.port=8645 \
  --authrpc.port=8651 \
  --ws.port=8646 \
  --torrent.port=42169 \
  --port=30403 \
  --p2p.allowed-ports=30403,30404,30405,30406,30407 \
  --caplin.discovery.port=4100 \
  --caplin.discovery.tcpport=4101 \
  --sentinel.port=7877 \
  --beacon.api.port=5655 \
  --mcp.port=8653
```

## Notes

- `--private.api.addr` takes a full `host:port` value, not just a port number.
- `--p2p.allowed-ports` must list 5 consecutive ports matching the base `--port` value (one per eth p2p protocol version).
- The torrent port binds on both TCP and UDP; it is typically the first port conflict encountered at startup.
- If `--pprof` or `--metrics` are enabled on both instances, their ports must also differ.
