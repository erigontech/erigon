# P2P Networking

Erigon uses DevP2P for execution layer networking with an optional Sentry service for isolation.

## Architecture

```
Execution Node ←→ gRPC ←→ Sentry ←→ P2P Network
```

The Sentry can run in-process or as a separate service for:
- P2P isolation from execution
- Running multiple sentries for resilience
- Resource limiting

## Key Components

### Server (`server.go`)
- Main P2P server managing peer connections
- Dial/accept logic with fairness mechanisms
- Protocol negotiation and message routing
- Peer lifecycle (connect/disconnect/ban)

### Discovery (`discover/`)
- `v4/` - UDP-based node discovery (legacy)
- `v5/` - ENR-based discovery protocol
- Handles peer finding and routing table

### RLPx (`rlpx/`)
- Encrypted transport protocol
- Handshake and session establishment
- Frame encoding/decoding

### Protocols (`protocols/`)
- `eth/` - ETH protocol (headers, bodies, transactions)
- `wit/` - Witness protocol for stateless clients

## Sentry Service (`sentry/`)

gRPC-based P2P bridge:
- `sentry_grpc_server.go` - Main sentry implementation
- `eth_handshake.go` - ETH protocol handshake
- `libsentry/` - Client library for multi-sentry support

## ETH Protocol Messages

| Message | Purpose |
|---------|---------|
| Status | Initial handshake |
| NewBlockHashes | Announce new blocks |
| Transactions | Broadcast txs |
| GetBlockHeaders | Request headers |
| GetBlockBodies | Request bodies |
| NewBlock | Full block announcement |
| PooledTransactions | Tx pool sync |

## Ports

| Port | Protocol | Purpose |
|------|----------|---------|
| 30303 | TCP/UDP | eth/68 peering |
| 30304 | TCP/UDP | eth/69 peering |
| 9091 | TCP | Sentry gRPC (internal) |

## Running Separate Sentry

```bash
# Start sentry
./build/bin/sentry --datadir=./data

# Connect erigon to sentry
./build/bin/erigon --sentry.api.addr=localhost:9091
```
