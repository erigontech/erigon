# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Erigon is a high-performance Ethereum execution client (with embedded consensus layer) written in Go. It prioritizes efficiency through flat KV storage instead of tries, staged synchronization, and modular architecture.

**Requirements**: Go 1.24+, GCC 10+ or Clang, 32GB+ RAM, SSD/NVMe storage

## Build Commands

```bash
make erigon              # Build main Erigon binary (output: ./build/bin/erigon)
make all                 # Build all binaries (erigon + rpcdaemon, caplin, sentry, etc.)
make rpcdaemon           # Build RPC daemon separately
make caplin              # Build consensus layer client
```

## Testing

```bash
make test-short          # Quick unit tests (-short -failfast)
make test-all            # Full test suite with coverage (60m timeout)
make test-all-race       # Full tests with race detector

# Run specific package tests
go test ./execution/stagedsync/...
go test -run TestName ./path/to/package/...
```

Build tags required: `goexperiment.synctest` (set automatically by Makefile)

## Linting

```bash
make lint                # Run all linters (golangci-lint + mod tidy check)
make lint-deps           # Install lint dependencies
```

Uses golangci-lint v2.8.0+ with custom rules in `rules.go` for defer rollback patterns.

## Code Generation

```bash
make gen                 # Generate all auto-generated code
make mocks               # Generate test mocks (mockgen)
make grpc                # Generate protobuf/gRPC code
make solc                # Generate Solidity contracts
```

## Architecture

### Directory Structure

- **`cmd/`** - Entry points: `erigon/` (main node), `rpcdaemon/`, `caplin/` (consensus), `sentry/` (P2P), `downloader/`
- **`execution/`** - Execution layer: `stagedsync/` (staged sync), `state/`, `vm/` (EVM), `tracing/`
- **`cl/`** - Consensus layer (Caplin): `beacon/`, `cltypes/`, `phase1/`, `validator/`
- **`db/`** - Storage: `kv/` (interfaces), `mdbx-go/`, `seg/` (snapshots), `etl/` (preprocessing)
- **`rpc/jsonrpc/`** - JSON-RPC API implementation
- **`p2p/`** - P2P networking

### Staged Sync

Synchronization happens through ordered stages defined in `execution/stagedsync/`. Reorgs unwind stages in reverse order.

**Key Files:**
- `stage.go` - Core `Stage`, `StageState`, `UnwindState` types
- `sync.go` - Main `Sync` coordinator and execution logic
- `default_stages.go` - Stage factory functions and order definitions (`DefaultStages()`, `DefaultForwardOrder`, `DefaultUnwindOrder`)
- `stages/stages.go` - `SyncStage` constants and progress tracking functions
- `stageloop/stageloop.go` - Continuous sync loop (`StageLoop()`)

**Stage Implementations:**
- `stage_snapshots.go` - Download snapshot segments
- `stage_headers.go` - Download and verify headers
- `stage_blockhashes.go` - Index blockHash → blockNumber
- `stage_bodies.go` - Download block bodies
- `stage_senders.go` - Recover transaction senders from signatures
- `stage_execute.go` - Execute blocks and compute state
- `stage_txlookup.go` - Index txHash → blockNumber
- `stage_finish.go` - Update current block for RPC

**Supporting Modules:**
- `headerdownload/` - Header P2P download algorithms
- `bodydownload/` - Block body P2P download algorithms
- `dataflow/states.go` - Header/body download state tracking

**Stage Interface:**
```go
type Stage struct {
    ID      stages.SyncStage
    Forward ExecFunc   // Execute stage forward
    Unwind  UnwindFunc // Handle reorgs
    Prune   PruneFunc  // Prune old data
}
```

Each stage has a config struct (e.g., `HeadersCfg`, `BodiesCfg`, `ExecuteBlockCfg`) created via factory functions.

### Database

- Uses MDBX (fork of LMDB) via `erigontech/mdbx-go`
- ETL framework (`db/etl/`) sorts data before insertion to reduce write amplification
- Snapshots (`.seg` files) store immutable historical data in `datadir/snapshots/`
- Four domains: account, storage, code, commitment

### Temporal Database (State Storage)

The temporal DB enables time-travel queries by separating hot (mutable) state from cold (immutable) snapshots.

**Key Files:**
- `db/kv/temporal/kv_temporal.go` - Main `TemporalDB` interface wrapping `RwDB` + `Aggregator`
- `db/state/aggregator.go` - Central hub managing all domain and index files
- `db/state/domain.go` - State domain with embedded history
- `db/state/history.go` - Historical value tracking
- `db/state/inverted_index.go` - Time-travel indices (key → [txNums])
- `db/kv/tables.go` - Domain and table definitions

**Four Domains** (defined in `kv/tables.go`):
| Domain | Purpose | Files |
|--------|---------|-------|
| AccountsDomain | Ethereum account state | `.kv`, `.bt`, `.kvei` |
| StorageDomain | Account storage slots | `.kv`, `.bt`, `.kvei` |
| CodeDomain | Contract bytecode | `.kv`, `.bt`, `.kvei` |
| CommitmentDomain | Merkle trie/state root | `.kv`, `.bt`, `.kvei` |

**Data Layers:**
1. **Hot (MDBX)** - Recent transactions, can be updated during reorgs
2. **Warm (Snapshots)** - Immutable segment files (`.seg`, `.v`, `.ef`)
3. **Cold (Frozen)** - Compressed historical archives

**Time-Travel Methods** (in `kv_temporal.go`):
- `GetLatest()` - Current value
- `GetAsOf(txNum)` - Value at specific transaction
- `HistorySeek()` - Find value before given txNum
- `RangeAsOf()` - Range scan at point in time
- `IndexRange()` - Get all txNums where key changed

### Key Interfaces

Defined in `interfaces.go`:
- `ChainReader` - Block and header access
- `TransactionReader` - Transaction and receipt lookup
- `ChainStateReader` - State access (balance, storage, code, nonce)
- `BlockExecutor` - Block execution

### Caplin (Consensus Layer)

Embedded consensus client in `cl/` directory implementing Ethereum Beacon Chain.

**Key Directories:**
- `cl/beacon/` - Beacon API and HTTP handlers
  - `handler/` - API endpoints (block production, validator duties, states)
  - `builder/` - MEV builder integration
- `cl/phase1/` - Core consensus implementation
  - `forkchoice/` - LMD-GHOST fork choice algorithm (`forkchoice.go`, `on_block.go`, `on_attestation.go`)
  - `core/state/` - Beacon state machine with fork upgrades
  - `execution_client/` - Engine API integration with execution layer
  - `network/` - Gossip handlers and services
- `cl/cltypes/` - Consensus types (blocks, attestations, validators)
  - `solid/` - Memory-efficient flat buffer types
- `cl/transition/` - State transition logic (`impl/eth2/operations.go`)
- `cl/sentinel/` - P2P networking service (libp2p-based)
- `cl/pool/` - Operations pools (attestations, slashings, exits)
- `cl/validator/` - Attestation producer, committee subscriptions

**Key Interfaces:**
- `abstract/beacon_state.go` - `BeaconState` interface (getters, mutators, SSZ)
- `phase1/forkchoice/interface.go` - `ForkChoiceStorage` (reader/writer)
- `phase1/execution_client/interface.go` - `ExecutionEngine` for EL communication

### P2P Networking

**Execution Layer (DevP2P):**
- `p2p/server.go` - Main P2P server managing peer connections
- `p2p/peer.go` - Peer connection state
- `p2p/rlpx/` - RLPx encryption protocol
- `p2p/discover/` - Node discovery (UDPv4/v5)
- `p2p/enode/` - Ethereum Node Record (ENR) management
- `p2p/protocols/eth/` - ETH protocol (messages, handlers)
- `p2p/protocols/wit/` - Witness protocol for stateless clients

**Sentry Service (gRPC Bridge):**
- `p2p/sentry/sentry_grpc_server.go` - Main sentry implementation
- `p2p/sentry/eth_handshake.go` - ETH protocol handshake
- `p2p/sentry/libsentry/` - Client library for multi-sentry support
- Architecture: `Execution Node ↔ gRPC ↔ Sentry ↔ P2P Network`

**Consensus Layer (libp2p):**
- `cl/p2p/p2p.go` - libp2p host initialization
- `cl/p2p/libp2p_setting.go` - GossipSub peer scoring parameters
- `cl/sentinel/sentinel.go` - Beacon P2P service
- `cl/phase1/network/gossip/` - Gossip topic handlers

**ETH Protocol Messages (eth68/69):**
- Status, NewBlockHashes, Transactions, GetBlockHeaders/Bodies
- NewBlock, GetReceipts, PooledTransactions

### Downloader (BitTorrent Snapshots)

Snapshot distribution via BitTorrent with HTTP WebSeed fallback.

**Key Files:**
- `db/downloader/downloader.go` - Main orchestrator using anacrolix/torrent
- `db/downloader/torrent_files.go` - `.torrent` metainfo management
- `db/downloader/webseed.go` - HTTP WebSeed support (S3/R2/CDN)
- `db/downloader/downloader_grpc_server.go` - gRPC service for Erigon
- `db/downloader/downloadercfg/` - Configuration management
- `cmd/downloader/main.go` - CLI with subcommands (torrent_create, manifest, etc.)

**Features:**
- Piece size: 2MB default
- Automatic torrent discovery and seeding
- "Download once" pattern with `preverified.toml` marker
- Rate limiting (separate for P2P and WebSeeds)
- Verification on download

**CLI Commands:**
```bash
./build/bin/downloader                    # Run downloader service
./build/bin/downloader torrent_create     # Generate .torrent files
./build/bin/downloader manifest           # Generate manifest.txt for WebSeeds
./build/bin/downloader torrent_hashes     # Print snapshot hashes
```

## Running Locally

```bash
./build/bin/erigon --datadir=./data --chain=mainnet

# Development chain
./build/bin/erigon --datadir=dev --chain=dev --private.api.addr=localhost:9090 --mine

# Separate RPC daemon
./build/bin/rpcdaemon --datadir=./data --http.api=eth,debug,net,trace,web3,erigon
```

## Commit Message Convention

Prefix with package(s) modified: `eth, rpc: make trace configs optional`

## Datadir Structure

```
datadir/
├── chaindata/     # Recent state and blocks (MDBX)
├── snapshots/
│   ├── domain/    # Latest state
│   ├── history/   # Historical values
│   ├── idx/       # Inverted indices for search
│   └── accessor/  # Additional indices
├── txpool/        # Pending transactions (safe to delete)
├── nodes/         # P2P peers (safe to delete)
└── temp/          # Sorting buffer (cleaned at startup)
```
