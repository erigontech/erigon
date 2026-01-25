# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Erigon is a high-performance Ethereum execution client (with embedded consensus layer) written in Go. It prioritizes efficiency through flat KV storage instead of tries, staged synchronization, and modular architecture.

**Requirements**: Go 1.24+, GCC 10+ or Clang, 32GB+ RAM, SSD/NVMe storage

## Build & Test

```bash
make erigon              # Build main binary (./build/bin/erigon)
make all                 # Build all binaries
make test-short          # Quick unit tests (-short -failfast)
make test-all            # Full test suite with coverage
make lint                # Run golangci-lint + mod tidy check
make gen                 # Generate all auto-generated code (mocks, grpc, etc.)
```

Run specific tests:
```bash
go test ./execution/stagedsync/...
go test -run TestName ./path/to/package/...
```

Build tags required: `goexperiment.synctest` (set automatically by Makefile)

## Directory Structure

- **`cmd/`** - Entry points: `erigon/`, `rpcdaemon/`, `caplin/`, `sentry/`, `downloader/`
- **`execution/`** - Execution layer: `stagedsync/`, `state/`, `vm/` (EVM), `tracing/`
- **`cl/`** - Consensus layer (Caplin): `beacon/`, `cltypes/`, `phase1/`, `validator/`
- **`db/`** - Storage: `kv/`, `mdbx-go/`, `seg/` (snapshots), `etl/`
- **`rpc/jsonrpc/`** - JSON-RPC API implementation
- **`p2p/`** - P2P networking (DevP2P for execution, libp2p for consensus)

## Core Architecture

### Staged Sync

Synchronization via ordered stages in `execution/stagedsync/`. Reorgs unwind in reverse order.

**Stage Pipeline**: Snapshots → Headers → BlockHashes → Bodies → Senders → Execution → TxLookup → Finish

Each stage implements:
- `Forward` - Execute stage forward
- `Unwind` - Handle reorgs by rolling back
- `Prune` - Remove old data

Key files:
- `sync.go` - `Sync` struct orchestrates stage execution
- `stageloop/stageloop.go` - `StageLoop()` runs continuous sync cycle
- `stage_*.go` - Individual stage implementations

### Database

**Temporal Database** (`db/kv/temporal/`): Separates hot (MDBX) from cold (snapshots) data.

**Four Domains** (accounts, storage, code, commitment):
- Hot values in MDBX for recent state
- Immutable snapshots (.seg files) for historical data
- Inverted indices for time-travel queries

Key methods: `GetLatest()`, `GetAsOf(txNum)`, `HistorySeek()`, `RangeAsOf()`

ETL framework (`db/etl/`) sorts data before insertion to reduce write amplification.

### Caplin (Consensus Layer)

Embedded beacon client in `cl/`:
- `phase1/forkchoice/` - LMD-GHOST fork choice algorithm
- `phase1/execution_client/` - Engine API integration
- `beacon/handler/` - Beacon API endpoints
- `sentinel/` - libp2p networking

### P2P

- **Execution**: DevP2P in `p2p/`, with sentry gRPC bridge (`p2p/sentry/`)
- **Consensus**: libp2p in `cl/sentinel/`

Sentry architecture allows running separate P2P nodes for isolation/scaling.

### Engine API

Bridge between execution and consensus (`execution/engineapi/`):
- `NewPayload` - Receive blocks from consensus
- `ForkchoiceUpdated` - Set canonical chain head
- `GetPayload` - Builder requests block data

## Running

```bash
./build/bin/erigon --datadir=./data --chain=mainnet
./build/bin/erigon --datadir=dev --chain=dev --mine  # Development
```

## Conventions

Commit messages: prefix with package(s) modified, e.g., `eth, rpc: make trace configs optional`
