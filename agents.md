# Erigon Agent Guidelines

**Requirements**: Go 1.25+, GCC 10+ or Clang, 32GB+ RAM, SSD/NVMe storage

## Build & Test

```bash
make erigon              # Build main binary (./build/bin/erigon)
make integration         # Build integration test binary
make lint                # Run golangci-lint + mod tidy check
make test-short          # Quick unit tests (-short -failfast)
make test-all            # Full test suite with coverage
make gen                 # Generate all auto-generated code (mocks, grpc, etc.)
```

Verify changes before committing: `make lint && make erigon integration`

## Architecture Overview

- Erigon is an Ethereum execution client
- Data flow: `db -> snapshots`
- `snapshots` are immutable
- `Unwind` beyond data in snapshots not allowed

## Key Directories

- `cmd/` — Entry points: erigon, rpcdaemon, caplin, sentry, downloader
- `execution/stagedsync/` — Staged sync pipeline ([agents.md](execution/stagedsync/agents.md))
- `db/` — Storage: MDBX, snapshots, ETL ([agents.md](db/agents.md))
- `cl/` — Consensus layer / Caplin ([agents.md](cl/agents.md))
- `p2p/` — P2P networking / DevP2P ([agents.md](p2p/agents.md))
- `rpc/jsonrpc/` — JSON-RPC API

## Conventions

Commit messages: prefix with package(s) modified, e.g., `eth, rpc: make trace configs optional`

Run `make lint` before every push. The linter is non-deterministic — run it repeatedly until clean.

## Running

```bash
./build/bin/erigon --datadir=./data --chain=mainnet
```

