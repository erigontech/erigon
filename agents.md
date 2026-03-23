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

Run specific tests:
```bash
go test ./execution/stagedsync/...
go test -run TestName ./path/to/package/...
```

## Architecture Overview

Erigon is a high-performance Ethereum execution client with embedded consensus layer. Key design principles:
- **Flat KV storage** instead of tries (reduces write amplification)
- **Staged synchronization** (ordered pipeline, independent unwind)
- **Modular services** (sentry, txpool, downloader can run separately)

## Directory Structure

- `cmd/` — Entry points: erigon, rpcdaemon, caplin, sentry, downloader
- `execution/stagedsync/` — Staged sync pipeline ([agents.md](execution/stagedsync/agents.md))
- `db/` — Storage: MDBX, snapshots, ETL ([agents.md](db/agents.md))
- `cl/` — Consensus layer / Caplin ([agents.md](cl/agents.md))
- `p2p/` — P2P networking / DevP2P ([agents.md](p2p/agents.md))
- `rpc/jsonrpc/` — JSON-RPC API

## Running

```bash
./build/bin/erigon --datadir=./data --chain=mainnet
./build/bin/erigon --datadir=dev --chain=dev --mine  # Development
```

## Conventions

Commit messages: prefix with package(s) modified, e.g., `eth, rpc: make trace configs optional`

Cherry-pick PRs: when opening a PR that cherry-picks a commit to a `release/X.Y` branch, prepend the PR title with `[rX.Y]`, e.g., a cherry-pick to `release/3.4` → `[r3.4] eth, rpc: make trace configs optional`

## Linting

Run `make lint` before every push. The linter is non-deterministic — run it repeatedly until clean. See [.claude/rules/lint-fixes.md](.claude/rules/lint-fixes.md) for common categories and fixes.
