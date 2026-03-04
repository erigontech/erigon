# Erigon Agent Guidelines

This file provides guidance for AI agents working with this codebase.

**Requirements**: Go 1.24+, GCC 10+ or Clang, 32GB+ RAM, SSD/NVMe storage

## Build & Test

```bash
make erigon              # Build main binary (./build/bin/erigon)
make integration         # Build integration test binary
make lint                # Run golangci-lint + mod tidy check
make test-short          # Quick unit tests (-short -failfast)
make test-all            # Full test suite with coverage
make gen                 # Generate all auto-generated code (mocks, grpc, etc.)
```

Before committing, always verify changes with: `make lint && make erigon integration`

Run specific tests:
```bash
go test ./execution/stagedsync/...
go test -run TestName ./path/to/package/...
```

Build tag required: `goexperiment.synctest` (set automatically by Makefile)

## Architecture Overview

Erigon is a high-performance Ethereum execution client with embedded consensus layer. Key design principles:
- **Flat KV storage** instead of tries (reduces write amplification)
- **Staged synchronization** (ordered pipeline, independent unwind)
- **Modular services** (sentry, txpool, downloader can run separately)

## Directory Structure

| Directory | Purpose | Component Docs |
|-----------|---------|----------------|
| `cmd/` | Entry points: erigon, rpcdaemon, caplin, sentry, downloader | - |
| `execution/stagedsync/` | Staged sync pipeline | [agents.md](execution/stagedsync/agents.md) |
| `db/` | Storage: MDBX, snapshots, ETL | [agents.md](db/agents.md) |
| `cl/` | Consensus layer (Caplin) | [agents.md](cl/agents.md) |
| `p2p/` | P2P networking (DevP2P) | [agents.md](p2p/agents.md) |
| `rpc/jsonrpc/` | JSON-RPC API | - |

## Running

```bash
./build/bin/erigon --datadir=./data --chain=mainnet
./build/bin/erigon --datadir=dev --chain=dev --mine  # Development
```

## Conventions

Commit messages: prefix with package(s) modified, e.g., `eth, rpc: make trace configs optional`

**Important**: Always run `make lint` after making code changes and before committing. Fix any linter errors before proceeding.

## Lint Notes

The linter (`make lint`) is non-deterministic in which files it scans — new issues may appear on subsequent runs. Run lint repeatedly until clean.

Common lint categories and fixes:
- **ruleguard (defer tx.Rollback/cursor.Close):** The error check must come *before* `defer tx.Rollback()`. Never remove an explicit `.Close()` or `.Rollback()` — add `defer` as a safety net alongside it, since the timing of the explicit call may matter.
- **prealloc:** Pre-allocate slices when the length is known from a range.
- **unslice:** Remove redundant `[:]` on variables that are already slices.
- **newDeref:** Replace `*new(T)` with `T{}`.
- **appendCombine:** Combine consecutive `append` calls into one.
- **rangeExprCopy:** Use `&x` in `range` to avoid copying large arrays.
- **dupArg:** For intentional `x.Equal(x)` self-equality tests, suppress with `//nolint:gocritic`.
- **Loop ruleguard in benchmarks:** For `BeginRw`/`BeginRo` inside loops where `defer` doesn't apply, suppress with `//nolint:gocritic`.

## MultiGas (Arbitrum)

Multi-dimensional gas tracking categorizes gas by spending type (computation, storage, calldata, etc.). The sum of all categories always equals the single-gas total. Required for ArbOS 50+ and Stylus.

### Key files
- `arb/multigas/resources.go` — `MultiGas` type, `IntrinsicMultiGas()`, `ZeroGas()`
- `execution/vm/evm.go` — EVM Call/Create methods return `multigas.MultiGas`
- `execution/vm/evm_arb_tx_hook.go` — `TxProcessingHook` interface (StartTxHook, GasChargingHook, EndTxHook)
- `execution/protocol/state_transition.go` — MultiGas accumulation through `TransitionDb`
- `execution/protocol/state_transition_arb.go` — `handleRevertedTx`, Arbitrum-specific helpers

### Accumulation flow in TransitionDb
```
usedMultiGas = ZeroGas()
├── += IntrinsicMultiGas(data, accessList, ...)     // L2Calldata, Computation, StorageAccess
├── += GasChargingHook(&gasRemaining, intrinsicGas) // L1 poster cost (set by arbos TxProcessor)
├── += evm.Call() or evm.Create()                   // Computation, Storage (from opcodes)
├── .WithRefund(refund)                             // SSTORE refunds
├── .SaturatingIncrement(L2Calldata, floorDelta)    // Prague EIP-7623 floor (if enabled)
└── → result.UsedMultiGas
```

### EVM method signatures
All EVM Call/Create methods return `multigas.MultiGas` as an additional return value:
- `Call/CallCode/DelegateCall/StaticCall` → `(ret, leftOverGas, usedMultiGas, err)`
- `Create/Create2/SysCreate` → `(ret, contractAddr, leftOverGas, usedMultiGas, err)`

### ProcessingHook (TxProcessingHook interface)
Arbitrum injects transaction processing behavior via `evm.ProcessingHook`:
- `StartTxHook()` — can short-circuit TransitionDb (returns `takeover=true` for retryable tickets, deposits)
- `GasChargingHook()` — charges L1 poster gas, returns multigas contribution
- `EndTxHook(gasRemaining, success)` — finalizes Arbitrum-specific accounting
- `DropTip()` — returns true for delayed messages (gasPrice capped to baseFee before preCheck)
- `IsArbitrum()` — gates Arbitrum-specific paths (skip intrinsic gas check, custom refund logic)
- `ForceRefundGas()` / `NonrefundableGas()` — Arbitrum refund controls

### Testing multigas
```bash
go test ./execution/protocol/... -run TestMultiGas -count=1 -v
go test ./execution/protocol/... -run TestTransitionDb -count=1 -v
```
Invariant: `usedMultiGas.SingleGas() == result.ReceiptGasUsed` for DefaultTxProcessor (non-Arbitrum) transactions.
