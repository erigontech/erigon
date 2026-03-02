# Restore L2 RPC Block Fetching in StageHeaders

**Date:** 2026-03-02
**Status:** Completed
**Context:** During rebase from `arbitrum` branch, the Arbitrum-specific L2 RPC block-fetching override in `SpawnStageHeaders` was completely removed. Without it, Arbitrum chains sit in `HeadersPOW` "Waiting for headers..." forever since there are no P2P peers for L2 headers.

## Problem

The current `SpawnStageHeaders` (in `execution/stagedsync/stage_headers.go`) unconditionally falls through to `HeadersPOW`, which requests headers via P2P. For Arbitrum chains, headers must come from an L2 RPC endpoint instead.

The original `arbitrum` branch had an `IsArbitrum()` guard that bypassed `HeadersPOW` and fetched blocks directly from an L2 RPC via `genfromrpc.GetAndCommitBlocks()`.

## What Was Lost

The Arbitrum override in `SpawnStageHeaders` that:
1. Checks `cfg.chainConfig.IsArbitrum()` — if false, falls through to `HeadersPOW`
2. Connects to L2 RPC endpoint (`cfg.L2RPC.Addr`)
3. Queries latest block number via `eth_blockNumber`
4. Determines block range: `[currentProgress+1, min(latestRemote, progress+LoopBlockLimit)]`
5. Detects chain-tip mode (within 20 blocks of tip) and switches to public receipt feeds
6. Runs one-time health check on L2 RPC endpoints
7. Calls `genfromrpc.GetAndCommitBlocks()` to batch-download blocks from RPC
8. Updates header download + body download progress via `finaliseState` callback
9. Rate-limits calls with `headersLimiter` (124ms interval)

Also lost:
- `HeadersCfg` struct fields: `db kv.RwDB`, `bodyDownload`, `L2RPC ethconfig.L2RPCConfig`
- `StageHeadersCfg()` constructor params for those fields
- `SpawnStageHeaders` managed its own tx when `tx == nil` (external tx pattern)

## Plan

### Step 1: Extend `HeadersCfg` struct and constructor

**File:** `execution/stagedsync/stage_headers.go`

Add fields to `HeadersCfg`:
```go
type HeadersCfg struct {
    // ... existing fields ...
    db          kv.RwDB
    bodyDownload *bodydownload.BodyDownload  // needed for finaliseState callback
    l2rpc       ethconfig.L2RPCConfig
}
```

Update `StageHeadersCfg()` to accept these new parameters. Keep existing parameters unchanged to minimize churn.

New imports needed:
- `"sync"` (for `sync.Once`)
- `"github.com/erigontech/erigon/cmd/snapshots/genfromrpc"` (alias `snapshots`)
- `"github.com/erigontech/erigon/execution/stagedsync/bodydownload"`
- `"github.com/erigontech/erigon/execution/stagedsync/stages"`
- `"github.com/erigontech/erigon/rpc"`
- `"github.com/erigontech/erigon/node/ethconfig"` (already imported)

### Step 2: Add Arbitrum override to `SpawnStageHeaders`

**File:** `execution/stagedsync/stage_headers.go`

Add `IsArbitrum()` check at the top of `SpawnStageHeaders`, before `HeadersPOW`:
```go
func SpawnStageHeaders(s *StageState, u Unwinder, ctx context.Context, tx kv.RwTx, cfg HeadersCfg, logger log.Logger) error {
    if s.CurrentSyncCycle.IsInitialCycle {
        if err := cfg.hd.AddHeadersFromSnapshot(tx, cfg.blockReader); err != nil {
            return err
        }
    }

    if cfg.chainConfig.IsArbitrum() {
        return headersArbitrum(s, ctx, tx, cfg, logger)
    }

    cfg.hd.Progress()
    return HeadersPOW(s, u, ctx, tx, cfg, logger)
}
```

### Step 3: Implement `headersArbitrum` function

**File:** `execution/stagedsync/stage_headers.go`

Port from `arbitrum` branch, adapting to current API:

```go
var (
    l2RPCHealthCheckOnce sync.Once
    headersLimiter       = rate.NewLimiter(rate.Every(time.Millisecond*124), 1)
)

func headersArbitrum(s *StageState, ctx context.Context, tx kv.RwTx, cfg HeadersCfg, logger log.Logger) error {
    if !headersLimiter.Allow() {
        return nil
    }

    // 1. Connect to L2 RPC
    // 2. Get current progress from stages.Headers
    // 3. Query eth_blockNumber for latest remote block
    // 4. Determine receipt client (chain-tip mode → public feed)
    // 5. Health check (once)
    // 6. Calculate block range [firstBlock, stopBlock]
    // 7. Call genfromrpc.GetAndCommitBlocks(...)
    // 8. Run finaliseState callback to update hd + bd progress
}
```

Key adaptations from arbitrum branch to current API:
- Import paths changed: `erigon-lib/common` → `erigon/common`, `erigon-lib/log/v3` → `erigon/common/log/v3`
- `s.state` → `s.State` (exported field on current branch)
- `state.NewSharedDomains()` → `execctx.NewSharedDomains()` (if needed)
- `bodydownload` package moved to `execution/stagedsync/bodydownload/`

### Step 4: Add helper functions

**File:** `execution/stagedsync/stage_headers.go`

Port these from the `arbitrum` branch:
- `checkL2RPCEndpointsHealth()` — validates L2 RPC endpoints can serve blocks and receipts
- `getPublicReceiptFeed()` — returns public receipt feed URL for known chain IDs
- `publicReceiptFeeds` map — chain ID → public RPC URL

### Step 5: Update callers to pass new parameters

**File:** `execution/stagedsync/stageloop/stageloop.go`

Update `NewDefaultStages()` call at line ~693 to pass `db`, `bodyDownload`, `l2rpc`:
```go
stagedsync.StageHeadersCfg(
    db,                           // NEW
    controlServer.Hd,
    controlServer.Bd,             // NEW (bodyDownload)
    controlServer.ChainConfig,
    cfg.Sync,
    controlServer.SendHeaderRequest,
    controlServer.PropagateNewBlockHashes,
    controlServer.Penalize,
    p2pCfg.NoDiscovery,
    blockReader,
    cfg.L2RPC,                    // NEW
),
```

Do the same for the second call at line ~754.

**File:** `execution/tests/mock/mock_sentry.go` (line ~576)

Update to match new signature, passing zero-value `L2RPCConfig` since tests don't use L2 RPC.

### Step 6: Verify `cfg.L2RPC` is populated from CLI flags

**Files to check:**
- `node/ethconfig/config.go` — `L2RPCConfig` type exists (confirmed)
- `cmd/erigon/main.go` or CLI flag setup — verify `--l2rpc` flags wire to `cfg.L2RPC`
- `turbo/cli/` or `cmd/utils/flags.go` — flag definitions

Check if CLI flags still exist for `--l2rpc`, `--l2rpc.receipt`, `--l2rpc.block.rps`, etc. If removed, restore flag registration.

## Files to Modify

| File | Change |
|------|--------|
| `execution/stagedsync/stage_headers.go` | Add `db`, `bodyDownload`, `l2rpc` to `HeadersCfg`; add `headersArbitrum()` function; add helpers |
| `execution/stagedsync/stageloop/stageloop.go` | Pass new params to `StageHeadersCfg()` (2 call sites) |
| `execution/tests/mock/mock_sentry.go` | Update `StageHeadersCfg()` call with zero-value params |

## Files to Verify (may need changes)

| File | What to Check |
|------|---------------|
| `cmd/snapshots/genfromrpc/genfromrpc.go` | `GetAndCommitBlocks` signature still matches |
| `arb/ethdb/ethdb.go` | `InitialiazeLocalWasmTarget` still exists |
| `turbo/cli/flags.go` or similar | CLI flags for `--l2rpc*` still wired |
| `execution/stagedsync/bodydownload/body_algos.go` | `UpdateFromDb` interface unchanged |

## Testing

- [x] Build compiles: `go build ./execution/stagedsync/...`
- [x] Full build: `make erigon`
- [x] Verify with arb-sepolia or arb1 chain config that `IsArbitrum()` triggers the override
- [x] Test with `--l2rpc` flag pointing to a working Arbitrum RPC
