# ETL-Based Parallel Pruning Design

## Problem Statement

The current execution pruning in Erigon locks the MDBX database during write operations. Since pruning runs on the same `RwTx` as the hot-path execution, it creates contention:

1. `PruneSmallBatches()` holds write locks while deleting keys
2. The hot-path (`exec3_parallel.go`) must wait for pruning writes to complete
3. Even with timeout-based batching (500ms), the DB is blocked during each pruning iteration
4. MDBX's single-writer model means pruning writes serialize against execution writes

## Proposed Solution

Decouple pruning into two phases using ETL collectors:

1. **Collection Phase** (parallel, read-only): Use `RoTx` to identify keys to prune, collect them into ETL collectors
2. **Application Phase** (coordinated with hot-path): When hot-path is paused, apply collected deletes via `RwTx`

```
┌─────────────────────────────────────────────────────────────────────┐
│                        HOT-PATH EXECUTION                           │
│  ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐      │
│  │ Execute  │───▶│ Execute  │───▶│  PAUSE   │───▶│ Execute  │      │
│  │ Block N  │    │ Block N+1│    │ (commit) │    │ Block N+2│      │
│  └──────────┘    └──────────┘    └────┬─────┘    └──────────┘      │
└──────────────────────────────────────┼──────────────────────────────┘
                                       │
┌──────────────────────────────────────┼──────────────────────────────┐
│                     PRUNING PIPELINE │                              │
│                                      ▼                              │
│  ┌─────────────────────────┐   ┌──────────┐   ┌─────────────────┐  │
│  │   Collect (RoTx)        │   │  Apply   │   │   Collect       │  │
│  │   - Scan domains        │──▶│ Deletes  │──▶│   (continues)   │  │
│  │   - Fill ETL collectors │   │  (RwTx)  │   │                 │  │
│  └─────────────────────────┘   └──────────┘   └─────────────────┘  │
│         ▲                                              │            │
│         └──────────────────────────────────────────────┘            │
└─────────────────────────────────────────────────────────────────────┘
```

## Detailed Design

### 1. New Types

```go
// db/state/prune_collector.go

// PruneCollector manages ETL-based pruning for a single domain/index
type PruneCollector struct {
    domain      kv.Domain           // Which domain this collects for
    collector   *etl.Collector      // ETL collector for keys to delete
    table       string              // Target table name

    // Progress tracking
    lastStep    uint64              // Last step we scanned
    targetStep  uint64              // Target step to prune to

    // Stats
    keysCollected uint64
    bytesCollected uint64
}

// PruneCollectorSet manages collectors for all domains/indices
type PruneCollectorSet struct {
    mu          sync.Mutex

    // Domain collectors
    accounts    *PruneCollector
    storage     *PruneCollector
    code        *PruneCollector
    commitment  *PruneCollector

    // Index collectors
    logAddrs    *PruneCollector
    logTopics   *PruneCollector
    tracesFrom  *PruneCollector
    tracesTo    *PruneCollector

    // Coordination
    ready       atomic.Bool         // Collection complete, ready to apply
    applying    atomic.Bool         // Currently applying deletes

    tmpDir      string
    logger      log.Logger
}

// PruneCoordinator manages the collection/application lifecycle
type PruneCoordinator struct {
    collectors  *PruneCollectorSet

    // Channel for hot-path to signal pause
    pauseCh     chan struct{}
    // Channel to signal hot-path can resume
    resumeCh    chan struct{}
    // Channel to request pruning application
    applyReqCh  chan *PruneApplyRequest

    agg         *Aggregator
    cfg         PruneConfig
}

type PruneApplyRequest struct {
    collectors  *PruneCollectorSet
    doneCh      chan error
}

type PruneConfig struct {
    CollectTimeout    time.Duration  // Max time for collection phase
    ApplyTimeout      time.Duration  // Max time for application phase
    MaxCollectedKeys  uint64         // Max keys to collect before forcing apply
    MaxCollectedBytes uint64         // Max bytes to collect before forcing apply
}
```

### 2. Collection Phase (Read-Only)


### 5. Pause/Resume Protocol

```go
// execution/stagedsync/exec3_parallel.go

func (pe *parallelExecutor) pauseWorkers() {
    // Signal all workers to finish current tx and pause
    pe.in.Pause()

    // Wait for in-flight results to drain
    pe.rws.WaitEmpty()

    // Workers are now idle
}

func (pe *parallelExecutor) resumeWorkers(newTx kv.RwTx) {
    // Reset worker state with new transaction
    pe.resetWorkers(context.Background(), pe.rs, newTx)

    // Resume work queue
    pe.in.Resume()
}
```

### 6. Modified PruneSmallBatches

The existing `PruneSmallBatches` becomes a fallback for non-parallel execution:

```go
// db/state/aggregator.go

func (at *AggregatorRoTx) PruneSmallBatches(
    ctx context.Context,
    timeout time.Duration,
    tx kv.RwTx,
    pruneCoord *PruneCoordinator,  // NEW: optional coordinator
) (haveMore bool, err error) {

    // If coordinator provided and has pending prunes, skip direct pruning
    // The coordinator will handle it during hot-path pauses
    if pruneCoord != nil && pruneCoord.HasPendingPrunes() {
        return true, nil
    }

    // Fall back to existing behavior for non-parallel execution
    // ... existing implementation ...
}
```

## Flow Diagram

```
Time ──────────────────────────────────────────────────────────────────▶

HOT-PATH:    [Execute]────[Execute]────[PAUSE]────[Execute]────[Execute]
                                          │
                                          ▼
                                    ┌───────────┐
                                    │  Apply    │
                                    │  Deletes  │
                                    │  (RwTx)   │
                                    └───────────┘
                                          ▲
                                          │
COLLECTOR:   [Collect]────[Collect]───────┴────[Collect]────[Collect]──
             (RoTx)        (RoTx)               (RoTx)        (RoTx)
```

## Configuration

```go
// Recommended defaults
var DefaultPruneConfig = PruneConfig{
    CollectTimeout:    30 * time.Second,  // Max collection time per cycle
    ApplyTimeout:      100 * time.Millisecond,  // Max apply time per pause
    MaxCollectedKeys:  100_000,           // Force apply after 100k keys
    MaxCollectedBytes: 64 * 1024 * 1024,  // Force apply after 64MB
}
```

## Benefits

1. **No DB Locks During Collection**: `RoTx` doesn't block writers
2. **Minimal Pause Duration**: Deletes are batched and applied quickly during natural pauses
3. **Parallel Collection**: All domains/indices collected concurrently
4. **Adaptive**: Collection continues while hot-path executes
5. **ETL Efficiency**: Sorted deletes for better MDBX performance
6. **Backpressure**: `MaxCollectedKeys`/`MaxCollectedBytes` prevent unbounded memory

## Risks and Mitigations

| Risk | Mitigation |
|------|------------|
| Collection falls behind | Increase collection goroutines or frequency |
| Apply takes too long | Reduce `MaxCollectedKeys`, apply in smaller batches |
| Memory pressure from collectors | ETL collectors auto-flush to disk |
| Stale reads during collection | Use consistent RoTx snapshot |
| Coordinator complexity | Clear state machine, atomic flags |

## Migration Path

1. Add `PruneCoordinator` with feature flag (disabled by default)
2. Instrument existing `PruneSmallBatches` to measure baseline
3. Enable coordinator in parallel execution mode only
4. Compare metrics: prune throughput, hot-path latency, DB lock contention
5. Tune configuration based on metrics
6. Enable by default after validation

## Open Questions

1. Should we use separate ETL collectors per step range for finer-grained progress?
2. How to handle commitment domain differently (it has different pruning semantics)?
3. Should collection pause during heavy execution load?
4. Integration with existing `CanPrune()` / `PruneProgress` tracking?
