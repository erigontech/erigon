# Staged Sync

Erigon synchronizes via ordered stages. Each stage processes blocks independently and can unwind during reorgs.

## Stage Pipeline Order

1. **Snapshots** - Download/verify immutable historical data
2. **Headers** - Download and validate block headers
3. **BlockHashes** - Index blockHash → blockNumber
4. **Bodies** - Download block bodies and transactions
5. **Senders** - Recover transaction senders from signatures
6. **Execution** - Execute blocks and compute state
7. **TxLookup** - Index txHash → blockNumber
8. **Finish** - Update RPC-visible head block

## Key Files

- `sync.go` - `Sync` struct orchestrates stage execution, tracks unwind points
- `stageloop/stageloop.go` - `StageLoop()` runs continuous sync cycle
- `default_stages.go` - Stage factory functions and ordering (`DefaultStages()`, `DefaultForwardOrder`, `DefaultUnwindOrder`)
- `stages/stages.go` - `SyncStage` constants and progress tracking

## Stage Interface

Each stage implements:
```go
type Stage struct {
    ID      stages.SyncStage
    Forward ExecFunc   // Execute stage forward
    Unwind  UnwindFunc // Handle reorgs
    Prune   PruneFunc  // Remove old data
}
```

## Configuration Pattern

Each stage has a config struct (e.g., `HeadersCfg`, `ExecuteBlockCfg`) containing:
- Database handles
- P2P handlers
- Chain config
- Batch sizes and tuning parameters

## Reorg Handling

1. `UnwindTo()` called with target block and reason
2. Stages unwind in reverse order (`DefaultUnwindOrder`)
3. State rolled back via domain writers
4. Execution resumes from unwind point

## Stage Implementations

| File | Stage | Purpose |
|------|-------|---------|
| `stage_snapshots.go` | Snapshots | Download segment files |
| `stage_headers.go` | Headers | P2P header download |
| `stage_blockhashes.go` | BlockHashes | blockHash→blockNum index |
| `stage_bodies.go` | Bodies | P2P body download |
| `stage_senders.go` | Senders | Signature recovery |
| `stage_execute.go` | Execution | Block execution, state changes |
| `stage_txlookup.go` | TxLookup | txHash→blockNum index |
| `stage_finish.go` | Finish | Update head for RPC |

## Supporting Modules

- `headerdownload/` - Header P2P download algorithms
- `bodydownload/` - Body P2P download algorithms
