# Refactor: Extract BlobHistoryDownloader into dedicated struct

## Summary

This PR refactors the blob history downloading logic from `downloadBlobHistoryWorker` function into a dedicated `BlobHistoryDownloader` struct in the `cl/phase1/network` package. This provides better encapsulation, testability, and allows for dynamic control of the blob backfilling process.

## Changes

### New: `cl/phase1/network/blob_downloader.go`

Introduces `BlobHistoryDownloader` struct with the following features:

- **Configurable head slot**: The `headSlot` can be updated via `SetHeadSlot()` to target higher slots as the chain progresses
- **Periodic downloading**: Runs a download loop every 12 seconds when started
- **Peer count check**: Skips iterations if peer count is below 16 and logs a warning
- **Conditional start**: Only starts the goroutine if `archiveBlobs` or `immediateBlobsBackfilling` is enabled
- **Progress tracking**: Maintains `highestBackfilledSlot` to track backfilling progress
- **Callback support**: `SetNotifyBlobBackfilled()` allows setting a callback when backfilling completes

#### Key Methods

| Method | Description |
|--------|-------------|
| `NewBlobHistoryDownloader()` | Constructor with all dependencies |
| `SetHeadSlot(slot)` | Sets the target head slot (currentSlot + 1) |
| `SetNotifyBlobBackfilled(fn)` | Sets completion callback |
| `HeadSlot()` | Returns current head slot |
| `HighestBackfilledSlot()` | Returns highest backfilled slot |
| `Running()` | Returns whether downloader is active |
| `Start()` | Begins the download loop |

### Modified: `cl/phase1/stages/clstages.go`

- Added `blobDownloader` field to `Cfg` struct
- Updated `ClStagesCfg()` to accept `ctx context.Context` parameter
- Creates `BlobHistoryDownloader` instance in the config constructor

### Modified: `cl/phase1/stages/stage_history_download.go`

- Added `blobDownloader` field to `StageHistoryReconstructionCfg`
- Updated `StageHistoryReconstruction()` to accept `blobDownloader` parameter
- Removed inline `downloadBlobHistoryWorker` function (~140 lines)
- Stage now uses the passed-in `blobDownloader` instance

### Modified: `cmd/caplin/caplin1/run.go`

- Added `ctx` as first argument to `ClStagesCfg()` call

### Modified: `cmd/capcli/cli.go`

- Added `nil` for the new `blobDownloader` parameter

## Design Decisions

1. **Interfaces for decoupling**: Uses `SyncedChecker` and `PeerDasGetter` interfaces to avoid import cycles with the forkchoice package

2. **Minimum peer requirement**: Requires at least 16 peers before attempting blob downloads to ensure reliable data availability

3. **12-second interval**: Matches slot time for periodic checks, balancing responsiveness with network efficiency

4. **Conditional execution**: The `Start()` method checks `archiveBlobs || immediateBlobsBackfilling` before spawning the goroutine, making it safe to call unconditionally

## Testing

- [x] `make erigon` builds successfully
- [x] `make caplin` builds successfully

## Migration Notes

This is a refactor with no behavioral changes to the blob downloading logic. The same functionality is preserved but now encapsulated in a reusable struct.
