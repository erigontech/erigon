# Caplin (Consensus Layer)

Caplin is Erigon's embedded Beacon Chain client implementing Ethereum's proof-of-stake consensus.

## Directory Structure

| Directory | Purpose |
|-----------|---------|
| `beacon/` | Beacon API HTTP handlers |
| `phase1/forkchoice/` | LMD-GHOST fork choice |
| `phase1/execution_client/` | Engine API bridge |
| `phase1/core/state/` | Beacon state machine |
| `phase1/network/` | Gossip handlers |
| `cltypes/` | Consensus types (blocks, attestations) |
| `sentinel/` | libp2p P2P networking |
| `pool/` | Operations pools (attestations, slashings) |
| `validator/` | Attestation producer |
| `antiquary/` | Freezes history to snapshots; prunes frozen state rows from the indexing DB |

## Key Components

### Fork Choice (`phase1/forkchoice/`)
- `forkchoice.go` - LMD-GHOST implementation
- `on_block.go` - Block processing
- `on_attestation.go` - Attestation handling
- Tracks finality and justification checkpoints

### Engine API (`phase1/execution_client/`)
Bridge to execution layer:
- `NewPayload` - Receive blocks from consensus
- `ForkchoiceUpdated` - Set canonical head
- `GetPayload` - Request block for building

### Beacon State (`phase1/core/state/`)
- State machine with fork upgrades (Altair, Bellatrix, Capella, Deneb)
- Validator registry management
- Epoch processing

### Sentinel (`sentinel/`)
- libp2p-based P2P networking
- GossipSub for block/attestation propagation
- Peer scoring and discovery

### Antiquary (`antiquary/`)
- Freezes historical blocks/states into `.seg` snapshots and prunes the frozen rows from the indexing DB
- State-prune invariant: a state table's rows may be deleted only below that table's contiguous-from-genesis snapshot coverage (`CaplinStateSnapshots.ContiguousCoverageEnd`); the state reader resolves per-table, segment-first, so covered DB rows are unreachable
- Progress markers in `kv.StatesPruneProgress`; kill-switch `CAPLIN_STATE_PRUNE_DISABLE`
- Blob retirement is gated on `blobBackfilled` (`antiquary.go`), set only when the blob history backfill reports complete. One gap the backfill cannot fill — for example older than `MIN_EPOCHS_FOR_BLOB_SIDECARS_REQUESTS`, which no peer can serve — leaves it false indefinitely, so **no** blob range is retired, including ranges far below the gap that are provably complete. The gate returns silently: no log, no metric
- In archive mode the backfill applies no retention floor (`blob_downloader.go`); only the non-archive path clamps `targetSlot` and `retryFloor` to the retention window. An archive node therefore re-requests past-retention slots indefinitely at the full request timeout
- `frozen_blobs` is published only from the forkchoice stage (`phase1/stages/forkchoice.go`), so it reads 0 until Caplin reaches it. Zero means "not yet observed", not "no blob snapshots" — dashboards guard it with `> 0`

## Beacon API (`beacon/handler/`)

REST API endpoints:
- Block production and validation
- Validator duties
- Chain state queries
- Node status

## Enable/Disable

```bash
# Caplin enabled by default (--internalcl)
./build/bin/erigon --datadir=./data

# Use external consensus client
./build/bin/erigon --externalcl --datadir=./data
```

## Archive Mode

```bash
# Enable historical state/block storage
./build/bin/erigon --caplin.archive --datadir=./data
```
