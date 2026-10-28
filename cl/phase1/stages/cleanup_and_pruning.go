package stages

import (
	"context"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
)

// blockPruneSlot keeps the block-serving window and every block after the finalized
// checkpoint, which peers need to sync through a long period of non-finality.
func blockPruneSlot(tx kv.Tx, beaconCfg *clparams.BeaconChainConfig, headSlot uint64) (uint64, error) {
	highestFinalized, err := beacon_indicies.ReadHighestFinalized(tx)
	if err != nil {
		return 0, err
	}
	finalizedEpochStart := highestFinalized / beaconCfg.SlotsPerEpoch * beaconCfg.SlotsPerEpoch
	return min(beaconCfg.BlockServeRangeStartSlot(headSlot), finalizedEpochStart), nil
}

// cleanupAndPruning cleans up the database and prunes old data.
func cleanupAndPruning(ctx context.Context, logger log.Logger, cfg *Cfg, args Args) error {
	tx, err := cfg.indiciesDB.BeginRw(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	currentSlot := cfg.ethClock.GetCurrentSlot()
	if !cfg.caplinConfig.ArchiveBlocks {
		pruneTo, err := blockPruneSlot(tx, cfg.beaconCfg, min(args.seenSlot, currentSlot))
		if err != nil {
			return err
		}
		if err := beacon_indicies.PruneBlocks(tx, pruneTo); err != nil {
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	// Sidecar retention cuts against wall-clock, not the stage head, so a stalled
	// stage does not retain data forever.
	blobFloor := cfg.beaconCfg.BlobSidecarServeRangeStartSlot(currentSlot)
	if cfg.caplinConfig.ArchiveBlobs || cfg.caplinConfig.BlobPruningDisabled {
		blobFloor = 0
	}
	if err := cfg.blobStore.PruneBelow(blobFloor); err != nil {
		logger.Warn("failed to prune blob sidecars", "err", err)
	}
	columnFloor := cfg.beaconCfg.DataColumnSidecarServeRangeStartSlot(currentSlot)
	if keep := cfg.caplinConfig.ColumnKeepSlots; keep > 0 {
		columnFloor = floorFor(currentSlot, keep)
	}
	if err := cfg.peerDas.PruneBelow(columnFloor); err != nil {
		logger.Warn("failed to prune data column sidecars", "err", err)
	}
	return nil
}

func floorFor(head, keep uint64) uint64 {
	if head <= keep {
		return 0
	}
	return head - keep
}
