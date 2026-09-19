package stages

import (
	"context"

	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/common/log/v3"
)

// cleanupAndPruning cleans up the database and prunes old data.
func cleanupAndPruning(ctx context.Context, logger log.Logger, cfg *Cfg, args Args) error {
	tx, err := cfg.indiciesDB.BeginRw(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	const blockPruneDistance = uint64(1_000_000)

	if !cfg.caplinConfig.ArchiveBlocks && args.seenSlot > blockPruneDistance {
		if err := beacon_indicies.PruneBlocks(ctx, tx, args.seenSlot-blockPruneDistance); err != nil {
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return err
	}
	currentSlot := cfg.ethClock.GetCurrentSlot()
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
