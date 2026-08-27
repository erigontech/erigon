package stages

import (
	"context"
	"math"

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

	// Sidecar retention cuts against wall-clock, not the stage head, so a stalled
	// stage does not retain data forever.
	currentSlot := cfg.ethClock.GetCurrentSlot()
	pruneBlobDistance := uint64(128600)
	if cfg.caplinConfig.ArchiveBlobs || cfg.caplinConfig.BlobPruningDisabled {
		pruneBlobDistance = math.MaxUint64
	}
	if err := cfg.blobStore.PruneBelow(floorFor(currentSlot, pruneBlobDistance)); err != nil {
		logger.Warn("failed to prune blob sidecars", "err", err)
	}
	columnKeepSlots := cfg.caplinConfig.ColumnKeepSlots
	if columnKeepSlots == 0 {
		// Default: MIN_EPOCHS_FOR_DATA_COLUMN_SIDECARS_REQUESTS * SLOTS_PER_EPOCH
		columnKeepSlots = cfg.beaconCfg.MinEpochsForDataColumnSidecarsRequests * cfg.beaconCfg.SlotsPerEpoch
	}
	if err := cfg.peerDas.PruneBelow(floorFor(currentSlot, columnKeepSlots)); err != nil {
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
