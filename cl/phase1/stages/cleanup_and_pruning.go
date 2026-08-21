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
	// Pruning runs after the index tx is already committed, so a failure here is
	// a disk-space-reclaim miss, not a correctness issue — log and keep going.
	if err := cfg.blobStore.Prune(); err != nil {
		logger.Warn("failed to prune blob store", "err", err)
	}
	columnKeepSlots := cfg.caplinConfig.ColumnKeepSlots
	if columnKeepSlots == 0 {
		// Default: MIN_EPOCHS_FOR_DATA_COLUMN_SIDECARS_REQUESTS * SLOTS_PER_EPOCH
		columnKeepSlots = cfg.beaconCfg.MinEpochsForDataColumnSidecarsRequests * cfg.beaconCfg.SlotsPerEpoch
	}
	if err := cfg.peerDas.Prune(columnKeepSlots); err != nil {
		logger.Warn("failed to prune data columns", "err", err)
	}
	return nil
}
