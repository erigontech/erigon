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

	if !cfg.caplinConfig.ArchiveBlocks {
		if err := beacon_indicies.PruneBlocks(ctx, tx, args.seenSlot-blockPruneDistance); err != nil {
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return err
	}
	cfg.blobStore.Prune()
	columnKeepSlots := cfg.caplinConfig.ColumnKeepSlots
	if columnKeepSlots == 0 {
		// Default: MIN_EPOCHS_FOR_DATA_COLUMN_SIDECARS_REQUESTS * SLOTS_PER_EPOCH
		columnKeepSlots = cfg.beaconCfg.MinEpochsForDataColumnSidecarsRequests * cfg.beaconCfg.SlotsPerEpoch
	}
	cfg.peerDas.Prune(columnKeepSlots)
	return nil
}
