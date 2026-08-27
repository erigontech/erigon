package stages

import (
	"context"

	"github.com/erigontech/erigon/cl/clparams"
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
	cfg.blobStore.Prune()
	columnKeepSlots := cfg.caplinConfig.ColumnKeepSlots
	if columnKeepSlots == 0 {
		columnKeepSlots = specColumnKeepSlots(cfg.beaconCfg)
	}
	cfg.peerDas.Prune(columnKeepSlots)
	return nil
}

// specColumnKeepSlots is the retention distance for MIN_EPOCHS_FOR_DATA_COLUMN_SIDECARS_REQUESTS.
// The window is stated in epochs and measured from the start of current_epoch - MIN_EPOCHS, so an
// exact epochs * SLOTS_PER_EPOCH distance cuts above that boundary whenever the head sits inside
// an epoch. The extra epoch keeps the cut at or below it, which matters because this distance also
// sets the earliest slot we advertise as servable.
func specColumnKeepSlots(beaconCfg *clparams.BeaconChainConfig) uint64 {
	return (beaconCfg.MinEpochsForDataColumnSidecarsRequests + 1) * beaconCfg.SlotsPerEpoch
}
