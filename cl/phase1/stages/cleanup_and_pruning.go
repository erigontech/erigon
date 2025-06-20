package stages

import (
	"context"

	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
)

// cleanupAndPruning cleans up the database and prunes old data.
func cleanupAndPruning(ctx context.Context, logger log.Logger, cfg *Cfg, args Args) error {
	tx, err := cfg.indiciesDB.BeginRw(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	pruneDistance := uint64(1_000_000)

	if !cfg.caplinConfig.ArchiveBlocks {
		if err := beacon_indicies.PruneBlocks(ctx, tx, args.seenSlot-pruneDistance); err != nil {
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return err
	}
	cfg.blobStore.Prune()
	cfg.peerDas.Prune(pruneDistance)
	return nil
}
