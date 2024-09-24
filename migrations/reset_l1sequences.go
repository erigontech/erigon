package migrations

import (
	"context"
	"fmt"

	"github.com/gateway-fm/cdk-erigon-lib/common/datadir"
	"github.com/gateway-fm/cdk-erigon-lib/kv"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
)

var resetL1Sequences = Migration{
	Name: "remove l1 sequences and stage_l1sync progress to download all l1 sequences anew",
	Up: func(db kv.RwDB, dirs datadir.Dirs, progress []byte, BeforeCommit Callback) (err error) {
		tx, err := db.BeginRw(context.Background())
		if err != nil {
			return err
		}
		defer tx.Rollback()
		tx.ClearBucket(kv.L1SEQUENCES)

		// already checked
		if err := stages.SaveStageProgress(tx, stages.L1Syncer, 0); err != nil {
			return fmt.Errorf("failed to get highest checked block, %w", err)
		}

		// This migration is no-op, but it forces the migration mechanism to apply it and thus write the DB schema version info
		if err := BeforeCommit(tx, nil, true); err != nil {
			return err
		}
		return tx.Commit()
	},
}
