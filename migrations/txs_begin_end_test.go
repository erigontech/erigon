package migrations

import (
	"context"
	"testing"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/stretchr/testify/require"
)

func TestTxsBeginEnd(t *testing.T) {
	require, tmpDir, db := require.New(t), t.TempDir(), memdb.NewTestDB(t)

	migrator := NewMigrator(kv.ChainDB)
	migrator.Migrations = []Migration{txsBeginEnd}
	err := migrator.Apply(db, tmpDir)
	require.NoError(err)

	err = db.Update(context.Background(), func(tx kv.RwTx) error {
		if err := tx.ClearBucket(kv.Migrations); err != nil {
			return err
		}
		if err := stages.SaveStageProgress(tx, stages.Bodies, 1); err != nil {
			return err
		}
		return nil
	})
	require.NoError(err)

	err = migrator.Apply(db, tmpDir)
	require.ErrorIs(ErrTxsBeginEndNoMigration, err)
}
