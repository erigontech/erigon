package migrations

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/stretchr/testify/require"
)

const oldExecutionKey = 4 // it won't change anymore, not before we apply this migration

func TestSyncStagesToUseNamedKeys(t *testing.T) {
	require, db := require.New(t), ethdb.NewTestDB(t)

	err := db.RwKV().Update(context.Background(), func(tx ethdb.RwTx) error {
		return tx.(ethdb.BucketMigrator).CreateBucket(dbutils.SyncStageProgressOld1)
	})
	require.NoError(err)

	// pretend that execution stage is at block 42
	err = db.Put(dbutils.SyncStageProgressOld1, []byte{byte(oldExecutionKey)}, dbutils.EncodeBlockNumber(42))
	require.NoError(err)

	migrator := NewMigrator()
	migrator.Migrations = []Migration{stagesToUseNamedKeys}
	err = migrator.Apply(db, "")
	require.NoError(err)

	i := 0
	err = db.Walk(dbutils.SyncStageProgress, nil, 0, func(k, v []byte) (bool, error) {
		i++
		return true, nil
	})
	require.NoError(err)
	require.Equal(1, i)

	v, err := db.GetOne(dbutils.SyncStageProgress, []byte{byte(oldExecutionKey)})
	require.NoError(err)
	require.Nil(v)

	v, err = db.GetOne(dbutils.SyncStageProgress, []byte(stages.Execution))
	require.NoError(err)
	require.Equal(42, int(binary.BigEndian.Uint64(v)))
}

func TestUnwindStagesToUseNamedKeys(t *testing.T) {
	require, db := require.New(t), ethdb.NewTestKV(t)

	err := db.Update(context.Background(), func(tx ethdb.RwTx) error {
		err := tx.CreateBucket(dbutils.SyncStageUnwindOld1)
		require.NoError(err)
		// pretend that execution stage is at block 42
		err = tx.Put(dbutils.SyncStageUnwindOld1, []byte{byte(oldExecutionKey)}, dbutils.EncodeBlockNumber(42))
		require.NoError(err)
		return nil
	})
	require.NoError(err)

	migrator := NewMigrator()
	migrator.Migrations = []Migration{unwindStagesToUseNamedKeys}
	err = migrator.Apply(ethdb.NewObjectDatabase(db), "")
	require.NoError(err)

	i := 0
	err = db.View(context.Background(), func(tx ethdb.Tx) error {
		err = tx.ForEach(dbutils.SyncStageUnwind, nil, func(k, v []byte) error {
			i++
			return nil
		})
		require.NoError(err)
		require.Equal(1, i)

		v, err := tx.GetOne(dbutils.SyncStageUnwind, []byte{byte(oldExecutionKey)})
		require.NoError(err)
		require.Nil(v)

		v, err = tx.GetOne(dbutils.SyncStageUnwind, []byte(stages.Execution))
		require.NoError(err)
		require.Equal(42, int(binary.BigEndian.Uint64(v)))
		return nil
	})
	require.NoError(err)
}
