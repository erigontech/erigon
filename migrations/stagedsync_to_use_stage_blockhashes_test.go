package migrations

import (
	"context"
	"testing"

	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStagedsyncToUseStageBlockhashes(t *testing.T) {

	require, db := require.New(t), ethdb.NewMemDatabase()
	var expected uint64 = 12

	err := db.KV().Update(context.Background(), func(tx ethdb.Tx) error {
		return tx.Bucket(dbutils.SyncStageProgressOld2).(ethdb.BucketMigrator).Create()
	})

	require.NoError(err)
	err = db.Put(dbutils.SyncStageProgressOld2, stages.DBKeys[stages.Headers], dbutils.EncodeBlockNumber(expected))
	require.NoError(err)

	migrator := NewMigrator()
	migrator.Migrations = []Migration{stagedsyncToUseStageBlockhashes}
	err = migrator.Apply(db, "")
	require.NoError(err)

	actual, _, err := stages.GetStageProgress(db, stages.BlockHashes)
	assert.NoError(t, err)
	assert.Equal(t, expected, actual)
}

func TestUnwindStagedsyncToUseStageBlockhashes(t *testing.T) {

	require, db := require.New(t), ethdb.NewMemDatabase()
	var expected uint64 = 12

	err := db.KV().Update(context.Background(), func(tx ethdb.Tx) error {
		return tx.Bucket(dbutils.SyncStageUnwindOld2).(ethdb.BucketMigrator).Create()
	})

	require.NoError(err)
	err = db.Put(dbutils.SyncStageUnwindOld2, stages.DBKeys[stages.Headers], dbutils.EncodeBlockNumber(expected))
	require.NoError(err)

	migrator := NewMigrator()
	migrator.Migrations = []Migration{unwindStagedsyncToUseStageBlockhashes}
	err = migrator.Apply(db, "")
	require.NoError(err)

	actual, _, err := stages.GetStageUnwind(db, stages.BlockHashes)
	assert.NoError(t, err)
	assert.Equal(t, expected, actual)
}
