package migrations

import (
	"encoding/binary"
	"errors"
	"testing"

	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/stretchr/testify/require"
)

func TestSyncStagesToUseNamedKeys(t *testing.T) {
	require, db := require.New(t), ethdb.NewMemDatabase()

	err := db.KV().CreateBuckets(dbutils.SyncStageProgressOld1)
	require.NoError(err)

	// pretend that execution stage is at block 42
	err = db.Put(dbutils.SyncStageProgressOld1, []byte{byte(stages.Execution)}, dbutils.EncodeBlockNumber(42))
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

	_, err = db.Get(dbutils.SyncStageProgress, []byte{byte(stages.Execution)})
	require.True(errors.Is(err, ethdb.ErrKeyNotFound))

	v, err := db.Get(dbutils.SyncStageProgress, stages.DBKeys[stages.Execution])
	require.NoError(err)
	require.Equal(42, int(binary.BigEndian.Uint64(v)))
}

func TestUnwindStagesToUseNamedKeys(t *testing.T) {
	require, db := require.New(t), ethdb.NewMemDatabase()

	err := db.KV().CreateBuckets(dbutils.SyncStageUnwindOld1)
	require.NoError(err)

	// pretend that execution stage is at block 42
	err = db.Put(dbutils.SyncStageUnwindOld1, []byte{byte(stages.Execution)}, dbutils.EncodeBlockNumber(42))
	require.NoError(err)

	migrator := NewMigrator()
	migrator.Migrations = []Migration{unwindStagesToUseNamedKeys}
	err = migrator.Apply(db, "")
	require.NoError(err)

	i := 0
	err = db.Walk(dbutils.SyncStageUnwind, nil, 0, func(k, v []byte) (bool, error) {
		i++
		return true, nil
	})
	require.NoError(err)
	require.Equal(1, i)

	_, err = db.Get(dbutils.SyncStageUnwind, []byte{byte(stages.Execution)})
	require.True(errors.Is(err, ethdb.ErrKeyNotFound))

	v, err := db.Get(dbutils.SyncStageUnwind, stages.DBKeys[stages.Execution])
	require.NoError(err)
	require.Equal(42, int(binary.BigEndian.Uint64(v)))
}
