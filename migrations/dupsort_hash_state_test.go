package migrations

import (
	"context"
	"fmt"
	"testing"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/stretchr/testify/require"
)

func TestDupsortHashState(t *testing.T) {
	require, db := require.New(t), ethdb.NewMemDatabase()

	err := db.KV().Update(context.Background(), func(tx ethdb.Tx) error {
		return tx.Bucket(dbutils.CurrentStateBucketOld1).(ethdb.BucketMigrator).Create()
	})
	require.NoError(err)

	accKey := string(common.FromHex(fmt.Sprintf("%064x", 0)))
	inc := string(common.FromHex("0000000000000001"))
	storageKey := accKey + inc + accKey

	err = db.Put(dbutils.CurrentStateBucketOld1, []byte(accKey), []byte{1})
	require.NoError(err)
	err = db.Put(dbutils.CurrentStateBucketOld1, []byte(storageKey), []byte{2})
	require.NoError(err)

	migrator := NewMigrator()
	migrator.Migrations = []Migration{dupsortHashState}
	err = migrator.Apply(db, "")
	require.NoError(err)

	i := 0
	err = db.Walk(dbutils.CurrentStateBucket, nil, 0, func(k, v []byte) (bool, error) {
		i++
		return true, nil
	})
	require.NoError(err)
	require.Equal(2, i)

	v, err := db.Get(dbutils.CurrentStateBucket, []byte(accKey))
	require.NoError(err)
	require.Equal([]byte{1}, v)

	v, err = db.Get(dbutils.CurrentStateBucket, []byte(storageKey))
	require.NoError(err)
	require.Equal([]byte{2}, v)

}
