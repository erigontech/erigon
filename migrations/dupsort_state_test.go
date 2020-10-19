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

func TestDupSortHashState(t *testing.T) {
	require, db := require.New(t), ethdb.NewMemDatabase()

	err := db.KV().Update(context.Background(), func(tx ethdb.Tx) error {
		return tx.(ethdb.BucketMigrator).CreateBucket(dbutils.CurrentStateBucketOld1)
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
	migrator.Migrations = []Migration{dupSortHashState}
	err = migrator.Apply(db, "")
	require.NoError(err)

	// test high-level data access didn't change
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

	tx, err := db.Begin(context.Background(), ethdb.RW)
	require.NoError(err)
	defer tx.Rollback()

	c := tx.(ethdb.HasTx).Tx().CursorDupSort(dbutils.CurrentStateBucket)
	// test low-level data layout
	require.NoError(err)

	v, err = c.SeekExact([]byte(accKey))
	require.NoError(err)
	require.Equal([]byte{1}, v)

	keyLen := common.HashLength + common.IncarnationLength
	k, v, err := c.SeekBothRange([]byte(storageKey)[:keyLen], []byte(storageKey)[keyLen:])
	require.NoError(err)
	require.Equal([]byte(storageKey)[:keyLen], k)
	require.Equal([]byte(storageKey)[keyLen:], v[:common.HashLength])
	require.Equal([]byte{2}, v[common.HashLength:])
}

func TestDupSortPlainState(t *testing.T) {
	require, db := require.New(t), ethdb.NewMemDatabase()

	err := db.KV().Update(context.Background(), func(tx ethdb.Tx) error {
		return tx.(ethdb.BucketMigrator).CreateBucket(dbutils.PlainStateBucketOld1)
	})
	require.NoError(err)

	accKey := string(common.FromHex(fmt.Sprintf("%040x", 0)))
	inc := string(common.FromHex("0000000000000001"))
	storageKey := accKey + inc + string(common.FromHex(fmt.Sprintf("%064x", 0)))

	err = db.Put(dbutils.PlainStateBucketOld1, []byte(accKey), []byte{1})
	require.NoError(err)
	err = db.Put(dbutils.PlainStateBucketOld1, []byte(storageKey), []byte{2})
	require.NoError(err)

	migrator := NewMigrator()
	migrator.Migrations = []Migration{dupSortPlainState}
	err = migrator.Apply(db, "")
	require.NoError(err)

	// test high-level data access didn't change
	i := 0
	err = db.Walk(dbutils.PlainStateBucket, nil, 0, func(k, v []byte) (bool, error) {
		i++
		return true, nil
	})
	require.NoError(err)
	require.Equal(2, i)

	v, err := db.Get(dbutils.PlainStateBucket, []byte(accKey))
	require.NoError(err)
	require.Equal([]byte{1}, v)

	v, err = db.Get(dbutils.PlainStateBucket, []byte(storageKey))
	require.NoError(err)
	require.Equal([]byte{2}, v)

	tx, err := db.Begin(context.Background(), ethdb.RW)
	require.NoError(err)
	defer tx.Rollback()

	c := tx.(ethdb.HasTx).Tx().CursorDupSort(dbutils.PlainStateBucket)
	v, err = c.SeekExact([]byte(accKey))
	require.NoError(err)
	require.Equal([]byte{1}, v)

	keyLen := common.AddressLength + common.IncarnationLength
	k, v, err := c.SeekBothRange([]byte(storageKey)[:keyLen], []byte(storageKey)[keyLen:])
	require.NoError(err)
	require.Equal([]byte(storageKey)[:keyLen], k)
	require.Equal([]byte(storageKey)[keyLen:], v[:common.HashLength])
	require.Equal([]byte{2}, v[common.HashLength:])
}
