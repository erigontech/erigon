package migrations

import (
	"context"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/stretchr/testify/require"
	"math/big"
	"testing"
)

func TestHeaderPrefix(t *testing.T) {
	require:=require.New(t)
	db := ethdb.NewMemDatabase()

	err := db.KV().Update(context.Background(), func(tx ethdb.Tx) error {
		return tx.(ethdb.BucketMigrator).CreateBucket(dbutils.HeaderPrefixOld)
	})
	require.NoError(err)

	headers:=[]*types.Header{
		{
			Number: big.NewInt(1),
		},
		{
			Number: big.NewInt(2),
		},
	}
	headers[1].ParentHash = headers[0].Hash()

	for i:=range headers {
		rawdb.WriteHeader(context.Background(), db, headers[i])
		err = rawdb.WriteCanonicalHash(db, headers[i].Hash(), headers[i].Number.Uint64())
	}


	require.NoError(err)

	migrator := NewMigrator()
	migrator.Migrations = []Migration{dupSortHashState}
	err = migrator.Apply(db, "")
	require.NoError(err)

	// test high-level data access didn't change
	i := 0
	err = db.Walk(dbutils.HashedStorageBucket, nil, 0, func(k, v []byte) (bool, error) {
		i++
		return true, nil
	})
	require.NoError(err)
	require.Equal(1, i)

	v, err := db.Get(dbutils.HashedStorageBucket, []byte(storageKey))
	require.NoError(err)
	require.Equal([]byte{2}, v)

	tx, err := db.Begin(context.Background(), ethdb.RW)
	require.NoError(err)
	defer tx.Rollback()

	c := tx.(ethdb.HasTx).Tx().CursorDupSort(dbutils.HashedStorageBucket)
	// test low-level data layout
	require.NoError(err)

	keyLen := common.HashLength + common.IncarnationLength
	k, v, err := c.SeekBothRange([]byte(storageKey)[:keyLen], []byte(storageKey)[keyLen:])
	require.NoError(err)
	require.Equal([]byte(storageKey)[:keyLen], k)
	require.Equal([]byte(storageKey)[keyLen:], v[:common.HashLength])
	require.Equal([]byte{2}, v[common.HashLength:])

}