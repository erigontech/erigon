package migrations

import (
	"context"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"math/big"
	"testing"
)

func TestHeaderTypeDetection(t *testing.T) {

	// good input
	headerHashKey := common.Hex2Bytes("00000000000000006e")
	assert.False(t, IsHeaderKey(headerHashKey))
	assert.False(t, IsHeaderTDKey(headerHashKey))
	assert.True(t, IsHeaderHashKey(headerHashKey))

	headerKey := common.Hex2Bytes("0000000000004321ed7240d411782ae438adfd85f7edad373cea722318c6e7f5f5b30f9abc9b36fd")
	assert.True(t, IsHeaderKey(headerKey))
	assert.False(t, IsHeaderTDKey(headerKey))
	assert.False(t, IsHeaderHashKey(headerKey))

	headerTdKey := common.Hex2Bytes("0000000000004321ed7240d411782ae438adfd85f7edad373cea722318c6e7f5f5b30f9abc9b36fd74")
	assert.False(t, IsHeaderKey(headerTdKey))
	assert.True(t, IsHeaderTDKey(headerTdKey))
	assert.False(t, IsHeaderHashKey(headerTdKey))

	// bad input
	emptyKey := common.Hex2Bytes("")
	assert.False(t, IsHeaderKey(emptyKey))
	assert.False(t, IsHeaderTDKey(emptyKey))
	assert.False(t, IsHeaderHashKey(emptyKey))

	tooLongKey := common.Hex2Bytes("0000000000004321ed7240d411782ae438adfd85f7edad373cea722318c6e7f5f5b30f9abc9b36fd0000000000004321ed7240d411782ae438adfd85f7edad373cea722318c6e7f5f5b30f9abc9b36fd0000000000004321ed7240d411782ae438adfd85f7edad373cea722318c6e7f5f5b30f9abc9b36fd0000000000004321ed7240d411782ae438adfd85f7edad373cea722318c6e7f5f5b30f9abc9b36fd")
	assert.False(t, IsHeaderKey(tooLongKey))
	assert.False(t, IsHeaderTDKey(tooLongKey))
	assert.False(t, IsHeaderHashKey(tooLongKey))

	notRelatedInput := common.Hex2Bytes("alex")
	assert.False(t, IsHeaderKey(notRelatedInput))
	assert.False(t, IsHeaderTDKey(notRelatedInput))
	assert.False(t, IsHeaderHashKey(notRelatedInput))

}
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