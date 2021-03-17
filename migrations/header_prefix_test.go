package migrations

import (
	"bytes"
	"context"
	"encoding/binary"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"os"
	"strconv"
	"testing"
)

func TestHeaderPrefix(t *testing.T) {
	require:=require.New(t)
	db := ethdb.NewMemDatabase()

	err := db.KV().Update(context.Background(), func(tx ethdb.Tx) error {
		err :=  tx.(ethdb.BucketMigrator).CreateBucket(dbutils.HeaderPrefixOld)
		if err!=nil {
			return err
		}
		c:=tx.Cursor(dbutils.HeaderPrefixOld)
		for i:=uint64(0); i<10; i++ {
			//header
			err = c.Put(dbutils.HeaderKey(i, common.Hash{uint8(i)}), []byte("header "+strconv.Itoa(int(i))))
			require.NoError(err)
			//canonical
			err = c.Put(HeaderHashKey(i), common.Hash{uint8(i)}.Bytes())
			require.NoError(err)
			err = c.Put(append(dbutils.HeaderKey(i, common.Hash{uint8(i)}), HeaderTDSuffix...), []byte{uint8(i)})
			require.NoError(err)
		}
		return nil
	})
	require.NoError(err)



	migrator := NewMigrator()
	migrator.Migrations = []Migration{headerPrefixToSeparateBuckets}
	err = migrator.Apply(db, os.TempDir())
	require.NoError(err)

	num:=0
	err = db.Walk(dbutils.HeaderCanonicalBucket, []byte{}, 0, func(k, v []byte) (bool, error) {
		require.Len(k, 8)
		bytes.Equal(v, common.Hash{uint8(binary.BigEndian.Uint64(k))}.Bytes())
		num++
		return true, nil
	})
	require.NoError(err)
	require.Equal(num, 10)

	num=0
	err = db.Walk(dbutils.HeaderTDBucket, []byte{}, 0, func(k, v []byte) (bool, error) {
		require.Len(k, 40)
		bytes.Equal(v,[]byte{uint8(binary.BigEndian.Uint64(k))})
		num++
		return true, nil
	})
	require.NoError(err)
	require.Equal(num, 10)

	num=0
	err = db.Walk(dbutils.HeadersBucket, []byte{}, 0, func(k, v []byte) (bool, error) {
		require.Len(k, 40)
		bytes.Equal(v,[]byte("header "+ strconv.Itoa(int(binary.BigEndian.Uint64(k)))))
		num++
		return true, nil
	})
	require.NoError(err)
	require.Equal(num,10)


}

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
