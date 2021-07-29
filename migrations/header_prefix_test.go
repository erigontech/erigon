package migrations

import (
	"bytes"
	"context"
	"encoding/binary"
	"strconv"
	"testing"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHeaderPrefix(t *testing.T) {
	require := require.New(t)
	db := memdb.NewTestDB(t)

	err := db.Update(context.Background(), func(tx kv.RwTx) error {
		err := tx.CreateBucket(kv.HeaderPrefixOld)
		if err != nil {
			return err
		}
		for i := uint64(0); i < 10; i++ {
			//header
			err = tx.Put(kv.HeaderPrefixOld, dbutils.HeaderKey(i, common.Hash{uint8(i)}), []byte("header "+strconv.Itoa(int(i))))
			require.NoError(err)
			//canonical
			err = tx.Put(kv.HeaderPrefixOld, HeaderHashKey(i), common.Hash{uint8(i)}.Bytes())
			require.NoError(err)
			err = tx.Put(kv.HeaderPrefixOld, append(dbutils.HeaderKey(i, common.Hash{uint8(i)}), HeaderTDSuffix...), []byte{uint8(i)})
			require.NoError(err)
		}
		return nil
	})
	require.NoError(err)

	migrator := NewMigrator(kv.ChainDB)
	migrator.Migrations = []Migration{headerPrefixToSeparateBuckets}
	err = migrator.Apply(db, t.TempDir())
	require.NoError(err)

	num := 0
	err = db.View(context.Background(), func(tx kv.Tx) error {
		return tx.ForEach(kv.HeaderCanonical, []byte{}, func(k, v []byte) error {
			require.Len(k, 8)
			bytes.Equal(v, common.Hash{uint8(binary.BigEndian.Uint64(k))}.Bytes())
			num++
			return nil
		})
	})
	require.NoError(err)
	require.Equal(num, 10)

	num = 0
	err = db.View(context.Background(), func(tx kv.Tx) error {
		return tx.ForEach(kv.HeaderTD, []byte{}, func(k, v []byte) error {
			require.Len(k, 40)
			bytes.Equal(v, []byte{uint8(binary.BigEndian.Uint64(k))})
			num++
			return nil
		})
	})
	require.NoError(err)
	require.Equal(num, 10)

	num = 0
	err = db.View(context.Background(), func(tx kv.Tx) error {
		return tx.ForEach(kv.Headers, []byte{}, func(k, v []byte) error {
			require.Len(k, 40)
			bytes.Equal(v, []byte("header "+strconv.Itoa(int(binary.BigEndian.Uint64(k)))))
			num++
			return nil
		})
	})
	require.NoError(err)
	require.Equal(num, 10)

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
