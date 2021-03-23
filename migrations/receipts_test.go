package migrations

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/ethdb/cbor"
	"github.com/stretchr/testify/require"
)

func TestReceiptCbor(t *testing.T) {
	require, db := require.New(t), ethdb.NewMemDatabase()

	err := db.KV().Update(context.Background(), func(tx ethdb.RwTx) error {
		return tx.(ethdb.BucketMigrator).CreateBucket(dbutils.BlockReceiptsPrefix)
	})
	require.NoError(err)

	migrator := NewMigrator()
	migrator.Migrations = []Migration{receiptsCborEncode}
	err = migrator.Apply(db, "")
	require.NoError(err)

	err = receiptsCborEncode.Up(db, "tmp-test-dir", nil, func(db ethdb.Putter, key []byte, isDone bool) error {
		return nil
	})
	require.NoError(err)

	err = receiptsCborEncode.Up(db, "tmp-test-dir", []byte("load"), func(db ethdb.Putter, key []byte, isDone bool) error {
		return nil
	})
	require.True(errors.Is(err, ErrMigrationETLFilesDeleted))
}

func TestReceiptOnePerTx(t *testing.T) {
	require, db := require.New(t), ethdb.NewMemDatabase()

	err := db.KV().Update(context.Background(), func(tx ethdb.RwTx) error {
		return tx.(ethdb.BucketMigrator).CreateBucket(dbutils.BlockReceiptsPrefix)
	})
	require.NoError(err)

	type LegacyReceipt struct {
		PostState         []byte       `codec:"1"`
		Status            uint64       `codec:"2"`
		CumulativeGasUsed uint64       `codec:"3"`
		Logs              []*types.Log `codec:"4"`
	}

	buf := bytes.NewBuffer(nil)
	k := make([]byte, 8+32)

	binary.BigEndian.PutUint64(k, 1)
	block1 := []*LegacyReceipt{
		{
			CumulativeGasUsed: 1_000_000_000,
			Logs: []*types.Log{
				{Address: common.HexToAddress("01"), Data: common.FromHex("02")},
				{Address: common.HexToAddress("03"), Data: common.FromHex("04")},
			},
		},
		{
			CumulativeGasUsed: 2_000_000_000,
			Logs: []*types.Log{
				{Address: common.HexToAddress("05"), Data: common.FromHex("06")},
				{Address: common.HexToAddress("07"), Data: common.FromHex("08")},
			},
		},
	}
	err = cbor.Marshal(buf, block1)
	require.NoError(err)
	err = db.Put(dbutils.BlockReceiptsPrefix, common.CopyBytes(k), common.CopyBytes(buf.Bytes()))
	require.NoError(err)

	buf.Reset()
	binary.BigEndian.PutUint64(k, 2)
	block2 := []*LegacyReceipt{
		{
			CumulativeGasUsed: 3_000_000_000,
			Logs: []*types.Log{
				{Address: common.HexToAddress("09"), Data: common.FromHex("10")},
				{Address: common.HexToAddress("11"), Data: common.FromHex("12")},
			},
		},
	}
	err = cbor.Marshal(buf, block2)
	require.NoError(err)

	err = db.Put(dbutils.BlockReceiptsPrefix, common.CopyBytes(k), common.CopyBytes(buf.Bytes()))
	require.NoError(err)

	migrator := NewMigrator()
	migrator.Migrations = []Migration{receiptsOnePerTx}
	err = migrator.Apply(db, "")
	require.NoError(err)

	// test high-level data access didn't change
	i := 0
	err = db.Walk(dbutils.BlockReceiptsPrefix, nil, 0, func(k, v []byte) (bool, error) {
		i++
		return true, nil
	})
	require.NoError(err)
	require.Equal(2, i)

	i = 0
	err = db.Walk(dbutils.Log, nil, 0, func(k, v []byte) (bool, error) {
		i++
		return true, nil
	})
	require.NoError(err)
	require.Equal(3, i)

	{
		newK := make([]byte, 8)
		binary.BigEndian.PutUint64(newK, 1)
		v, err := db.Get(dbutils.BlockReceiptsPrefix, newK)
		require.NoError(err)

		var r types.Receipts
		err = cbor.Unmarshal(&r, bytes.NewReader(v))
		require.NoError(err)

		require.Equal(len(r), len(block1))
		require.Equal(r[0].CumulativeGasUsed, block1[0].CumulativeGasUsed)
		require.Equal(r[1].CumulativeGasUsed, block1[1].CumulativeGasUsed)
		require.Nil(r[0].Logs)
		require.Nil(r[1].Logs)
	}
	{
		newK := make([]byte, 8)
		binary.BigEndian.PutUint64(newK, 2)
		v, err := db.Get(dbutils.BlockReceiptsPrefix, newK)
		require.NoError(err)

		var r types.Receipts
		err = cbor.Unmarshal(&r, bytes.NewReader(v))
		require.NoError(err)
		require.Equal(len(r), len(block2))
		require.Equal(r[0].CumulativeGasUsed, block2[0].CumulativeGasUsed)
		require.Nil(r[0].Logs)
	}
	{
		newK := make([]byte, 8+4)
		binary.BigEndian.PutUint64(newK, 1)
		binary.BigEndian.PutUint32(newK[8:], 0)
		v, err := db.Get(dbutils.Log, newK)
		require.NoError(err)

		var l types.Logs
		err = cbor.Unmarshal(&l, bytes.NewReader(v))
		require.NoError(err)
		require.Equal(len(l), len(block1[0].Logs))
		require.Equal(l[0], block1[0].Logs[0])
		require.Equal(l[1], block1[0].Logs[1])
	}
	{
		newK := make([]byte, 8+4)
		binary.BigEndian.PutUint64(newK, 1)
		binary.BigEndian.PutUint32(newK[8:], 1)
		v, err := db.Get(dbutils.Log, newK)
		require.NoError(err)

		var l types.Logs
		err = cbor.Unmarshal(&l, bytes.NewReader(v))
		require.NoError(err)
		require.Equal(len(l), len(block1[1].Logs))
		require.Equal(l[0], block1[1].Logs[0])
		require.Equal(l[1], block1[1].Logs[1])
	}
	{
		newK := make([]byte, 8+4)
		binary.BigEndian.PutUint64(newK, 2)
		binary.BigEndian.PutUint32(newK[8:], 0)
		v, err := db.Get(dbutils.Log, newK)
		require.NoError(err)

		var l types.Logs
		err = cbor.Unmarshal(&l, bytes.NewReader(v))
		require.NoError(err)
		require.Equal(len(l), len(block2[0].Logs))
		require.Equal(l[0], block2[0].Logs[0])
	}
}
