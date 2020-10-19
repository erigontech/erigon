package migrations

import (
	"context"
	"errors"
	"testing"

	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/stretchr/testify/require"
)

func TestReceiptCbor(t *testing.T) {
	require, db := require.New(t), ethdb.NewMemDatabase()

	err := db.KV().Update(context.Background(), func(tx ethdb.Tx) error {
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

	err := db.KV().Update(context.Background(), func(tx ethdb.Tx) error {
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
			Logs: []*types.Log{
				{Address: common.HexToAddress("01"), Data: common.FromHex("02")},
				{Address: common.HexToAddress("03"), Data: common.FromHex("04")},
			},
		},
		{
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
	require.Equal(3, i)

	newK := make([]byte, 8+4)
	{
		binary.BigEndian.PutUint64(newK, 1)
		binary.BigEndian.PutUint32(newK[8:], 0)
		v, err := db.Get(dbutils.BlockReceiptsPrefix, newK)
		require.NoError(err)

		r := &types.Receipt{}
		err = cbor.Unmarshal(r, bytes.NewReader(v))
		require.NoError(err)
		require.Equal(len(block1[0].Logs), len(r.Logs))
		require.Equal(block1[0].Logs[0], r.Logs[0])
		require.Equal(block1[0].Logs[1], r.Logs[1])
	}
	{
		binary.BigEndian.PutUint64(newK, 1)
		binary.BigEndian.PutUint32(newK[8:], 1)
		v, err := db.Get(dbutils.BlockReceiptsPrefix, newK)
		require.NoError(err)

		r := &types.Receipt{}
		err = cbor.Unmarshal(r, bytes.NewReader(v))
		require.NoError(err)
		require.Equal(len(block1[1].Logs), len(r.Logs))
		require.Equal(block1[1].Logs[0], r.Logs[0])
		require.Equal(block1[1].Logs[1], r.Logs[1])
	}
	{
		binary.BigEndian.PutUint64(newK, 2)
		binary.BigEndian.PutUint32(newK[8:], 0)
		v, err := db.Get(dbutils.BlockReceiptsPrefix, newK)
		require.NoError(err)

		r := &types.Receipt{}
		err = cbor.Unmarshal(r, bytes.NewReader(v))
		require.NoError(err)
		require.Equal(len(block2[0].Logs), len(r.Logs))
		require.Equal(block2[0].Logs[0], r.Logs[0])
		require.Equal(block2[0].Logs[1], r.Logs[1])
	}

}
