package bitmapdb_test

import (
	"context"
	"testing"

	"github.com/RoaringBitmap/roaring"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/ethdb/bitmapdb"
	"github.com/stretchr/testify/require"
)

func TestSharding(t *testing.T) {
	db := ethdb.NewMemDatabase()
	defer db.Close()

	txn, err := db.Begin(context.Background())
	require.NoError(t, err)
	defer txn.Rollback()

	tx := txn.(ethdb.HasTx).Tx()
	c := tx.Cursor(dbutils.LogTopicIndex)

	{
		k := []byte{1}
		// Write/Read large bitmap works expected
		for i := uint32(0); i < 3_000_000; i += 1_000_000 {
			bm1 := roaring.New()
			for j := i; j < i+1_000_000; j += 20 {
				bm1.AddRange(uint64(j), uint64(j+10))
			}
			err := bitmapdb.AppendMergeByOr(c, k, bm1)
			require.NoError(t, err)
		}

		fromDb, err := bitmapdb.Get(c, k, 0, 10_000_000)
		require.NoError(t, err)
		expect := roaring.New()
		for i := uint32(0); i < 3_000_000; i += 1_000_000 {
			for j := i; j < i+1_000_000; j += 20 {
				expect.AddRange(uint64(j), uint64(j+10))
			}
		}

		expect.Xor(fromDb)
		require.Equal(t, 0, int(expect.GetCardinality()))

		// TruncateRange can remove large part
		err = bitmapdb.TruncateRange(tx, dbutils.LogTopicIndex, k, 2_000_000, 3_000_000) // [from, to)
		require.NoError(t, err)

		fromDb, err = bitmapdb.Get(c, k, 0, 10_000_000)
		require.NoError(t, err)

		expect = roaring.New()
		for i := uint32(0); i < 2_000_000; i += 1_000_000 {
			for j := i; j < i+1_000_000; j += 20 {
				expect.AddRange(uint64(j), uint64(j+10))
			}
		}
		expect.Xor(fromDb)
		require.Equal(t, 0, int(expect.GetCardinality()))

		// check that TruncateRange will preserve right interval: [from, to)
		max := fromDb.Maximum()
		err = bitmapdb.TruncateRange(tx, dbutils.LogTopicIndex, k, 0, uint64(fromDb.Maximum())) // [from, to)
		require.NoError(t, err)

		fromDb, err = bitmapdb.Get(c, k, 0, 10_000_000)
		require.NoError(t, err)
		require.Equal(t, 1, int(fromDb.GetCardinality()))
		require.Equal(t, int(max), int(fromDb.Maximum()))
	}
}

func TestCutLeft(t *testing.T) {
	bm := roaring.New()
	for j := 0; j < 10_000; j += 20 {
		bm.AddRange(uint64(j), uint64(j+10))
	}
	N := uint64(1024)
	Precision := uint64(256)
	for bm.GetCardinality() > 0 {
		lft := bitmapdb.CutLeft(bm, N, Precision)
		lftSz := lft.GetSerializedSizeInBytes()
		if bm.GetCardinality() > 0 {
			require.True(t, lftSz > N-Precision && lftSz < N+Precision)
		} else {
			require.True(t, lft.GetSerializedSizeInBytes() > 0)
			require.True(t, lftSz < N+Precision)
		}
	}

	bm = roaring.New()
	for j := 0; j < 10_000; j += 20 {
		bm.AddRange(uint64(j), uint64(j+10))
	}
	N = uint64(2048)
	Precision = uint64(128)
	for bm.GetCardinality() > 0 {
		lft := bitmapdb.CutLeft(bm, N, Precision)
		lftSz := lft.GetSerializedSizeInBytes()
		if bm.GetCardinality() > 0 {
			require.True(t, lftSz > N-Precision && lftSz < N+Precision)
		} else {
			require.True(t, lft.GetSerializedSizeInBytes() > 0)
			require.True(t, lftSz < N+Precision)
		}
	}

	bm = roaring.New()
	bm.Add(1)
	lft := bitmapdb.CutLeft(bm, N, Precision)
	require.True(t, lft.GetSerializedSizeInBytes() > 0)
	require.True(t, lft.GetCardinality() == 1)
	require.True(t, bm.GetCardinality() == 0)

	bm = roaring.New()
	lft = bitmapdb.CutLeft(bm, N, Precision)
	require.True(t, lft == nil)
	require.True(t, bm.GetCardinality() == 0)
}
