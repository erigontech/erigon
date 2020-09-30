package bitmapdb_test

import (
	"context"
	"testing"

	"github.com/RoaringBitmap/gocroaring"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/ethdb/bitmapdb"
	"github.com/stretchr/testify/require"
)

func TestSharding(t *testing.T) {
	db := ethdb.NewMemDatabase()
	defer db.Close()

	tx, err := db.Begin(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	c := tx.(ethdb.HasTx).Tx().Cursor(dbutils.LogTopicIndex)

	{
		k := []byte{1}
		// Write/Read large bitmap works expected
		for i := uint32(0); i < 3_000_000; i += 1_000_000 {
			bm1 := gocroaring.New()
			for j := i; j < i+1_000_000; j += 20 {
				bm1.AddRange(uint64(j), uint64(j+10))
			}
			err := bitmapdb.AppendMergeByOr(c, k, bm1)
			require.NoError(t, err)
		}

		fromDb, err := bitmapdb.Get(c, k, 0, 10_000_000)
		require.NoError(t, err)
		expect := gocroaring.New()
		for i := uint32(0); i < 3_000_000; i += 1_000_000 {
			for j := i; j < i+1_000_000; j += 20 {
				expect.AddRange(uint64(j), uint64(j+10))
			}
		}

		expect.Xor(fromDb)
		require.Equal(t, 0, int(expect.GetCardinality()))

		// TruncateRange can remove large part
		err = bitmapdb.TruncateRange(c, k, 2_000_000, 3_000_000) // [from, to)
		require.NoError(t, err)

		fromDb, err = bitmapdb.Get(c, k, 0, 10_000_000)
		require.NoError(t, err)

		expect = gocroaring.New()
		for i := uint32(0); i < 2_000_000; i += 1_000_000 {
			for j := i; j < i+1_000_000; j += 20 {
				expect.AddRange(uint64(j), uint64(j+10))
			}
		}
		expect.Xor(fromDb)
		require.Equal(t, 0, int(expect.GetCardinality()))

		// check that TruncateRange will preserve right interval: [from, to)
		max := fromDb.Maximum()
		err = bitmapdb.TruncateRange(c, k, 0, uint64(fromDb.Maximum())) // [from, to)
		require.NoError(t, err)

		fromDb, err = bitmapdb.Get(c, k, 0, 10_000_000)
		require.NoError(t, err)
		require.Equal(t, 1, int(fromDb.GetCardinality()))
		require.Equal(t, int(max), int(fromDb.Maximum()))
	}
}
