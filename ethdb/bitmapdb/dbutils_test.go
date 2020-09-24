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

//func TestBug(t *testing.T) {
//	bm1 := roaring.NewBitmap()
//	bm1.Add(940287)
//	bm1.Add(940288)
//
//	bm1.RunOptimize()
//	buf := make([]byte, bm1.GetSerializedSizeInBytes())
//	_, err := bm1.WriteTo(bytes.NewBuffer(buf[:0]))
//	if err != nil {
//		panic(err)
//	}
//
//	bm2 := roaring.New()
//	_, err = bm2.FromBuffer(buf)
//	if err != nil {
//		panic(err)
//	}
//
//	bm3 := roaring.NewBitmap()
//	bm3.Add(1_000_000)
//	bm3.Add(1_000_001)
//	bm3.Add(1_000_002)
//	bm2.Or(bm3)
//
//	bm2.RunOptimize()
//	buf = make([]byte, bm2.GetSerializedSizeInBytes())
//	_, err = bm1.WriteTo(bytes.NewBuffer(buf[:0]))
//	if err != nil {
//		panic(err)
//	}
//
//	bm4 := roaring.New()
//	_, err = bm4.FromBuffer(buf)
//	if err != nil {
//		panic(err)
//	}
//
//	bm4.RemoveRange(1_000_001, 1_000_003)
//
//	bm4.RunOptimize()
//	buf = make([]byte, bm4.GetSerializedSizeInBytes())
//	_, err = bm4.WriteTo(bytes.NewBuffer(buf[:0]))
//	if err != nil {
//		panic(err)
//	}
//
//	bm5 := roaring.New()
//	_, err = bm5.FromBuffer(buf)
//	if err != nil {
//		panic(err)
//	}
//
//	bm5.Iterate(func(x uint32) bool {
//		fmt.Printf("%d\n", x)
//		return true
//	})
//}

func TestSharding(t *testing.T) {
	db := ethdb.NewMemDatabase()
	defer db.Close()

	tx, err := db.Begin(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	c := tx.(ethdb.HasTx).Tx().Cursor(dbutils.LogIndex)

	{
		k := []byte{1}
		// Write/Read large bitmap works expected
		for i := uint32(0); i < 3_000_000; i += 5_000 {
			bm1 := roaring.New()
			for j := i; j < i+5_000; j += 2 {
				bm1.Add(j)
			}
			err := bitmapdb.AppendMergeByOr(c, k, bm1)
			require.NoError(t, err)
		}

		fromDb, err := bitmapdb.Get(c, k)
		require.NoError(t, err)
		expect := roaring.NewBitmap()
		for i := uint32(0); i < 3_000_000; i += 5_000 {
			for j := i; j < i+5_000; j += 2 {
				expect.Add(j)
			}
		}
		expect.Xor(fromDb)
		require.Equal(t, 0, int(expect.GetCardinality()))

		// TrimShardedRange can remove large part
		err = bitmapdb.TrimShardedRange(c, k, 2_000_000, 3_000_000) // [from, to)
		require.NoError(t, err)

		fromDb, err = bitmapdb.Get(c, k)
		require.NoError(t, err)

		expect = roaring.New()
		for i := uint32(0); i < 2_000_000; i += 100_000 {
			for j := uint32(0); j < i+100_000; j += 2 {
				expect.Add(j)
			}
		}
		expect.Xor(fromDb)
		require.Equal(t, 0, int(expect.GetCardinality()))

		// check that TrimShardedRange will preserve right interval: [from, to)
		max := fromDb.Maximum()
		err = bitmapdb.TrimShardedRange(c, k, 0, uint64(fromDb.Maximum())) // [from, to)
		require.NoError(t, err)

		fromDb, err = bitmapdb.Get(c, k)
		require.NoError(t, err)
		require.Equal(t, 1, int(fromDb.GetCardinality()))
		require.Equal(t, int(max), int(fromDb.Maximum()))

	}

}
