package bitmapdb_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/RoaringBitmap/roaring"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/ethdb/bitmapdb"
	"github.com/stretchr/testify/require"
)

func TestRoaringBitmapAddOffset(t *testing.T) {
	db := ethdb.NewMemDatabase()
	defer db.Close()

	{ // Simple encoding
		k := []byte{1}
		bm1 := roaring.NewBitmap()
		bm1.Add(940287)
		bm1.Add(940288)
		err := bitmapdb.PutMergeByOr(db, dbutils.LogIndex, k, bm1)
		require.NoError(t, err)

		bm2 := roaring.NewBitmap()
		bm2.Add(1_000_000)
		bm2.Add(1_000_001)
		bm2.Add(1_000_002)
		err = bitmapdb.PutMergeByOr(db, dbutils.LogIndex, k, bm2)
		require.NoError(t, err)
		err = bitmapdb.RemoveRange(db, dbutils.LogIndex, k, 1_000_001, 1_000_003) // [from, to)
		require.NoError(t, err)

		bm3, err := bitmapdb.Get(db, dbutils.LogIndex, k)
		require.NoError(t, err)
		arr := bm3.ToArray()
		require.Equal(t, 3, len(arr))
		require.Equal(t, uint32(940287), arr[0])
		require.Equal(t, uint32(940288), arr[1])
		require.Equal(t, uint32(1_000_000), arr[2])
	}
}

func TestBug(t *testing.T) {
	bm1 := roaring.NewBitmap()
	bm1.Add(940287)
	bm1.Add(940288)

	bm1.RunOptimize()
	buf := make([]byte, bm1.GetSerializedSizeInBytes())
	_, err := bm1.WriteTo(bytes.NewBuffer(buf[:0]))
	if err != nil {
		panic(err)
	}

	bm2 := roaring.New()
	_, err = bm2.FromBuffer(buf)
	if err != nil {
		panic(err)
	}

	bm3 := roaring.NewBitmap()
	bm3.Add(1_000_000)
	bm3.Add(1_000_001)
	bm3.Add(1_000_002)
	bm2.Or(bm3)

	bm2.RunOptimize()
	buf = make([]byte, bm2.GetSerializedSizeInBytes())
	_, err = bm1.WriteTo(bytes.NewBuffer(buf[:0]))
	if err != nil {
		panic(err)
	}

	bm4 := roaring.New()
	_, err = bm4.FromBuffer(buf)
	if err != nil {
		panic(err)
	}

	bm4.RemoveRange(1_000_001, 1_000_003)

	bm4.RunOptimize()
	buf = make([]byte, bm4.GetSerializedSizeInBytes())
	_, err = bm4.WriteTo(bytes.NewBuffer(buf[:0]))
	if err != nil {
		panic(err)
	}

	bm5 := roaring.New()
	_, err = bm5.FromBuffer(buf)
	if err != nil {
		panic(err)
	}

	bm5.Iterate(func(x uint32) bool {
		fmt.Printf("%d\n", x)
		return true
	})
}

func TestRemoveRange(t *testing.T) {
	db := ethdb.NewMemDatabase()
	defer db.Close()

	{ // Simple encoding
		k := []byte{1}
		bm1 := roaring.NewBitmap()
		bm1.Add(940287)
		bm1.Add(940288)
		err := bitmapdb.PutMergeByOr(db, dbutils.LogIndex, k, bm1)
		require.NoError(t, err)

		bm2 := roaring.NewBitmap()
		bm2.Add(1_000_000)
		bm2.Add(1_000_001)
		bm2.Add(1_000_002)
		err = bitmapdb.PutMergeByOr(db, dbutils.LogIndex, k, bm2)
		require.NoError(t, err)
		err = bitmapdb.RemoveRange(db, dbutils.LogIndex, k, 1_000_001, 1_000_002) // [from, to)
		require.NoError(t, err)

		bm3, err := bitmapdb.Get(db, dbutils.LogIndex, k)
		require.NoError(t, err)
		arr := bm3.ToArray()
		require.Equal(t, 4, len(arr))
		require.Equal(t, uint32(940287), arr[0])
		require.Equal(t, uint32(940288), arr[1])
		require.Equal(t, uint32(1_000_000), arr[2])
		require.Equal(t, uint32(1_000_002), arr[3])
	}

}
