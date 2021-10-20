package bitmapdb_test

import (
	"testing"

	"github.com/RoaringBitmap/roaring"
	"github.com/ledgerwatch/erigon/ethdb/bitmapdb"
	"github.com/stretchr/testify/require"
)

func TestCutLeft(t *testing.T) {
	bm := roaring.New()
	for j := 0; j < 10_000; j += 20 {
		bm.AddRange(uint64(j), uint64(j+10))
	}
	N := uint64(1024)
	for !bm.IsEmpty() {
		lft := bitmapdb.CutLeft(bm, N)
		lftSz := lft.GetSerializedSizeInBytes()
		if !bm.IsEmpty() {
			require.True(t, lftSz > N-256 && lftSz < N+256)
		} else {
			require.True(t, lft.GetSerializedSizeInBytes() > 0)
			require.True(t, lftSz < N+256)
		}
	}

	bm = roaring.New()
	for j := 0; j < 10_000; j += 20 {
		bm.AddRange(uint64(j), uint64(j+10))
	}
	N = uint64(2048)
	for !bm.IsEmpty() {
		lft := bitmapdb.CutLeft(bm, N)
		lftSz := lft.GetSerializedSizeInBytes()
		if !bm.IsEmpty() {
			require.True(t, lftSz > N-256 && lftSz < N+256)
		} else {
			require.True(t, lft.GetSerializedSizeInBytes() > 0)
			require.True(t, lftSz < N+256)
		}
	}

	bm = roaring.New()
	bm.Add(1)
	lft := bitmapdb.CutLeft(bm, N)
	require.True(t, lft.GetSerializedSizeInBytes() > 0)
	require.True(t, lft.GetCardinality() == 1)
	require.True(t, bm.GetCardinality() == 0)

	bm = roaring.New()
	lft = bitmapdb.CutLeft(bm, N)
	require.True(t, lft.GetCardinality() == 0)
	require.True(t, bm.GetCardinality() == 0)
}
