// Copyright 2022 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package bitmapdb_test

import (
	"testing"

	"github.com/RoaringBitmap/roaring"
	"github.com/erigontech/erigon-lib/kv/bitmapdb"
	"github.com/stretchr/testify/require"
)

func TestCutLeft(t *testing.T) {
	bm := roaring.New()
	for j := 0; j < 10_000; j += 20 {
		bm.AddRange(uint64(j), uint64(j+10))
	}
	N := uint64(1024)
	for bm.GetCardinality() > 0 {
		lft := bitmapdb.CutLeft(bm, N)
		lftSz := lft.GetSerializedSizeInBytes()
		if bm.GetCardinality() > 0 {
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
	for bm.GetCardinality() > 0 {
		lft := bitmapdb.CutLeft(bm, N)
		lftSz := lft.GetSerializedSizeInBytes()
		if bm.GetCardinality() > 0 {
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
	require.True(t, lft == nil)
	require.True(t, bm.GetCardinality() == 0)
}
