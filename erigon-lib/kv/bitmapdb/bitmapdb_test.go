/*
   Copyright 2022 Erigon contributors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package bitmapdb_test

import (
	"testing"

	"github.com/RoaringBitmap/roaring"
	"github.com/ledgerwatch/erigon-lib/kv/bitmapdb"
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
