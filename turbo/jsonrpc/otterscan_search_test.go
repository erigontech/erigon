// Copyright 2024 The Erigon Authors
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

package jsonrpc

import (
	"testing"

	"github.com/RoaringBitmap/roaring/roaring64"
)

func createBitmap(t *testing.T, blocks []uint64) []byte {
	bm := roaring64.NewBitmap()
	bm.AddMany(blocks)

	chunk, err := bm.ToBytes()
	if err != nil {
		t.Fatal(err)
	}
	return chunk
}

func checkNext(t *testing.T, blockProvider BlockProvider, expectedBlock uint64, expectedHasNext bool) {
	bl, hasNext, err := blockProvider()
	if err != nil {
		t.Fatal(err)
	}
	if bl != expectedBlock {
		t.Fatalf("Expected block %d, received %d", expectedBlock, bl)
	}
	if expectedHasNext != hasNext {
		t.Fatalf("Expected hasNext=%t, received=%t; at block=%d", expectedHasNext, hasNext, expectedBlock)
	}
}
