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
