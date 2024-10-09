package jsonrpc

import (
	"bytes"
	"testing"

	"github.com/RoaringBitmap/roaring/roaring64"
)

func newMockForwardChunkLocator(chunks [][]byte) ChunkLocator {
	return func(block uint64) (ChunkProvider, bool, error) {
		for i, v := range chunks {
			bm := roaring64.NewBitmap()
			if _, err := bm.ReadFrom(bytes.NewReader(v)); err != nil {
				return nil, false, err
			}
			if block > bm.Maximum() {
				continue
			}

			return newMockForwardChunkProvider(chunks[i:]), true, nil
		}

		// Not found; return the last to simulate the behavior of returning
		// the 0xffff... chunk
		if len(chunks) > 0 {
			return newMockForwardChunkProvider(chunks[len(chunks)-1:]), true, nil
		}

		return nil, true, nil
	}
}

func newMockForwardChunkProvider(chunks [][]byte) ChunkProvider {
	i := 0
	return func() ([]byte, bool, error) {
		if i >= len(chunks) {
			return nil, false, nil
		}

		chunk := chunks[i]
		i++
		return chunk, true, nil
	}
}

func TestForwardBlockProviderWith1Chunk(t *testing.T) {
	// Mocks 1 chunk
	chunk1 := createBitmap(t, []uint64{1000, 1005, 1010})

	chunkLocator := newMockForwardChunkLocator([][]byte{chunk1})
	blockProvider := NewForwardBlockProvider(chunkLocator, 0)

	checkNext(t, blockProvider, 1000, true)
	checkNext(t, blockProvider, 1005, true)
	checkNext(t, blockProvider, 1010, false)
}

func TestForwardBlockProviderWith1ChunkMiddleBlock(t *testing.T) {
	// Mocks 1 chunk
	chunk1 := createBitmap(t, []uint64{1000, 1005, 1010})

	chunkLocator := newMockForwardChunkLocator([][]byte{chunk1})
	blockProvider := NewForwardBlockProvider(chunkLocator, 1005)

	checkNext(t, blockProvider, 1005, true)
	checkNext(t, blockProvider, 1010, false)
}

func TestForwardBlockProviderWith1ChunkNotExactBlock(t *testing.T) {
	// Mocks 1 chunk
	chunk1 := createBitmap(t, []uint64{1000, 1005, 1010})

	chunkLocator := newMockForwardChunkLocator([][]byte{chunk1})
	blockProvider := NewForwardBlockProvider(chunkLocator, 1007)

	checkNext(t, blockProvider, 1010, false)
}

func TestForwardBlockProviderWith1ChunkLastBlock(t *testing.T) {
	// Mocks 1 chunk
	chunk1 := createBitmap(t, []uint64{1000, 1005, 1010})

	chunkLocator := newMockForwardChunkLocator([][]byte{chunk1})
	blockProvider := NewForwardBlockProvider(chunkLocator, 1010)

	checkNext(t, blockProvider, 1010, false)
}

func TestForwardBlockProviderWith1ChunkBlockNotFound(t *testing.T) {
	// Mocks 1 chunk
	chunk1 := createBitmap(t, []uint64{1000, 1005, 1010})

	chunkLocator := newMockForwardChunkLocator([][]byte{chunk1})
	blockProvider := NewForwardBlockProvider(chunkLocator, 1100)

	checkNext(t, blockProvider, 0, false)
}

func TestForwardBlockProviderWithNoChunks(t *testing.T) {
	chunkLocator := newMockForwardChunkLocator([][]byte{})
	blockProvider := NewForwardBlockProvider(chunkLocator, 0)

	checkNext(t, blockProvider, 0, false)
}

func TestForwardBlockProviderWithMultipleChunks(t *testing.T) {
	// Mocks 2 chunks
	chunk1 := createBitmap(t, []uint64{1000, 1005, 1010})
	chunk2 := createBitmap(t, []uint64{1501, 1600})

	chunkLocator := newMockForwardChunkLocator([][]byte{chunk1, chunk2})
	blockProvider := NewForwardBlockProvider(chunkLocator, 0)

	checkNext(t, blockProvider, 1000, true)
	checkNext(t, blockProvider, 1005, true)
	checkNext(t, blockProvider, 1010, true)
	checkNext(t, blockProvider, 1501, true)
	checkNext(t, blockProvider, 1600, false)
}

func TestForwardBlockProviderWithMultipleChunksBlockBetweenChunks(t *testing.T) {
	// Mocks 2 chunks
	chunk1 := createBitmap(t, []uint64{1000, 1005, 1010})
	chunk2 := createBitmap(t, []uint64{1501, 1600})

	chunkLocator := newMockForwardChunkLocator([][]byte{chunk1, chunk2})
	blockProvider := NewForwardBlockProvider(chunkLocator, 1300)

	checkNext(t, blockProvider, 1501, true)
	checkNext(t, blockProvider, 1600, false)
}

func TestForwardBlockProviderWithMultipleChunksBlockNotFound(t *testing.T) {
	// Mocks 2 chunks
	chunk1 := createBitmap(t, []uint64{1000, 1005, 1010})
	chunk2 := createBitmap(t, []uint64{1501, 1600})

	chunkLocator := newMockForwardChunkLocator([][]byte{chunk1, chunk2})
	blockProvider := NewForwardBlockProvider(chunkLocator, 1700)

	checkNext(t, blockProvider, 0, false)
}
