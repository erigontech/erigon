package jsonrpc

import (
	"testing"
)

func TestFromToForwardBlockProviderWith1Chunk(t *testing.T) {
	// Mocks 1 chunk
	chunk1 := createBitmap(t, []uint64{1000, 1005, 1010})

	chunkLocator := newMockForwardChunkLocator([][]byte{chunk1})
	fromBlockProvider := NewForwardBlockProvider(chunkLocator, 0)
	toBlockProvider := NewForwardBlockProvider(newMockForwardChunkLocator([][]byte{}), 0)
	blockProvider := newCallFromToBlockProvider(false, fromBlockProvider, toBlockProvider)

	checkNext(t, blockProvider, 1000, true)
	checkNext(t, blockProvider, 1005, true)
	checkNext(t, blockProvider, 1010, false)
}

func TestFromToForwardBlockProviderWith1ChunkMiddleBlock(t *testing.T) {
	// Mocks 1 chunk
	chunk1 := createBitmap(t, []uint64{1000, 1005, 1010})

	chunkLocator := newMockForwardChunkLocator([][]byte{chunk1})
	fromBlockProvider := NewForwardBlockProvider(chunkLocator, 1005)
	toBlockProvider := NewForwardBlockProvider(newMockForwardChunkLocator([][]byte{}), 0)
	blockProvider := newCallFromToBlockProvider(false, fromBlockProvider, toBlockProvider)

	checkNext(t, blockProvider, 1005, true)
	checkNext(t, blockProvider, 1010, false)
}

func TestFromToForwardBlockProviderWith1ChunkNotExactBlock(t *testing.T) {
	// Mocks 1 chunk
	chunk1 := createBitmap(t, []uint64{1000, 1005, 1010})

	chunkLocator := newMockForwardChunkLocator([][]byte{chunk1})
	fromBlockProvider := NewForwardBlockProvider(chunkLocator, 1007)
	toBlockProvider := NewForwardBlockProvider(newMockForwardChunkLocator([][]byte{}), 0)
	blockProvider := newCallFromToBlockProvider(false, fromBlockProvider, toBlockProvider)

	checkNext(t, blockProvider, 1010, false)
}

func TestFromToForwardBlockProviderWith1ChunkLastBlock(t *testing.T) {
	// Mocks 1 chunk
	chunk1 := createBitmap(t, []uint64{1000, 1005, 1010})

	chunkLocator := newMockForwardChunkLocator([][]byte{chunk1})
	fromBlockProvider := NewForwardBlockProvider(chunkLocator, 1010)
	toBlockProvider := NewForwardBlockProvider(newMockForwardChunkLocator([][]byte{}), 0)
	blockProvider := newCallFromToBlockProvider(false, fromBlockProvider, toBlockProvider)

	checkNext(t, blockProvider, 1010, false)
}

func TestFromToForwardBlockProviderWith1ChunkBlockNotFound(t *testing.T) {
	// Mocks 1 chunk
	chunk1 := createBitmap(t, []uint64{1000, 1005, 1010})

	chunkLocator := newMockForwardChunkLocator([][]byte{chunk1})
	fromBlockProvider := NewForwardBlockProvider(chunkLocator, 1100)
	toBlockProvider := NewForwardBlockProvider(newMockForwardChunkLocator([][]byte{}), 0)
	blockProvider := newCallFromToBlockProvider(false, fromBlockProvider, toBlockProvider)

	checkNext(t, blockProvider, 0, false)
}

func TestFromToForwardBlockProviderWithNoChunks(t *testing.T) {
	chunkLocator := newMockForwardChunkLocator([][]byte{})
	fromBlockProvider := NewForwardBlockProvider(chunkLocator, 0)
	toBlockProvider := NewForwardBlockProvider(newMockForwardChunkLocator([][]byte{}), 0)
	blockProvider := newCallFromToBlockProvider(false, fromBlockProvider, toBlockProvider)

	checkNext(t, blockProvider, 0, false)
}

func TestFromToForwardBlockProviderWithMultipleChunks(t *testing.T) {
	// Mocks 2 chunks
	chunk1 := createBitmap(t, []uint64{1000, 1005, 1010})
	chunk2 := createBitmap(t, []uint64{1501, 1600})

	chunkLocator := newMockForwardChunkLocator([][]byte{chunk1, chunk2})
	fromBlockProvider := NewForwardBlockProvider(chunkLocator, 0)
	toBlockProvider := NewForwardBlockProvider(newMockForwardChunkLocator([][]byte{}), 0)
	blockProvider := newCallFromToBlockProvider(false, fromBlockProvider, toBlockProvider)

	checkNext(t, blockProvider, 1000, true)
	checkNext(t, blockProvider, 1005, true)
	checkNext(t, blockProvider, 1010, true)
	checkNext(t, blockProvider, 1501, true)
	checkNext(t, blockProvider, 1600, false)
}

func TestFromToForwardBlockProviderWithMultipleChunksBlockBetweenChunks(t *testing.T) {
	// Mocks 2 chunks
	chunk1 := createBitmap(t, []uint64{1000, 1005, 1010})
	chunk2 := createBitmap(t, []uint64{1501, 1600})

	chunkLocator := newMockForwardChunkLocator([][]byte{chunk1, chunk2})
	fromBlockProvider := NewForwardBlockProvider(chunkLocator, 1300)
	toBlockProvider := NewForwardBlockProvider(newMockForwardChunkLocator([][]byte{}), 0)
	blockProvider := newCallFromToBlockProvider(false, fromBlockProvider, toBlockProvider)

	checkNext(t, blockProvider, 1501, true)
	checkNext(t, blockProvider, 1600, false)
}
