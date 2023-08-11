package jsonrpc

import (
	"testing"
)

func TestFromToBackwardBlockProviderWith1Chunk(t *testing.T) {
	// Mocks 1 chunk
	chunk1 := createBitmap(t, []uint64{1000, 1005, 1010})

	chunkLocator := newMockBackwardChunkLocator([][]byte{chunk1})
	fromBlockProvider := NewBackwardBlockProvider(chunkLocator, 0)
	toBlockProvider := NewBackwardBlockProvider(newMockBackwardChunkLocator([][]byte{}), 0)
	blockProvider := newCallFromToBlockProvider(true, fromBlockProvider, toBlockProvider)

	checkNext(t, blockProvider, 1010, true)
	checkNext(t, blockProvider, 1005, true)
	checkNext(t, blockProvider, 1000, false)
}

func TestFromToBackwardBlockProviderWith1ChunkMiddleBlock(t *testing.T) {
	// Mocks 1 chunk
	chunk1 := createBitmap(t, []uint64{1000, 1005, 1010})

	chunkLocator := newMockBackwardChunkLocator([][]byte{chunk1})
	fromBlockProvider := NewBackwardBlockProvider(chunkLocator, 1005)
	toBlockProvider := NewBackwardBlockProvider(newMockBackwardChunkLocator([][]byte{}), 0)
	blockProvider := newCallFromToBlockProvider(true, fromBlockProvider, toBlockProvider)

	checkNext(t, blockProvider, 1005, true)
	checkNext(t, blockProvider, 1000, false)
}

func TestFromToBackwardBlockProviderWith1ChunkNotExactBlock(t *testing.T) {
	// Mocks 1 chunk
	chunk1 := createBitmap(t, []uint64{1000, 1005, 1010})

	chunkLocator := newMockBackwardChunkLocator([][]byte{chunk1})
	fromBlockProvider := NewBackwardBlockProvider(chunkLocator, 1003)
	toBlockProvider := NewBackwardBlockProvider(newMockBackwardChunkLocator([][]byte{}), 0)
	blockProvider := newCallFromToBlockProvider(true, fromBlockProvider, toBlockProvider)

	checkNext(t, blockProvider, 1000, false)
}

func TestFromToBackwardBlockProviderWith1ChunkLastBlock(t *testing.T) {
	// Mocks 1 chunk
	chunk1 := createBitmap(t, []uint64{1000, 1005, 1010})

	chunkLocator := newMockBackwardChunkLocator([][]byte{chunk1})
	fromBlockProvider := NewBackwardBlockProvider(chunkLocator, 1000)
	toBlockProvider := NewBackwardBlockProvider(newMockBackwardChunkLocator([][]byte{}), 0)
	blockProvider := newCallFromToBlockProvider(true, fromBlockProvider, toBlockProvider)

	checkNext(t, blockProvider, 1000, false)
}

func TestFromToBackwardBlockProviderWith1ChunkBlockNotFound(t *testing.T) {
	// Mocks 1 chunk
	chunk1 := createBitmap(t, []uint64{1000, 1005, 1010})

	chunkLocator := newMockBackwardChunkLocator([][]byte{chunk1})
	fromBlockProvider := NewBackwardBlockProvider(chunkLocator, 900)
	toBlockProvider := NewBackwardBlockProvider(newMockBackwardChunkLocator([][]byte{}), 0)
	blockProvider := newCallFromToBlockProvider(true, fromBlockProvider, toBlockProvider)

	checkNext(t, blockProvider, 0, false)
}

func TestFromToBackwardBlockProviderWithNoChunks(t *testing.T) {
	chunkLocator := newMockBackwardChunkLocator([][]byte{})
	fromBlockProvider := NewBackwardBlockProvider(chunkLocator, 0)
	toBlockProvider := NewBackwardBlockProvider(newMockBackwardChunkLocator([][]byte{}), 0)
	blockProvider := newCallFromToBlockProvider(true, fromBlockProvider, toBlockProvider)

	checkNext(t, blockProvider, 0, false)
}

func TestFromToBackwardBlockProviderWithMultipleChunks(t *testing.T) {
	// Mocks 2 chunks
	chunk1 := createBitmap(t, []uint64{1000, 1005, 1010})
	chunk2 := createBitmap(t, []uint64{1501, 1600})

	chunkLocator := newMockBackwardChunkLocator([][]byte{chunk1, chunk2})
	fromBlockProvider := NewBackwardBlockProvider(chunkLocator, 0)
	toBlockProvider := NewBackwardBlockProvider(newMockBackwardChunkLocator([][]byte{}), 0)
	blockProvider := newCallFromToBlockProvider(true, fromBlockProvider, toBlockProvider)

	checkNext(t, blockProvider, 1600, true)
	checkNext(t, blockProvider, 1501, true)
	checkNext(t, blockProvider, 1010, true)
	checkNext(t, blockProvider, 1005, true)
	checkNext(t, blockProvider, 1000, false)
}

func TestFromToBackwardBlockProviderWithMultipleChunksBlockBetweenChunks(t *testing.T) {
	// Mocks 2 chunks
	chunk1 := createBitmap(t, []uint64{1000, 1005, 1010})
	chunk2 := createBitmap(t, []uint64{1501, 1600})

	chunkLocator := newMockBackwardChunkLocator([][]byte{chunk1, chunk2})
	fromBlockProvider := NewBackwardBlockProvider(chunkLocator, 1500)
	toBlockProvider := NewBackwardBlockProvider(newMockBackwardChunkLocator([][]byte{}), 0)
	blockProvider := newCallFromToBlockProvider(true, fromBlockProvider, toBlockProvider)

	checkNext(t, blockProvider, 1010, true)
	checkNext(t, blockProvider, 1005, true)
	checkNext(t, blockProvider, 1000, false)
}
