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
	"bytes"
	"testing"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon/cmd/rpcdaemon/rpcdaemontest"
	"github.com/stretchr/testify/require"
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

func TestSearchTransactionsAfter(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestSentry(t)
	api := NewOtterscanAPI(newBaseApiForTest(m), m.DB, 25)

	addr := common.HexToAddress("0x537e697c7ab75a26f9ecf0ce810e3154dfcaaf44")
	t.Run("small page size", func(t *testing.T) {
		require := require.New(t)
		results, err := api.SearchTransactionsAfter(m.Ctx, addr, 2, 2)
		require.NoError(err)
		require.False(results.FirstPage)
		require.False(results.LastPage)
		require.Len(results.Txs, 2)
		require.Len(results.Receipts, 2)
	})
	t.Run("big page size", func(t *testing.T) {
		require := require.New(t)
		results, err := api.SearchTransactionsAfter(m.Ctx, addr, 2, 10)
		require.NoError(err)
		require.True(results.FirstPage)
		require.False(results.LastPage)
		require.Len(results.Txs, 3)
		require.Len(results.Receipts, 3)
	})
	t.Run("filter last block", func(t *testing.T) {
		require := require.New(t)
		results, err := api.SearchTransactionsAfter(m.Ctx, addr, 3, 10)

		require.NoError(err)
		require.True(results.FirstPage)
		require.False(results.LastPage)
		require.Len(results.Txs, 2)
		require.Len(results.Receipts, 2)

		require.Equal(5, int(results.Txs[0].BlockNumber.ToInt().Uint64()))
		require.Equal(0, int(results.Txs[0].Nonce))
		require.Equal(5, int(results.Receipts[0]["blockNumber"].(hexutil.Uint64)))
		require.Equal(common.HexToHash("0x469bd6281c0a1b1c2225b692752b627e3b935e988d8878925cb7e26e40e3ca14"), results.Receipts[0]["transactionHash"].(common.Hash))
		require.Equal(common.HexToAddress("0x703c4b2bD70c169f5717101CaeE543299Fc946C7"), results.Receipts[0]["from"].(common.Address))
		require.Equal(addr, *results.Receipts[0]["to"].(*common.Address))

		require.Equal(4, int(results.Txs[1].BlockNumber.ToInt().Uint64()))
		require.Equal(0, int(results.Txs[1].Nonce))
		require.Equal(4, int(results.Receipts[1]["blockNumber"].(hexutil.Uint64)))
		require.Equal(common.HexToHash("0x79491e16fd1b1ceea44c46af850b2ef121683055cd579fd4d877beba22e77c1c"), results.Receipts[1]["transactionHash"].(common.Hash))
		require.Equal(common.HexToAddress("0x0D3ab14BBaD3D99F4203bd7a11aCB94882050E7e"), results.Receipts[1]["from"].(common.Address))
		require.Equal(addr, *results.Receipts[1]["to"].(*common.Address))
	})
}
