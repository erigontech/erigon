// Copyright 2026 The Erigon Authors
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

package testfixtures

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
)

// decodeBlock decodes a recorded block with Erigon's standard types and
// requires the re-encoding to reproduce the recorded bytes.
func decodeBlock(t *testing.T, w *Window, b WindowBlock) *types.Block {
	t.Helper()
	var block types.Block
	require.NoError(t, rlp.DecodeBytes(b.RLP, &block), "%s block %d", w.Name, b.Number)
	require.Equal(t, b.Number, block.NumberU64(), "%s block %d", w.Name, b.Number)
	enc, err := rlp.EncodeToBytes(&block)
	require.NoError(t, err)
	require.Equal(t, b.RLP, enc, "%s block %d re-encodes differently", w.Name, b.Number)
	return &block
}

// TestWindowsDecodeAndChain is T-RLP-2: every recorded block decodes and
// re-encodes exactly, chains to its predecessor, and its witness ends with
// the parent header.
func TestWindowsDecodeAndChain(t *testing.T) {
	for _, name := range WindowNames {
		t.Run(name, func(t *testing.T) {
			w, err := LoadWindow(name)
			require.NoError(t, err)
			var prev *types.Block
			for _, b := range w.Blocks {
				block := decodeBlock(t, w, b)
				if prev != nil {
					require.Equal(t, prev.Hash(), block.ParentHash(), "block %d parent", b.Number)
				}
				if b.Hash != (common.Hash{}) {
					require.Equal(t, b.Hash, block.Hash(), "block %d hash against blocks.json", b.Number)
					require.Equal(t, b.ParentHash, block.ParentHash(), "block %d parent against blocks.json", b.Number)
				}
				require.NotEmpty(t, b.Witness.Headers, "block %d witness has no headers", b.Number)
				var parent types.Header
				require.NoError(t, rlp.DecodeBytes(b.Witness.Headers[len(b.Witness.Headers)-1], &parent))
				require.Equal(t, block.ParentHash(), parent.Hash(), "block %d witness parent header", b.Number)
				prev = block
			}
		})
	}
}

// TestWindowOracles checks each window against the values recorded with it.
func TestWindowOracles(t *testing.T) {
	load := func(name string) (*Window, []*types.Block) {
		w, err := LoadWindow(name)
		require.NoError(t, err)
		blocks := make([]*types.Block, len(w.Blocks))
		for i, b := range w.Blocks {
			blocks[i] = decodeBlock(t, w, b)
		}
		return w, blocks
	}
	oracle := func(w *Window, key string) common.Hash {
		h, err := w.OracleHash(key)
		require.NoError(t, err)
		return h
	}

	t.Run("stateless-block-13", func(t *testing.T) {
		// Expected values asserted by eez-rollup0's stateless backend tests.
		_, blocks := load("stateless-block-13")
		require.Equal(t, common.HexToHash("0x16b64a78e9b3e0d533cafe81f9121735f6a2c8122c69b0bb5994ee75fe7bface"), blocks[0].Hash())
		require.Equal(t, common.HexToHash("0xf09d8f7da5bc5036f8dd9536c953e2212390a46fb3e553ece2b7d419131537b1"), blocks[0].Root())
	})
	t.Run("stateless-checkpoint-2175", func(t *testing.T) {
		w, blocks := load("stateless-checkpoint-2175")
		require.Equal(t, oracle(w, "block_hash"), blocks[0].Hash())
	})
	t.Run("nonzero-outbound-630", func(t *testing.T) {
		w, blocks := load("nonzero-outbound-630")
		last := blocks[len(blocks)-1]
		require.Equal(t, oracle(w, "settling_block_hash"), last.Hash())
		require.Equal(t, oracle(w, "final_state_root"), last.Root())
	})
	t.Run("captured-devnet-window-84", func(t *testing.T) {
		w, blocks := load("captured-devnet-window-84")
		require.Equal(t, oracle(w, "window_pre_block_hash"), blocks[0].ParentHash())
		require.Equal(t, oracle(w, "window_post_block_hash"), blocks[len(blocks)-1].Hash())
		require.NotEmpty(t, w.PostBatch)
	})
}
