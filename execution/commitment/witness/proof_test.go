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

package witness

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/execution/commitment/trie"
	"github.com/erigontech/erigon/execution/rlp"
)

func TestProofFromNodesMatchesProve(t *testing.T) {
	for _, valueLen := range []int{1, 40} {
		tr := trie.New(common.Hash{})
		keys, values := make([][]byte, 300), make([][]byte, 300)
		for i := range keys {
			keys[i] = crypto.Keccak256([]byte{byte(i), byte(i >> 8)})
			if i%3 != 0 {
				values[i] = bytes.Repeat([]byte{byte(i) | 1}, valueLen)
				tr.Update(keys[i], values[i])
			}
		}
		byHash, want := map[string][]byte{}, make([][][]byte, len(keys))
		for i, key := range keys {
			nodes, err := tr.Prove(key, 0, false)
			require.NoError(t, err)
			want[i] = nodes
			for _, n := range nodes {
				byHash[string(crypto.Keccak256(n))] = n
			}
		}
		root := tr.Hash()
		for i, key := range keys {
			got, value, err := ProofFromNodes(byHash, root[:], key)
			require.NoError(t, err)
			require.Equal(t, want[i], got, "key %d", i)
			var wantValue []byte
			if values[i] != nil { // a leaf holds its value as an RLP string, which is what an account proof decodes
				wantValue = make([]byte, rlp.StringLen(values[i]))
				rlp.EncodeStringToBuf(values[i], wantValue)
			}
			require.Equal(t, wantValue, value, "key %d", i)
		}
	}
}

func TestProofFromNodesEmptyTrie(t *testing.T) {
	proof, value, err := ProofFromNodes(map[string][]byte{}, empty.RootHash[:], crypto.Keccak256([]byte("any")))
	require.NoError(t, err)
	require.Empty(t, proof)
	require.Nil(t, value)
}

func TestProofFromNodesSkipsInlineChildren(t *testing.T) {
	tr := trie.New(common.Hash{})
	keys := make([][]byte, 8)
	for i := range keys {
		keys[i] = crypto.Keccak256([]byte{byte(i)})[:1]
		tr.Update(keys[i], []byte{byte(i) | 1})
	}
	root := tr.Hash()

	byHash := map[string][]byte{}
	for _, k := range keys {
		nodes, err := tr.Prove(k, 0, false)
		require.NoError(t, err)
		for _, n := range nodes {
			byHash[string(crypto.Keccak256(n))] = n
		}
	}

	for _, k := range keys {
		got, _, err := ProofFromNodes(byHash, root[:], k)
		require.NoError(t, err)
		for i, n := range got {
			require.True(t, i == 0 || len(n) >= length.Hash,
				"element %d of %d bytes is inline in its parent, so it must not be emitted", i, len(n))
		}
	}
}
