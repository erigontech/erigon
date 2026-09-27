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

package pbt

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTrieFoldsDirtyRowsOncePerBatch(t *testing.T) {
	keyA := trieCodeKey(0, 0, 1)
	keyB := trieCodeKey(0, 2, 2)
	keyC := trieCodeKey(0, 8, 3)
	ctx := newTrieTestContext()
	_, err := NewTrie(ctx).Process([]Op{{Key: keyA, Value: testTrieValue(1)}, {Key: keyB, Value: testTrieValue(2)}, {Key: keyC, Value: testTrieValue(3)}})
	require.NoError(t, err)

	folded := make(map[string]int)
	trie := NewTrie(ctx)
	trie.SetFoldHook(func(key []byte) { folded[string(bytes.Clone(key))]++ })
	_, err = trie.Process([]Op{{Key: keyA, Value: testTrieValue(4)}, {Key: keyB, Value: testTrieValue(5)}})
	require.NoError(t, err)
	require.NotEmpty(t, folded)
	for key, count := range folded {
		require.Equal(t, 1, count, "row %x folded more than once", []byte(key))
	}
}
