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
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTrieCancelsDeleteRewriteDelta(t *testing.T) {
	ctx := newTrieTestContext()
	key := trieCodeKey(0, 0, 1)
	value := testTrieValue(1)
	_, err := NewTrie(ctx).Process([]Op{{Key: key, Value: value}})
	require.NoError(t, err)
	ctx.writes = nil

	trie := NewTrie(ctx)
	_, err = trie.Process([]Op{{Key: key, Value: [32]byte{}}, {Key: key, Value: value}})
	require.NoError(t, err)
	require.Empty(t, trie.TakeDeltas())
	require.Empty(t, ctx.writes)
	require.NotEmpty(t, ctx.records[string(GlobalRootKey())])
}

func TestTrieDeltasRestoreRoundStartRecords(t *testing.T) {
	ctx := newTrieTestContext()
	keyA := trieCodeKey(0, 0, 1)
	keyB := trieCodeKey(0, 2, 2)
	_, err := NewTrie(ctx).Process([]Op{{Key: keyA, Value: testTrieValue(1)}, {Key: keyB, Value: testTrieValue(2)}})
	require.NoError(t, err)
	start := cloneRecords(ctx.records)

	trie := NewTrie(ctx)
	_, err = trie.Process([]Op{{Key: keyA, Value: testTrieValue(3)}, {Key: keyB, Value: [32]byte{}}})
	require.NoError(t, err)
	deltas := trie.TakeDeltas()
	require.NotEmpty(t, deltas)
	for _, delta := range deltas {
		require.Equal(t, start[string(delta.Key)], delta.Prev)
	}
	for _, delta := range slices.Backward(deltas) {
		require.NoError(t, ctx.PutBranch(delta.Key, delta.Prev, delta.Data))
	}
	require.Equal(t, start, ctx.records)
}

func cloneRecords(records map[string][]byte) map[string][]byte {
	clone := make(map[string][]byte, len(records))
	for key, value := range records {
		clone[key] = append([]byte(nil), value...)
	}
	return clone
}
