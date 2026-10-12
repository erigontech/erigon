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
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/commitment/eip8297"
)

func TestTrieCancelsDeleteRewriteDelta(t *testing.T) {
	ctx := newTrieTestContext()
	key := eip8297.TreeKeyStorage(bytes.Repeat([]byte{0x71}, 20), storageSlot(64))
	value := testTrieValue(1)
	entries := []Op{{Key: key, Value: value}}
	_, err := NewTrie(ctx).Process(entries)
	require.NoError(t, err)
	assertPersistedTrie(t, ctx, entries)
	ctx.writes = nil

	trie := NewTrie(ctx)
	_, err = trie.Process([]Op{Drop(bytes.Clone(key[:33])), {Key: key, Value: value}})
	require.NoError(t, err)
	require.Empty(t, trie.TakeDeltas())
	require.Empty(t, ctx.writes)
	require.NotEmpty(t, ctx.records[string(GlobalRootKey())])
	assertPersistedTrie(t, ctx, entries)
}

func TestTrieDeltasRestoreRoundStartRecords(t *testing.T) {
	ctx := newTrieTestContext()
	keyA := trieCodeKey(0, 0, 1)
	keyB := trieCodeKey(0, 2, 2)
	entries := []Op{{Key: keyA, Value: testTrieValue(1)}, {Key: keyB, Value: testTrieValue(2)}}
	_, err := NewTrie(ctx).Process(entries)
	require.NoError(t, err)
	assertPersistedTrie(t, ctx, entries)
	start := cloneRecords(ctx.records)

	trie := NewTrie(ctx)
	_, err = trie.Process([]Op{{Key: keyA, Value: testTrieValue(3)}, {Key: keyB, Value: [32]byte{}}})
	require.NoError(t, err)
	deltas := trie.TakeDeltas()
	require.NotEmpty(t, deltas)
	for _, delta := range deltas {
		require.Equal(t, start[string(delta.Key)], delta.Prev)
	}
	assertPersistedTrie(t, ctx, []Op{{Key: keyA, Value: testTrieValue(3)}})
	for _, delta := range slices.Backward(deltas) {
		require.NoError(t, ctx.PutBranch(delta.Key, delta.Prev, delta.Data))
	}
	require.Equal(t, start, ctx.records)
}

func TestTrieRootFormChangeKeepsRoundStartPreviousRecord(t *testing.T) {
	ctx := newTrieTestContext()
	ctx.rejectNilPrev = true
	key := accountKey(0, eip8297.BasicDataLeafKey)
	value := testTrieValue(1)
	storage := eip8297.TreeKeyStorage([]byte{0x71, 0x72, 0x73, 0x74, 0x75, 0x76, 0x77, 0x78, 0x79, 0x7a, 0x7b, 0x7c, 0x7d, 0x7e, 0x7f, 0x80, 0x81, 0x82, 0x83, 0x84}, storageSlot(64))
	requireProcess(t, ctx, []Op{{Key: key, Value: value}})
	start := cloneRecords(ctx.records)
	trie := NewTrie(ctx)
	rewrite := testTrieValue(2)
	_, err := trie.Process([]Op{{Key: key, Value: rewrite}, {Key: storage, Value: testTrieValue(3)}})
	require.NoError(t, err)
	deltas := trie.TakeDeltas()
	for _, delta := range deltas {
		require.Equal(t, start[string(delta.Key)], delta.Prev, "delta %x", delta.Key)
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
