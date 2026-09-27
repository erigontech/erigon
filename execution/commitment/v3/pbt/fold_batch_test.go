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
	"context"
	"runtime"
	"sort"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/race"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
)

func TestTrieFoldsDirtyRowsOncePerBatch(t *testing.T) {
	keyA := trieCodeKey(0, 0, 1)
	keyB := trieCodeKey(0, 2, 2)
	keyC := trieCodeKey(0, 8, 3)
	ctx := newTrieTestContext()
	_, err := NewTrie(ctx).Process([]Op{{Key: keyA, Value: testTrieValue(1)}, {Key: keyB, Value: testTrieValue(2)}, {Key: keyC, Value: testTrieValue(3)}})
	require.NoError(t, err)

	folded := make(map[string]int)
	var hashCalls atomic.Int64
	previousHashHook := hashHook
	hashHook = func([]byte) { hashCalls.Add(1) }
	t.Cleanup(func() { hashHook = previousHashHook })
	trie := NewTrie(ctx)
	trie.SetFoldHook(func(key []byte) { folded[string(bytes.Clone(key))]++ })
	_, err = trie.Process([]Op{{Key: keyA, Value: testTrieValue(4)}, {Key: keyB, Value: testTrieValue(5)}})
	require.NoError(t, err)
	require.NotEmpty(t, folded)
	for key, count := range folded {
		require.Equal(t, 1, count, "row %x folded more than once", []byte(key))
	}
	require.Equal(t, int64(5), hashCalls.Load())
}

func TestTrieHashHookSeesParallelRows(t *testing.T) {
	ctx := newTrieTestContext()
	var hashCalls atomic.Int64
	previousHashHook := hashHook
	hashHook = func([]byte) { hashCalls.Add(1) }
	t.Cleanup(func() { hashHook = previousHashHook })
	trie := NewTrie(ctx)
	trie.SetTrieContextFactory(func(context.Context) (commitment.PatriciaContext, func()) { return ctx, func() {} })
	_, err := trie.ProcessParallel([]Op{
		{Key: trieCodeKey(0, 0, 1), Value: testTrieValue(1)},
		{Key: trieCodeKey(0x80, 0, 2), Value: testTrieValue(2)},
	}, 2)
	require.NoError(t, err)
	require.Equal(t, int64(5), hashCalls.Load())
}

func TestTrieRowArenaReusesAllChunksAfterReset(t *testing.T) {
	ops := make([]Op, 1000)
	for i := range ops {
		ops[i] = Op{Key: trieCodeKey(byte(i>>8), byte(i), byte(i)), Value: testTrieValue(byte(i))}
	}
	trie := NewTrie(newTrieTestContext())
	chunks := 0
	for range 10 {
		trie.ResetContext(newTrieTestContext())
		_, err := trie.Process(ops)
		require.NoError(t, err)
		if chunks == 0 {
			chunks = len(trie.rowChunks)
		}
		if len(trie.rowChunks) != chunks {
			t.Fatalf("row arena retained %d chunks", len(trie.rowChunks))
		}
	}
}

func TestTrieSteadyStateAllocations(t *testing.T) {
	ops := make([]Op, 0, 64)
	for i := range 64 {
		ops = append(ops, Op{Key: trieCodeKey(byte(i*4), byte(i), byte(i+1)), Value: testTrieValue(byte(i))})
	}
	ctx := newTrieTestContext()
	trie := NewTrie(ctx)
	_, err := trie.Process(ops)
	require.NoError(t, err)
	allocs := testing.AllocsPerRun(10, func() {
		trie.ResetContext(ctx)
		_, err = trie.Process(ops)
		if err != nil {
			t.Fatalf("process: %v", err)
		}
	})
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	for range 3 {
		trie.ResetContext(ctx)
		_, err = trie.Process(ops)
		require.NoError(t, err)
	}
	runtime.ReadMemStats(&after)
	t.Logf("steady-state allocations=%.2f per op=%.2f bytes per op=%.2f", allocs, allocs/float64(len(ops)), float64(after.TotalAlloc-before.TotalAlloc)/float64(3*len(ops)))
	limit := 16.0
	if race.Enabled {
		limit = 24.0
	}
	require.Less(t, allocs/float64(len(ops)), limit)
}

func TestTrieHashHookCountsBucketAndJoinRows(t *testing.T) {
	address := bytes.Repeat([]byte{0x77}, 20)
	ops := make([]Op, 0, 16)
	stem := eip8297.TreeKeyStorage(address, storageSlot(64))[:33]
	for slot := range 8 {
		ops = append(ops,
			Op{Key: storageKeyWithSuffix(stem, 0x20, byte(slot)), Value: testTrieValue(byte(slot))},
			Op{Key: storageKeyWithSuffix(stem, 0x40, byte(slot)), Value: testTrieValue(byte(slot + 8))})
	}
	sort.Slice(ops, func(i, j int) bool { return bytes.Compare(ops[i].Key, ops[j].Key) < 0 })
	var hashCalls atomic.Int64
	previousHashHook := hashHook
	hashHook = func([]byte) { hashCalls.Add(1) }
	t.Cleanup(func() { hashHook = previousHashHook })
	trie := NewTrie(newTrieTestContext())
	_, err := trie.ProcessParallelWithThreshold(ops, 2, 8)
	require.NoError(t, err)
	require.Equal(t, int64(33), hashCalls.Load())
}
