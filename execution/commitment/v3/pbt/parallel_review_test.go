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
	"maps"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
)

func TestReviewCachedStorageChainCollapse(t *testing.T) {
	var initial, ops []Op
	for _, first := range []byte{0x10, 0x11, 0x20, 0x21} {
		stem := append([]byte{eip8297.StorageZone, first}, make([]byte, 31)...)
		for _, suffix := range []byte{0x10, 0x20} {
			key := storageKeyWithSuffix(stem, suffix, 1)
			initial = append(initial, Op{Key: key, Value: testTrieValue(first + suffix)})
			if first < 0x20 {
				ops = append(ops, Op{Key: key})
			} else {
				ops = append(ops, Op{Key: key, Value: testTrieValue(99)})
			}
		}
	}
	seed := newTrieTestContext()
	requireProcess(t, seed, sortParallelRoundOps(initial))
	sequential := &trieTestContext{records: maps.Clone(seed.records)}
	want, err := NewTrie(sequential).Process(sortParallelRoundOps(ops))
	require.NoError(t, err)
	base := newParallelRoundBase(seed.records)
	trie := base.trie()
	holdFirstTaskForSecond(trie)
	prefix := chainPrefix(eip8297.StorageZone, 2)
	key := parallelRoundRowKey(t, prefix)
	_, err = trie.loadRow(prefix)
	require.NoError(t, err)
	var joined, cached, stale bool
	var branchErr error
	trie.joinHook = func(task phaseTask, ctx commitment.PatriciaContext) error {
		if task.kind != phaseJoin {
			return nil
		}
		joined = true
		row := trie.rows[string(key)]
		cached = row != nil
		if cached {
			stale = !bytes.Equal(row.raw, ctx.(*phaseContext).records[string(key)])
		}
		_, _, branchErr = ctx.Branch(key)
		_, err := trie.loadRow(prefix)
		return err
	}
	got, err := trie.ProcessParallelContext(t.Context(), ops, 2)
	require.True(t, joined)
	require.True(t, cached, "the chain row must be cached before the join")
	require.True(t, stale, "worker must rewrite the cached chain row")
	require.ErrorContains(t, branchErr, "is owned by task")
	if err == nil {
		require.Equal(t, want, got)
		require.Equal(t, sequential.records, base.main.records)
		require.NotContains(t, base.main.records, string(parallelRoundRowKey(t, eip8297.PathFromBits([]byte{eip8297.StorageZone}, 8))))
		root := parallelRoundRecord(t, base.main.records, GlobalRootKey())
		require.Equal(t, ExtRoot, root.Form)
		require.True(t, pathHasPrefix(&root.SelfExt, &prefix))
	}
	require.ErrorContains(t, err, "is owned by task", "cached chain-owned row read was accepted by the join")
}
