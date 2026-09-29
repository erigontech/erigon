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
	"context"
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
)

func TestVerifyIsReadOnlyAcrossChurnRounds(t *testing.T) {
	previous := eip8297.HashSuiteName()
	t.Cleanup(func() { require.NoError(t, eip8297.SetHashSuite(previous)) })
	seeds := []int64{37, 148, 185, 259, 296, 407, 481, 518, 629, 666, 913}
	for _, suite := range []string{eip8297.HashKeccak, eip8297.HashBlake3} {
		require.NoError(t, eip8297.SetHashSuite(suite))
		for _, seed := range seeds {
			t.Run(fmt.Sprintf("%s/%d", suite, seed), func(t *testing.T) {
				testVerifyReadOnlyChurn(t, seed)
			})
		}
	}
}

func testVerifyReadOnlyChurn(t *testing.T, seed int64) {
	ctx := newTrieTestContext()
	reference := newTrieTestContext()
	trie := NewTrie(ctx)
	keys, prefixes := churnKeys()
	state := make(map[string]Op)
	trie.SetTrieContextFactory(func(context.Context) (commitment.PatriciaContext, func()) { return ctx, func() {} })
	rng := rand.New(rand.NewSource(seed))
	for batch := range 300 {
		ops := churnBatch(rng, keys, prefixes, state)
		if batch == 0 {
			ops = ops[:0]
			for i, key := range keys {
				ops = append(ops, Op{Key: key, Value: testTrieValue(byte(i%255 + 1))})
			}
			sort.Slice(ops, func(i, j int) bool { return string(ops[i].Key) < string(ops[j].Key) })
		}
		if batch%9 == 4 {
			trie.ResetContext(ctx)
		}
		if batch%9 == 7 {
			trie = NewTrie(ctx)
			trie.SetTrieContextFactory(func(context.Context) (commitment.PatriciaContext, func()) { return ctx, func() {} })
		}
		require.NoError(t, trie.Verify(), "verify before batch %d", batch)
		var root [32]byte
		var err error
		if batch%9 == 2 || batch%9 == 5 {
			root, err = trie.ProcessParallelWithThreshold(ops, 8, 2)
		} else {
			root, err = trie.Process(ops)
		}
		require.NoError(t, err)
		want, err := NewTrie(reference).Process(ops)
		require.NoError(t, err)
		updateChurnState(state, ops)
		spec := [32]byte(eip8297.StateRootWithHash(entriesFromOps(churnEntries(state)), eip8297.SelectedHash()))
		require.Equal(t, [32]byte(want), root, "reference mismatch at batch %d", batch)
		require.Equal(t, spec, root, "spec mismatch at batch %d", batch)
		require.NoError(t, trie.Verify(), "verify after batch %d", batch)
	}
}
