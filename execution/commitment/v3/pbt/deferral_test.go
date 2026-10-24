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
	"fmt"
	"maps"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPBinDeferredRoundLeavesBaseUntouched(t *testing.T) {
	corpus := newParallelRoundCorpus(t, parallelRoundTestShape)
	ops := corpus.updates(rand.New(rand.NewSource(12)), 200, 20)
	for _, tc := range []struct {
		name    string
		ops     []Op
		workers int
	}{
		{name: "parallel", ops: ops, workers: 4},
		{name: "one-op", ops: ops[:1], workers: 4},
		{name: "workers=1", ops: ops, workers: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sequential := &trieTestContext{records: maps.Clone(corpus.snapshot)}
			wantRoot, err := NewTrie(sequential).Process(tc.ops)
			require.NoError(t, err)
			base := newParallelRoundBase(corpus.snapshot)
			trie := base.trie()
			trie.SetDeferCommitmentUpdates(true)
			gotRoot, err := trie.ProcessParallelContext(t.Context(), tc.ops, tc.workers)
			require.NoError(t, err)
			require.Equal(t, wantRoot, gotRoot)
			require.Empty(t, base.main.writes, "a deferred round wrote the base")
			require.Equal(t, corpus.snapshot, base.main.records)
			root, err := trie.RootHash()
			require.NoError(t, err)
			require.Equal(t, wantRoot, root, "root between the round and the flush")
			parts := trie.TakeDeferredDeltas()
			require.NotEmpty(t, parts)
			require.Nil(t, trie.TakeDeferredDeltas())
			for _, part := range parts {
				for _, delta := range part {
					require.NoError(t, base.main.PutBranch(delta.Key, delta.Data, delta.Prev))
				}
			}
			require.Equal(t, sequential.records, base.main.records)
		})
	}
}

func TestPBinDeferredRoundRefusesUntakenParts(t *testing.T) {
	corpus := newParallelRoundCorpus(t, parallelRoundTestShape)
	r := rand.New(rand.NewSource(13))
	first, second := corpus.updates(r, 200, 20), corpus.updates(r, 200, 20)
	for _, workers := range []int{4, 1} {
		t.Run(fmt.Sprintf("workers=%d", workers), func(t *testing.T) {
			trie := newParallelRoundBase(corpus.snapshot).trie()
			trie.SetDeferCommitmentUpdates(true)
			root, err := trie.ProcessParallelContext(t.Context(), first, workers)
			require.NoError(t, err)
			_, err = trie.ProcessParallelContext(t.Context(), second, workers)
			require.ErrorContains(t, err, "deferred updates were not taken")
			_, err = trie.Process(second)
			require.ErrorContains(t, err, "deferred updates were not taken")
			kept, err := trie.RootHash()
			require.NoError(t, err)
			require.Equal(t, root, kept, "the refused round reset the trie")
		})
	}
}

func TestPBinDeferredRoundEncodesNewRootState(t *testing.T) {
	corpus := newParallelRoundCorpus(t, parallelRoundTestShape)
	ops := corpus.updates(rand.New(rand.NewSource(14)), 200, 20)
	for _, tc := range []struct {
		name    string
		ops     []Op
		workers int
	}{
		{name: "parallel", ops: ops, workers: 4},
		{name: "one-op", ops: ops[:1], workers: 4},
		{name: "workers=1", ops: ops, workers: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sequential := NewTrie(&trieTestContext{records: maps.Clone(corpus.snapshot)})
			_, err := sequential.Process(tc.ops)
			require.NoError(t, err)
			want, err := sequential.EncodeCurrentState(nil)
			require.NoError(t, err)
			trie := newParallelRoundBase(corpus.snapshot).trie()
			trie.SetDeferCommitmentUpdates(true)
			_, err = trie.ProcessParallelContext(t.Context(), tc.ops, tc.workers)
			require.NoError(t, err)
			require.NotEmpty(t, trie.TakeDeferredDeltas())
			got, err := trie.EncodeCurrentState(nil)
			require.NoError(t, err)
			require.Equal(t, want, got, "state encoded between the round and the flush")
		})
	}
}

func TestPBinDeferredRootStateFollowsContextReset(t *testing.T) {
	corpus := newParallelRoundCorpus(t, parallelRoundTestShape)
	ops := corpus.updates(rand.New(rand.NewSource(15)), 200, 20)
	sequential := NewTrie(&trieTestContext{records: maps.Clone(corpus.snapshot)})
	_, err := sequential.Process(ops)
	require.NoError(t, err)
	deferredState, err := sequential.EncodeCurrentState(nil)
	require.NoError(t, err)
	baseState, err := NewTrie(&trieTestContext{records: maps.Clone(corpus.snapshot)}).EncodeCurrentState(nil)
	require.NoError(t, err)
	require.NotEqual(t, baseState, deferredState)

	base := newParallelRoundBase(corpus.snapshot)
	trie := base.trie()
	trie.SetDeferCommitmentUpdates(true)
	_, err = trie.ProcessParallelContext(t.Context(), ops, 4)
	require.NoError(t, err)
	trie.ResetContext(base.main)
	kept, err := trie.EncodeCurrentState(nil)
	require.NoError(t, err)
	require.Equal(t, deferredState, kept, "a reset with untaken parts dropped their root")
	require.NotEmpty(t, trie.TakeDeferredDeltas())
	trie.ResetContext(base.main)
	reset, err := trie.EncodeCurrentState(nil)
	require.NoError(t, err)
	require.Equal(t, baseState, reset, "a reset after the take kept the deferred root")
}
