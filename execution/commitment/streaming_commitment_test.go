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

package commitment

import (
	"bytes"
	"context"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/length"
)

// streamingRoot drives a StreamingCommitter over keys/upds touched in the order
// given by idxOrder, returning its root and MockState (with committed branches).
func streamingRoot(t *testing.T, workers int, keys [][]byte, upds []Update, idxOrder []int) ([]byte, *MockState) {
	t.Helper()
	ms := NewMockState(t)
	ms.SetConcurrentCommitment(true)
	require.NoError(t, ms.applyPlainUpdates(keys, upds))

	sc := NewStreamingCommitter(mockTrieCtxFactory(ms), length.Addr, DefaultTrieConfig())
	defer sc.Release()
	sc.SetNumWorkers(workers)
	for _, i := range idxOrder {
		sc.TouchKey(KeyToHexNibbleHash(keys[i]), keys[i], nil)
	}
	root, err := sc.Process(context.Background())
	require.NoError(t, err)
	return root, ms
}

// sequentialRoot drives the sequential HexPatriciaHashed, returning root + state.
func sequentialRoot(t *testing.T, keys [][]byte, upds []Update) ([]byte, *MockState) {
	t.Helper()
	ms := NewMockState(t)
	require.NoError(t, ms.applyPlainUpdates(keys, upds))
	tr := NewHexPatriciaHashed(length.Addr, ms, DefaultTrieConfig())
	defer tr.Release()
	ut := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, keys, upds)
	defer ut.Close()
	root, err := tr.Process(context.Background(), ut, "", nil, WarmupConfig{})
	require.NoError(t, err)
	return root, ms
}

// requireBranchParity asserts the two MockStates hold byte-identical branches.
func requireBranchParity(t *testing.T, seq, got *MockState) {
	t.Helper()
	mism := 0
	seen := map[string]struct{}{}
	for k := range seq.cm {
		seen[k] = struct{}{}
	}
	for k := range got.cm {
		seen[k] = struct{}{}
	}
	for k := range seen {
		sb, sok := seq.cm[k]
		pb, pok := got.cm[k]
		if !sok || !pok || !bytes.Equal(sb, pb) {
			mism++
		}
	}
	if mism != 0 {
		branchDiff(t, seq, got)
	}
	require.Equal(t, len(seq.cm), len(got.cm), "branch count must match")
	require.Zero(t, mism, "stored branch metadata differs between streaming and sequential")
}

// TestStreaming_RandomOrderParity feeds the mixed corpus through TouchKey in
// randomized (execution) order — order-independence is the premise; the in-order
// prefix-trie walk re-sorts at fold — and asserts root + every stored branch
// match the sequential run.
func TestStreaming_RandomOrderParity(t *testing.T) {
	t.Parallel()
	keys, upds := buildMixedCorpus(99, 6000)

	idx := make([]int, len(keys))
	for i := range idx {
		idx[i] = i
	}
	rnd := rand.New(rand.NewSource(0xBEEF))
	rnd.Shuffle(len(idx), func(i, j int) { idx[i], idx[j] = idx[j], idx[i] })

	seqRoot, seqMs := sequentialRoot(t, keys, upds)
	for _, w := range []int{1, 4, 8} {
		strRoot, strMs := streamingRoot(t, w, keys, upds, idx)
		require.Equalf(t, seqRoot, strRoot, "streaming(workers=%d) root != sequential", w)
		requireBranchParity(t, seqMs, strMs)
	}
}
