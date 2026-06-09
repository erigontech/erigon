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
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/length"
)

// buildMultiDepthCorpus mixes account-trie forks (thousands of independent
// accounts → split-points at several shallow account-trie depths) with a whale
// storage subtree whose deep forks split below the account/storage boundary
// (depth > 64). Together they exercise concurrent folding at many depths in one
// batch, which a single-whale or accounts-only corpus cannot.
func buildMultiDepthCorpus() (keys [][]byte, upds []Update) {
	mk, mu := buildMixedCorpus(0xD15C0DE, 6000)
	_, _, _, _, pk, pu, _ := whaleByNibble(20_000)
	keys = append(keys, mk...)
	keys = append(keys, pk...)
	upds = append(upds, mu...)
	upds = append(upds, pu...)
	return keys, upds
}

// parallelRoot drives ModeParallel over a single batch, returning the root and
// the MockState (with committed branches) for parity comparison.
func parallelRoot(t *testing.T, workers int, keys [][]byte, upds []Update) ([]byte, *MockState) {
	t.Helper()
	ms := NewMockState(t)
	ms.SetConcurrentCommitment(true)
	require.NoError(t, ms.applyPlainUpdates(keys, upds))

	tr := NewParallelPatriciaHashed(mockTrieCtxFactory(ms), length.Addr, DefaultTrieConfig())
	defer tr.Release()
	tr.SetNumWorkers(workers)
	tr.SetMinSplitKeys(parallelEquivMinSplitKeys)
	tr.ResetContext(ms)

	ut := NewUpdates(ModeParallel, t.TempDir(), KeyToHexNibbleHash)
	defer ut.Close()
	for i, k := range keys {
		ks := string(k)
		ut.TouchPlainKey(ks, nil, func(c *KeyUpdate, _ []byte) {
			c.plainKey = ks
			c.hashedKey = KeyToHexNibbleHash(k)
			c.update = &upds[i]
		})
	}
	root, err := tr.Process(context.Background(), ut, "", nil, WarmupConfig{})
	require.NoError(t, err)
	return root, ms
}

// TestStreaming_MultiDepthSplitParity is the headline Task-5 parity gate: a
// corpus with split-points at SEVERAL depths must fold via the streaming
// concurrent engine to the SAME root and stored-branch set as sequential
// ModeDirect AND ModeParallel, at every worker count. DeepLocalFolds asserts the
// whale's account@64 boundary still routes through storageRootLocal (the flat
// per-first-nibble fan-out); depth > 64 splits are a follow-up, so the
// StorageSplits seam is not asserted here.
func TestStreaming_MultiDepthSplitParity(t *testing.T) {
	t.Parallel()
	keys, upds := buildMultiDepthCorpus()

	seqRoot, seqMs := sequentialRoot(t, keys, upds)

	parRoot, parMs := parallelRoot(t, 4, keys, upds)
	require.Equal(t, seqRoot, parRoot, "parallel root != sequential")
	requireBranchParity(t, seqMs, parMs)

	for _, w := range []int{1, 4, 8} {
		ms := NewMockState(t)
		ms.SetConcurrentCommitment(true)
		require.NoError(t, ms.applyPlainUpdates(keys, upds))

		sc := NewStreamingCommitter(mockTrieCtxFactory(ms), length.Addr, DefaultTrieConfig())
		sc.SetNumWorkers(w)
		for i := range keys {
			sc.TouchKey(KeyToHexNibbleHash(keys[i]), keys[i], nil)
		}
		root, err := sc.Process(context.Background())
		require.NoError(t, err)

		require.Equalf(t, seqRoot, root, "multi-depth streaming(workers=%d) root != ModeDirect", w)
		require.Equalf(t, parRoot, root, "multi-depth streaming(workers=%d) root != ModeParallel", w)
		requireBranchParity(t, seqMs, ms)
		require.NotZerof(t, sc.DeepLocalFolds(), "account@64 must fold through storageRootLocal (workers=%d)", w)
		sc.Release()
	}
}

// TestStreaming_MultiDepthCollapseParity is the deep-collapse parity gate: a
// whale account whose deep storage collapses (block 2 deletes 1/3 + updates 1/3)
// while embedded among thousands of other accounts must fold via the streaming
// concurrent engine to the SAME root and stored-branch set as sequential
// ModeDirect AND ModeParallel, at every worker count. This surfaces the
// streaming-mode deep-collapse divergence fixed by
// docs/plans/20260609-streaming-collapse-fold-fix.md.
func TestStreaming_MultiDepthCollapseParity(t *testing.T) {
	t.Parallel()
	wk1, wu1, wk2, wu2 := whaleCollapseCorpus()
	mk, mu := buildMixedCorpus(0xC0FFEE, 4000)
	k1 := append(append([][]byte{}, mk...), wk1...)
	u1 := append(append([]Update{}, mu...), wu1...)
	for _, w := range []int{1, 4, 8} {
		requireIncrementalEquiv(t, k1, u1, wk2, wu2, w)
	}
}
