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

// TestIsSplitPoint exercises the inline split predicate's three gates: the
// child-count gate, the MinSplitKeys size gate, and the terminator
// (plainKey == nil) correctness gate.
func TestIsSplitPoint(t *testing.T) {
	t.Parallel()

	// Branch with two children and enough keys, no terminator → split.
	split := &prefixNode{bitmap: 0b101, subtreeCount: MinSplitKeys}
	split.children = []*prefixNode{{}, {}}
	require.True(t, isSplitPoint(split))

	// Same shape but below the size threshold → not a split.
	small := &prefixNode{bitmap: 0b101, subtreeCount: MinSplitKeys - 1}
	small.children = []*prefixNode{{}, {}}
	require.False(t, isSplitPoint(small))

	// Single child → not a split regardless of size.
	single := &prefixNode{bitmap: 0b10, subtreeCount: MinSplitKeys * 4}
	single.children = []*prefixNode{{}}
	require.False(t, isSplitPoint(single))

	// Terminator present (e.g. account@64 above its storage) → never a split,
	// even with many children and keys.
	term := &prefixNode{bitmap: 0b111, subtreeCount: MinSplitKeys * 4, plainKey: []byte{0x01}}
	term.children = []*prefixNode{{}, {}, {}}
	require.False(t, isSplitPoint(term))

	require.False(t, isSplitPoint(nil))
}

// TestStreaming_StorageInteriorSplits is the headline Task-3 check: a whale
// account whose storage spans many slots must fold with concurrency BELOW the
// account/storage boundary (split-points at depth > 64), while still matching the
// sequential root and stored branch set. Parity alone cannot prove the depth > 64
// concurrency fired, so the StorageSplits seam is asserted directly; the account
// itself folds through storageRootLocal (DeepLocalFolds), never as a split-point
// (its depth-64 node carries the account terminator).
func TestStreaming_StorageInteriorSplits(t *testing.T) {
	t.Parallel()
	_, _, _, _, pk, upds, _ := whaleByNibble(20_000)

	seqRoot, seqMs := sequentialRoot(t, pk, upds)

	for _, w := range []int{1, 4, 8} {
		ms := NewMockState(t)
		ms.SetConcurrentCommitment(true)
		require.NoError(t, ms.applyPlainUpdates(pk, upds))

		sc := NewStreamingCommitter(mockTrieCtxFactory(ms), length.Addr, DefaultTrieConfig())
		sc.SetNumWorkers(w)
		for i := range pk {
			sc.TouchKey(KeyToHexNibbleHash(pk[i]), pk[i], nil)
		}
		root, err := sc.Process(context.Background())
		require.NoError(t, err)

		require.Equalf(t, seqRoot, root, "whale storage-interior split(workers=%d) root != sequential", w)
		requireBranchParity(t, seqMs, ms)
		require.NotZerof(t, sc.DeepLocalFolds(), "account must fold through storageRootLocal (workers=%d)", w)
		require.NotZerof(t, sc.StorageSplits(), "storage must split at depth > 64 (workers=%d)", w)
		sc.Release()
	}
}

// TestFoldSubtreeAtPrefix_MatchesDepth64 pins the arbitrary-depth raw fold to the
// proven depth-64 deep-fold helper: folding one first-storage-nibble group via
// the hand-mounted foldSubtreeAtPrefix(accHash[:64], group) must yield the same
// child cell hash as foldStorageChildCell (the auto-break depth-64 mount).
func TestFoldSubtreeAtPrefix_MatchesDepth64(t *testing.T) {
	t.Parallel()
	_, accHash, accNib, _, pk, upds, groups := whaleByNibble(8_000)
	ms := NewMockState(t)
	require.NoError(t, ms.applyPlainUpdates(pk, upds))

	nib := -1
	for x := range groups {
		if len(groups[x]) > 0 {
			nib = x
			break
		}
	}
	require.GreaterOrEqual(t, nib, 0, "whale must span at least one storage nibble")

	group := make([]touchedKey, len(groups[nib]))
	for i := range groups[nib] {
		group[i] = touchedKey{hk: groups[nib][i].hk, pk: groups[nib][i].pk, upd: &groups[nib][i].upd}
	}

	wRef := NewHexPatriciaHashed(length.Addr, ms, DefaultTrieConfig())
	wRef.grid[0][accNib].reset()
	ref, err := foldStorageChildCell(wRef, accNib, group)
	require.NoError(t, err)
	wRef.Release()

	wGot := NewHexPatriciaHashed(length.Addr, ms, DefaultTrieConfig())
	got, err := foldSubtreeAtPrefix(wGot, accHash[:64], group)
	require.NoError(t, err)
	wGot.Release()

	require.Equal(t,
		computeCellHashAt(t, ms, ref, 65),
		computeCellHashAt(t, ms, got, 65),
		"hand-mounted foldSubtreeAtPrefix diverged from the depth-64 deep-fold helper")
}
