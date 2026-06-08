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

// TestStreaming_DeepBranchParity drives the deep-fan-out corpus (an account whose
// touched storage exceeds deepStorageThreshold, alongside many small accounts)
// through TouchKey in randomized order and asserts root + every stored branch
// match sequential. This exercises foldSplit's big-storage path
// (concurrentStorageRoot) and the account-leaf storageRoot/CodeHash assembly.
func TestStreaming_DeepBranchParity(t *testing.T) {
	t.Parallel()
	keys, upds := buildBigAccountCorpus(15_000)

	idx := make([]int, len(keys))
	for i := range idx {
		idx[i] = i
	}
	rnd := rand.New(rand.NewSource(0xD00D))
	rnd.Shuffle(len(idx), func(i, j int) { idx[i], idx[j] = idx[j], idx[i] })

	seqRoot, seqMs := sequentialRoot(t, keys, upds)
	for _, w := range []int{1, 4, 8} {
		strRoot, strMs := streamingRoot(t, w, keys, upds, idx)
		require.Equalf(t, seqRoot, strRoot, "streaming(workers=%d) deep root != sequential", w)
		requireBranchParity(t, seqMs, strMs)
	}
}

// snapshotBranches deep-copies a MockState's stored branches so a later
// comparison can detect any mid-block write.
func snapshotBranches(ms *MockState) map[string][]byte {
	snap := make(map[string][]byte, len(ms.cm))
	for k, v := range ms.cm {
		snap[k] = append([]byte(nil), v...)
	}
	return snap
}

// requireBranchesUnchanged asserts ms holds exactly the snapshot branches — used
// to prove a mid-block re-fold deferred everything and wrote nothing.
func requireBranchesUnchanged(t *testing.T, snap map[string][]byte, ms *MockState) {
	t.Helper()
	require.Equalf(t, len(snap), len(ms.cm), "a mid-block re-fold changed the stored branch count")
	for k, v := range ms.cm {
		require.Truef(t, bytes.Equal(snap[k], v), "a mid-block re-fold wrote branch %x", []byte(k))
	}
}

// TestStreaming_NonEmptyPrevRefold is the novel-correctness claim: re-folding a
// split repeatedly over a NON-EMPTY on-disk pre-image stays parity-clean because
// the committer flushes nothing mid-block, so every re-fold reads the same prev.
// After each re-fold the store must still equal the post-batch-1 snapshot. A
// from-scratch run cannot falsify this, hence the batch-1 commit first. The
// corpus is collapse-free (sparseBatch2 without deletes) — the regime where the
// invariant holds; see foldDirtySplits for the collapse caveat.
func TestStreaming_NonEmptyPrevRefold(t *testing.T) {
	t.Parallel()
	const workers = 4
	ctx := context.Background()
	k1, u1 := genRandomAccountsStorage(400)
	k2, u2 := sparseBatch2(k1, 3, false)

	seqRoot, seqMs := runIncremental(t, modeSeq, 0, k1, u1, k2, u2)

	ms := NewMockState(t)
	ms.SetConcurrentCommitment(true)

	require.NoError(t, ms.applyPlainUpdates(k1, u1))
	sc1 := NewStreamingCommitter(mockTrieCtxFactory(ms), length.Addr, DefaultTrieConfig())
	sc1.SetNumWorkers(workers)
	for _, k := range k1 {
		sc1.TouchKey(KeyToHexNibbleHash(k), k, nil)
	}
	_, err := sc1.Process(ctx)
	require.NoError(t, err)
	sc1.Release()
	require.NotEmpty(t, ms.cm)
	snap := snapshotBranches(ms)

	require.NoError(t, ms.applyPlainUpdates(k2, u2))
	sc2 := NewStreamingCommitter(mockTrieCtxFactory(ms), length.Addr, DefaultTrieConfig())
	defer sc2.Release()
	sc2.SetNumWorkers(workers)
	for _, k := range k2 {
		sc2.TouchKey(KeyToHexNibbleHash(k), k, nil)
	}
	for range 4 {
		require.NoError(t, sc2.foldDirtySplits(ctx))
		requireBranchesUnchanged(t, snap, ms)
	}
	root2, err := sc2.Process(ctx)
	require.NoError(t, err)
	require.Equal(t, seqRoot, root2, "streaming block-2 root after re-folds != sequential")
	requireBranchParity(t, seqMs, ms)
}

// TestStreaming_RefoldAfterCollapse is the focused collapse assertion: a streaming
// fold over a delete batch that collapses branches, layered on a non-empty prev,
// must match sequential root + branches. The lazy path folds each split once at
// Process, so the engine's mid-fold self-flush stays correct here; the multi
// re-fold of a collapsed split is a Task-4 concern (see foldDirtySplits).
func TestStreaming_RefoldAfterCollapse(t *testing.T) {
	t.Parallel()
	k1, u1 := genRandomAccountsStorage(400)
	k2, u2 := sparseBatch2(k1, 3, true)
	for _, w := range []int{1, 4} {
		seqRoot, seqMs := runIncremental(t, modeSeq, 0, k1, u1, k2, u2)
		strRoot, strMs := runIncremental(t, modeStreaming, w, k1, u1, k2, u2)
		require.Equalf(t, seqRoot, strRoot, "streaming(workers=%d) collapse root != sequential", w)
		requireBranchParity(t, seqMs, strMs)
	}
}

// makeBranch builds a hash-only-cell deferred branch update at prefix: afterMap
// declares the full child set, touched lists the nibbles this update supplies
// cells for (seeded so each nibble's hash is distinguishable by source), and prev
// is the on-disk pre-image the update merges onto.
func makeBranch(prefix []byte, afterMap uint16, touched []int, seed byte, prev []byte) *DeferredBranchUpdate {
	var cells [16]cellEncodeData
	var tm uint16
	for _, n := range touched {
		tm |= uint16(1) << uint(n)
		cells[n].hashLen = 32
		for b := range cells[n].hash {
			cells[n].hash[b] = seed + byte(n)
		}
	}
	return getDeferredUpdate(prefix, tm, tm, afterMap, &cells, prev)
}

// TestStreaming_SplitMergeCollisionDedup models a prefix emitted by two slices
// (a split set and the merge set) over a non-empty pre-image: each supplies a
// different half of the same full child set. The duplicate-prefix flush guard
// re-reads prev so the second update merges onto the first's write — both halves
// survive. Bare ApplyDeferredBranchUpdates does not dedup: the second update
// still carries the block-start prev and its merge clobbers the first half. The
// branch merger treats branch2's afterMap as authoritative, so the clobber is
// silent — exactly the data loss the guard prevents.
func TestStreaming_SplitMergeCollisionDedup(t *testing.T) {
	t.Parallel()
	prefix := []byte{0x0a, 0x03}
	all := []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}

	storePrev := func(ms *MockState) []byte {
		_, err := ApplyDeferredBranchUpdates(
			[]*DeferredBranchUpdate{makeBranch(prefix, 0xFFFF, all, 0x10, nil)},
			1, ms.PutBranch)
		require.NoError(t, err)
		return append([]byte(nil), ms.cm[string(prefix)]...)
	}

	guardMs := NewMockState(t)
	gp := storePrev(guardMs)
	require.NoError(t, applyDeferredGuarded(guardMs, []*DeferredBranchUpdate{
		makeBranch(prefix, 0xFFFF, []int{0, 1, 2, 3}, 0x40, gp),
		makeBranch(prefix, 0xFFFF, []int{4, 5, 6, 7}, 0x80, gp),
	}, 4))
	_, _, grow, err := BranchData(guardMs.cm[string(prefix)]).decodeCells()
	require.NoError(t, err)
	require.Equal(t, byte(0x40+0), grow[0].hash[0], "guard kept the split set's low-half cell")
	require.Equal(t, byte(0x80+4), grow[4].hash[0], "guard kept the merge set's high-half cell")
	require.Equal(t, byte(0x10+8), grow[8].hash[0], "guard kept prev's untouched cell")

	bareMs := NewMockState(t)
	bp := storePrev(bareMs)
	_, err = ApplyDeferredBranchUpdates([]*DeferredBranchUpdate{
		makeBranch(prefix, 0xFFFF, []int{0, 1, 2, 3}, 0x40, bp),
		makeBranch(prefix, 0xFFFF, []int{4, 5, 6, 7}, 0x80, bp),
	}, 4, bareMs.PutBranch)
	require.NoError(t, err)
	_, _, brow, err := BranchData(bareMs.cm[string(prefix)]).decodeCells()
	require.NoError(t, err)
	require.Equal(t, byte(0x10+0), brow[0].hash[0], "bare apply dropped the split set's low half (clobbered by prev)")
	require.Equal(t, byte(0x80+4), brow[4].hash[0], "bare apply kept the merge set's high half")
}
