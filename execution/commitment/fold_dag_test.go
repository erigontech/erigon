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
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

// alwaysSeedable is the seedable prober for the common case: every candidate
// merge prefix carries an on-disk branch, so no node ever demotes.
func alwaysSeedable([]byte) bool { return true }

// buildDAGTrie inserts each nibble-form hashed key with a distinct dummy plainKey.
func buildDAGTrie(keys [][]byte) *prefixTrie {
	tr := newPrefixTrie()
	for i, hk := range keys {
		pk := make([]byte, 4)
		binary.BigEndian.PutUint32(pk, uint32(i))
		tr.Insert(hk, pk, &Update{Flags: BalanceUpdate})
	}
	return tr
}

// acctKeyNib builds a 64-nibble account key pinned to top nibble, with idx encoded
// into nibbles 1..8 (LSB first) so distinct (top, idx) pairs are distinct keys.
func acctKeyNib(top byte, idx int) []byte {
	k := make([]byte, 64)
	k[0] = top
	v := uint32(idx)
	for i := 1; i <= 8; i++ {
		k[i] = byte(v & 0xf)
		v >>= 4
	}
	return k
}

// storKeyNib builds a 128-nibble storage key sharing acct's 64 account nibbles,
// with slot encoded into the storage nibbles so distinct slots are distinct keys.
func storKeyNib(acct []byte, slot int) []byte {
	k := make([]byte, 128)
	copy(k, acct)
	v := uint64(slot) + 1
	for i := 64; i < 128; i++ {
		k[i] = byte(v & 0xf)
		v >>= 4
	}
	return k
}

func collectTasks(root *foldTask) []*foldTask {
	if root == nil {
		return nil
	}
	out := []*foldTask{root}
	for _, c := range root.children {
		out = append(out, collectTasks(c)...)
	}
	if root.storage != nil {
		out = append(out, collectTasks(root.storage)...)
	}
	return out
}

func countLeaves(root *foldTask) int {
	n := 0
	for _, t := range collectTasks(root) {
		if t.kind == foldLeaf {
			n++
		}
	}
	return n
}

func childByNib(t *foldTask, nib int) *foldTask {
	for _, c := range t.children {
		if c.nib == nib {
			return c
		}
	}
	return nil
}

func findByPrefix(root *foldTask, prefix []byte) *foldTask {
	for _, t := range collectTasks(root) {
		if bytes.Equal(t.prefix, prefix) {
			return t
		}
	}
	return nil
}

// leafCoverage counts, per plainKey, how many leaf tasks serially replay it —
// skipping account-leaf tasks whose storage is delegated to a subtask. A clean
// partition covers every key exactly once.
func leafCoverage(root *foldTask) map[string]int {
	counts := map[string]int{}
	for _, task := range collectTasks(root) {
		if task.kind != foldLeaf || task.storage != nil {
			continue
		}
		path := append([]byte(nil), task.prefix...)
		_ = dfsSubtree(task.node, path, func(_, pk []byte, _ *Update) error {
			counts[string(pk)]++
			return nil
		})
	}
	return counts
}

// requireKInvariant asserts the core sizing rule under a no-demotion (all-seedable)
// derivation: every leaf folds ≤ k keys, every merge > k. Account-leaf tasks are
// exempt — their subtreeCount includes storage delegated to a subtask.
func requireKInvariant(t *testing.T, root *foldTask, k uint32) {
	t.Helper()
	for _, task := range collectTasks(root) {
		if task.storage != nil {
			continue
		}
		switch task.kind {
		case foldLeaf:
			require.LessOrEqualf(t, task.node.subtreeCount, k, "leaf at %x folds %d keys > k=%d", task.prefix, task.node.subtreeCount, k)
		case foldMerge:
			require.Greaterf(t, task.node.subtreeCount, k, "merge at %x folds %d keys ≤ k=%d", task.prefix, task.node.subtreeCount, k)
		}
	}
}

func TestFoldK(t *testing.T) {
	t.Parallel()
	require.EqualValues(t, foldKMin, foldK(16*foldKMin, 16), "balanced at c=1 floors to K_min")
	require.EqualValues(t, 2*foldKMin, foldK(32*foldKMin, 16), "twice the load doubles K above the floor")
	require.EqualValues(t, foldKMin, foldK(100, 8), "tiny total is floored, never below K_min")
	require.EqualValues(t, foldKMin, foldK(0, 4), "empty trie is floored")
	require.EqualValues(t, 50000, foldK(50000, 0), "numWorkers<1 degrades to a single serial task")
	require.EqualValues(t, foldKMin, foldK(2048, 2), "boundary total/(c·workers)==K_min stays at the floor")
	require.EqualValues(t, 2*foldKMin, foldK(4096, 2), "above the floor tracks total/(c·workers)")
}

func TestDeriveFoldDAG_Degenerate(t *testing.T) {
	t.Parallel()

	require.Nil(t, deriveFoldDAG(nil, 10, alwaysSeedable), "nil root yields no task")
	require.Nil(t, deriveFoldDAG(newPrefixTrie().root, 10, alwaysSeedable), "untouched trie yields no task")

	single := buildDAGTrie([][]byte{acctKeyNib(3, 0)})
	root := deriveFoldDAG(single.root, 10, alwaysSeedable)
	require.NotNil(t, root)
	require.Equal(t, foldLeaf, root.kind, "single-key trie is one serial leaf")
	require.Empty(t, root.children)
	require.EqualValues(t, 0, root.pending.Load())

	// root.ext non-empty: a whole trie sharing a leading path folds as one leaf at that prefix.
	extRoot := &prefixNode{ext: []byte{1, 2, 3}, plainKey: []byte{9}, subtreeCount: 1}
	task := deriveFoldDAG(extRoot, 10, alwaysSeedable)
	require.NotNil(t, task)
	require.Equal(t, foldLeaf, task.kind)
	require.Equal(t, []byte{1, 2, 3}, task.prefix, "leaf prefix carries the root extension")
}

func TestDeriveFoldDAG_KBoundary(t *testing.T) {
	t.Parallel()
	// nibble 3 carries 4 keys forking at depth 1; nibbles 0..2 carry one key each.
	keys := [][]byte{
		acctKeyNib(0, 0), acctKeyNib(1, 0), acctKeyNib(2, 0),
		acctKeyNib(3, 0), acctKeyNib(3, 1), acctKeyNib(3, 2), acctKeyNib(3, 3),
	}
	trie := buildDAGTrie(keys)

	atK := deriveFoldDAG(trie.root, 4, alwaysSeedable)
	require.Equal(t, foldMerge, atK.kind)
	nib3 := childByNib(atK, 3)
	require.NotNil(t, nib3)
	require.Equal(t, foldLeaf, nib3.kind, "subtreeCount==k folds as one leaf")
	require.EqualValues(t, 0, nib3.pending.Load())

	atKm1 := deriveFoldDAG(trie.root, 3, alwaysSeedable)
	nib3 = childByNib(atKm1, 3)
	require.NotNil(t, nib3)
	require.Equal(t, foldMerge, nib3.kind, "subtreeCount==k+1 subdivides into a merge")
	require.Len(t, nib3.children, 4, "the four sub-keys become four child tasks")
	require.EqualValues(t, 4, nib3.pending.Load(), "pending counts the confirmed children")
	for _, c := range nib3.children {
		require.Equal(t, foldLeaf, c.kind)
		require.Same(t, nib3, c.parent, "child parent edge points back at the merge")
	}
}

func TestDeriveFoldDAG_NoOpPerNibble(t *testing.T) {
	t.Parallel()
	// Balanced: 16 top nibbles, perNibble keys each. At numWorkers = natural fan-out
	// K equals the per-nibble load, so every nibble folds as one leaf — no subdivision.
	const perNibble = 1100
	keys := make([][]byte, 0, 16*perNibble)
	for nib := range 16 {
		for j := range perNibble {
			keys = append(keys, acctKeyNib(byte(nib), j))
		}
	}
	trie := buildDAGTrie(keys)

	total := trie.root.subtreeCount
	k := foldK(total, 16)
	require.EqualValues(t, perNibble, k, "at the natural fan-out K is the per-nibble load")

	root := deriveFoldDAG(trie.root, k, alwaysSeedable)
	require.Equal(t, foldMerge, root.kind)
	require.Len(t, root.children, 16, "one task per touched top nibble")
	require.EqualValues(t, 16, root.pending.Load())
	require.Equal(t, 16, countLeaves(root), "every top nibble folds as a single leaf")
	for _, c := range root.children {
		require.Equal(t, foldLeaf, c.kind)
	}
}

func TestDeriveFoldDAG_UnseedableCollapse(t *testing.T) {
	t.Parallel()
	// nibble 5 carries 5 keys (a merge candidate at k=2) but its prefix is unseedable,
	// so its whole subtree collapses into one serial leaf with no independent sub-tasks.
	keys := [][]byte{
		acctKeyNib(0, 0), acctKeyNib(1, 0),
		acctKeyNib(5, 0), acctKeyNib(5, 1), acctKeyNib(5, 2), acctKeyNib(5, 3), acctKeyNib(5, 4),
	}
	trie := buildDAGTrie(keys)

	seedable := func(p []byte) bool { return !(len(p) == 1 && p[0] == 5) }
	root := deriveFoldDAG(trie.root, 2, seedable)
	require.Equal(t, foldMerge, root.kind)

	nib5 := childByNib(root, 5)
	require.NotNil(t, nib5)
	require.Equal(t, foldLeaf, nib5.kind, "unseedable merge candidate demotes to a serial leaf")
	require.Empty(t, nib5.children, "a demoted subtree spawns no independent tasks")
	require.EqualValues(t, 0, nib5.pending.Load())
	require.EqualValues(t, 5, nib5.node.subtreeCount, "the leaf absorbs the whole collapsed subtree")

	require.Nil(t, findByPrefix(root, []byte{5, 0}), "no task exists below the collapsed prefix")
	require.Nil(t, findByPrefix(root, []byte{5, 1}))
}

func TestDeriveFoldDAG_DepthSeam(t *testing.T) {
	t.Parallel()
	const slots = 100
	const k = 10
	whale := acctKeyNib(0xd, 0)
	keys := [][]byte{whale}
	for s := range slots {
		keys = append(keys, storKeyNib(whale, s))
	}
	trie := buildDAGTrie(keys)

	root := deriveFoldDAG(trie.root, k, alwaysSeedable)
	require.Equal(t, foldMerge, root.kind)

	acct := childByNib(root, 0xd)
	require.NotNil(t, acct)
	require.Equal(t, foldLeaf, acct.kind, "the depth-64 account is a leaf, not an account-plane merge")
	require.Equal(t, planeAccount, acct.plane)
	require.Len(t, acct.prefix, 64, "the account leaf mounts at the 64-nibble account prefix")

	storage := acct.storage
	require.NotNil(t, storage, "the account leaf carries an explicit storage-root dependency")
	require.Equal(t, foldMerge, storage.kind)
	require.Equal(t, planeStorage, storage.plane)
	require.Same(t, acct, storage.parent, "the storage-root subtask reports up to the account leaf")
	require.EqualValues(t, 1, acct.pending.Load(), "the account leaf waits on its storage-root subtask")
	require.Equal(t, acct.prefix, storage.prefix, "the storage-root subtask seeds at the account prefix")

	// Storage subdivides into properly-sized leaves; the account key is not among them.
	require.Greater(t, countLeaves(storage), (slots+k-1)/k-1, "storage subdivides to at least ⌈slots/k⌉ leaves")
	cover := leafCoverage(storage)
	require.Len(t, cover, slots, "storage leaves cover every slot")
	for pk, n := range cover {
		require.Equalf(t, 1, n, "slot %x folded by more than one leaf", []byte(pk))
	}
	requireKInvariant(t, root, k)
}

func TestDeriveFoldDAG_AccountPlaneSubdivides(t *testing.T) {
	t.Parallel()
	const n = 240
	const k = 8
	keys := make([][]byte, 0, n)
	for i := range n {
		keys = append(keys, acctKeyNib(byte(i%16), i/16))
	}
	trie := buildDAGTrie(keys)
	require.EqualValues(t, n, trie.root.subtreeCount)

	root := deriveFoldDAG(trie.root, k, alwaysSeedable)
	require.Equal(t, foldMerge, root.kind)

	requireKInvariant(t, root, k)

	cover := leafCoverage(root)
	require.Len(t, cover, n, "leaves partition every account key")
	for pk, c := range cover {
		require.Equalf(t, 1, c, "account %x folded by more than one leaf", []byte(pk))
	}
	require.GreaterOrEqual(t, countLeaves(root), (n+k-1)/k, "at least ⌈total/k⌉ leaves cover the account plane")
}
