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
	"math/bits"
	"sync/atomic"

	"github.com/erigontech/erigon/common"
)

// foldKMin floors the leaf/merge boundary so an internal task always folds
// enough keys to amortize its fixed cost — an own trie context, an mmap pin,
// and a Branch(prefix) seed read.
const foldKMin = 1024

// foldK sizes the leaf/merge boundary K for the fold DAG. Subtrees at or below K
// fold as one serial leaf task; larger subtrees subdivide into a merge over child
// tasks. total is the root subtree key count; c=1 keeps K a no-op at the natural
// fan-out (one task per worker on balanced data) and is only raised on bench
// evidence. Floored at foldKMin.
func foldK(total uint32, numWorkers int) uint32 {
	if numWorkers < 1 {
		numWorkers = 1
	}
	const c = 1
	k := total / (c * uint32(numWorkers))
	if k < foldKMin {
		return foldKMin
	}
	return k
}

type foldTaskKind uint8

const (
	foldLeaf  foldTaskKind = iota // serial replay of a subtree mounted on the parent's base
	foldMerge                     // seed Branch(prefix), stitch child cells, fold
)

// plane separates the account trie (depth ≤ 64) from a single account's storage
// trie (depth 64..128); the two fold to different cell conventions.
type foldPlane uint8

const (
	planeAccount foldPlane = iota
	planeStorage
)

// foldTask is one node of the fold DAG: a leaf that serially replays a subtree,
// or a merge that stitches its child tasks. A merge (and an account leaf waiting
// on its storage-root subtask) starts with a non-zero pending counter; each child
// completion decrements it, and a task becomes ready to fold at zero.
type foldTask struct {
	prefix   []byte
	node     *prefixNode
	kind     foldTaskKind
	plane    foldPlane
	nib      int // slot in the parent merge's row, -1 for the root and for a storage-root subtask
	parent   *foldTask
	children []*foldTask
	// storage is an account-leaf task's dependency on the storage-root subtask that
	// folds its storage trie; the resulting root hash is injected via setAccountStorageRoot.
	storage *foldTask

	pending  atomic.Int32
	cell     cell
	deferred []*DeferredBranchUpdate

	// base is a merge task's own seeded trie (Branch(prefix) unfolded into row 0): the mount
	// wall its children copy from and the trie it stitches and folds. Nil on leaf tasks and on
	// the finale root, whose base is the caller's pre-built root wall.
	base        *HexPatriciaHashed
	baseCleanup func()
	// storageRoot carries a storage-root subtask's collapsed hash to its account-leaf parent.
	storageRoot common.Hash

	// freshWhaleCandidate marks a demoted big-storage account leaf (unseedable, so no branch
	// on disk at its prefix). foldLeafTask confirms freshness at fold time (the account cell is
	// empty on disk) and then folds its storage in parallel against an empty wall, recovering the
	// parallelism the seedable-only seam gives a re-touched whale. Not fresh ⇒ serial replay.
	freshWhaleCandidate bool
}

// deriveFoldDAG walks the prefix trie top-down once, classifying each node as a
// leaf task (subtreeCount ≤ k) or a merge task (> k). A merge candidate is kept
// only when seedable(prefix) confirms an on-disk Branch at that exact prefix;
// otherwise it demotes to a leaf whose serial replay is absorbed by the nearest
// seedable ancestor, so an unseedable node never reaches the pool as a task with
// a pending counter its parent already booked. Returns nil for an empty trie.
func deriveFoldDAG(root *prefixNode, k uint32, seedable func(prefix []byte) bool) *foldTask {
	if root == nil || root.subtreeCount == 0 {
		return nil
	}
	b := &foldDAGBuilder{k: k, seedable: seedable}
	prefix := append([]byte(nil), root.ext...)
	// The root is the lone serial finale: it always folds against the on-disk root
	// branch or a synthesized empty wall, so it never demotes on seedability.
	if root.subtreeCount <= k {
		return b.newTask(prefix, root, foldLeaf, planeFor(len(prefix)))
	}
	m := b.newTask(prefix, root, foldMerge, planeFor(len(prefix)))
	b.addChildren(m, root, prefix)
	return m
}

type foldDAGBuilder struct {
	k        uint32
	seedable func(prefix []byte) bool
}

func (b *foldDAGBuilder) newTask(prefix []byte, node *prefixNode, kind foldTaskKind, pl foldPlane) *foldTask {
	return &foldTask{prefix: prefix, node: node, kind: kind, plane: pl, nib: -1}
}

// derive classifies one non-root node at its prefix into a task subtree.
func (b *foldDAGBuilder) derive(node *prefixNode, prefix []byte) *foldTask {
	sc := node.subtreeCount
	depth := len(prefix)

	// Depth-64 account/storage seam. A touched account (plainKey != nil) with a large,
	// seedable storage subtree splits into an account-leaf task that depends on a
	// storage-root subtask — its children are storage nibbles, not siblings. A storage-only
	// node (account untouched) must not split: with no account update, the seam's
	// setAccountStorageRoot would hash a cell missing the on-disk nonce/balance/codeHash, so
	// it demotes to a serial leaf whose replay unfolds the account from disk.
	if depth == 64 && node.bitmap != 0 {
		if node.plainKey != nil && sc-1 > b.k && b.seedable(prefix) {
			leaf := b.newTask(prefix, node, foldLeaf, planeAccount)
			storage := b.newTask(prefix, node, foldMerge, planeStorage)
			b.addChildren(storage, node, prefix)
			storage.parent = leaf
			leaf.storage = storage
			leaf.pending.Store(1)
			return leaf
		}
		leaf := b.newTask(prefix, node, foldLeaf, planeAccount)
		// A touched big-storage account with no on-disk branch is a fresh-whale candidate:
		// foldLeafTask parallelizes its storage if the account proves fresh at fold time.
		if node.plainKey != nil && sc-1 > b.k {
			leaf.freshWhaleCandidate = true
		}
		return leaf
	}

	if sc <= b.k || !b.seedable(prefix) {
		return b.newTask(prefix, node, foldLeaf, planeFor(depth))
	}
	m := b.newTask(prefix, node, foldMerge, planeFor(depth))
	b.addChildren(m, node, prefix)
	return m
}

// addChildren derives a task for every child nibble, wires the parent/child edges,
// and sets the parent's pending counter to the child count.
func (b *foldDAGBuilder) addChildren(parent *foldTask, node *prefixNode, prefix []byte) {
	childIdx := 0
	for bm := node.bitmap; bm != 0; {
		nib := bits.TrailingZeros16(bm)
		childNode := node.children[childIdx]
		childPrefix := make([]byte, len(prefix), len(prefix)+1+len(childNode.ext))
		copy(childPrefix, prefix)
		childPrefix = append(childPrefix, byte(nib))
		childPrefix = append(childPrefix, childNode.ext...)

		child := b.derive(childNode, childPrefix)
		child.parent = parent
		child.nib = nib
		parent.children = append(parent.children, child)

		childIdx++
		bm &^= uint16(1) << nib
	}
	parent.pending.Store(int32(len(parent.children)))
}

func planeFor(depth int) foldPlane {
	if depth > 64 {
		return planeStorage
	}
	return planeAccount
}
