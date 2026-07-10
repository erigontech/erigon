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
	"errors"
	"fmt"
	"io"
	"math/bits"
	"sync"

	"github.com/erigontech/erigon/execution/commitment/nibbles"
)

// errStorageBaseNotBranch: no on-disk branch exactly at prefix P (its subtree top is
// a deeper extension, embedded, or absent); a merge candidate at P must demote to its
// nearest seedable ancestor's serial leaf rather than seed an empty — and sibling-dropping — wall.
var errStorageBaseNotBranch = errors.New("commitment: no branch at seed prefix")

// seedBaseAtPrefix seeds base's row 0 from the on-disk Branch(P) at any depth, so a merge
// task folds its children against the real state and every untouched sibling survives instead
// of dropping and diverging the root. It returns errStorageBaseNotBranch when no branch exists
// exactly at P; that same probe is what the fold DAG uses to decide seed-vs-demote.
func seedBaseAtPrefix(base *HexPatriciaHashed, prefix []byte) error {
	d := int16(len(prefix))
	copy(base.currentKey[:], prefix)
	base.currentKeyLen = d
	base.depths[0] = d + 1
	base.activeRows = 1
	for i := range base.grid[0] {
		base.grid[0][i].reset()
	}
	base.touchMap[0], base.afterMap[0], base.branchBefore[0] = 0, 0, false

	branch, err := base.branchFromCacheOrDB(nibbles.HexToCompact(prefix))
	if err != nil {
		return err
	}
	if len(branch) == 0 {
		return errStorageBaseNotBranch
	}
	// A stored branch is always >= 4 bytes (touchMap+afterMap); a shorter non-empty read is corrupt, not missing.
	if len(branch) < 4 {
		return fmt.Errorf("seedBaseAtPrefix: corrupt branch record at %x: %d bytes", prefix, len(branch))
	}
	base.branchBefore[0] = true
	return base.decodeBranchIntoRow(0, d+1, branch[2:], false)
}

// seedOrDemote seeds base at P and reports whether P carries an independent merge task:
// true when an on-disk branch is present, false (demote to an ancestor's serial leaf) when
// absent. It is the seed-or-demote decision the DAG derivation makes at every merge candidate.
// A false-empty seed would drop P's untouched on-disk siblings and diverge the root, so absence
// must demote — never guess a branch; a present branch loses only parallelism if wrong, so it is
// the safe classification.
func seedOrDemote(base *HexPatriciaHashed, prefix []byte) (seeded bool, err error) {
	err = seedBaseAtPrefix(base, prefix)
	if errors.Is(err, errStorageBaseNotBranch) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// stitchChildrenIntoRow0 drops each folded child cell into base's seeded row 0 at its nibble
// slot, marking every stitched slot touched; an empty child clears its slot, a present one
// replaces it. Untouched slots keep the on-disk sibling the seed decoded.
func stitchChildrenIntoRow0(base *HexPatriciaHashed, children *[16]cell, bitmap uint16) {
	for bm := bitmap; bm != 0; {
		bit := bm & -bm
		x := bits.TrailingZeros16(bit)
		base.touchMap[0] |= bit
		if children[x].IsEmpty() {
			base.afterMap[0] &^= bit
			base.grid[0][x].reset()
		} else {
			base.afterMap[0] |= bit
			base.grid[0][x] = children[x]
		}
		bm ^= bit
	}
}

// aggregateMountedStorageRoot folds a whale's stitched storage row into the account's single
// storage-root cell — the depth-64 account/storage seam. It collapses to a bare root hash the
// caller injects via setAccountStorageRoot, so a single surviving child is rehashed at depth 64
// rather than propagated as a leaf/extension the way an account-plane merge would.
func aggregateMountedStorageRoot(base *HexPatriciaHashed, children *[16]cell, bitmap uint16) (cell, error) {
	stitchChildrenIntoRow0(base, children, bitmap)
	if base.afterMap[0] == 0 && !base.branchBefore[0] {
		base.activeRows = 0
		return cell{}, nil
	}
	// A single surviving first-nibble child is an extension/leaf storage root; base.fold() would
	// misencode it by prepending the account prefix and returning the child hash, so build it directly.
	if kind, _ := afterMapUpdateKind(base.afterMap[0]); kind == updateKindPropagate {
		return storageRootFromSingleChild(base)
	}
	if err := base.fold(); err != nil {
		return cell{}, err
	}
	return base.root, nil
}

// mergeChildrenAtPrefix folds a merge task's stitched row into the one cell its parent stitches
// at the mount wall. Unlike the depth-64 storage seam it never collapses to a bare storage-root
// hash: it runs the standard fold (branch, single-survivor propagate, or delete) so an account-plane
// or interior-storage survivor stays a propagated leaf/extension cell the parent can keep folding,
// then strips the leading mountWall nibbles so the returned cell excludes the parent prefix and the
// slot nibble (invariant M). mountWall is len(parentPrefix)+1.
func mergeChildrenAtPrefix(base *HexPatriciaHashed, children *[16]cell, bitmap uint16, mountWall int16) (cell, error) {
	stitchChildrenIntoRow0(base, children, bitmap)
	if base.afterMap[0] == 0 && !base.branchBefore[0] {
		base.activeRows = 0
		return cell{}, nil
	}
	for base.activeRows > 0 {
		if err := base.fold(); err != nil {
			return cell{}, err
		}
	}
	return stripCellToMountWall(&base.root, mountWall), nil
}

// stripCellToMountWall re-expresses a merge's folded root — whose extension spans the merge's
// whole prefix P — as the mount-wall-relative cell the parent stitches: the leading mountWall
// nibbles (parent prefix plus slot nibble) drop out of extension and hashedExtension, leaving
// only the tail beyond the wall. Hash and leaf/state payload are unchanged.
func stripCellToMountWall(root *cell, mountWall int16) cell {
	out := *root
	if root.extLen > mountWall {
		n := root.extLen - mountWall
		copy(out.extension[:n], root.extension[mountWall:root.extLen])
		out.extLen = n
	} else {
		out.extLen = 0
	}
	if root.hashedExtLen > mountWall {
		n := root.hashedExtLen - mountWall
		copy(out.hashedExtension[:n], root.hashedExtension[mountWall:root.hashedExtLen])
		out.hashedExtLen = n
	} else {
		out.hashedExtLen = 0
	}
	return out
}

// storageRootFromSingleChild builds the storage root for a single-surviving-child collapse — an
// extension over a branch survivor, or the survivor leaf itself — without the account prefix.
func storageRootFromSingleChild(base *HexPatriciaHashed) (cell, error) {
	survNib := bits.TrailingZeros16(base.afterMap[0])
	child := base.grid[0][survNib]

	// The prior on-disk branch at the account prefix, if any, is now an extension: no branch record.
	if base.branchBefore[0] {
		if err := base.collectDeleteUpdate(nibbles.HexToCompact(base.currentKey[:base.currentKeyLen]), 0, true); err != nil {
			return cell{}, err
		}
	}
	base.activeRows = 0

	var root cell
	if child.hashLen > 0 {
		root.extLen = child.extLen + 1
		root.extension[0] = byte(survNib)
		copy(root.extension[1:], child.extension[:child.extLen])
		root.hashLen = child.hashLen
		copy(root.hash[:], child.hash[:child.hashLen])
	} else {
		root = child // single storage leaf: rehashed from its full storage key at depth 64
	}
	h, err := base.computeCellHash(&root, 64, nil)
	if err != nil {
		return cell{}, err
	}
	var out cell
	out.hashLen = int16(len(h) - 1)
	copy(out.hash[:], h[1:])
	return out, nil
}

// newDeferredStorageWorker yields a pooled trie worker for a deferring storage fold
// and a release that returns it to the pool and frees its context.
func newDeferredStorageWorker(pool *sync.Pool, factory TrieContextFactory, traceW io.Writer) (*HexPatriciaHashed, func()) {
	w := pool.Get().(*HexPatriciaHashed)
	wctx, cleanup := factory()
	w.ResetContext(wctx)
	w.SetTraceWriter(traceW)
	w.branchEncoder.setDeferUpdates(true)
	w.SetLeaveDeferredForCaller(true)
	return w, func() {
		w.resetForReuse()
		pool.Put(w)
		if cleanup != nil {
			cleanup()
		}
	}
}

// collectSubtreeKeys walks a subtree in sorted order; it copies each key's hashed
// nibbles off the reused walk path but leaves plainKey/update aliased.
func collectSubtreeKeys(node *prefixNode, path []byte) []touchedKey {
	out := make([]touchedKey, 0, node.subtreeCount)
	var arena byteArena
	_ = dfsSubtree(node, path, func(hk, pk []byte, upd *Update) error {
		out = append(out, touchedKey{hk: arena.intern(hk), pk: pk, upd: upd})
		return nil
	})
	return out
}
