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
	"errors"
	"fmt"
	"math/bits"
	"sync"

	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/execution/commitment/nibbles"
)

// foldChildSubtree folds one subtree's collected keys in a standalone worker and
// returns its cell at column col, stripped of the leading extension nibble the
// parent column carries. The destination grid cell is reset first so no stale
// pooled-grid field survives into the folded cell.
func foldChildSubtree(w *HexPatriciaHashed, col int, group []touchedKey) (cell, error) {
	w.grid[0][col].reset()
	for i := range group {
		if err := w.followAndUpdate(group[i].hk, group[i].pk, group[i].upd); err != nil {
			return cell{}, err
		}
	}
	for w.activeRows > 1 {
		if err := w.fold(); err != nil {
			return cell{}, err
		}
	}
	c := w.grid[0][col]
	stripLeadingChildExt(&c)
	return c, nil
}

// aggregateSubtreeRoot stitches the present child cells into the branch and folds
// it once to the subtree's root cell. A child that folded to an empty cell stays in
// touchMap (so the branch update records the deletion) but is dropped from afterMap;
// if every child collapsed an empty cell is returned.
func aggregateSubtreeRoot(w *HexPatriciaHashed, prefix []byte, children *[16]cell, present uint16) (cell, error) {
	d := int16(len(prefix))
	col := int(prefix[d-1])
	after := present
	for x := range 16 {
		if present&(uint16(1)<<x) != 0 && children[x].IsEmpty() {
			after &^= uint16(1) << x
		}
	}
	if after == 0 {
		return cell{}, nil
	}
	copy(w.currentKey[:], prefix)
	w.currentKeyLen = d
	w.depths[0] = d
	w.depths[1] = d + 1
	w.activeRows = 2
	w.grid[0][col].reset()
	w.touchMap[0] = uint16(1) << col
	w.afterMap[0] = uint16(1) << col
	for x := range 16 {
		if after&(uint16(1)<<x) != 0 {
			w.grid[1][x] = children[x]
		} else {
			w.grid[1][x].reset()
		}
	}
	w.touchMap[1] = present
	w.afterMap[1] = after
	if err := w.fold(); err != nil {
		return cell{}, err
	}
	return w.grid[0][col], nil
}

// aggregateStorageRoot folds the storage branch to the account's storageRoot cell.
func aggregateStorageRoot(w *HexPatriciaHashed, accHash []byte, children *[16]cell, present uint16) (cell, error) {
	return aggregateSubtreeRoot(w, accHash[:64], children, present)
}

// foldStorageLeaf folds one first-storage-nibble subtree confined to childPrefix,
// unfolding the on-disk branch there (if any) so an incremental collapse preserves
// untouched on-disk interior siblings without reading the storage-root branch above.
// The returned cell carries no extension; the caller lifts child.ext.
func foldStorageLeaf(w *HexPatriciaHashed, childPrefix []byte, group []touchedKey) (cell, error) {
	pd := int16(len(childPrefix))
	col := int(childPrefix[pd-1])
	copy(w.currentKey[:], childPrefix)
	w.currentKeyLen = pd - 1
	w.depths[0] = pd
	w.activeRows = 1
	w.grid[0][col].reset()
	// The seeded hashLen below is only a needUnfolding signal; unfoldBranchNode
	// replaces the cell with the real on-disk children.
	branch, err := w.branchFromCacheOrDB(nibbles.HexToCompact(childPrefix))
	if err != nil {
		return cell{}, err
	}
	if len(branch) > 0 {
		w.grid[0][col].hashLen = 32
	}
	for i := range group {
		if err := w.followAndUpdate(group[i].hk, group[i].pk, group[i].upd); err != nil {
			return cell{}, err
		}
	}
	for w.activeRows > 1 {
		if err := w.fold(); err != nil {
			return cell{}, err
		}
	}
	return w.grid[0][col], nil
}

// deepStorageFold gates the concurrent per-storage-nibble fold of one whale
// account's storage subtree (foldStorageRoot). Disabled: it produced a wrong
// trie root and a concurrent-read SIGSEGV on mainnet block 25142734. The
// top-level per-account-nibble mount fold stays parallel; only the second-tier
// storage split is off until its correctness + file-view pinning is fixed.
var deepStorageFold = true

// isDeepStorageAccount reports whether node is an account leaf whose touched storage
// is large and forked enough to fold concurrently.
func isDeepStorageAccount(node *prefixNode, depth int) bool {
	return depth == 64 && node.plainKey != nil &&
		bits.OnesCount16(node.bitmap) >= 2 && node.subtreeCount > deepStorageThreshold
}

// dfsSubtreeDeep walks node's subtree applying each key to w, but at a big-storage
// account it injects storageRoot's result instead of streaming the slots.
func dfsSubtreeDeep(w *HexPatriciaHashed, node *prefixNode, path []byte, storageRoot func(node *prefixNode, path []byte) (common.Hash, error)) error {
	if node == nil {
		return nil
	}
	if node.plainKey != nil {
		if err := w.followAndUpdate(path, node.plainKey, node.update); err != nil {
			return err
		}
	} else if node.bitmap == 0 {
		return errors.New("commitment: trie leaf without a plainKey")
	}

	if deepStorageFold && isDeepStorageAccount(node, len(path)) {
		sr, err := storageRoot(node, path)
		if err != nil {
			return fmt.Errorf("storageRoot: %w", err)
		}
		setAccountStorageRoot(w, path, sr)
		return nil
	}

	childIdx := 0
	for bm := node.bitmap; bm != 0; {
		nib := byte(bits.TrailingZeros16(bm))
		child := node.children[childIdx]
		base := len(path)
		path = append(path, nib)
		path = append(path, child.ext...)
		if err := dfsSubtreeDeep(w, child, path, storageRoot); err != nil {
			return err
		}
		path = path[:base]
		childIdx++
		bm &^= uint16(1) << nib
	}
	return nil
}

// foldStorageRoot folds each first-storage-nibble subtree of one account in its own
// worker, aggregates the storage branch, and returns the account's storageRoot.
func foldStorageRoot(ctx context.Context, numWorkers int, newWorker func() (*HexPatriciaHashed, func()), pu *parallelUpdate, node *prefixNode, path []byte) (common.Hash, error) {
	accPrefix := append([]byte(nil), path...)
	var children [16]cell
	present := node.bitmap

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(numWorkers)
	childIdx := 0
	for bm := node.bitmap; bm != 0; {
		nib := int(bits.TrailingZeros16(bm))
		child := node.children[childIdx]
		ni, ch := nib, child
		childPrefix := make([]byte, len(accPrefix), len(accPrefix)+1+len(ch.ext))
		copy(childPrefix, accPrefix)
		childPrefix = append(childPrefix, byte(ni))
		childPrefix = append(childPrefix, ch.ext...)
		group := collectSubtreeKeys(ch, childPrefix)
		g.Go(func() error {
			if err := gctx.Err(); err != nil {
				return err
			}
			w, release := newWorker()
			c, err := foldStorageLeaf(w, childPrefix, group)
			if err == nil {
				if d := w.TakeDeferredUpdates(); len(d) > 0 {
					pu.appendDeferred(d)
				}
			}
			release()
			if err != nil {
				return fmt.Errorf("storage nibble[%x] fold: %w", ni, err)
			}
			if len(ch.ext) > 0 && !c.IsEmpty() {
				copy(c.extension[:], ch.ext)
				c.extLen = int16(len(ch.ext))
			}
			children[ni] = c
			return nil
		})
		childIdx++
		bm &^= uint16(1) << nib
	}
	if err := g.Wait(); err != nil {
		return common.Hash{}, err
	}

	w, release := newWorker()
	sr, err := aggregateStorageRoot(w, path, &children, present)
	if err == nil {
		if deferred := w.TakeDeferredUpdates(); len(deferred) > 0 {
			pu.appendDeferred(deferred)
		}
	}
	release()
	if err != nil {
		return common.Hash{}, fmt.Errorf("storage branch fold: %w", err)
	}
	// A fully collapsed aggregate means a storage-less account: empty-trie root, not zero.
	if sr.IsEmpty() {
		return empty.RootHash, nil
	}
	return sr.hash, nil
}

// newDeferredStorageWorker yields a pooled trie worker for a deferring storage fold
// and a release that returns it to the pool and frees its context.
func newDeferredStorageWorker(pool *sync.Pool, factory TrieContextFactory, trace bool) (*HexPatriciaHashed, func()) {
	w := pool.Get().(*HexPatriciaHashed)
	wctx, cleanup := factory()
	w.ResetContext(wctx)
	w.SetTrace(trace)
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
	var arena keyArena
	_ = dfsSubtree(node, path, func(hk, pk []byte, upd *Update) error {
		out = append(out, touchedKey{hk: arena.copy(hk), pk: pk, upd: upd})
		return nil
	})
	return out
}
