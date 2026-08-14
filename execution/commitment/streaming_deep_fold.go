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
	"io"
	"math/bits"
	"runtime"
	"sync"

	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"

	"github.com/erigontech/erigon/execution/commitment/nibbles"
)

// Use GOMAXPROCS, not numWorkers, to avoid core starvation when multiple whales fold concurrently.
func maxFoldConcurrency() int { return max(1, runtime.GOMAXPROCS(0)) }

func newFoldSem() *semaphore.Weighted { return semaphore.NewWeighted(int64(maxFoldConcurrency())) }

var errStorageBaseNotBranch = errors.New("streaming: storage base has no branch at account prefix")

func unfoldStorageBase(base *HexPatriciaHashed, accPrefix []byte) error {
	d := int16(len(accPrefix))
	copy(base.currentKey[:], accPrefix)
	base.currentKeyLen = d
	base.depths[0] = d + 1
	base.activeRows = 1
	for i := range base.grid[0] {
		base.grid[0][i].reset()
	}
	base.touchMap[0], base.afterMap[0], base.branchBefore[0] = 0, 0, false

	branch, err := base.branchFromCacheOrDB(nibbles.HexToCompact(accPrefix))
	if err != nil {
		return err
	}
	if len(branch) == 0 {
		return errStorageBaseNotBranch
	}
	if len(branch) < 4 {
		// a stored branch always carries touchMap+afterMap, so a shorter non-empty read is corrupt, not missing.
		return fmt.Errorf("unfoldStorageBase: corrupt branch record at %x: %d bytes", accPrefix, len(branch))
	}
	if BranchData(branch).ChildCount() == 0 {
		// childless record is a collapse tombstone; caller must rebuild from the account leaf, not this empty base.
		return errStorageBaseNotBranch
	}
	base.branchBefore[0] = true
	return base.decodeBranchIntoRow(0, d+1, branch[2:], false)
}
func foldStorageLeaf(ctx context.Context, w *HexPatriciaHashed, base *HexPatriciaHashed, nib int, group []touchedKey) (cell, error) {
	w.mountTo(base, nib)
	for i := range group {
		if err := w.followAndUpdate(group[i].hk, group[i].pk, group[i].upd); err != nil {
			return cell{}, err
		}
	}
	return w.foldMounted(ctx, nib)
}

func isDeepStorageAccount(node *prefixNode, depth int) bool {
	return depth == 64 && node.plainKey != nil &&
		bits.OnesCount16(node.bitmap) >= 2 && node.subtreeCount > deepStorageThreshold
}

func dfsSubtreeDeep(w *HexPatriciaHashed, node *prefixNode, path []byte, storageRoot func(node *prefixNode, path []byte, accountFresh bool) (cell, error)) error {
	if node == nil {
		return nil
	}
	accountFresh := false
	if node.plainKey != nil {
		if err := w.followAndUpdate(path, node.plainKey, node.update); err != nil {
			return err
		}
		accountFresh = w.lastUpdateCellWasEmpty
	} else if node.bitmap == 0 {
		return errors.New("commitment: trie leaf without a plainKey")
	}

	if isDeepStorageAccount(node, len(path)) {
		sr, err := storageRoot(node, path, accountFresh)
		if err == nil {
			setAccountStorageRoot(w, path, sr)
			return nil
		}
		if !errors.Is(err, errStorageBaseNotBranch) {
			return fmt.Errorf("storageRoot: %w", err)
		}
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

func foldStorageRoot(ctx context.Context, sem *semaphore.Weighted, newWorker func(context.Context) (*HexPatriciaHashed, func()), pu *parallelUpdate, node *prefixNode, path []byte, accountFresh bool) (cell, error) {
	accPrefix := append([]byte(nil), path...)

	base, releaseBase := newWorker(ctx)
	defer releaseBase()

	var accTag string
	if base.traceW != nil {
		accID := node.plainKey
		if accID == nil {
			accID = accPrefix
		}
		accTag = fmt.Sprintf("[%x] ", accID)
		base.SetTraceWriter(tracePrefix(base.traceW, accTag))
	}
	if err := unfoldStorageBase(base, accPrefix); err != nil {
		if !accountFresh || !errors.Is(err, errStorageBaseNotBranch) {
			return cell{}, fmt.Errorf("unfold storage root: %w", err)
		}
	}

	var children [16]cell
	g, gctx := errgroup.WithContext(ctx)
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
			if err := sem.Acquire(gctx, 1); err != nil {
				return err
			}
			defer sem.Release(1)
			w, release := newWorker(gctx)
			if w.traceW != nil {
				w.SetTraceWriter(tracePrefix(w.traceW, accTag))
			}
			c, err := foldStorageLeaf(gctx, w, base, ni, group)
			if err == nil {
				if d := w.TakeDeferredUpdates(); len(d) > 0 {
					pu.appendDeferred(d)
				}
			}
			release()
			if err != nil {
				return fmt.Errorf("storage nibble[%x] fold: %w", ni, err)
			}
			children[ni] = c
			return nil
		})
		childIdx++
		bm &^= uint16(1) << nib
	}
	if err := g.Wait(); err != nil {
		return cell{}, err
	}

	sr, err := aggregateMountedStorageRoot(base, &children, node.bitmap)
	if err != nil {
		return cell{}, fmt.Errorf("storage branch fold: %w", err)
	}
	if deferred := base.TakeDeferredUpdates(); len(deferred) > 0 {
		pu.appendDeferred(deferred)
	}
	if sr.IsEmpty() {
		// storage-less account: leave the leaf's storage-root hashLen at 0, not empty.RootHash.
		return cell{}, nil
	}
	return sr, nil
}

func aggregateMountedStorageRoot(base *HexPatriciaHashed, children *[16]cell, bitmap uint16) (cell, error) {
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
	if base.afterMap[0] == 0 && !base.branchBefore[0] {
		base.activeRows = 0
		return cell{}, nil
	}
	if kind, _ := afterMapUpdateKind(base.afterMap[0]); kind == updateKindPropagate {
		return storageRootFromSingleChild(base)
	}
	if err := base.fold(); err != nil {
		return cell{}, err
	}
	out := base.root
	out.extLen = 0
	return out, nil
}

func storageRootFromSingleChild(base *HexPatriciaHashed) (cell, error) {
	survNib := bits.TrailingZeros16(base.afterMap[0])
	child := base.grid[0][survNib]

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
	return root, nil
}

func newDeferredStorageWorker(ctx context.Context, pool *sync.Pool, factory TrieContextFactory, traceW io.Writer) (*HexPatriciaHashed, func()) {
	w := pool.Get().(*HexPatriciaHashed)
	wctx, cleanup := factory(ctx)
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

func collectSubtreeKeys(node *prefixNode, path []byte) []touchedKey {
	out := make([]touchedKey, 0, node.subtreeCount)
	var arena keyArena
	_ = dfsSubtree(node, path, func(hk, pk []byte, upd *Update) error {
		out = append(out, touchedKey{hk: arena.copy(hk), pk: pk, upd: upd})
		return nil
	})
	return out
}
