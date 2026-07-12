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

// maxFoldConcurrency caps the whale-storage fold fan-out at the CPUs the process
// may run on. The account-mount and whale-storage fan-outs nest, so an unshared
// per-level limit of numWorkers permits ~numWorkers² runnable leaf goroutines; one
// shared budget of this size caps that product. GOMAXPROCS, not numWorkers, which
// would starve cores when numWorkers < GOMAXPROCS and several whales fold at once.
func maxFoldConcurrency() int { return max(1, runtime.GOMAXPROCS(0)) }

func newFoldSem() *semaphore.Weighted { return semaphore.NewWeighted(int64(maxFoldConcurrency())) }

// errStorageBaseNotBranch: no on-disk branch exactly at the account prefix (its
// storage top is a deeper extension); callers fall back to streaming recursion.
var errStorageBaseNotBranch = errors.New("streaming: storage base has no branch at account prefix")

// Seed the base from the real on-disk branch, not a hand-seed, so untouched first-nibble subtrees survive instead of dropping and diverging the root from the sequential trie.

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
	// A stored branch is always >= 4 bytes (touchMap+afterMap); a shorter non-empty read is corrupt, not missing.
	if len(branch) < 4 {
		return fmt.Errorf("unfoldStorageBase: corrupt branch record at %x: %d bytes", accPrefix, len(branch))
	}
	// A childless (afterMap == 0) record is a tombstone left by a prior single-child collapse (or full
	// delete): its live storage top is a deeper extension carried on the account leaf, not here. Seeding
	// an empty base from it would drop that survivor on a later re-expansion; treat it as "no branch" so
	// the caller rebuilds from the account leaf's storage root instead.
	if BranchData(branch).ChildCount() == 0 {
		return errStorageBaseNotBranch
	}
	base.branchBefore[0] = true
	return base.decodeBranchIntoRow(0, d+1, branch[2:], false)
}

// Mounts the shared unfolded base so concurrent first-nibble workers fold against the same on-disk storage state.
func foldStorageLeaf(ctx context.Context, w *HexPatriciaHashed, base *HexPatriciaHashed, nib int, group []touchedKey) (cell, error) {
	w.mountTo(base, nib)
	for i := range group {
		if err := w.followAndUpdate(group[i].hk, group[i].pk, group[i].upd); err != nil {
			return cell{}, err
		}
	}
	return w.foldMounted(ctx, nib)
}

// isDeepStorageAccount reports whether node is an account leaf whose touched storage
// is large and forked enough to fold concurrently.
func isDeepStorageAccount(node *prefixNode, depth int) bool {
	return depth == 64 && node.plainKey != nil &&
		bits.OnesCount16(node.bitmap) >= 2 && node.subtreeCount > deepStorageThreshold
}

// dfsSubtreeDeep walks node's subtree applying each key to w, but at a big-storage
// account it injects storageRoot's result instead of streaming the slots.
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
		// fall through to normal streaming recursion, which recovers the untouched
		// on-disk siblings via per-key unfolds
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

// Storage-root analogue of the account mount fold: parallelize a whale's storage by first nibble.
// sem is the shared fold-concurrency budget: acquired per first-nibble worker so that
// this whale's fan-out plus every other concurrently-folding subtree stays within the core count.
func foldStorageRoot(ctx context.Context, sem *semaphore.Weighted, newWorker func() (*HexPatriciaHashed, func()), pu *parallelUpdate, node *prefixNode, path []byte, accountFresh bool) (cell, error) {
	accPrefix := append([]byte(nil), path...)

	base, releaseBase := newWorker()
	defer releaseBase()

	// Tag this account's storage-fold workers with its address so one account's
	// fold can be grepped out of the interleaved parallel trace. Only paid for
	// when tracing is on (base.traceW mirrors every worker's trace state).
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
		// A fresh account proves nothing exists on disk beneath accPrefix, so the reset
		// (empty) wall rows unfoldStorageBase left behind are the correct seed.
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
			w, release := newWorker()
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
	// A storage-less account must leave the account leaf's storage-root hashLen at 0, not
	// empty.RootHash: computeCellHash supplies empty.RootHash at hash time, so the root is the
	// same, but a stored hash makes needUnfolding descend into a storage-root branch that was
	// never written, failing a later storage re-touch with "empty branch data read during unfold".
	if sr.IsEmpty() {
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
	// A single surviving first-nibble child is an extension/leaf storage root; base.fold() would
	// misencode it by prepending the account prefix and returning the child hash, so build it directly.
	if kind, _ := afterMapUpdateKind(base.afterMap[0]); kind == updateKindPropagate {
		return storageRootFromSingleChild(base)
	}
	if err := base.fold(); err != nil {
		return cell{}, err
	}
	// A multi-child storage root's subtree persists as branch records, so the account leaf references
	// it by hash without an extension; a single-child collapse (handled above) carries one instead.
	out := base.root
	out.extLen = 0
	return out, nil
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
	return root, nil
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
	var arena keyArena
	_ = dfsSubtree(node, path, func(hk, pk []byte, upd *Update) error {
		out = append(out, touchedKey{hk: arena.copy(hk), pk: pk, upd: upd})
		return nil
	})
	return out
}
