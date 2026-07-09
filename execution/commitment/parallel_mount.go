package commitment

import (
	"context"
	"fmt"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
)

// deepStorageThreshold is the touched-slot count above which an account's storage subtree folds concurrently instead of streaming through its worker.
const deepStorageThreshold = 1_000

// unfoldRootWall unfolds base at the root until row 0 forms the top-nibble mount wall,
// consuming at most one nibble per step: a restored root extension sharing the probe's
// leading nibble would otherwise unfold several levels at once and misplace the wall.
func unfoldRootWall(ctx context.Context, base *HexPatriciaHashed) error {
	zero := []byte{0}
	for u := base.needUnfolding(zero); u > 0; u = base.needUnfolding(zero) {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := base.unfold(zero, min(u, 1)); err != nil {
			return err
		}
	}
	return nil
}

// seedRootBase synthesizes a row-0 wall when the on-disk root has no branch, so foldMounted stops
// at the mount boundary and returns cells excluding the mount nibble for empty and non-empty bases alike.
func seedRootBase(base *HexPatriciaHashed) {
	if base.activeRows != 0 {
		return
	}
	base.activeRows = 1
	base.currentKeyLen = 0
	base.depths[0] = 1
	base.touchMap[0] = 0
	base.afterMap[0] = 0
	base.branchBefore[0] = false
	for i := range base.grid[0] {
		base.grid[0][i].reset()
	}
}

// if nibble set is -1 then subtrie is not mounted to the nibble, but limited by depth: eg do not fold mounted trie above depth 63
func (hph *HexPatriciaHashed) mountTo(root *HexPatriciaHashed, nibble int) {
	hph.Reset()

	hph.root = root.root

	hph.activeRows = root.activeRows
	hph.currentKeyLen = root.currentKeyLen
	copy(hph.currentKey[:], root.currentKey[:])
	copy(hph.depths[:], root.depths[:])
	copy(hph.branchBefore[:], root.branchBefore[:])
	copy(hph.touchMap[:], root.touchMap[:])
	copy(hph.afterMap[:], root.afterMap[:])
	copy(hph.depthsToTxNum[:], root.depthsToTxNum[:])

	hph.mountedNib = nibble
	hph.mounted = true
	hph.mountWall = root.currentKeyLen + 1
	for row := 0; row <= hph.activeRows; row++ {
		for nib := 0; nib < len(hph.grid[row]); nib++ {
			hph.grid[row][nib] = root.grid[row][nib]
		}
	}
}

// processMounted derives the fold DAG over the touched prefix trie and folds it with the shared
// worker pool, stitching the top-nibble cells into the template base and folding to the root — the
// same dispatch the streaming Process path uses, minus the scheduler-cell reuse.
func (p *ParallelPatriciaHashed) processMounted(ctx context.Context, updates *Updates) ([]byte, error) {
	pu := updates.parallel
	base := p.template
	if base == nil {
		return nil, fmt.Errorf("processMounted: nil template")
	}
	if dbg.AssertEnabled && p.templateCtxFromFactory {
		panic("processMounted: template carried a factory-owned ctx across Process calls")
	}
	if base.ctx == nil && p.trieCtxFactory != nil {
		bctx, cleanup := p.trieCtxFactory()
		base.ResetContext(bctx)
		p.templateCtxFromFactory = true
		// The fallback ctx is per-Process (cleanup munmaps it); release it before returning so
		// the template never carries a factory-owned, freed ctx into the next Process.
		defer func() {
			if cleanup != nil {
				cleanup()
			}
			base.ResetContext(nil)
			p.templateCtxFromFactory = false
		}()
	}
	base.branchEncoder.setDeferUpdates(true)
	base.SetLeaveDeferredForCaller(true)

	root := pu.trie.root
	if len(root.ext) != 0 {
		return nil, fmt.Errorf("processMounted: root.ext len %d not yet supported", len(root.ext))
	}

	if err := unfoldRootWall(ctx, base); err != nil {
		return nil, fmt.Errorf("processMounted: unfold root: %w", err)
	}
	seedRootBase(base)

	deferred, err := p.newFoldPool().dispatchFrontier(ctx, base, root, nil)
	if err != nil {
		return nil, err
	}
	pu.appendDeferred(deferred)
	return base.RootHash()
}

// setAccountStorageRoot sets the account leaf's storage root to sr; computeCellHash uses cell.hash as the storageRoot when no storage cell was processed.
func setAccountStorageRoot(w *HexPatriciaHashed, accHash []byte, sr common.Hash) {
	var c *cell
	if w.activeRows == 0 {
		c = &w.root
	} else {
		c = &w.grid[w.activeRows-1][accHash[w.currentKeyLen]]
	}
	// sr already covers the whole storage subtree, so a stale storage plain key on this cell must
	// go, or computeCellHash rehashes it as a singleton from the stale slot and discards sr.
	c.storageAddrLen = 0
	c.StorageLen = 0
	c.Flags &^= StorageUpdate
	c.loaded &^= cellLoadStorage
	c.hash = sr
	c.hashLen = 32
	c.stateHashLen = 0
}
