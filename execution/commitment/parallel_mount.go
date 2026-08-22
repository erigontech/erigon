package commitment

import (
	"cmp"
	"context"
	"fmt"
	"io"
	"math/bits"
	"os"
	"slices"
	"time"

	"golang.org/x/sync/errgroup"
)

var cmtTiming = os.Getenv("ERIGON_CMT_TIMING") == "1"

// above this touched-slot count, storage subtree folds concurrently instead of streaming through one worker
const deepStorageThreshold = 128

// unfold one nibble per step (avoids misplacing the wall)
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

func (hph *HexPatriciaHashed) mountTo(root *HexPatriciaHashed, nibble int) {
	hph.rootTouched = false
	hph.rootChecked = false
	hph.rootPresent = true

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
	n := hph.activeRows + 1
	copy(hph.grid[:n], root.grid[:n])
}

func stitchSplitCells(base *HexPatriciaHashed, cells *[16]cell, present *[16]bool) {
	for nib := range 16 {
		if !present[nib] {
			continue
		}
		c := cells[nib]
		base.touchMap[0] |= uint16(1) << nib
		if !c.IsEmpty() {
			base.afterMap[0] |= uint16(1) << nib
		} else {
			base.afterMap[0] &^= uint16(1) << nib
		}
		base.depths[0] = 1
		base.grid[0][nib] = c
	}
}

func (p *ParallelPatriciaHashed) processMounted(ctx context.Context, updates *Updates) ([]byte, error) {
	pu := updates.parallel
	base := p.template
	if base == nil {
		return nil, fmt.Errorf("processMounted: nil template")
	}
	if base.ctx == nil && p.trieCtxFactory != nil {
		bctx, cleanup := p.trieCtxFactory(ctx)
		if cleanup != nil {
			defer cleanup()
		}
		base.ResetContext(bctx)
	}
	base.branchEncoder.setDeferUpdates(true)
	base.SetLeaveDeferredForCaller(true)

	root := pu.trie.root
	if len(root.ext) != 0 {
		return nil, fmt.Errorf("processMounted: root.ext len %d not yet supported", len(root.ext))
	}

	var tStart, tUnfolded, tWorkers time.Time
	var buildDur, foldDur [16]time.Duration
	var keyCnt [16]uint32
	if cmtTiming {
		tStart = time.Now()
	}

	if err := unfoldRootWall(ctx, base); err != nil {
		return nil, fmt.Errorf("processMounted: unfold root: %w", err)
	}
	seedRootBase(base)
	if cmtTiming {
		tUnfolded = time.Now()
	}

	var (
		cells   [16]cell
		present [16]bool
	)
	foldSem := newFoldSem()
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(min(p.numWorkers, maxFoldConcurrency()))

	childIdx := 0
	for bm := root.bitmap; bm != 0; {
		nib := bits.TrailingZeros16(bm)
		child := root.children[childIdx]
		ni, ch := nib, child
		g.Go(func() error {
			w := NewHexPatriciaHashed(p.accountKeyLen, nil, p.cfg)
			w.mountTo(base, ni)
			if p.template != nil && p.template.traceW != nil {
				w.traceW = tracePrefix(p.template.traceW, fmt.Sprintf("[mnt %x] ", ni))
			} else {
				w.traceW = nil
			}
			wctx, cleanup := p.trieCtxFactory(gctx)
			if cleanup != nil {
				defer cleanup()
			}
			w.ResetContext(wctx)
			w.branchEncoder.setDeferUpdates(true)
			w.SetLeaveDeferredForCaller(true)

			var tb time.Time
			if cmtTiming {
				tb = time.Now()
				keyCnt[ni] = ch.subtreeCount
			}
			path := make([]byte, 0, 144)
			path = append(path, byte(ni))
			path = append(path, ch.ext...)
			buildErr := dfsSubtreeDeep(w, ch, path, func(n *prefixNode, pth []byte, accountFresh bool) (cell, error) {
				sr, err := foldStorageRoot(gctx, foldSem, p.newStorageWorker, pu, n, pth, accountFresh)
				if err == nil {
					p.deepLocalFolds.Add(1)
				}
				return sr, err
			})
			if buildErr != nil {
				w.Release()
				return fmt.Errorf("mount[%x] build: %w", ni, buildErr)
			}
			var tf time.Time
			if cmtTiming {
				tf = time.Now()
				buildDur[ni] = tf.Sub(tb)
			}
			c, err := w.foldMounted(gctx, ni)
			if cmtTiming {
				foldDur[ni] = time.Since(tf)
			}
			if err != nil {
				w.Release()
				return fmt.Errorf("mount[%x] fold: %w", ni, err)
			}
			cells[ni] = c
			present[ni] = true
			if deferred := w.TakeDeferredUpdates(); len(deferred) > 0 {
				pu.appendDeferred(deferred)
			}
			w.Release()
			return nil
		})
		childIdx++
		bm &^= uint16(1) << nib
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	if cmtTiming {
		tWorkers = time.Now()
	}

	stitchSplitCells(base, &cells, &present)

	if base.activeRows == 0 {
		base.activeRows = 1
	}
	for base.activeRows > 0 {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := base.fold(); err != nil {
			return nil, fmt.Errorf("processMounted: root fold: %w", err)
		}
	}
	if deferred := base.TakeDeferredUpdates(); len(deferred) > 0 {
		pu.appendDeferred(deferred)
	}
	if cmtTiming {
		printMountTiming(tStart, tUnfolded, tWorkers, &buildDur, &foldDur, &keyCnt)
	}
	return base.RootHash()
}

func printMountTiming(tStart, tUnfolded, tWorkers time.Time, buildDur, foldDur *[16]time.Duration, keyCnt *[16]uint32) {
	type wstat struct {
		nib              int
		keys             uint32
		build, fold, sum time.Duration
	}
	stats := make([]wstat, 0, 16)
	var maxBuild, maxFold, maxSum time.Duration
	var maxSumNib int
	for nib := range 16 {
		if keyCnt[nib] == 0 && buildDur[nib] == 0 && foldDur[nib] == 0 {
			continue
		}
		sum := buildDur[nib] + foldDur[nib]
		stats = append(stats, wstat{nib, keyCnt[nib], buildDur[nib], foldDur[nib], sum})
		if buildDur[nib] > maxBuild {
			maxBuild = buildDur[nib]
		}
		if foldDur[nib] > maxFold {
			maxFold = foldDur[nib]
		}
		if sum > maxSum {
			maxSum, maxSumNib = sum, nib
		}
	}
	slices.SortFunc(stats, func(a, b wstat) int { return cmp.Compare(b.sum, a.sum) })
	fmt.Printf("\n[CMT_TIMING] baseUnfold=%v workerWall=%v rootFold=%v | criticalWorker=nib %x sum=%v (build=%v fold=%v)\n",
		tUnfolded.Sub(tStart), tWorkers.Sub(tUnfolded), time.Since(tWorkers), maxSumNib, maxSum, stats[0].build, stats[0].fold)
	fmt.Printf("[CMT_TIMING] sum(maxBuild=%v maxFold=%v) = ideal critical path if build & fold each split perfectly across nibbles\n", maxBuild, maxFold)
	for _, s := range stats {
		fmt.Printf("[CMT_TIMING]   nib %x keys=%-8d build=%-10v fold=%-10v sum=%v\n", s.nib, s.keys, s.build, s.fold, s.sum)
	}
}

func (p *ParallelPatriciaHashed) newStorageWorker(ctx context.Context) (*HexPatriciaHashed, func()) {
	var traceW io.Writer
	if p.template != nil {
		traceW = p.template.traceW
	}
	return newDeferredStorageWorker(ctx, p.accountKeyLen, p.cfg, p.trieCtxFactory, traceW)
}

func setAccountStorageRoot(w *HexPatriciaHashed, accHash []byte, sr cell) {
	var c *cell
	if w.activeRows == 0 {
		c = &w.root
	} else {
		c = &w.grid[w.activeRows-1][accHash[w.currentKeyLen]]
	}
	// drop stale storage plain key: computeCellHash would rehash storage from a leftover slot, not sr
	c.storageAddrLen = 0
	c.StorageLen = 0
	c.Flags &^= StorageUpdate
	c.loaded &^= cellLoadStorage
	// Carry sr's extension onto the leaf; a hash-only sr must still clear a leftover extension from a prior collapse.
	if sr.storageAddrLen > 0 {
		c.storageAddrLen = sr.storageAddrLen
		copy(c.storageAddr[:], sr.storageAddr[:sr.storageAddrLen])
		c.StorageLen = sr.StorageLen
		if sr.StorageLen > 0 {
			copy(c.Storage[:], sr.Storage[:sr.StorageLen])
		}
		c.loaded |= sr.loaded & cellLoadStorage
	}
	c.extLen = sr.extLen
	if sr.extLen > 0 {
		copy(c.extension[:], sr.extension[:sr.extLen])
	}
	c.hashLen = sr.hashLen
	if sr.hashLen > 0 {
		copy(c.hash[:], sr.hash[:sr.hashLen])
	}
	c.stateHashLen = 0
}
