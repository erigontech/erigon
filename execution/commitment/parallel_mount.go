package commitment

import (
	"context"
	"fmt"
	"io"
	"math/bits"
	"os"
	"sort"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon/common"
)

var cmtTiming = os.Getenv("ERIGON_CMT_TIMING") == "1"

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

// processMounted folds each touched root-child subtree concurrently, stitches the resulting cells back into the base row, and folds the base up to the root.
func (p *ParallelPatriciaHashed) processMounted(ctx context.Context, updates *Updates) ([]byte, error) {
	pu := updates.parallel
	base := p.template
	if base == nil {
		return nil, fmt.Errorf("processMounted: nil template")
	}
	if base.ctx == nil && p.trieCtxFactory != nil {
		bctx, cleanup := p.trieCtxFactory()
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
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(p.numWorkers)

	childIdx := 0
	for bm := root.bitmap; bm != 0; {
		nib := bits.TrailingZeros16(bm)
		child := root.children[childIdx]
		ni, ch := nib, child
		g.Go(func() error {
			w := p.workerPool.Get().(*HexPatriciaHashed)
			w.mountTo(base, ni)
			if p.template != nil && p.template.traceW != nil {
				w.traceW = tracePrefix(p.template.traceW, fmt.Sprintf("[mnt %x] ", ni))
			} else {
				w.traceW = nil
			}
			wctx, cleanup := p.trieCtxFactory()
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
			buildErr := dfsSubtreeDeep(w, ch, path, func(n *prefixNode, pth []byte, accountFresh bool) (common.Hash, error) {
				return foldStorageRoot(gctx, p.numWorkers, p.newStorageWorker, pu, n, pth, accountFresh)
			})
			if buildErr != nil {
				w.resetForReuse()
				p.workerPool.Put(w)
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
				w.resetForReuse()
				p.workerPool.Put(w)
				return fmt.Errorf("mount[%x] fold: %w", ni, err)
			}
			cells[ni] = c
			present[ni] = true
			if deferred := w.TakeDeferredUpdates(); len(deferred) > 0 {
				pu.appendDeferred(deferred)
			}
			w.resetForReuse()
			p.workerPool.Put(w)
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
	sort.Slice(stats, func(i, j int) bool { return stats[i].sum > stats[j].sum })
	fmt.Printf("\n[CMT_TIMING] baseUnfold=%v workerWall=%v rootFold=%v | criticalWorker=nib %x sum=%v (build=%v fold=%v)\n",
		tUnfolded.Sub(tStart), tWorkers.Sub(tUnfolded), time.Since(tWorkers), maxSumNib, maxSum, stats[0].build, stats[0].fold)
	fmt.Printf("[CMT_TIMING] sum(maxBuild=%v maxFold=%v) = ideal critical path if build & fold each split perfectly across nibbles\n", maxBuild, maxFold)
	for _, s := range stats {
		fmt.Printf("[CMT_TIMING]   nib %x keys=%-8d build=%-10v fold=%-10v sum=%v\n", s.nib, s.keys, s.build, s.fold, s.sum)
	}
}

func (p *ParallelPatriciaHashed) newStorageWorker() (*HexPatriciaHashed, func()) {
	var traceW io.Writer
	if p.template != nil {
		traceW = p.template.traceW
	}
	return newDeferredStorageWorker(&p.workerPool, p.trieCtxFactory, traceW)
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
