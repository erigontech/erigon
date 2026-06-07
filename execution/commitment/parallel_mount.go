package commitment

import (
	"context"
	"errors"
	"fmt"
	"math/bits"
	"os"
	"sort"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon/common"
)

var cmtTiming = os.Getenv("ERIGON_CMT_TIMING") == "1"

// cmtDeep enables concurrent folding of an account's storage subtree (split
// below depth 64) for accounts whose touched storage exceeds deepStorageThreshold.
var cmtDeep = os.Getenv("ERIGON_CMT_DEEP") == "1"

const deepStorageThreshold = 10_000

// processMounted unfolds one base instance to the root branch, mounts a worker
// per touched child nibble that inherits the grid state, folds each child's
// subtree into a cell at its true depth, drops those cells back into the base
// row (untouched siblings stay in place), and folds the base up — the mount/fold
// model of ConcurrentPatriciaHashed, which avoids the deposit/rebuild barrier
// that mis-depths storage cell hashes. Single-level; nesting is layered on top.
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

	zero := []byte{0}
	for u := base.needUnfolding(zero); u > 0; u = base.needUnfolding(zero) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := base.unfold(zero, u); err != nil {
			return nil, fmt.Errorf("processMounted: unfold root: %w", err)
		}
	}
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
			if p.template != nil {
				w.trace = p.template.trace
				w.traceDomain = p.template.traceDomain
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
			var buildErr error
			if cmtDeep {
				buildErr = p.dfsSubtreeDeep(gctx, w, pu, ch, path)
			} else {
				buildErr = dfsSubtree(ch, path, func(hk, pk []byte, upd *Update) error {
					return w.followAndUpdate(hk, pk, upd)
				})
			}
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

	for nib := range 16 {
		if !present[nib] {
			continue
		}
		c := cells[nib]
		// Strip the child nibble the folded cell carries as its leading extension
		// nibble, since that nibble is the row slot it is dropped into. Only a
		// hash-only sub-branch carries the slot in its extension; a leaf's
		// extension is a key tail that must be left intact.
		if c.extLen > 0 && c.accountAddrLen == 0 && c.storageAddrLen == 0 {
			c.extLen--
			copy(c.extension[:], c.extension[1:])
			c.hashedExtLen -= 2
			copy(c.hashedExtension[:], c.hashedExtension[2:])
		}
		base.touchMap[0] |= uint16(1) << nib
		if !c.IsEmpty() {
			base.afterMap[0] |= uint16(1) << nib
		} else {
			base.afterMap[0] &^= uint16(1) << nib
		}
		base.depths[0] = 1
		base.grid[0][nib] = c
	}

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

// dfsSubtreeDeep walks a mount worker's subtree like dfsSubtree, but at an
// account whose touched storage exceeds deepStorageThreshold it folds that
// account's storage subtries concurrently (split below depth 64), injects the
// resulting storageRoot into the account leaf, and skips the storage stream.
func (p *ParallelPatriciaHashed) dfsSubtreeDeep(ctx context.Context, w *HexPatriciaHashed, pu *parallelUpdate, node *prefixNode, path []byte) error {
	if node == nil {
		return nil
	}
	if node.plainKey != nil {
		if err := w.followAndUpdate(path, node.plainKey, node.update); err != nil {
			return err
		}
	} else if node.bitmap == 0 {
		return errors.New("ParallelPatriciaHashed: trie leaf without a plainKey")
	}

	// Big-storage account: the node carries the account (plainKey at depth 64)
	// and its storage trie (children). Fold the storage subtries concurrently.
	if node.plainKey != nil && len(path) == 64 && bits.OnesCount16(node.bitmap) >= 2 &&
		node.subtreeCount > deepStorageThreshold {
		sr, err := p.concurrentStorageRoot(ctx, pu, node, path)
		if err != nil {
			return fmt.Errorf("concurrentStorageRoot: %w", err)
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
		if err := p.dfsSubtreeDeep(ctx, w, pu, child, path); err != nil {
			return err
		}
		path = path[:base]
		childIdx++
		bm &^= uint16(1) << nib
	}
	return nil
}

// concurrentStorageRoot folds each first-storage-nibble subtree of one account in
// its own worker, then stitches the storage branch and returns its hash (the
// account's storageRoot). path is the 64-nibble account hash.
func (p *ParallelPatriciaHashed) concurrentStorageRoot(ctx context.Context, pu *parallelUpdate, node *prefixNode, path []byte) (common.Hash, error) {
	accNib := int(path[63])
	var children [16]cell
	var present uint16

	var g errgroup.Group
	g.SetLimit(p.numWorkers)
	childIdx := 0
	for bm := node.bitmap; bm != 0; {
		nib := int(bits.TrailingZeros16(bm))
		child := node.children[childIdx]
		cpath := make([]byte, len(path), len(path)+1+len(child.ext))
		copy(cpath, path)
		cpath = append(cpath, byte(nib))
		cpath = append(cpath, child.ext...)
		ni, ch, cp := nib, child, cpath
		present |= uint16(1) << nib
		g.Go(func() error {
			w := p.workerPool.Get().(*HexPatriciaHashed)
			wctx, cleanup := p.trieCtxFactory()
			if cleanup != nil {
				defer cleanup()
			}
			w.ResetContext(wctx)
			w.branchEncoder.setDeferUpdates(true)
			w.SetLeaveDeferredForCaller(true)
			if err := dfsSubtree(ch, cp, func(hk, pk []byte, upd *Update) error {
				return w.followAndUpdate(hk, pk, upd)
			}); err != nil {
				w.resetForReuse()
				p.workerPool.Put(w)
				return fmt.Errorf("storage mount[%x] build: %w", ni, err)
			}
			for w.activeRows > 1 {
				if err := w.fold(); err != nil {
					w.resetForReuse()
					p.workerPool.Put(w)
					return fmt.Errorf("storage mount[%x] fold: %w", ni, err)
				}
			}
			c := w.grid[0][accNib]
			// strip the leading storage nibble carried in the extension; the column
			// index at the storage branch now carries it.
			if c.hashedExtLen > 0 {
				c.hashedExtLen--
				copy(c.hashedExtension[:], c.hashedExtension[1:])
			}
			if c.extLen > 0 {
				c.extLen--
				copy(c.extension[:], c.extension[1:])
			}
			children[ni] = c
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
		return common.Hash{}, err
	}

	asm := p.workerPool.Get().(*HexPatriciaHashed)
	actx, acleanup := p.trieCtxFactory()
	if acleanup != nil {
		defer acleanup()
	}
	asm.ResetContext(actx)
	asm.branchEncoder.setDeferUpdates(true)
	asm.SetLeaveDeferredForCaller(true)
	copy(asm.currentKey[:], path[:64])
	asm.currentKeyLen = 64
	asm.depths[0] = 64
	asm.depths[1] = 65
	asm.activeRows = 2
	asm.touchMap[0] = uint16(1) << accNib
	asm.afterMap[0] = uint16(1) << accNib
	for x := 0; x < 16; x++ {
		if present&(uint16(1)<<x) != 0 {
			asm.grid[1][x] = children[x]
		}
	}
	asm.touchMap[1] = present
	asm.afterMap[1] = present
	if err := asm.fold(); err != nil {
		asm.resetForReuse()
		p.workerPool.Put(asm)
		return common.Hash{}, fmt.Errorf("storage branch fold: %w", err)
	}
	sr := asm.grid[0][accNib].hash
	if deferred := asm.TakeDeferredUpdates(); len(deferred) > 0 {
		pu.appendDeferred(deferred)
	}
	asm.resetForReuse()
	p.workerPool.Put(asm)
	return sr, nil
}

// setAccountStorageRoot sets the just-placed account leaf's storage root to sr,
// so computeCellHash hashes the account with that storageRoot (computeCellHash
// uses cell.hash as the storageRoot when no storage cell was processed).
func setAccountStorageRoot(w *HexPatriciaHashed, accHash []byte, sr common.Hash) {
	var c *cell
	if w.activeRows == 0 {
		c = &w.root
	} else {
		c = &w.grid[w.activeRows-1][accHash[w.currentKeyLen]]
	}
	c.hash = sr
	c.hashLen = 32
	c.stateHashLen = 0
}
