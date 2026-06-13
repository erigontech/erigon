package commitment

import (
	"context"
	"fmt"
	"math/bits"

	"golang.org/x/sync/errgroup"
)

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

	zero := []byte{0}
	for u := base.needUnfolding(zero); u > 0; u = base.needUnfolding(zero) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := base.unfold(zero, u); err != nil {
			return nil, fmt.Errorf("processMounted: unfold root: %w", err)
		}
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

			path := make([]byte, 0, 144)
			path = append(path, byte(ni))
			path = append(path, ch.ext...)
			if err := dfsSubtree(ch, path, func(hk, pk []byte, upd *Update) error {
				return w.followAndUpdate(hk, pk, upd)
			}); err != nil {
				w.resetForReuse()
				p.workerPool.Put(w)
				return fmt.Errorf("mount[%x] build: %w", ni, err)
			}
			c, err := w.foldMounted(gctx, ni)
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
	// The root-row fold reaches the template via foldPropagate (single child) or
	// foldBranch (multi child); only the latter sets rootPresent, so set it here
	// to keep EncodeCurrentState's restore-then-continue state correct.
	base.rootPresent = !base.root.IsEmpty()
	if deferred := base.TakeDeferredUpdates(); len(deferred) > 0 {
		pu.appendDeferred(deferred)
	}
	return base.RootHash()
}
