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
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/bits"
	"sync/atomic"

	"golang.org/x/sync/errgroup"
)

const (
	minForkGrain       = 128
	forkGrainPerWorker = 4
	forkPathCap        = 144
)

const ForkGrainNever = ^uint32(0)

func forkGrainFor(roundKeys, numWorkers int) uint32 {
	return uint32(max(minForkGrain, roundKeys/(forkGrainPerWorker*max(numWorkers, 1))))
}

type ctxLease struct {
	ctx     PatriciaContext
	cleanup func()
	made    bool
}

type ctxLeasePool struct {
	round   context.Context
	factory TrieContextFactory
	free    chan *ctxLease
	entries []*ctxLease
}

func newCtxLeasePool(round context.Context, factory TrieContextFactory, n int) *ctxLeasePool {
	p := &ctxLeasePool{round: round, factory: factory, free: make(chan *ctxLease, n), entries: make([]*ctxLease, n)}
	for i := range p.entries {
		p.entries[i] = &ctxLease{}
		p.free <- p.entries[i]
	}
	return p
}

func (p *ctxLeasePool) acquire(ctx context.Context) (*ctxLease, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	select {
	case l := <-p.free:
		return l, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (p *ctxLeasePool) release(l *ctxLease) { p.free <- l }

func (p *ctxLeasePool) context(l *ctxLease) PatriciaContext {
	if !l.made {
		l.ctx, l.cleanup = p.factory(p.round)
		l.made = true
	}
	return l.ctx
}

func (p *ctxLeasePool) close() {
	for _, l := range p.entries {
		if l.cleanup != nil {
			l.cleanup()
			l.cleanup = nil
		}
	}
}

type walker struct {
	trie     *HexPatriciaHashed
	lease    *ctxLease
	bindsCtx bool
}

type forkWalk struct {
	leases        *ctxLeasePool
	accountKeyLen int16
	cfg           TrieConfig
	pu            *parallelUpdate
	metrics       *Metrics
	traceW        io.Writer
	grain         uint32
	forks         atomic.Uint64
}

func (fw *forkWalk) attach(ctx context.Context, wk *walker) error {
	l, err := fw.leases.acquire(ctx)
	if err != nil {
		return err
	}
	wk.lease = l
	if wk.bindsCtx {
		wk.trie.ResetContext(fw.leases.context(l))
	}
	return nil
}

func (fw *forkWalk) detach(wk *walker) {
	if wk.lease == nil {
		return
	}
	if wk.bindsCtx {
		wk.trie.ResetContext(nil)
	}
	fw.leases.release(wk.lease)
	wk.lease = nil
}

func (fw *forkWalk) splits(node *prefixNode) bool {
	if bits.OnesCount16(node.bitmap) < 2 {
		return false
	}
	var children, largest uint32
	for _, c := range node.children {
		children += c.subtreeCount
		largest = max(largest, c.subtreeCount)
	}
	return children-largest >= fw.grain
}

func (fw *forkWalk) walk(ctx context.Context, wk *walker, node *prefixNode, path []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if node == nil {
		return nil
	}
	if node.plainKey != nil {
		if err := wk.trie.followAndUpdate(path, node.plainKey, node.update); err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
	} else if node.bitmap == 0 {
		return errors.New("commitment: trie leaf without a plainKey")
	}
	if fw.splits(node) {
		return fw.fork(ctx, wk, node, path)
	}
	return fw.walkChildren(ctx, wk, node, path)
}

func (fw *forkWalk) walkChildren(ctx context.Context, wk *walker, node *prefixNode, path []byte) error {
	childIdx := 0
	for bm := node.bitmap; bm != 0; {
		if err := ctx.Err(); err != nil {
			return err
		}
		nib := byte(bits.TrailingZeros16(bm))
		child := node.children[childIdx]
		base := len(path)
		path = append(path, nib)
		path = append(path, child.ext...)
		if err := fw.walk(ctx, wk, child, path); err != nil {
			return err
		}
		path = path[:base]
		childIdx++
		bm &^= uint16(1) << nib
	}
	return nil
}

func (fw *forkWalk) fork(ctx context.Context, wk *walker, node *prefixNode, path []byte) error {
	w := wk.trie
	positioned, err := unfoldToRow(ctx, w, path)
	if err != nil {
		return fmt.Errorf("fork[%x]: position: %w", path, err)
	}
	var opened openedRow
	leafNib, leafKey := -1, []byte(nil)
	if positioned {
		leafNib, leafKey = unfoldedLeaf(w, path)
	} else {
		opened = openEmptyRow(w, path)
	}
	fw.forks.Add(1)

	cells := &w.stitchScratch
	var deferredByChild [16][]*DeferredBranchUpdate
	var touched, present [16]bool
	var nibs [16]byte
	n := 0
	for bm := node.bitmap; bm != 0; {
		nib := bits.TrailingZeros16(bm)
		nibs[n] = byte(nib)
		n++
		bm &^= uint16(1) << nib
	}

	g, gctx := errgroup.WithContext(ctx)
	fw.detach(wk)
	var claimed atomic.Int32
	for range min(n, len(fw.leases.entries)) {
		g.Go(func() error {
			l, aerr := fw.leases.acquire(gctx)
			if aerr != nil {
				return aerr
			}
			held := &walker{lease: l, bindsCtx: true}
			defer func() {
				if held.lease != nil {
					fw.leases.release(held.lease)
				}
			}()
			for {
				i := int(claimed.Add(1)) - 1
				if i >= n {
					return nil
				}
				if cerr := fw.runChild(gctx, w, held, node, i, int(nibs[i]), path, cells, &touched, &present, &deferredByChild); cerr != nil {
					return cerr
				}
			}
		})
	}

	werr := g.Wait()
	for _, d := range deferredByChild {
		fw.pu.appendDeferred(d)
	}
	if werr != nil {
		return werr
	}
	if aerr := fw.attach(ctx, wk); aerr != nil {
		return aerr
	}
	var touchedBits, presentBits uint16
	for nib := range 16 {
		if touched[nib] {
			touchedBits |= uint16(1) << nib
		}
		if present[nib] {
			presentBits |= uint16(1) << nib
		}
	}
	stitchSplitCells(w, cells, touchedBits, presentBits)
	if leafNib >= 0 {
		bit := uint16(1) << leafNib
		row := w.activeRows - 1
		if touchedBits&^presentBits&bit != 0 && w.touchMap[row]&^bit != 0 && bytes.Equal(firstKeyUnder(node, path), leafKey) {
			w.touchMap[row] &^= bit
		}
	}
	opened.extendSingleSurvivor(w, path)
	opened.closeIfEmpty(w)
	return nil
}

func (fw *forkWalk) runChild(ctx context.Context, base *HexPatriciaHashed, cw *walker, node *prefixNode,
	idx, nib int, path []byte, cells *[16]cell, touched, present *[16]bool, deferred *[16][]*DeferredBranchUpdate) error {
	child := node.children[idx]
	childPath := make([]byte, 0, max(len(path)+1+len(child.ext), forkPathCap))
	childPath = append(childPath, path...)
	childPath = append(childPath, byte(nib))
	childPath = append(childPath, child.ext...)

	fw.checkout(cw, childPath)
	defer fw.checkin(cw)
	cw.trie.mountTo(base, nib)
	if err := fw.walk(ctx, cw, child, childPath); err != nil {
		deferred[nib] = cw.trie.TakeDeferredUpdates()
		return fmt.Errorf("fork[%x]: child %x: %w", path, nib, err)
	}
	c, ferr := cw.trie.foldMounted(ctx, nib)
	deferred[nib] = cw.trie.TakeDeferredUpdates()
	if ferr != nil {
		return fmt.Errorf("fork[%x]: child %x fold: %w", path, nib, ferr)
	}
	bit := uint16(1) << nib
	touched[nib] = cw.trie.touchMap[0]&bit != 0
	present[nib] = cw.trie.afterMap[0]&bit != 0
	cells[nib] = c
	return nil
}

func (fw *forkWalk) checkout(wk *walker, path []byte) {
	w := NewHexPatriciaHashed(fw.accountKeyLen, nil, fw.cfg)
	w.ResetContext(fw.leases.context(wk.lease))
	w.branchEncoder.setDeferUpdates(true)
	w.SetLeaveDeferredForCaller(true)
	w.metrics.Reset()
	if fw.traceW != nil {
		w.SetTraceWriter(tracePrefix(fw.traceW, fmt.Sprintf("[%x] ", path)))
	}
	wk.trie = w
}

func (fw *forkWalk) checkin(wk *walker) {
	w := wk.trie
	if w == nil {
		return
	}
	wk.trie = nil
	w.ResetContext(nil)
	fw.metrics.Merge(w.metrics)
	w.Release()
}
