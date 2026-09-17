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
	"sync"
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

func (p *ctxLeasePool) tryAcquire() *ctxLease {
	select {
	case l := <-p.free:
		return l
	default:
		return nil
	}
}

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
	helpers       atomic.Uint64
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
	passNib := -1
	if positioned {
		passNib = unfoldedPassThrough(w)
	} else {
		opened = openEmptyRow(w, path)
	}
	fw.forks.Add(1)

	cells := &w.stitchScratch
	var touched, present atomic.Uint32
	var nibs [16]byte
	n := 0
	for bm := node.bitmap; bm != 0; {
		nib := bits.TrailingZeros16(bm)
		nibs[n] = byte(nib)
		n++
		bm &^= uint16(1) << nib
	}

	cctx, cancel := context.WithCancel(ctx)
	var g errgroup.Group
	defer func() {
		cancel()
		_ = g.Wait()
	}()
	var (
		claimed  atomic.Int32
		failOnce sync.Once
		firstErr error
	)
	fail := func(err error) {
		failOnce.Do(func() {
			firstErr = err
			cancel()
		})
	}
	next := func() int { return int(claimed.Add(1)) - 1 }
	run := func(cw *walker, i int) error {
		return fw.runChild(cctx, w, cw, node, i, int(nibs[i]), path, cells, &touched, &present)
	}

	own := &walker{lease: wk.lease, bindsCtx: true}
	if wk.bindsCtx {
		w.ResetContext(nil)
	}
	wk.lease = nil
	started := 0
	for {
		for started < n-1 && int(claimed.Load()) < n-1 {
			l := fw.leases.tryAcquire()
			if l == nil {
				break
			}
			started++
			fw.helpers.Add(1)
			g.Go(func() error {
				held := &walker{lease: l, bindsCtx: true}
				defer func() {
					if held.lease != nil {
						fw.leases.release(held.lease)
					}
				}()
				for i := next(); i < n; i = next() {
					if err := run(held, i); err != nil {
						fail(err)
						return nil
					}
				}
				return nil
			})
		}
		i := next()
		if i >= n {
			break
		}
		if err := run(own, i); err != nil {
			fail(err)
			break
		}
	}

	if started > 0 && own.lease != nil {
		fw.leases.release(own.lease)
		own.lease = nil
	}
	_ = g.Wait()
	if firstErr != nil {
		wk.lease = own.lease
		return firstErr
	}
	if started > 0 {
		if err := fw.attach(ctx, wk); err != nil {
			return err
		}
	} else {
		wk.lease = own.lease
		if wk.bindsCtx {
			w.ResetContext(fw.leases.context(wk.lease))
		}
	}
	touchedBits, presentBits := uint16(touched.Load()), uint16(present.Load())
	stitchSplitCells(w, cells, touchedBits, presentBits)
	if passNib >= 0 && int(nibs[0]) == passNib {
		bit := uint16(1) << passNib
		row := w.activeRows - 1
		if touchedBits&^presentBits&bit != 0 && w.touchMap[row]&^bit != 0 {
			w.touchMap[row] &^= bit
		}
	}
	opened.extendSingleSurvivor(w, path)
	opened.closeIfEmpty(w)
	return nil
}

func (fw *forkWalk) runChild(ctx context.Context, base *HexPatriciaHashed, cw *walker, node *prefixNode,
	idx, nib int, path []byte, cells *[16]cell, touched, present *atomic.Uint32) error {
	child := node.children[idx]
	childPath := make([]byte, 0, max(len(path)+1+len(child.ext), forkPathCap))
	childPath = append(childPath, path...)
	childPath = append(childPath, byte(nib))
	childPath = append(childPath, child.ext...)

	fw.checkout(cw, childPath)
	defer fw.checkin(cw)
	cw.trie.mountTo(base, nib)
	if err := fw.walk(ctx, cw, child, childPath); err != nil {
		return fmt.Errorf("fork[%x]: child %x: %w", path, nib, err)
	}
	c, ferr := cw.trie.foldMounted(ctx, nib)
	if ferr != nil {
		return fmt.Errorf("fork[%x]: child %x fold: %w", path, nib, ferr)
	}
	if merr := PremergeDeferredUpdates(cw.trie.branchEncoder.deferred); merr != nil {
		return fmt.Errorf("fork[%x]: child %x premerge: %w", path, nib, merr)
	}
	bit := uint32(1) << nib
	touched.Or(uint32(cw.trie.touchMap[0]) & bit)
	present.Or(uint32(cw.trie.afterMap[0]) & bit)
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
	recs := w.branchEncoder.deferred
	fw.pu.appendDeferred(recs)
	clear(recs)
	w.branchEncoder.deferred = recs[:0]
	w.Release()
}
