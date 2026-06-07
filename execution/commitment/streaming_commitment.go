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
	"runtime"
	"sync"

	"golang.org/x/sync/errgroup"
)

// splitState holds the per-split-point state owned by a StreamingCommitter.
// In the lazy fold-at-Process path only prefix is consulted at fold time; the
// dirty/gen/deferred/mu fields are populated by the touch path and consumed by
// the background scheduler layered on top later.
type splitState struct {
	prefix   []byte
	cell     cell
	deferred []*DeferredBranchUpdate
	gen      uint64
	dirty    bool
	mu       sync.Mutex
}

// StreamingCommitter overlaps commitment fold work with block execution. It owns
// a persistent prefix trie of touched keys (Insert is order-independent) and,
// per top-nibble split point, the state needed to (re-)fold that subtree.
//
// Process folds every dirty split statelessly — mount a pooled worker on a
// freshly unfolded base, replay the split's keys in sorted order via an in-order
// prefix-trie walk, fold to a cell — then stitches the cells into the base row
// and folds to the root. It reuses the proven mount/fold engine verbatim; the
// only new logic is orchestration.
type StreamingCommitter struct {
	trieCtxFactory TrieContextFactory
	cfg            TrieConfig
	accountKeyLen  int16
	numWorkers     int

	workerPool sync.Pool
	trie       *prefixTrie
	splits     map[byte]*splitState

	// pph is held only to reuse the proven deep storage fan-out
	// (dfsSubtreeDeep / concurrentStorageRoot) for big-storage accounts; its
	// factory and worker count are kept in sync with sc at Process time.
	pph *ParallelPatriciaHashed

	leaveDeferredForCaller bool
	deferredForCaller      []*DeferredBranchUpdate

	trace bool
}

func (sc *StreamingCommitter) SetTrace(b bool) { sc.trace = b }

// NewStreamingCommitter constructs a StreamingCommitter ready to accept touches.
// Process requires a non-nil ctxFactory.
func NewStreamingCommitter(ctxFactory TrieContextFactory, accountKeyLen int16, cfg TrieConfig) *StreamingCommitter {
	sc := &StreamingCommitter{
		trieCtxFactory: ctxFactory,
		cfg:            cfg,
		accountKeyLen:  accountKeyLen,
		numWorkers:     runtime.NumCPU(),
		trie:           newPrefixTrie(),
		splits:         make(map[byte]*splitState),
		pph:            NewParallelPatriciaHashed(ctxFactory, accountKeyLen, cfg),
	}
	sc.resetPool()
	return sc
}

func (sc *StreamingCommitter) resetPool() {
	akl := sc.accountKeyLen
	cfg := sc.cfg
	sc.workerPool = sync.Pool{
		New: func() any { return NewHexPatriciaHashed(akl, nil, cfg) },
	}
}

// SetNumWorkers overrides the worker count for the next Process call. Values
// <= 0 fall back to runtime.NumCPU.
func (sc *StreamingCommitter) SetNumWorkers(n int) {
	if n <= 0 {
		n = runtime.NumCPU()
	}
	sc.numWorkers = n
}

// SetTrieContextFactory replaces the per-worker context factory.
func (sc *StreamingCommitter) SetTrieContextFactory(f TrieContextFactory) {
	sc.trieCtxFactory = f
	if sc.pph != nil {
		sc.pph.SetTrieContextFactory(f)
	}
}

// SetLeaveDeferredForCaller makes Process leave the accumulated deferred branch
// updates for the caller to flush instead of applying them inline.
func (sc *StreamingCommitter) SetLeaveDeferredForCaller(leave bool) {
	sc.leaveDeferredForCaller = leave
}

// TakeDeferredUpdates returns the deferred branch updates staged for the caller
// and clears them; the caller takes ownership and returns them to the pool.
func (sc *StreamingCommitter) TakeDeferredUpdates() []*DeferredBranchUpdate {
	d := sc.deferredForCaller
	sc.deferredForCaller = nil
	return d
}

// TouchKey records a touched key. hashedKey is in nibble form; plainKey and
// update backing must stay stable until Process (or the next Reset). update may
// be nil, in which case the fold re-reads the value from ctx. Insert is
// order-independent, so callers may touch keys in execution order.
func (sc *StreamingCommitter) TouchKey(hashedKey, plainKey []byte, update *Update) {
	sc.trie.Insert(hashedKey, plainKey, update)
	if len(hashedKey) == 0 {
		return
	}
	nib := hashedKey[0]
	s := sc.splits[nib]
	if s == nil {
		s = &splitState{prefix: []byte{nib}}
		sc.splits[nib] = s
	}
	s.mu.Lock()
	s.dirty = true
	s.gen++
	s.mu.Unlock()
}

// Process folds every touched top-nibble split into a cell, stitches the cells
// into the base row, and folds to the root. Branch updates are applied to ctx
// (or staged for the caller when SetLeaveDeferredForCaller is set).
func (sc *StreamingCommitter) Process(ctx context.Context) ([]byte, error) {
	if sc.trieCtxFactory == nil {
		return nil, errors.New("StreamingCommitter.Process requires a TrieContextFactory")
	}
	if sc.trie == nil {
		return nil, errors.New("StreamingCommitter.Process called after Release")
	}

	base := NewHexPatriciaHashed(sc.accountKeyLen, nil, sc.cfg)
	defer base.Release()
	bctx, bclean := sc.trieCtxFactory()
	if bclean != nil {
		defer bclean()
	}
	base.ResetContext(bctx)
	base.SetTrace(sc.trace)
	base.branchEncoder.setDeferUpdates(true)
	base.SetLeaveDeferredForCaller(true)

	root := sc.trie.root
	if root == nil || root.subtreeCount == 0 {
		return base.RootHash()
	}
	if len(root.ext) != 0 {
		return nil, fmt.Errorf("StreamingCommitter: root.ext len %d not yet supported", len(root.ext))
	}

	zero := []byte{0}
	for u := base.needUnfolding(zero); u > 0; u = base.needUnfolding(zero) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := base.unfold(zero, u); err != nil {
			return nil, fmt.Errorf("StreamingCommitter: unfold root: %w", err)
		}
	}

	sc.pph.SetNumWorkers(sc.numWorkers)
	sc.pph.SetTrieContextFactory(sc.trieCtxFactory)

	var present [16]bool
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(sc.numWorkers)

	childIdx := 0
	for bm := root.bitmap; bm != 0; {
		nib := bits.TrailingZeros16(bm)
		child := root.children[childIdx]
		ni := byte(nib)
		s := sc.splits[ni]
		if s == nil {
			s = &splitState{prefix: []byte{ni}}
			sc.splits[ni] = s
		}
		present[nib] = true
		ch := child
		g.Go(func() error { return sc.foldSplit(gctx, base, s, ch) })
		childIdx++
		bm &^= uint16(1) << nib
	}
	if err := g.Wait(); err != nil {
		sc.dropSplitDeferred()
		return nil, err
	}

	var (
		cells    [16]cell
		deferred []*DeferredBranchUpdate
	)
	for nib := range 16 {
		if !present[nib] {
			continue
		}
		s := sc.splits[byte(nib)]
		cells[nib] = s.cell
		deferred = append(deferred, s.deferred...)
		s.deferred = nil
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
			return nil, fmt.Errorf("StreamingCommitter: root fold: %w", err)
		}
	}
	if d := base.TakeDeferredUpdates(); len(d) > 0 {
		deferred = append(deferred, d...)
	}

	if sc.leaveDeferredForCaller {
		sc.deferredForCaller = deferred
	} else if err := sc.applyDeferred(deferred); err != nil {
		return nil, err
	}
	return base.RootHash()
}

// stitchSplitCells drops each folded split cell back into the base row at its
// top-nibble slot, stripping the leading extension nibble a hash-only sub-branch
// carries (the row slot already implies it) and leaving a leaf's key tail
// intact — the proven mount stitch.
func stitchSplitCells(base *HexPatriciaHashed, cells *[16]cell, present *[16]bool) {
	for nib := range 16 {
		if !present[nib] {
			continue
		}
		c := cells[nib]
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
}

// foldSplit re-folds one top-nibble subtree statelessly: mount a pooled worker
// on the unfolded base, replay the split's keys in sorted (in-order) prefix-trie
// order via the deep walk (a big-storage account's storage fans out concurrently
// through concurrentStorageRoot), fold to the split cell — never to the root —
// and capture the fold's deferred branch updates into s, replacing any prior set.
func (sc *StreamingCommitter) foldSplit(ctx context.Context, base *HexPatriciaHashed, s *splitState, child *prefixNode) error {
	ni := s.prefix[0]
	w := sc.workerPool.Get().(*HexPatriciaHashed)
	w.mountTo(base, int(ni))
	w.SetTrace(sc.trace)
	wctx, cleanup := sc.trieCtxFactory()
	if cleanup != nil {
		defer cleanup()
	}
	w.ResetContext(wctx)
	w.branchEncoder.setDeferUpdates(true)
	w.SetLeaveDeferredForCaller(true)

	var pu parallelUpdate
	path := make([]byte, 0, 144)
	path = append(path, ni)
	path = append(path, child.ext...)
	if err := sc.pph.dfsSubtreeDeep(ctx, w, &pu, child, path); err != nil {
		w.resetForReuse()
		sc.workerPool.Put(w)
		for _, upd := range pu.deferredCombined {
			putDeferredUpdate(upd)
		}
		return fmt.Errorf("split[%x] build: %w", ni, err)
	}
	c, err := w.foldMounted(ctx, int(ni))
	if err != nil {
		w.resetForReuse()
		sc.workerPool.Put(w)
		for _, upd := range pu.deferredCombined {
			putDeferredUpdate(upd)
		}
		return fmt.Errorf("split[%x] fold: %w", ni, err)
	}

	newDeferred := pu.deferredCombined
	if d := w.TakeDeferredUpdates(); len(d) > 0 {
		newDeferred = append(newDeferred, d...)
	}
	w.resetForReuse()
	sc.workerPool.Put(w)

	s.mu.Lock()
	for _, upd := range s.deferred {
		putDeferredUpdate(upd)
	}
	s.deferred = newDeferred
	s.cell = c
	s.dirty = false
	s.mu.Unlock()
	return nil
}

// dropSplitDeferred returns every split's staged deferred branch updates to the
// pool — the error-unwind for a failed Process so no pooled entries leak.
func (sc *StreamingCommitter) dropSplitDeferred() {
	for _, s := range sc.splits {
		for _, upd := range s.deferred {
			putDeferredUpdate(upd)
		}
		s.deferred = nil
	}
}

func (sc *StreamingCommitter) applyDeferred(deferred []*DeferredBranchUpdate) error {
	defer func() {
		for _, upd := range deferred {
			putDeferredUpdate(upd)
		}
	}()
	if len(deferred) == 0 {
		return nil
	}
	applyCtx, cleanup := sc.trieCtxFactory()
	if cleanup != nil {
		defer cleanup()
	}
	if applyCtx == nil {
		return errors.New("StreamingCommitter: trieCtxFactory returned nil context for deferred apply")
	}
	if _, err := ApplyDeferredBranchUpdates(deferred, sc.numWorkers, applyCtx.PutBranch); err != nil {
		return fmt.Errorf("apply deferred branch updates: %w", err)
	}
	return nil
}

// Reset clears per-split state, the prefix trie, and any staged deferred updates
// so the committer can be reused for the next block. The worker pool is dropped.
func (sc *StreamingCommitter) Reset() {
	if sc.trie != nil {
		sc.trie.Reset()
	}
	sc.dropSplitDeferred()
	clear(sc.splits)
	for _, upd := range sc.deferredForCaller {
		putDeferredUpdate(upd)
	}
	sc.deferredForCaller = nil
	if sc.pph != nil {
		sc.pph.Reset()
	}
	sc.resetPool()
}

// Release drops all owned state. After Release the committer must not be used.
// Repeat calls are safe no-ops.
func (sc *StreamingCommitter) Release() {
	sc.dropSplitDeferred()
	sc.trie = nil
	sc.splits = nil
	for _, upd := range sc.deferredForCaller {
		putDeferredUpdate(upd)
	}
	sc.deferredForCaller = nil
	if sc.pph != nil {
		sc.pph.Release()
		sc.pph = nil
	}
	sc.resetPool()
}
