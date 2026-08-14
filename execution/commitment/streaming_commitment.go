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
	"runtime"
	"sync"
	"sync/atomic"

	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"

	"github.com/erigontech/erigon/db/kv"
)

type splitState struct {
	prefix         []byte
	cell           cell
	deferred       []*DeferredBranchUpdate
	gen            uint64
	keyCount       uint64
	lastFoldedSize uint64
	dirty          bool
	folded         bool
	queued         bool
	mu             sync.Mutex
}

func (s *splitState) reusable() bool { return s.folded && !s.dirty }

const defaultEagerFold = 256

func (sc *StreamingCommitter) shouldEagerFold(s *splitState) bool {
	return s.keyCount >= sc.eagerFloor && s.keyCount >= 2*s.lastFoldedSize
}

func (sc *StreamingCommitter) SetEagerFold(n uint64) { sc.eagerFloor = n }

type StreamingCommitter struct {
	trieCtxFactory TrieContextFactory
	cfg            TrieConfig
	accountKeyLen  int16
	numWorkers     int

	workerPool sync.Pool
	trie       *prefixTrie
	splits     map[byte]*splitState
	eagerFloor uint64

	trieMu sync.RWMutex

	started     atomic.Bool
	quit        chan struct{}
	wg          sync.WaitGroup
	bgCtx       context.Context
	dirtyCh     chan byte
	base        *HexPatriciaHashed
	baseCleanup func()
	refoldTotal atomic.Uint64
	inFlight    atomic.Int64

	foldGate func(nib byte)

	leaveDeferredForCaller bool
	deferredForCaller      []*DeferredBranchUpdate

	deepLocalFolds atomic.Uint64

	rootCell    cell
	rootChecked bool
	rootTouched bool
	rootPresent bool
	rootValid   bool
	// rootSeeded: a collapsed root row has no branch record on disk, so the carried
	// root cell is the only way a fresh base sees it.
	rootSeeded bool

	traceW io.Writer
}

func (sc *StreamingCommitter) SetTraceWriter(w io.Writer) { sc.traceW = NewSyncWriter(w) }

func NewStreamingCommitter(ctxFactory TrieContextFactory, accountKeyLen int16, cfg TrieConfig) *StreamingCommitter {
	sc := &StreamingCommitter{
		trieCtxFactory: ctxFactory,
		cfg:            cfg,
		accountKeyLen:  accountKeyLen,
		numWorkers:     runtime.NumCPU(),
		trie:           newPrefixTrie(),
		splits:         make(map[byte]*splitState),
		eagerFloor:     defaultEagerFold,
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

func (sc *StreamingCommitter) SetNumWorkers(n int) {
	if n <= 0 {
		n = runtime.NumCPU()
	}
	sc.numWorkers = n
}

func (sc *StreamingCommitter) SetTrieContextFactory(f TrieContextFactory) {
	sc.trieCtxFactory = f
}

func (sc *StreamingCommitter) SetLeaveDeferredForCaller(leave bool) {
	sc.leaveDeferredForCaller = leave
}

func (sc *StreamingCommitter) TakeDeferredUpdates() []*DeferredBranchUpdate {
	d := sc.deferredForCaller
	sc.deferredForCaller = nil
	return d
}

// plainKey/update backing must stay stable until Process; a nil update re-reads from ctx.
func (sc *StreamingCommitter) TouchKey(hashedKey, plainKey []byte, update *Update) {
	sc.trieMu.Lock()
	isNew := sc.trie.Insert(hashedKey, plainKey, update)
	if len(hashedKey) == 0 {
		sc.trieMu.Unlock()
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
	if isNew {
		s.keyCount++
	}
	enqueue := sc.started.Load() && !s.queued && sc.shouldEagerFold(s)
	if enqueue {
		s.queued = true
	}
	s.mu.Unlock()
	sc.trieMu.Unlock()

	if enqueue {
		sc.enqueue(nib)
	}
}

func (sc *StreamingCommitter) Process(ctx context.Context) ([]byte, error) {
	if sc.trieCtxFactory == nil {
		return nil, errors.New("StreamingCommitter.Process requires a TrieContextFactory")
	}
	if sc.trie == nil {
		return nil, errors.New("StreamingCommitter.Process called after Release")
	}

	sc.Stop()
	sc.rootValid = false

	base, cleanup, root, err := sc.processBase(ctx)
	if err != nil {
		return nil, err
	}
	defer cleanup()

	if root == nil || root.subtreeCount == 0 {
		return base.RootHash()
	}

	present, err := sc.foldPresentSplits(ctx, base, root)
	if err != nil {
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
		deferred = mergeDeferredByPrefix(deferred, d)
	}

	if sc.leaveDeferredForCaller {
		sc.deferredForCaller = deferred
	} else if err := sc.applyDeferred(ctx, deferred); err != nil {
		return nil, err
	}
	sc.captureRoot(base)
	rh, err := base.RootHash()
	if err != nil {
		return nil, err
	}
	flushTrieStateRates()
	sc.endBlock()
	return rh, nil
}

func (sc *StreamingCommitter) endBlock() {
	if sc.trie != nil {
		sc.trie.Reset()
	}
	sc.dropSplitDeferred()
	clear(sc.splits)
	sc.releaseBase()
}

func (sc *StreamingCommitter) captureRoot(base *HexPatriciaHashed) {
	sc.rootCell = base.root
	sc.rootChecked = base.rootChecked
	sc.rootTouched = base.rootTouched
	sc.rootPresent = base.rootPresent
	sc.rootValid = true
	sc.rootSeeded = true
}

func (sc *StreamingCommitter) SeedRootFrom(tmpl *HexPatriciaHashed) {
	if tmpl == nil {
		return
	}
	if sc.rootCell == tmpl.root && sc.rootChecked == tmpl.rootChecked &&
		sc.rootTouched == tmpl.rootTouched && sc.rootPresent == tmpl.rootPresent {
		sc.rootSeeded = true
		return
	}
	sc.rootCell = tmpl.root
	sc.rootChecked = tmpl.rootChecked
	sc.rootTouched = tmpl.rootTouched
	sc.rootPresent = tmpl.rootPresent
	sc.rootSeeded = true
	if sc.base != nil {
		sc.Stop()
		sc.releaseBase()
	}
	// trieMu.RLock: a concurrent TouchKey inserts into sc.splits, and iterating a map
	// against a concurrent insert is a fatal runtime error.
	sc.trieMu.RLock()
	for _, s := range sc.splits {
		s.mu.Lock()
		s.folded = false
		s.dirty = true
		for _, upd := range s.deferred {
			putDeferredUpdate(upd)
		}
		s.deferred = nil
		s.mu.Unlock()
	}
	sc.trieMu.RUnlock()
}

func (sc *StreamingCommitter) PromoteRootInto(tmpl *HexPatriciaHashed) bool {
	if !sc.rootValid || tmpl == nil {
		return false
	}
	tmpl.root = sc.rootCell
	tmpl.rootChecked = sc.rootChecked
	tmpl.rootTouched = sc.rootTouched
	tmpl.rootPresent = sc.rootPresent
	return true
}

func (sc *StreamingCommitter) newProcessBase(ctx context.Context) (*HexPatriciaHashed, func(), *prefixNode, error) {
	root := sc.trie.root
	if root == nil || root.subtreeCount == 0 {
		base, cleanup := sc.newBaseTrie(ctx)
		return base, cleanup, root, nil
	}
	if len(root.ext) != 0 {
		return nil, nil, nil, fmt.Errorf("StreamingCommitter: root.ext len %d not yet supported", len(root.ext))
	}
	base, cleanup, err := sc.buildBase(ctx)
	if err != nil {
		return nil, nil, nil, err
	}
	return base, cleanup, root, nil
}

func (sc *StreamingCommitter) newBaseTrie(ctx context.Context) (*HexPatriciaHashed, func()) {
	base := NewHexPatriciaHashed(sc.accountKeyLen, nil, sc.cfg)
	bctx, bclean := sc.trieCtxFactory(ctx)
	base.ResetContext(bctx)
	base.SetTraceWriter(sc.traceW)
	base.branchEncoder.setDeferUpdates(true)
	base.SetLeaveDeferredForCaller(true)
	if sc.rootSeeded {
		base.root = sc.rootCell
		base.rootChecked = sc.rootChecked
		base.rootTouched = sc.rootTouched
		base.rootPresent = sc.rootPresent
	}
	return base, func() {
		base.Release()
		if bclean != nil {
			bclean()
		}
	}
}

func (sc *StreamingCommitter) processBase(ctx context.Context) (*HexPatriciaHashed, func(), *prefixNode, error) {
	if sc.base != nil {
		root := sc.trie.root
		if root != nil && len(root.ext) != 0 {
			return nil, nil, nil, fmt.Errorf("StreamingCommitter: root.ext len %d not yet supported", len(root.ext))
		}
		return sc.base, func() {}, root, nil
	}
	return sc.newProcessBase(ctx)
}

func (sc *StreamingCommitter) buildBase(ctx context.Context) (*HexPatriciaHashed, func(), error) {
	base, cleanup := sc.newBaseTrie(ctx)

	if err := unfoldRootWall(ctx, base); err != nil {
		cleanup()
		return nil, nil, fmt.Errorf("StreamingCommitter: unfold root: %w", err)
	}
	seedRootBase(base)
	return base, cleanup, nil
}

func (sc *StreamingCommitter) SetFoldGate(fn func(nib byte)) { sc.foldGate = fn }

func (sc *StreamingCommitter) RefoldCount() uint64 { return sc.refoldTotal.Load() }

func (sc *StreamingCommitter) StartScheduler(ctx context.Context) error {
	if sc.trieCtxFactory == nil {
		return errors.New("StreamingCommitter.StartScheduler requires a TrieContextFactory")
	}
	if sc.started.Load() {
		return nil
	}
	sc.releaseBase()
	base, cleanup, err := sc.buildBase(ctx)
	if err != nil {
		return err
	}
	sc.base = base
	sc.baseCleanup = cleanup
	sc.bgCtx = ctx
	sc.quit = make(chan struct{})
	sc.dirtyCh = make(chan byte, 256)
	sc.started.Store(true)

	for range sc.numWorkers {
		sc.wg.Go(sc.scheduleWorker)
	}
	return nil
}

func (sc *StreamingCommitter) Stop() {
	if !sc.started.CompareAndSwap(true, false) {
		return
	}
	close(sc.quit)
	sc.wg.Wait()
}

func (sc *StreamingCommitter) scheduleWorker() {
	for {
		select {
		case <-sc.quit:
			return
		case nib := <-sc.dirtyCh:
			sc.inFlight.Add(1)
			sc.foldSplitBg(nib)
			sc.inFlight.Add(-1)
		}
	}
}

func (sc *StreamingCommitter) enqueue(nib byte) {
	if !sc.started.Load() {
		return
	}
	select {
	case sc.dirtyCh <- nib:
	default:
	}
}

type touchedKey struct {
	hk  []byte
	pk  []byte
	upd *Update
}

func (sc *StreamingCommitter) foldSplitBg(nib byte) {
	sc.trieMu.RLock()
	root := sc.trie.root
	child, ok := childForNib(root, nib)
	s := sc.splits[nib]
	if !ok || s == nil {
		if s != nil {
			s.mu.Lock()
			s.queued = false
			s.mu.Unlock()
		}
		sc.trieMu.RUnlock()
		return
	}

	keys := collectSplitKeys(child, nib)
	s.mu.Lock()
	genStart := s.gen
	// snapshot at fold start, not end: a stale size here makes shouldEagerFold re-fold O(N^2) on a growing split.
	s.lastFoldedSize = uint64(len(keys))
	s.queued = false
	s.mu.Unlock()
	sc.trieMu.RUnlock()

	if sc.foldGate != nil {
		sc.foldGate(nib)
	}

	c, deferred, flushed, err := sc.foldKeys(nib, keys)

	s.mu.Lock()
	if err != nil || flushed || s.gen != genStart {
		for _, upd := range deferred {
			putDeferredUpdate(upd)
		}
		sc.refoldTotal.Add(1)
		reEnqueue := s.gen != genStart && sc.shouldEagerFold(s)
		s.mu.Unlock()
		if reEnqueue {
			sc.markQueued(s, nib)
		}
		return
	}
	for _, upd := range s.deferred {
		putDeferredUpdate(upd)
	}
	s.deferred = deferred
	s.cell = c
	s.folded = true
	s.dirty = false
	s.mu.Unlock()
}

func (sc *StreamingCommitter) markQueued(s *splitState, nib byte) {
	s.mu.Lock()
	if s.queued {
		s.mu.Unlock()
		return
	}
	s.queued = true
	s.mu.Unlock()
	sc.enqueue(nib)
}

func (sc *StreamingCommitter) foldKeys(nib byte, keys []touchedKey) (cell, []*DeferredBranchUpdate, bool, error) {
	w := sc.workerPool.Get().(*HexPatriciaHashed)
	w.mountTo(sc.base, int(nib))
	if sc.traceW != nil {
		w.SetTraceWriter(tracePrefix(sc.traceW, fmt.Sprintf("[fold %x] ", nib)))
	} else {
		w.SetTraceWriter(nil)
	}
	rctx, cleanup := sc.trieCtxFactory(sc.bgCtx)
	if cleanup != nil {
		defer cleanup()
	}
	ov := &overlayContext{base: rctx}
	w.ResetContext(ov)
	w.branchEncoder.setDeferUpdates(true)
	w.SetLeaveDeferredForCaller(true)

	var err error
	for i := range keys {
		if err = w.followAndUpdate(keys[i].hk, keys[i].pk, keys[i].upd); err != nil {
			break
		}
	}
	var c cell
	if err == nil {
		c, err = w.foldMounted(sc.bgCtx, int(nib))
	}
	deferred := w.TakeDeferredUpdates()
	w.resetForReuse()
	sc.workerPool.Put(w)
	return c, deferred, ov.flushed, err
}

func childForNib(root *prefixNode, nib byte) (*prefixNode, bool) {
	if root == nil || len(root.ext) != 0 {
		return nil, false
	}
	idx, ok := childIndex(root, nib)
	if !ok {
		return nil, false
	}
	return root.children[idx], true
}

type keyArena struct{ buf []byte }

const keyArenaChunk = 64 * 1024

func (a *keyArena) copy(hk []byte) []byte {
	if len(hk) > cap(a.buf)-len(a.buf) {
		a.buf = make([]byte, 0, max(keyArenaChunk, len(hk)))
	}
	start := len(a.buf)
	a.buf = append(a.buf, hk...)
	return a.buf[start:len(a.buf):len(a.buf)]
}

func collectSplitKeys(child *prefixNode, nib byte) []touchedKey {
	path := make([]byte, 0, 144)
	path = append(path, nib)
	path = append(path, child.ext...)
	return collectSubtreeKeys(child, path)
}

// Writes never reach the real store, but a self-flushed prefix re-reads its own write.
type overlayContext struct {
	base    PatriciaContext
	writes  map[string][]byte
	flushed bool
}

func (o *overlayContext) Branch(prefix []byte) ([]byte, kv.Step, error) {
	if o.writes != nil {
		if d, ok := o.writes[string(prefix)]; ok {
			return d, 0, nil
		}
	}
	return o.base.Branch(prefix)
}

func (o *overlayContext) PutBranch(prefix, data, _ []byte) error {
	if o.writes == nil {
		o.writes = make(map[string][]byte)
	}
	o.writes[string(prefix)] = bytes.Clone(data)
	o.flushed = true
	return nil
}

func (o *overlayContext) Account(plainKey []byte) (*Update, error) { return o.base.Account(plainKey) }
func (o *overlayContext) Storage(plainKey []byte) (*Update, error) { return o.base.Storage(plainKey) }

func (sc *StreamingCommitter) foldPresentSplits(ctx context.Context, base *HexPatriciaHashed, root *prefixNode) ([16]bool, error) {
	var present [16]bool
	foldSem := newFoldSem()
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(min(sc.numWorkers, maxFoldConcurrency()))

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
		s.mu.Lock()
		reuse := s.reusable()
		s.mu.Unlock()
		if reuse {
			childIdx++
			bm &^= uint16(1) << nib
			continue
		}
		ch := child
		g.Go(func() error { return sc.foldSplit(gctx, foldSem, base, s, ch) })
		childIdx++
		bm &^= uint16(1) << nib
	}
	if err := g.Wait(); err != nil {
		return present, err
	}
	return present, nil
}

// Safe to repeat only while no touched branch collapses: a collapse self-flushes
// mid-fold and a second fold would double-apply.
func (sc *StreamingCommitter) foldDirtySplits(ctx context.Context) error {
	if sc.trieCtxFactory == nil {
		return errors.New("StreamingCommitter.foldDirtySplits requires a TrieContextFactory")
	}
	base, cleanup, root, err := sc.newProcessBase(ctx)
	if err != nil {
		return err
	}
	defer cleanup()
	if root == nil || root.subtreeCount == 0 {
		return nil
	}
	_, err = sc.foldPresentSplits(ctx, base, root)
	return err
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

func (sc *StreamingCommitter) foldSplit(ctx context.Context, foldSem *semaphore.Weighted, base *HexPatriciaHashed, s *splitState, child *prefixNode) error {
	ni := s.prefix[0]
	w := sc.workerPool.Get().(*HexPatriciaHashed)
	w.mountTo(base, int(ni))
	if sc.traceW != nil {
		w.SetTraceWriter(tracePrefix(sc.traceW, fmt.Sprintf("[split %x] ", ni)))
	} else {
		w.SetTraceWriter(nil)
	}
	wctx, cleanup := sc.trieCtxFactory(ctx)
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
	deepStorageRoot := func(n *prefixNode, pth []byte, accountFresh bool) (cell, error) {
		sr, err := foldStorageRoot(ctx, foldSem, sc.newStorageWorker, &pu, n, pth, accountFresh)
		if err == nil {
			sc.deepLocalFolds.Add(1)
		}
		return sr, err
	}
	if err := dfsSubtreeDeep(w, child, path, deepStorageRoot); err != nil {
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

func (sc *StreamingCommitter) DeepLocalFolds() uint64 { return sc.deepLocalFolds.Load() }

func (sc *StreamingCommitter) newStorageWorker(ctx context.Context) (*HexPatriciaHashed, func()) {
	return newDeferredStorageWorker(ctx, &sc.workerPool, sc.trieCtxFactory, sc.traceW)
}

func (sc *StreamingCommitter) dropSplitDeferred() {
	for _, s := range sc.splits {
		for _, upd := range s.deferred {
			putDeferredUpdate(upd)
		}
		s.deferred = nil
	}
}

func mergeDeferredByPrefix(older, newer []*DeferredBranchUpdate) []*DeferredBranchUpdate {
	if len(older) == 0 {
		return newer
	}
	inNewer := make(map[string]struct{}, len(newer))
	for _, u := range newer {
		inNewer[string(u.prefix)] = struct{}{}
	}
	out := newer
	for _, u := range older {
		if _, ok := inNewer[string(u.prefix)]; ok {
			putDeferredUpdate(u)
			continue
		}
		out = append(out, u)
	}
	return out
}

func (sc *StreamingCommitter) applyDeferred(ctx context.Context, deferred []*DeferredBranchUpdate) error {
	defer func() {
		for _, upd := range deferred {
			putDeferredUpdate(upd)
		}
	}()
	if len(deferred) == 0 {
		return nil
	}
	applyCtx, cleanup := sc.trieCtxFactory(ctx)
	if cleanup != nil {
		defer cleanup()
	}
	if applyCtx == nil {
		return errors.New("StreamingCommitter: trieCtxFactory returned nil context for deferred apply")
	}
	if err := applyDeferredGuarded(applyCtx, deferred, sc.numWorkers); err != nil {
		return fmt.Errorf("apply deferred branch updates: %w", err)
	}
	return nil
}

// Pre-merges duplicate prefixes in memory: the apply context may be write-only,
// so a colliding update can't re-read its predecessor from ctx.
func applyDeferredGuarded(ctx PatriciaContext, deferred []*DeferredBranchUpdate, numWorkers int) error {
	if !hasDuplicatePrefix(deferred) {
		_, err := ApplyDeferredBranchUpdates(deferred, numWorkers, ctx.PutBranch)
		return err
	}

	merger := workerMergerPool.Get().(*BranchMerger)
	defer workerMergerPool.Put(merger)

	applied := make(map[string][]byte, len(deferred))
	for _, upd := range deferred {
		if upd == nil {
			continue
		}
		key := string(upd.prefix)
		if prev, ok := applied[key]; ok {
			upd.prev = bytes.Clone(prev)
		} else {
			prev, _, err := ctx.Branch(upd.prefix)
			if err != nil {
				return err
			}
			upd.prev = bytes.Clone(prev)
		}
		if err := mergeDeferredUpdate(upd, merger); err != nil {
			return err
		}
		if upd.encoded == nil {
			applied[key] = upd.prev
			continue
		}
		if err := ctx.PutBranch(upd.prefix, upd.encoded, upd.prev); err != nil {
			return err
		}
		applied[key] = bytes.Clone(upd.encoded)
	}
	return nil
}

func hasDuplicatePrefix(deferred []*DeferredBranchUpdate) bool {
	seen := make(map[string]struct{}, len(deferred))
	for _, upd := range deferred {
		if upd == nil {
			continue
		}
		key := string(upd.prefix)
		if _, ok := seen[key]; ok {
			return true
		}
		seen[key] = struct{}{}
	}
	return false
}

func (sc *StreamingCommitter) Reset() {
	sc.Stop()
	sc.releaseBase()
	if sc.trie != nil {
		sc.trie.Reset()
	}
	sc.dropSplitDeferred()
	clear(sc.splits)
	for _, upd := range sc.deferredForCaller {
		putDeferredUpdate(upd)
	}
	sc.deferredForCaller = nil
	sc.rootValid, sc.rootSeeded = false, false
	sc.resetPool()
}

func (sc *StreamingCommitter) releaseBase() {
	if sc.baseCleanup != nil {
		sc.baseCleanup()
		sc.baseCleanup = nil
	}
	sc.base = nil
}

func (sc *StreamingCommitter) Release() {
	sc.Stop()
	sc.releaseBase()
	sc.dropSplitDeferred()
	sc.trie = nil
	sc.splits = nil
	for _, upd := range sc.deferredForCaller {
		putDeferredUpdate(upd)
	}
	sc.deferredForCaller = nil
	sc.rootValid, sc.rootSeeded = false, false
	sc.resetPool()
}
