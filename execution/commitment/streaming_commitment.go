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
	"sync/atomic"

	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon/common"
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

// reusable reports a cell cached by a background fold and not yet invalidated by
// a later touch. Callers must hold s.mu.
func (s *splitState) reusable() bool { return s.folded && !s.dirty }

const defaultEagerFold = 256

// shouldEagerFold re-folds only once the split's key count has at least doubled
// since its last fold (and cleared the floor), keeping total re-fold work linear.
// Callers hold s.mu.
func (sc *StreamingCommitter) shouldEagerFold(s *splitState) bool {
	return s.keyCount >= sc.eagerFloor && s.keyCount >= 2*s.lastFoldedSize
}

// SetEagerFold overrides the coalescing floor (default defaultEagerFold).
func (sc *StreamingCommitter) SetEagerFold(n uint64) { sc.eagerFloor = n }

// StreamingCommitter overlaps commitment fold work with block execution.
type StreamingCommitter struct {
	trieCtxFactory TrieContextFactory
	cfg            TrieConfig
	accountKeyLen  int16
	numWorkers     int

	workerPool sync.Pool
	trie       *prefixTrie
	splits     map[byte]*splitState
	eagerFloor uint64

	// trieMu serializes prefix-trie mutation against the scheduler's structural reads.
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

	// foldGate, when set, is a test seam invoked just before a background fold.
	foldGate func(nib byte)

	leaveDeferredForCaller bool
	deferredForCaller      []*DeferredBranchUpdate

	deepLocalFolds atomic.Uint64

	// rootValid gates root promotion: cleared each Process and set only on the
	// folded path, so the no-touch path leaves the template's prior root untouched.
	rootCell    cell
	rootChecked bool
	rootTouched bool
	rootPresent bool
	rootValid   bool

	traceW io.Writer
}

func (sc *StreamingCommitter) SetTraceWriter(w io.Writer) { sc.traceW = NewSyncWriter(w) }

// NewStreamingCommitter constructs a StreamingCommitter ready to accept touches.
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

// TouchKey records a touched key. hashedKey is in nibble form; plainKey/update
// backing must stay stable until Process, and a nil update makes the fold
// re-read the value from ctx.
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

// Process folds every touched top-nibble split into a cell, stitches the cells
// into the base row, and folds to the root.
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
	} else if err := sc.applyDeferred(deferred); err != nil {
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

// endBlock drains the per-block touch funnel and releases the scheduler base
// (Process folds it down to the terminal root, so it cannot be reused), keeping
// the worker pool and the caller's staged root/deferred snapshots.
func (sc *StreamingCommitter) endBlock() {
	if sc.trie != nil {
		sc.trie.Reset()
	}
	sc.dropSplitDeferred()
	clear(sc.splits)
	sc.releaseBase()
}

// captureRoot snapshots the base trie's terminal root cell and flags by value
// so the snapshot survives the base being released.
func (sc *StreamingCommitter) captureRoot(base *HexPatriciaHashed) {
	sc.rootCell = base.root
	sc.rootChecked = base.rootChecked
	sc.rootTouched = base.rootTouched
	sc.rootPresent = base.rootPresent
	sc.rootValid = true
}

// PromoteRootInto copies the most recently folded root cell and flags into tmpl,
// reporting whether a fold result was promoted; the no-touch path returns false
// and leaves the template's prior root in place.
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

// newProcessBase builds the per-Process base trie, unfolded at the root unless
// the prefix trie is empty (no touches).
func (sc *StreamingCommitter) newProcessBase(ctx context.Context) (*HexPatriciaHashed, func(), *prefixNode, error) {
	root := sc.trie.root
	if root == nil || root.subtreeCount == 0 {
		base, cleanup := sc.newBaseTrie()
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

// newBaseTrie constructs a fresh deferring base trie and a cleanup releasing it.
func (sc *StreamingCommitter) newBaseTrie() (*HexPatriciaHashed, func()) {
	base := NewHexPatriciaHashed(sc.accountKeyLen, nil, sc.cfg)
	bctx, bclean := sc.trieCtxFactory()
	base.ResetContext(bctx)
	base.SetTraceWriter(sc.traceW)
	base.branchEncoder.setDeferUpdates(true)
	base.SetLeaveDeferredForCaller(true)
	return base, func() {
		base.Release()
		if bclean != nil {
			bclean()
		}
	}
}

// processBase returns the base trie Process folds and stitches into, reusing the
// persistent scheduler base when one exists (its cleanup is then a no-op).
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

// buildBase builds a base trie unfolded one level at the on-disk root so its
// row 0 carries every top-nibble sibling the split cells stitch into.
func (sc *StreamingCommitter) buildBase(ctx context.Context) (*HexPatriciaHashed, func(), error) {
	base, cleanup := sc.newBaseTrie()

	zero := []byte{0}
	for u := base.needUnfolding(zero); u > 0; u = base.needUnfolding(zero) {
		if err := ctx.Err(); err != nil {
			cleanup()
			return nil, nil, err
		}
		if err := base.unfold(zero, u); err != nil {
			cleanup()
			return nil, nil, fmt.Errorf("StreamingCommitter: unfold root: %w", err)
		}
	}
	seedRootBase(base)
	return base, cleanup, nil
}

// SetFoldGate installs a test seam invoked just before a background fold.
func (sc *StreamingCommitter) SetFoldGate(fn func(nib byte)) { sc.foldGate = fn }

// RefoldCount reports how many background folds were discarded as wasted work.
func (sc *StreamingCommitter) RefoldCount() uint64 { return sc.refoldTotal.Load() }

// StartScheduler builds the persistent base and launches the background fold
// pool; after it returns TouchKey enqueues dirtied splits. Calling it twice is a no-op.
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

	sc.wg.Add(sc.numWorkers)
	for range sc.numWorkers {
		go sc.scheduleWorker()
	}
	return nil
}

// Stop drains the background fold pool, waiting for any in-flight fold to finish.
// Safe to call when no scheduler is running and to call twice.
func (sc *StreamingCommitter) Stop() {
	if !sc.started.CompareAndSwap(true, false) {
		return
	}
	close(sc.quit)
	sc.wg.Wait()
}

func (sc *StreamingCommitter) scheduleWorker() {
	defer sc.wg.Done()
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

// enqueue offers a dirtied split to the fold pool without blocking; a full queue
// just leaves the split dirty for Process, losing overlap but not safety.
func (sc *StreamingCommitter) enqueue(nib byte) {
	if !sc.started.Load() {
		return
	}
	select {
	case sc.dirtyCh <- nib:
	default:
	}
}

// touchedKey is a snapshotted touch a background fold replays; hk is copied off
// the walk path while pk/upd reference the caller's stable backing.
type touchedKey struct {
	hk  []byte
	pk  []byte
	upd *Update
}

// foldSplitBg folds one split against an isolating overlay, installing the result
// only if no touch bumped the split's gen and it did not self-flush meanwhile.
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
	// Close the coalescing gate at fold start (snapshot size), not end, or a stale
	// lastFoldedSize would let every mid-fold touch re-enqueue for the fold's duration.
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
		// Re-fold a mid-fold-touched split only if the gate still passes, bounding
		// a streaming whale to O(N) instead of O(N^2) re-folds.
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

// markQueued re-enqueues a split, deduped so a burst of touches schedules it once.
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

// foldKeys folds a snapshotted split's keys on a pooled worker whose overlay ctx
// discards branch writes; the returned flushed flag reports a mid-fold self-flush.
func (sc *StreamingCommitter) foldKeys(nib byte, keys []touchedKey) (cell, []*DeferredBranchUpdate, bool, error) {
	w := sc.workerPool.Get().(*HexPatriciaHashed)
	w.mountTo(sc.base, int(nib))
	if sc.traceW != nil {
		w.SetTraceWriter(tracePrefix(sc.traceW, fmt.Sprintf("[fold %x] ", nib)))
	} else {
		w.SetTraceWriter(nil)
	}
	rctx, cleanup := sc.trieCtxFactory()
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

// childForNib returns the top-nibble split-point child of root, or false if the
// nibble carries no touched keys.
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

// keyArena copies walk-path nibbles into chunked backing buffers so each
// collected key gets a stable slice without one allocation per key.
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

// collectSplitKeys walks a split's subtree in sorted order, copying each key's
// hashed nibbles off the reused walk path.
func collectSplitKeys(child *prefixNode, nib byte) []touchedKey {
	path := make([]byte, 0, 144)
	path = append(path, nib)
	path = append(path, child.ext...)
	return collectSubtreeKeys(child, path)
}

// overlayContext isolates a background fold: writes never reach the real store
// but a self-flushed prefix re-reads its own write, and flushed records that.
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
	o.writes[string(prefix)] = common.Copy(data)
	o.flushed = true
	return nil
}

func (o *overlayContext) Account(plainKey []byte) (*Update, error) { return o.base.Account(plainKey) }
func (o *overlayContext) Storage(plainKey []byte) (*Update, error) { return o.base.Storage(plainKey) }

// foldPresentSplits re-folds every touched top-nibble split concurrently onto the
// base, recording which slots were folded; it never applies or merges.
func (sc *StreamingCommitter) foldPresentSplits(ctx context.Context, base *HexPatriciaHashed, root *prefixNode) ([16]bool, error) {
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
		s.mu.Lock()
		reuse := s.reusable()
		s.mu.Unlock()
		if reuse {
			childIdx++
			bm &^= uint16(1) << nib
			continue
		}
		ch := child
		g.Go(func() error { return sc.foldSplit(gctx, base, s, ch) })
		childIdx++
		bm &^= uint16(1) << nib
	}
	if err := g.Wait(); err != nil {
		return present, err
	}
	return present, nil
}

// foldDirtySplits re-folds every touched split without merging to the root or
// writing to the store. Repeated calls are re-fold-invariant only while no touched
// branch collapses, since a collapse self-flushes mid-fold and a second fold
// would double-apply.
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

// stitchSplitCells drops each folded split cell into the base row at its top-nibble slot;
// foldMounted already returns cells excluding the mount nibble, so they are stitched verbatim.
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

// foldSplit re-folds one top-nibble subtree on a worker mounted at the unfolded
// base, to the split cell rather than the root, replacing the split's cell and
// deferred set.
func (sc *StreamingCommitter) foldSplit(ctx context.Context, base *HexPatriciaHashed, s *splitState, child *prefixNode) error {
	ni := s.prefix[0]
	w := sc.workerPool.Get().(*HexPatriciaHashed)
	w.mountTo(base, int(ni))
	if sc.traceW != nil {
		w.SetTraceWriter(tracePrefix(sc.traceW, fmt.Sprintf("[split %x] ", ni)))
	} else {
		w.SetTraceWriter(nil)
	}
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
	deepStorageRoot := func(n *prefixNode, pth []byte, accountFresh bool) (common.Hash, error) {
		sr, err := foldStorageRoot(ctx, sc.numWorkers, sc.newStorageWorker, &pu, n, pth, accountFresh)
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

// DeepLocalFolds reports how many big-storage accounts the streaming path deep-folded.
func (sc *StreamingCommitter) DeepLocalFolds() uint64 { return sc.deepLocalFolds.Load() }

// newStorageWorker sources a concurrent-storage-fold worker; disjoint subtree
// prefixes keep a mid-fold self-flush from racing another fold's writes.
func (sc *StreamingCommitter) newStorageWorker() (*HexPatriciaHashed, func()) {
	return newDeferredStorageWorker(&sc.workerPool, sc.trieCtxFactory, sc.traceW)
}

// dropSplitDeferred returns every split's staged deferred branch updates to the pool.
func (sc *StreamingCommitter) dropSplitDeferred() {
	for _, s := range sc.splits {
		for _, upd := range s.deferred {
			putDeferredUpdate(upd)
		}
		s.deferred = nil
	}
}

// mergeDeferredByPrefix combines two deferred-update slices, keeping newer's
// entry for any prefix both supply and recycling the superseded older one.
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
	if err := applyDeferredGuarded(applyCtx, deferred, sc.numWorkers); err != nil {
		return fmt.Errorf("apply deferred branch updates: %w", err)
	}
	return nil
}

// applyDeferredGuarded applies deferred branch updates, pre-merging in memory any
// prefix emitted by more than one fold set because the apply context may be
// write-only and a colliding update cannot re-read its predecessor from ctx.
func applyDeferredGuarded(ctx PatriciaContext, deferred []*DeferredBranchUpdate, numWorkers int) error {
	if !hasDuplicatePrefix(deferred) {
		_, err := ApplyDeferredBranchUpdates(deferred, numWorkers, ctx.PutBranch)
		return err
	}

	encoder := workerEncoderPool.Get().(*BranchEncoder)
	merger := workerMergerPool.Get().(*BranchMerger)
	defer workerEncoderPool.Put(encoder)
	defer workerMergerPool.Put(merger)

	applied := make(map[string][]byte, len(deferred))
	for _, upd := range deferred {
		if upd == nil {
			continue
		}
		key := string(upd.prefix)
		if prev, ok := applied[key]; ok {
			upd.prev = common.Copy(prev)
		} else {
			prev, _, err := ctx.Branch(upd.prefix)
			if err != nil {
				return err
			}
			upd.prev = common.Copy(prev)
		}
		if err := encodeDeferredUpdate(upd, encoder, merger); err != nil {
			return err
		}
		if upd.encoded == nil {
			applied[key] = upd.prev
			continue
		}
		if err := ctx.PutBranch(upd.prefix, upd.encoded, upd.prev); err != nil {
			return err
		}
		applied[key] = common.Copy(upd.encoded)
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

// Reset clears per-split state, the prefix trie, and staged deferred updates so the
// committer can be reused for the next block.
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
	sc.resetPool()
}

// releaseBase drops the scheduler's persistent base and its context.
func (sc *StreamingCommitter) releaseBase() {
	if sc.baseCleanup != nil {
		sc.baseCleanup()
		sc.baseCleanup = nil
	}
	sc.base = nil
}

// Release drops all owned state; the committer must not be used afterwards.
// Repeat calls are safe no-ops.
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
	sc.resetPool()
}
