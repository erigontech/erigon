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
	"sync/atomic"

	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
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
	refolds  uint64
	dirty    bool
	folded   bool
	queued   bool
	mu       sync.Mutex
}

// reusable reports whether a background fold cached an authoritative cell that
// has not been invalidated by a later touch — Process stitches it directly
// instead of re-folding. Callers must hold s.mu.
func (s *splitState) reusable() bool { return s.folded && !s.dirty }

// foldTrigger decides whether a dirtied split should be enqueued for an eager
// background fold. Callers hold s.mu.
type foldTrigger func(s *splitState) bool

// foldEager is the single shipped fold-trigger policy: fold-on-dirty. Every
// dirtied split is eligible, so a touched split folds in the background as soon
// as a worker is free; a split the scheduler never reaches (queue full or never
// started) falls through to fold-at-Process. Alternative policies are deferred
// until measurement shows re-fold waste worth a heuristic.
func foldEager(*splitState) bool { return true }

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
	foldPolicy foldTrigger

	// pph is held only to reuse the proven deep storage fan-out
	// (dfsSubtreeDeep / concurrentStorageRoot) for big-storage accounts; its
	// factory and worker count are kept in sync with sc at Process time.
	pph *ParallelPatriciaHashed

	// trieMu serializes prefix-trie mutation (TouchKey's Insert) against the
	// background scheduler's structural reads (snapshotting a split's keys).
	trieMu sync.RWMutex

	// Background scheduler state (Task 4). The scheduler is opt-in via
	// StartScheduler; when it never starts the committer stays in the lazy
	// fold-at-Process path and these fields are unused.
	started     atomic.Bool
	quit        chan struct{}
	wg          sync.WaitGroup
	bgCtx       context.Context
	dirtyCh     chan byte
	base        *HexPatriciaHashed
	baseCleanup func()
	refoldTotal atomic.Uint64

	// foldGate, when set, is invoked by a background fold right before it folds
	// a snapshotted split — a test seam to inject a mid-fold touch deterministically.
	foldGate func(nib byte)

	leaveDeferredForCaller bool
	deferredForCaller      []*DeferredBranchUpdate

	// rootCell + flags snapshot the base trie's terminal root after a successful
	// fold so the owning ParallelPatriciaHashed can promote them into its
	// persistence template (RootTrie) for EncodeCurrentState/SetState. rootValid
	// gates promotion: it is cleared each Process and set only on the folded
	// path, so the no-touch path leaves the template's prior root untouched.
	rootCell    cell
	rootChecked bool
	rootTouched bool
	rootPresent bool
	rootValid   bool

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
		foldPolicy:     foldEager,
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
// A storage-slot touch carries a 128-nibble key whose first nibble is the top
// nibble of the owning account's hash, so it routes to and dirties the account's
// split here — the storageRoot cross-dependency (an account leaf embeds its
// storageRoot) needs no separate mapping.
func (sc *StreamingCommitter) TouchKey(hashedKey, plainKey []byte, update *Update) {
	sc.trieMu.Lock()
	sc.trie.Insert(hashedKey, plainKey, update)
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
	enqueue := sc.started.Load() && !s.queued && sc.foldPolicy(s)
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
// into the base row, and folds to the root. Branch updates are applied to ctx
// (or staged for the caller when SetLeaveDeferredForCaller is set).
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
		deferred = append(deferred, d...)
	}

	if sc.leaveDeferredForCaller {
		sc.deferredForCaller = deferred
	} else if err := sc.applyDeferred(deferred); err != nil {
		return nil, err
	}
	sc.captureRoot(base)
	return base.RootHash()
}

// captureRoot snapshots the base trie's terminal root cell and flags (by value,
// so the snapshot survives the base being released) for promotion into the
// owning ParallelPatriciaHashed's persistence template.
func (sc *StreamingCommitter) captureRoot(base *HexPatriciaHashed) {
	sc.rootCell = base.root
	sc.rootChecked = base.rootChecked
	sc.rootTouched = base.rootTouched
	sc.rootPresent = base.rootPresent
	sc.rootValid = true
}

// PromoteRootInto copies the most recently folded root cell and flags into tmpl
// so RootTrie().EncodeCurrentState serializes a root SetState can restore. It
// reports whether a fold result was promoted; the no-touch path returns false
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

// newProcessBase builds the per-Process base trie: a fresh deferring
// HexPatriciaHashed unfolded one level at the root so its row 0 carries every
// on-disk top-nibble sibling the split cells stitch into. The returned cleanup
// releases the base and the context. root may be nil/empty (no touches).
func (sc *StreamingCommitter) newProcessBase(ctx context.Context) (*HexPatriciaHashed, func(), *prefixNode, error) {
	base := NewHexPatriciaHashed(sc.accountKeyLen, nil, sc.cfg)
	bctx, bclean := sc.trieCtxFactory()
	cleanup := func() {
		base.Release()
		if bclean != nil {
			bclean()
		}
	}
	base.ResetContext(bctx)
	base.SetTrace(sc.trace)
	base.branchEncoder.setDeferUpdates(true)
	base.SetLeaveDeferredForCaller(true)

	root := sc.trie.root
	if root == nil || root.subtreeCount == 0 {
		return base, cleanup, root, nil
	}
	if len(root.ext) != 0 {
		cleanup()
		return nil, nil, nil, fmt.Errorf("StreamingCommitter: root.ext len %d not yet supported", len(root.ext))
	}

	zero := []byte{0}
	for u := base.needUnfolding(zero); u > 0; u = base.needUnfolding(zero) {
		if err := ctx.Err(); err != nil {
			cleanup()
			return nil, nil, nil, err
		}
		if err := base.unfold(zero, u); err != nil {
			cleanup()
			return nil, nil, nil, fmt.Errorf("StreamingCommitter: unfold root: %w", err)
		}
	}
	return base, cleanup, root, nil
}

// processBase returns the base trie Process folds and stitches into. When the
// background scheduler ran it reuses the persistent base the cached cells were
// folded against (identical on-disk root row), so a freshly built base is only
// needed in the pure lazy path. The returned cleanup is a no-op for the
// persistent base (released by Reset/Release).
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

// buildBase builds a base trie unfolded one level at the on-disk root so its row
// 0 carries every top-nibble sibling the split cells stitch into. Unlike
// newProcessBase it unfolds unconditionally (the scheduler builds it before any
// touch arrives), so it never short-circuits on an empty prefix trie.
func (sc *StreamingCommitter) buildBase(ctx context.Context) (*HexPatriciaHashed, func(), error) {
	base := NewHexPatriciaHashed(sc.accountKeyLen, nil, sc.cfg)
	bctx, bclean := sc.trieCtxFactory()
	cleanup := func() {
		base.Release()
		if bclean != nil {
			bclean()
		}
	}
	base.ResetContext(bctx)
	base.SetTrace(sc.trace)
	base.branchEncoder.setDeferUpdates(true)
	base.SetLeaveDeferredForCaller(true)

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
	return base, cleanup, nil
}

// SetFoldGate installs a test seam invoked by a background fold just before it
// folds a snapshotted split, used to inject a mid-fold touch deterministically.
func (sc *StreamingCommitter) SetFoldGate(fn func(nib byte)) { sc.foldGate = fn }

// RefoldCount reports how many background folds were discarded (re-touched mid
// fold or self-flushed on a collapse) — wasted work the scheduler quantifies.
func (sc *StreamingCommitter) RefoldCount() uint64 { return sc.refoldTotal.Load() }

// StartScheduler builds the persistent base and launches the background fold
// pool. After it returns, TouchKey enqueues dirtied splits for the pool to fold
// under execution. Process stops the pool and folds whatever is left. Calling it
// twice is a no-op.
func (sc *StreamingCommitter) StartScheduler(ctx context.Context) error {
	if sc.trieCtxFactory == nil {
		return errors.New("StreamingCommitter.StartScheduler requires a TrieContextFactory")
	}
	if sc.started.Load() {
		return nil
	}
	base, cleanup, err := sc.buildBase(ctx)
	if err != nil {
		return err
	}
	sc.base = base
	sc.baseCleanup = cleanup
	sc.bgCtx = ctx
	sc.quit = make(chan struct{})
	sc.dirtyCh = make(chan byte, 256)
	sc.pph.SetNumWorkers(sc.numWorkers)
	sc.pph.SetTrieContextFactory(sc.trieCtxFactory)
	sc.started.Store(true)

	sc.wg.Add(sc.numWorkers)
	for range sc.numWorkers {
		go sc.scheduleWorker()
	}
	return nil
}

// Stop drains the background fold pool: it signals the workers to exit, waits for
// any in-flight fold to finish, and returns. Splits still dirty afterwards are
// folded by Process. Safe to call when no scheduler is running and to call twice.
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
			sc.foldSplitBg(nib)
		}
	}
}

// enqueue offers a dirtied split to the fold pool without blocking; if the queue
// is full the split stays dirty and Process folds it — overlap lost, not safety.
func (sc *StreamingCommitter) enqueue(nib byte) {
	if !sc.started.Load() {
		return
	}
	select {
	case sc.dirtyCh <- nib:
	default:
	}
}

// touchedKey is a snapshotted (hashedKey, plainKey, update) triple a background
// fold replays. hashedKey is copied off the walk path; plainKey/update reference
// the caller's stable backing.
type touchedKey struct {
	hk  []byte
	pk  []byte
	upd *Update
}

// foldSplitBg folds one split in the background: snapshot its keys under a brief
// read-lock (so TouchKey's Insert can proceed concurrently), fold them on a
// pooled worker against an isolating overlay ctx, then CAS the result into the
// split only if no touch bumped its gen meanwhile. A self-flush (collapse) is
// discarded and left for Process to fold against the real ctx — re-folding a
// collapsed split mid-block would double-apply.
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
		s.refolds++
		sc.refoldTotal.Add(1)
		reEnqueue := s.gen != genStart
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

// markQueued re-enqueues a split that a touch invalidated mid-fold, deduped so a
// burst of touches schedules it once.
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

// foldKeys folds a snapshotted split's keys on a pooled worker mounted on the
// persistent base, replaying them in sorted (snapshot) order. The worker's ctx is
// an overlay that discards branch writes, so the engine's mid-fold self-flush
// (readBranchAndCheckForFlushing on a collapse) never mutates the real store; the
// returned flushed flag reports whether that happened. Big-storage accounts fold
// sequentially here (the deep fan-out is a Process-time path); the cell is
// byte-identical either way.
func (sc *StreamingCommitter) foldKeys(nib byte, keys []touchedKey) (cell, []*DeferredBranchUpdate, bool, error) {
	w := sc.workerPool.Get().(*HexPatriciaHashed)
	w.mountTo(sc.base, int(nib))
	w.SetTrace(sc.trace)
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

// collectSplitKeys walks a split's subtree in sorted order, copying each key's
// hashed nibbles off the reused walk path; plainKey/update stay referenced.
func collectSplitKeys(child *prefixNode, nib byte) []touchedKey {
	out := make([]touchedKey, 0, child.subtreeCount)
	path := make([]byte, 0, 144)
	path = append(path, nib)
	path = append(path, child.ext...)
	_ = dfsSubtree(child, path, func(hk, pk []byte, upd *Update) error {
		out = append(out, touchedKey{hk: append([]byte(nil), hk...), pk: pk, upd: upd})
		return nil
	})
	return out
}

// overlayContext wraps a real PatriciaContext for a background fold: reads fall
// through (a self-flushed prefix re-reads its just-written value so the worker
// stays self-consistent) but writes never reach the real store. flushed records
// whether the engine self-flushed mid-fold, signalling a collapse whose result
// must be discarded.
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

// foldPresentSplits re-folds every touched top-nibble split concurrently onto
// the base, recording which slots were folded. It never applies or merges; the
// folded cell and the fold's deferred branch updates land in each splitState.
func (sc *StreamingCommitter) foldPresentSplits(ctx context.Context, base *HexPatriciaHashed, root *prefixNode) ([16]bool, error) {
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

// foldDirtySplits re-folds every touched split once, replacing each split's cell
// and deferred set, without the committer merging to the root or applying
// anything to the store. Repeated calls are re-fold-invariant ONLY while no
// touched branch collapses: the engine self-flushes a pending prefix to ctx when
// it re-reads it mid-fold (readBranchAndCheckForFlushing), which a delete-driven
// collapse triggers, so a second fold would read that mutated branch as prev and
// double-apply. The Task-4 scheduler must isolate or gate such re-folds; until
// then this exists so tests can exercise the invariant in the collapse-free
// regime, where mid-block folds never write.
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
	if err := applyDeferredGuarded(applyCtx, deferred, sc.numWorkers); err != nil {
		return fmt.Errorf("apply deferred branch updates: %w", err)
	}
	return nil
}

// applyDeferredGuarded applies deferred branch updates with a duplicate-prefix
// flush guard mirroring BranchEncoder.CollectDeferredUpdate: the combined slice
// concatenates the per-split sets and then the merge set, whose split-boundary
// prefix can collide with the merge's bottom row. Bare ApplyDeferredBranchUpdates
// does not dedup across the slice, so a colliding prefix's second update would
// merge against the stale pre-image and clobber the first. Here, whenever a
// prefix already staged in the pending batch reappears the batch is flushed, and
// prev is re-read from ctx for every update, so a prefix written by an earlier
// flush merges against the just-written value — last-writer-wins is cumulative,
// matching the sequential single-write.
func applyDeferredGuarded(ctx PatriciaContext, deferred []*DeferredBranchUpdate, numWorkers int) error {
	pending := make([]*DeferredBranchUpdate, 0, len(deferred))
	seen := make(map[string]struct{}, len(deferred))
	flush := func() error {
		if len(pending) == 0 {
			return nil
		}
		if _, err := ApplyDeferredBranchUpdates(pending, numWorkers, ctx.PutBranch); err != nil {
			return err
		}
		pending = pending[:0]
		clear(seen)
		return nil
	}
	for _, upd := range deferred {
		if upd == nil {
			continue
		}
		key := string(upd.prefix)
		if _, dup := seen[key]; dup {
			if err := flush(); err != nil {
				return err
			}
		}
		prev, _, err := ctx.Branch(upd.prefix)
		if err != nil {
			return err
		}
		upd.prev = common.Copy(prev)
		pending = append(pending, upd)
		seen[key] = struct{}{}
	}
	return flush()
}

// Reset clears per-split state, the prefix trie, and any staged deferred updates
// so the committer can be reused for the next block. The worker pool is dropped.
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
	if sc.pph != nil {
		sc.pph.Reset()
	}
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

// Release drops all owned state. After Release the committer must not be used.
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
	if sc.pph != nil {
		sc.pph.Release()
		sc.pph = nil
	}
	sc.resetPool()
}
