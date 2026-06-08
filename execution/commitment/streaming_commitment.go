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
	prefix         []byte
	cell           cell
	deferred       []*DeferredBranchUpdate
	gen            uint64
	refolds        uint64
	keyCount       uint64 // touches routed to this split (size proxy for the coalescing gate)
	lastFoldedSize uint64 // keyCount at the last (eager) fold; 0 until first fold
	dirty          bool
	folded         bool
	queued         bool
	mu             sync.Mutex
}

// reusable reports whether a background fold cached an authoritative cell that
// has not been invalidated by a later touch — Process stitches it directly
// instead of re-folding. Callers must hold s.mu.
func (s *splitState) reusable() bool { return s.folded && !s.dirty }

// defaultEagerFold is the floor below which a split is not eagerly
// background-folded; tiny splits just fold at Process, avoiding micro-fold overhead.
const defaultEagerFold = 256

// shouldEagerFold is the re-fold coalescing gate. A dirtied split is eligible for
// an eager background fold only once its touched-key count has at least doubled
// since its last fold (and cleared the floor). This forces geometric growth in
// fold sizes — ~log2(N) folds, O(N) total re-fold work — instead of the O(N^2)
// thrash that fold-on-every-touch causes for whale accounts whose storage streams
// in. Process always folds the final state regardless, so correctness is
// unaffected; only wasted eager work changes. Callers hold s.mu.
func (sc *StreamingCommitter) shouldEagerFold(s *splitState) bool {
	return s.keyCount >= sc.eagerFloor && s.keyCount >= 2*s.lastFoldedSize
}

// SetEagerFold overrides the coalescing floor (default defaultEagerFold). Test
// seam: a low floor makes small-corpus scheduler tests exercise eager folds.
func (sc *StreamingCommitter) SetEagerFold(n uint64) { sc.eagerFloor = n }

// SetNestedCache toggles the nested storage sub-cell cache (default on). The
// apples-to-apples bench flips it off to compare gate-only streaming against the
// cache-backed path; off makes a big-storage account fold its whole storage fresh.
func (sc *StreamingCommitter) SetNestedCache(on bool) { sc.nestedCacheOn = on }

// shouldFoldNibble is the per-nibble analogue of shouldEagerFold: a cached
// account's storage nibble is eligible for an eager re-fold once its touched-key
// count has at least doubled since its last fold and cleared the floor.
func (sc *StreamingCommitter) shouldFoldNibble(n *cacheNibble) bool {
	return n.keyCount >= sc.eagerFloor && n.keyCount >= 2*n.lastFoldedSize
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
	eagerFloor uint64

	// Nested storage sub-cell cache (streaming-only). caches holds promoted
	// big-storage accounts and survives endBlock for cross-block incremental
	// re-folds; accTouch accumulates per-block storage stats for accounts not yet
	// promoted. Both are guarded by trieMu. nestedCacheOn is the master switch;
	// nestedCap bounds how many accounts are cached before the cache-free fallback.
	caches        map[string]*accountStorageCache
	accTouch      map[string]*accStorageTouch
	nestedCacheOn bool
	nestedCap     int

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

	// deepLocalFolds counts big-storage accounts folded through the
	// streaming-local deep walk (dfsDeepLocal) — a test seam proving the
	// streaming path no longer routes through parallel_mount.go's deep fan-out.
	deepLocalFolds atomic.Uint64

	// nibbleFolds counts per-nibble storage re-folds the nested cache performed —
	// a test seam proving an incremental block re-folds only the changed nibbles.
	nibbleFolds atomic.Uint64

	// bgDeepFolds counts background folds that routed a cache-containing split
	// through the cache-aware deep walk — a test seam proving the background path
	// shares the cache rather than re-streaming a cached account's storage.
	bgDeepFolds atomic.Uint64

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
		eagerFloor:     defaultEagerFold,
		caches:         make(map[string]*accountStorageCache),
		accTouch:       make(map[string]*accStorageTouch),
		nestedCacheOn:  true,
		nestedCap:      defaultNestedCap,
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
	if sc.nestedCacheOn && len(hashedKey) == storageKeyNibbles && sc.routeCachedStorage(hashedKey, plainKey) {
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
	s.keyCount++
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

// routeCachedStorage handles a storage-slot touch under the nested cache. For a
// promoted account it marks the slot's first-storage-nibble dirty and reports
// true so TouchKey skips the top-nibble split (the cache, not the split gate,
// owns this account's storage). For an account not yet promoted it accumulates
// the per-block stats and promotes once they cross the deep walk's condition
// (and the cap allows it); until promotion it returns false so the slot still
// streams through the split. plainKey is the storage slot's un-hashed key
// (account address + location); its leading accountKeyLen bytes are the account
// key the cache keeps to re-load the leaf from ctx when only storage changes.
// Callers hold sc.trieMu.
func (sc *StreamingCommitter) routeCachedStorage(hashedKey, plainKey []byte) bool {
	accPrefix := string(hashedKey[:accountKeyNibbles])
	nib := hashedKey[accountKeyNibbles]
	if c := sc.caches[accPrefix]; c != nil {
		c.touchNibble(nib)
		// A later cached-storage touch dirties the cache nibble but not the split's
		// key stream; clear the split's reusable flag so Process re-folds it through
		// the cache instead of serving a background cell folded before this touch.
		sc.invalidateSplitReuse(hashedKey[0])
		return true
	}
	t := sc.accTouch[accPrefix]
	if t == nil {
		t = &accStorageTouch{}
		sc.accTouch[accPrefix] = t
	}
	if t.capped {
		return false
	}
	t.slots++
	t.nibbleMask |= uint16(1) << nib
	if !t.qualifies() {
		return false
	}
	if len(sc.caches) >= sc.nestedCap {
		t.capped = true
		return false
	}
	c := newPromotedCache(append([]byte(nil), hashedKey[:accountKeyNibbles]...), accountKeyOf(plainKey, sc.accountKeyLen), t.nibbleMask)
	c.touchNibble(nib)
	sc.caches[accPrefix] = c
	delete(sc.accTouch, accPrefix)
	// The account's pre-promotion storage streamed through the split, so a
	// background fold may already have cached a split cell that streamed a partial
	// storage trie. Invalidate it: dirty the split (so Process re-folds it through
	// the cache) and bump its gen (so an in-flight background fold is discarded).
	sc.dirtyPromotedSplit(hashedKey[0])
	return true
}

// invalidateSplitReuse clears a split's reusable flag (without touching the gate
// counters or gen) so Process re-folds it through the cache. A no-op when the
// split has not been created. Callers hold sc.trieMu.
func (sc *StreamingCommitter) invalidateSplitReuse(nib byte) {
	s := sc.splits[nib]
	if s == nil {
		return
	}
	s.mu.Lock()
	s.dirty = true
	s.mu.Unlock()
}

// dirtyPromotedSplit marks the owning top-nibble split dirty and bumps its gen so
// a stale (pre-promotion, storage-streamed) cell can neither be reused by Process
// nor installed by an in-flight background fold. Callers hold sc.trieMu.
func (sc *StreamingCommitter) dirtyPromotedSplit(nib byte) {
	s := sc.splits[nib]
	if s == nil {
		s = &splitState{prefix: []byte{nib}}
		sc.splits[nib] = s
	}
	s.mu.Lock()
	s.dirty = true
	s.folded = false
	s.gen++
	s.mu.Unlock()
}

// accountKeyOf returns a stable copy of the account key embedded in a storage
// plain key (its leading accountKeyLen bytes), or nil if plainKey is too short.
func accountKeyOf(plainKey []byte, accountKeyLen int16) []byte {
	if int16(len(plainKey)) < accountKeyLen {
		return nil
	}
	return append([]byte(nil), plainKey[:accountKeyLen]...)
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
	rh, err := base.RootHash()
	if err != nil {
		return nil, err
	}
	sc.endBlock()
	return rh, nil
}

// endBlock drains the per-block touch funnel (prefix trie + split state) so a
// reused committer folds only the next block's touches; the scheduler base and
// worker pool survive, and the caller's staged root/deferred snapshots are kept.
func (sc *StreamingCommitter) endBlock() {
	if sc.trie != nil {
		sc.trie.Reset()
	}
	sc.dropSplitDeferred()
	clear(sc.splits)
	clear(sc.accTouch)
	for k, c := range sc.caches {
		if c.invalid {
			delete(sc.caches, k)
		}
	}
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
	// A split that owns a cached big-storage account must fold through the
	// cache-aware deep walk (the same one Process uses), not the flat key replay
	// that would re-stream the account's storage. The deep fold runs under the
	// read lock held from the key snapshot through the install CAS, so no touch
	// (which needs the write lock) can land between snapshot and CAS — the install
	// is atomic w.r.t. the touch stream and can never publish a stale cell.
	if sc.splitHasCache(nib) {
		sc.foldSplitBgCached(nib, s, child)
		sc.trieMu.RUnlock()
		return
	}

	keys := collectSplitKeys(child, nib)
	s.mu.Lock()
	genStart := s.gen
	// Close the coalescing gate at fold START (snapshot size), not end: the fold
	// can take long, and queued is cleared here, so a stale lastFoldedSize would
	// keep the gate open for the whole fold and let every mid-fold touch re-enqueue.
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
		s.refolds++
		sc.refoldTotal.Add(1)
		// Only re-fold a mid-fold-touched split if the coalescing gate still
		// passes (it doubled again); otherwise leave it dirty for Process. This
		// is what bounds a streaming whale to O(N) instead of O(N^2) re-folds.
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

// splitHasCache reports whether any promoted big-storage account lives under the
// top-nibble split nib (its account hash, hence its storage keys, lead with nib).
// Callers hold sc.trieMu.
func (sc *StreamingCommitter) splitHasCache(nib byte) bool {
	if !sc.nestedCacheOn {
		return false
	}
	for _, c := range sc.caches {
		if !c.invalid && len(c.prefix) > 0 && c.prefix[0] == nib {
			return true
		}
	}
	return false
}

// foldSplitBgCached folds a split that owns a cached account through the
// cache-aware deep walk, installing the result. The caller holds sc.trieMu.RLock
// across this whole call, so s.gen cannot move under it: a touch would need the
// write lock, so the snapshot→fold→install sequence is atomic against the touch
// stream and the cache is the single storageRoot source shared with Process.
func (sc *StreamingCommitter) foldSplitBgCached(nib byte, s *splitState, child *prefixNode) {
	s.mu.Lock()
	s.lastFoldedSize = s.keyCount
	s.queued = false
	s.mu.Unlock()

	c, deferred, flushed, err := sc.foldKeysDeep(nib, child)

	s.mu.Lock()
	if err != nil || flushed {
		for _, upd := range deferred {
			putDeferredUpdate(upd)
		}
		s.refolds++
		sc.refoldTotal.Add(1)
		s.mu.Unlock()
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
	sc.bgDeepFolds.Add(1)
}

// foldKeysDeep is the cache-aware analogue of foldKeys for a split that owns a
// cached account: it walks the live subtree through dfsDeepLocal (which injects a
// cached account's storageRoot from the cache instead of streaming its storage)
// against an isolating overlay, then folds to the split cell. The caller holds
// sc.trieMu.RLock, so the walk and the cache reads it drives are race-free.
func (sc *StreamingCommitter) foldKeysDeep(nib byte, child *prefixNode) (cell, []*DeferredBranchUpdate, bool, error) {
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

	var pu parallelUpdate
	path := make([]byte, 0, 144)
	path = append(path, nib)
	path = append(path, child.ext...)
	err := sc.dfsDeepLocal(sc.bgCtx, w, &pu, child, path)
	var c cell
	if err == nil {
		c, err = w.foldMounted(sc.bgCtx, int(nib))
	}
	deferred := pu.deferredCombined
	if d := w.TakeDeferredUpdates(); len(d) > 0 {
		deferred = append(deferred, d...)
	}
	w.resetForReuse()
	sc.workerPool.Put(w)
	return c, deferred, ov.flushed, err
}

// BgDeepFolds reports how many background folds routed a cache-containing split
// through the cache-aware deep walk — a test seam proving the background and
// Process paths share the nested cache.
func (sc *StreamingCommitter) BgDeepFolds() uint64 { return sc.bgDeepFolds.Load() }

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
// order via the streaming-local deep walk (a big-storage account's storage fans
// out concurrently through storageRootLocal), fold to the split cell — never to the root —
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
	if err := sc.dfsDeepLocal(ctx, w, &pu, child, path); err != nil {
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

// DeepLocalFolds reports how many big-storage accounts the streaming-local deep
// walk folded — a test seam proving the deep path no longer calls parallel_mount.
func (sc *StreamingCommitter) DeepLocalFolds() uint64 { return sc.deepLocalFolds.Load() }

// NibbleFolds reports how many per-nibble storage re-folds the nested cache ran
// — a test seam proving an incremental block re-folds only its changed nibbles.
func (sc *StreamingCommitter) NibbleFolds() uint64 { return sc.nibbleFolds.Load() }

// dfsDeepLocal is the streaming-local copy of ParallelPatriciaHashed.dfsSubtreeDeep:
// it walks a mount worker's subtree and, at a big-storage account, folds that
// account's storage and injects the resulting storageRoot into the account leaf
// rather than streaming the storage. A promoted (cached) account always folds
// through the nested cache (foldStorageRootCached re-folds only its dirty
// nibbles); an uncached account whose touched storage crosses deepStorageThreshold
// folds the whole storage fresh via storageRootLocal. parallel_mount.go is
// untouched.
func (sc *StreamingCommitter) dfsDeepLocal(ctx context.Context, w *HexPatriciaHashed, pu *parallelUpdate, node *prefixNode, path []byte) error {
	if node == nil {
		return nil
	}
	if node.plainKey != nil {
		if err := w.followAndUpdate(path, node.plainKey, node.update); err != nil {
			return err
		}
	} else if node.bitmap == 0 {
		return errors.New("StreamingCommitter: trie leaf without a plainKey")
	}

	// A cached account whose storage was touched in a single first-storage-nibble
	// has no node at depth 64 — trie compression merges the account pass-through
	// into the storage subtree's extension, so dfsDeepLocal enters with path
	// already past 64. Route it through the cache (the account leaf is reloaded
	// from ctx) rather than folding the storage inline against the deep-written
	// on-disk subtree, which is not byte-compatible with a sequential fold.
	if len(path) > accountKeyNibbles {
		if cache := sc.cacheFor(path[:accountKeyNibbles]); cache != nil {
			accPath := path[:accountKeyNibbles]
			if err := w.followAndUpdate(accPath, cache.accPlainKey, nil); err != nil {
				return err
			}
			sr, err := sc.storageRootCachedNibble(pu, cache, node, path)
			if err != nil {
				return fmt.Errorf("storageRootCachedNibble: %w", err)
			}
			setAccountStorageRoot(w, accPath, sr)
			sc.deepLocalFolds.Add(1)
			return nil
		}
	}

	if len(path) == 64 {
		if cache := sc.cacheFor(path); cache != nil {
			if node.plainKey == nil {
				if err := w.followAndUpdate(path, cache.accPlainKey, nil); err != nil {
					return err
				}
			}
			sr, err := sc.storageRootCached(pu, cache, node, path)
			if err != nil {
				return fmt.Errorf("storageRootCached: %w", err)
			}
			setAccountStorageRoot(w, path, sr)
			sc.deepLocalFolds.Add(1)
			return nil
		}
		if node.plainKey != nil && bits.OnesCount16(node.bitmap) >= 2 &&
			node.subtreeCount > deepStorageThreshold {
			sr, err := sc.storageRootLocal(pu, node, path)
			if err != nil {
				return fmt.Errorf("storageRootLocal: %w", err)
			}
			setAccountStorageRoot(w, path, sr)
			sc.deepLocalFolds.Add(1)
			return nil
		}
	}

	childIdx := 0
	for bm := node.bitmap; bm != 0; {
		nib := byte(bits.TrailingZeros16(bm))
		child := node.children[childIdx]
		base := len(path)
		path = append(path, nib)
		path = append(path, child.ext...)
		if err := sc.dfsDeepLocal(ctx, w, pu, child, path); err != nil {
			return err
		}
		path = path[:base]
		childIdx++
		bm &^= uint16(1) << nib
	}
	return nil
}

// cacheFor returns the nested storage cache for the account at accHash (a
// 64-nibble path), or nil if the account is not cached or caching is off.
func (sc *StreamingCommitter) cacheFor(accHash []byte) *accountStorageCache {
	if !sc.nestedCacheOn {
		return nil
	}
	c := sc.caches[string(accHash)]
	if c == nil || c.invalid {
		return nil
	}
	return c
}

// storageRootCached re-folds the cached account's dirty storage nibbles (reusing
// its clean cached cells), aggregates them into the storage branch, and returns
// the account's storageRoot. It first folds the current block's touched slots
// (which the trie carries) into the cache's accumulated per-nibble slot set, so a
// dirty nibble re-folds from its FULL slot set — the deep fan-out rebuilds a
// nibble from keys alone and cannot read the on-disk subtree, so the cache is the
// authoritative key source across blocks. Deferred branch updates land in pu.
func (sc *StreamingCommitter) storageRootCached(pu *parallelUpdate, cache *accountStorageCache, node *prefixNode, path []byte) (common.Hash, error) {
	childIdx := 0
	for bm := node.bitmap; bm != 0; {
		nib := int(bits.TrailingZeros16(bm))
		child := node.children[childIdx]
		cpath := make([]byte, len(path), len(path)+1+len(child.ext))
		copy(cpath, path)
		cpath = append(cpath, byte(nib))
		cpath = append(cpath, child.ext...)
		for _, tk := range collectStorageNibbleKeys(child, cpath) {
			cache.retain(nib, tk)
		}
		childIdx++
		bm &^= uint16(1) << nib
	}
	return sc.foldCachedStorageRoot(pu, cache, int(path[63]))
}

// storageRootCachedNibble re-folds a cached account whose storage compressed past
// depth 64: node is the subtree of the single touched first-storage-nibble
// (path[64]). It retains that nibble's slots into the cache, marks it dirty, then
// re-folds through the cache exactly like storageRootCached.
func (sc *StreamingCommitter) storageRootCachedNibble(pu *parallelUpdate, cache *accountStorageCache, node *prefixNode, path []byte) (common.Hash, error) {
	nib := int(path[accountKeyNibbles])
	for _, tk := range collectStorageNibbleKeys(node, path) {
		cache.retain(nib, tk)
	}
	cache.perNibble[nib].dirty = true
	return sc.foldCachedStorageRoot(pu, cache, int(path[63]))
}

// foldCachedStorageRoot builds the per-nibble key groups from the cache's
// accumulated slot sets and re-folds only the dirty nibbles, returning the
// account's storageRoot hash. Deferred branch updates land in pu.
func (sc *StreamingCommitter) foldCachedStorageRoot(pu *parallelUpdate, cache *accountStorageCache, accNib int) (common.Hash, error) {
	var groups [16][]touchedKey
	for x := range 16 {
		groups[x] = cache.sortedKeys(x)
	}
	sr, folded, flushed, err := foldStorageRootCached(sc.newIsolatedStorageWorker, cache, accNib, &groups, pu.appendDeferred)
	if err != nil {
		return common.Hash{}, err
	}
	sc.nibbleFolds.Add(uint64(folded))
	// A self-flush means a nibble's subtree collapsed mid-fold; the storageRoot
	// stays correct (folded self-consistently against the overlay) but the cached
	// cells span a structural change, so drop the cache at endBlock and let the
	// account re-promote fresh.
	if flushed {
		cache.invalid = true
	}
	return sr.hash, nil
}

// storageRootLocal folds each first-storage-nibble subtree of one account in its
// own pooled worker, aggregates the depth-65 child cells into the storage branch,
// and returns its hash (the account's storageRoot). Streaming-local copy of
// concurrentStorageRoot reusing the Task-1 per-nibble fold + assembler; path is
// the 64-nibble account hash.
func (sc *StreamingCommitter) storageRootLocal(pu *parallelUpdate, node *prefixNode, path []byte) (common.Hash, error) {
	accNib := int(path[63])
	var children [16]cell
	var present uint16

	var g errgroup.Group
	g.SetLimit(sc.numWorkers)
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
			group := collectStorageNibbleKeys(ch, cp)
			w, release, _ := sc.newStorageWorker()
			c, err := foldStorageChildCell(w, accNib, group)
			if err != nil {
				release()
				return fmt.Errorf("storage mount[%x] build: %w", ni, err)
			}
			if deferred := w.TakeDeferredUpdates(); len(deferred) > 0 {
				pu.appendDeferred(deferred)
			}
			children[ni] = c
			release()
			return nil
		})
		childIdx++
		bm &^= uint16(1) << nib
	}
	if err := g.Wait(); err != nil {
		return common.Hash{}, err
	}

	w, release, _ := sc.newStorageWorker()
	sr, err := aggregateStorageRoot(w, path, accNib, &children, present)
	if err != nil {
		release()
		return common.Hash{}, fmt.Errorf("storage branch fold: %w", err)
	}
	if deferred := w.TakeDeferredUpdates(); len(deferred) > 0 {
		pu.appendDeferred(deferred)
	}
	release()
	return sr.hash, nil
}

// newStorageWorker yields a pooled trie worker set up for a deferring storage
// fold and a release that drains it back to the pool (resetForReuse + Put) and
// frees its context — the storageWorkerFactory the cache fold helpers consume.
func (sc *StreamingCommitter) newStorageWorker() (*HexPatriciaHashed, func(), func() bool) {
	w := sc.workerPool.Get().(*HexPatriciaHashed)
	wctx, cleanup := sc.trieCtxFactory()
	w.ResetContext(wctx)
	w.SetTrace(sc.trace)
	w.branchEncoder.setDeferUpdates(true)
	w.SetLeaveDeferredForCaller(true)
	return w, func() {
		w.resetForReuse()
		sc.workerPool.Put(w)
		if cleanup != nil {
			cleanup()
		}
	}, nil
}

// newIsolatedStorageWorker is the cache path's worker: its writes go to an
// overlay so a mid-fold self-flush (a delete-driven collapse re-reading a pending
// prefix) can never mutate the real store during a speculative cache re-fold. The
// returned flushed accessor reports whether that happened, so the caller can
// invalidate the cache rather than trust a cell folded across a collapse.
func (sc *StreamingCommitter) newIsolatedStorageWorker() (*HexPatriciaHashed, func(), func() bool) {
	w := sc.workerPool.Get().(*HexPatriciaHashed)
	wctx, cleanup := sc.trieCtxFactory()
	ov := &overlayContext{base: wctx}
	w.ResetContext(ov)
	w.SetTrace(sc.trace)
	w.branchEncoder.setDeferUpdates(true)
	w.SetLeaveDeferredForCaller(true)
	return w, func() {
		w.resetForReuse()
		sc.workerPool.Put(w)
		if cleanup != nil {
			cleanup()
		}
	}, func() bool { return ov.flushed }
}

// collectStorageNibbleKeys walks one first-storage-nibble subtree in sorted order,
// copying each key's hashed nibbles off the reused walk path; plainKey/update stay
// referenced. The fold replays them in this (dfsSubtree) order, matching
// concurrentStorageRoot's live walk byte-for-byte.
func collectStorageNibbleKeys(node *prefixNode, path []byte) []touchedKey {
	out := make([]touchedKey, 0, node.subtreeCount)
	_ = dfsSubtree(node, path, func(hk, pk []byte, upd *Update) error {
		out = append(out, touchedKey{hk: append([]byte(nil), hk...), pk: pk, upd: upd})
		return nil
	})
	return out
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

// applyDeferredGuarded applies deferred branch updates, collapsing any prefix
// emitted by more than one fold set (a per-split set colliding with the merge's
// bottom row). The apply context may be write-only — the concurrent factory
// routes PutBranch to a drain-later collector that Branch cannot read — so a
// colliding prefix's later update is merged against the earlier update's encoded
// result held in an in-memory overlay, not a re-read of ctx. Bare
// ApplyDeferredBranchUpdates does not dedup, so the collision-free slice (the
// common case) takes its parallel fast path.
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
	clear(sc.caches)
	clear(sc.accTouch)
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

// Release drops all owned state. After Release the committer must not be used.
// Repeat calls are safe no-ops.
func (sc *StreamingCommitter) Release() {
	sc.Stop()
	sc.releaseBase()
	sc.dropSplitDeferred()
	sc.trie = nil
	sc.splits = nil
	sc.caches = nil
	sc.accTouch = nil
	for _, upd := range sc.deferredForCaller {
		putDeferredUpdate(upd)
	}
	sc.deferredForCaller = nil
	sc.resetPool()
}
