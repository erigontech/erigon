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
	"math/bits"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"

	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon/db/etl"
)

// ParallelPatriciaHashed is the trie-side of the parallel commitment pipeline.
// It owns:
//
//   - a template *HexPatriciaHashed used ONLY to expose ctx/cache/metrics/trace
//     configuration to callers. Workers never write to it and it does not hold
//     live root state during Process.
//   - a worker pool of fresh *HexPatriciaHashed instances acquired per leafTask
//     in Process.
//   - a TrieContextFactory that yields per-worker PatriciaContext instances so
//     DB reads run concurrently.
//   - an atomic root-hash pointer published at the end of Process.
type ParallelPatriciaHashed struct {
	template       *HexPatriciaHashed
	trieCtxFactory TrieContextFactory
	workerPool     sync.Pool
	cfg            TrieConfig

	accountKeyLen int16
	numWorkers    int
	minSplitKeys  uint32

	// rootHash holds the bytes published by the last-finisher of the topmost
	// split-point. Nil until Process completes successfully.
	rootHash atomic.Pointer[[]byte]

	// pendingRoot stages the worker-published root (hash + cell) until the
	// main Process goroutine has confirmed applyDeferredUpdates succeeded.
	// Promoting to rootHash/template.root before then would leave the trie
	// caching a root that was never committed to the DB if the deferred
	// branch apply fails.
	pendingRoot atomic.Pointer[publishedRoot]
}

// publishedRoot bundles the candidate root hash with the worker's final root
// cell so both can be promoted (or discarded) atomically after the deferred
// branch apply completes. rootChecked/rootTouched/rootPresent mirror the
// worker's terminal flag state — they are serialized by EncodeCurrentState
// and consumed by SetState/unfold on a restore-then-continue flow.
type publishedRoot struct {
	hash        []byte
	cell        cell
	rootChecked bool
	rootTouched bool
	rootPresent bool
}

// NewParallelPatriciaHashed constructs a fresh ParallelPatriciaHashed. The
// returned instance is usable for configuration immediately; Process requires
// a non-nil ctxFactory.
func NewParallelPatriciaHashed(ctxFactory TrieContextFactory, accountKeyLen int16, cfg TrieConfig) *ParallelPatriciaHashed {
	p := &ParallelPatriciaHashed{
		template:       NewHexPatriciaHashed(accountKeyLen, nil, cfg),
		trieCtxFactory: ctxFactory,
		accountKeyLen:  accountKeyLen,
		cfg:            cfg,
		numWorkers:     runtime.NumCPU(),
		minSplitKeys:   MinSplitKeys,
	}
	p.resetPool()
	return p
}

// resetPool replaces workerPool with a fresh sync.Pool. Used by Reset and
// Release to drop any pooled workers that were configured for a prior run.
func (p *ParallelPatriciaHashed) resetPool() {
	akl := p.accountKeyLen
	cfg := p.cfg
	p.workerPool = sync.Pool{
		New: func() any {
			return NewHexPatriciaHashed(akl, nil, cfg)
		},
	}
}

// SetNumWorkers overrides the worker count for the next Process call. Values
// <= 0 fall back to runtime.NumCPU.
func (p *ParallelPatriciaHashed) SetNumWorkers(n int) {
	if n <= 0 {
		n = runtime.NumCPU()
	}
	p.numWorkers = n
}

// SetMinSplitKeys overrides the per-batch split-point threshold. Workers below
// this subtree size are collapsed into a single leafTask in Prepare.
func (p *ParallelPatriciaHashed) SetMinSplitKeys(n uint32) {
	if n == 0 {
		n = MinSplitKeys
	}
	p.minSplitKeys = n
}

// RootTrie exposes the configuration template. Callers must NOT use it as
// live root state; it carries ctx/cache/metrics/trace settings only.
func (p *ParallelPatriciaHashed) RootTrie() *HexPatriciaHashed {
	return p.template
}

// Reset clears the published root hash and drops any pooled workers from the
// previous run. The template's mutable trie state is also reset so callers can
// reuse the instance for another batch.
func (p *ParallelPatriciaHashed) Reset() {
	if p.template != nil {
		p.template.Reset()
	}
	p.rootHash.Store(nil)
	p.pendingRoot.Store(nil)
	p.resetPool()
}

// Release returns the template to its pool, drops the worker pool, and clears
// the published root hash. After Release the instance must not be used.
// Repeat calls are safe no-ops.
func (p *ParallelPatriciaHashed) Release() {
	if p.template != nil {
		p.template.Release()
		p.template = nil
	}
	p.rootHash.Store(nil)
	p.pendingRoot.Store(nil)
	p.resetPool()
}

// ResetContext propagates a new PatriciaContext to the template. Per-worker
// contexts are produced from trieCtxFactory by Process.
func (p *ParallelPatriciaHashed) ResetContext(ctx PatriciaContext) {
	if p.template != nil {
		p.template.ResetContext(ctx)
	}
}

// SetTrieContextFactory replaces the per-worker context factory. Useful for
// tests that swap in a mock factory.
func (p *ParallelPatriciaHashed) SetTrieContextFactory(f TrieContextFactory) {
	p.trieCtxFactory = f
}

func (p *ParallelPatriciaHashed) SetTrace(b bool) {
	if p.template != nil {
		p.template.SetTrace(b)
	}
}

func (p *ParallelPatriciaHashed) SetTraceDomain(b bool) {
	if p.template != nil {
		p.template.SetTraceDomain(b)
	}
}

func (p *ParallelPatriciaHashed) EnableWarmupCache(b bool) {
	if p.template != nil {
		p.template.EnableWarmupCache(b)
	}
}

func (p *ParallelPatriciaHashed) GetCapture(truncate bool) []string {
	if p.template == nil {
		return nil
	}
	return p.template.GetCapture(truncate)
}

func (p *ParallelPatriciaHashed) SetCapture(capture []string) {
	if p.template != nil {
		p.template.SetCapture(capture)
	}
}

func (p *ParallelPatriciaHashed) EnableCsvMetrics(filePathPrefix string) {
	if p.template != nil {
		p.template.EnableCsvMetrics(filePathPrefix)
	}
}

func (p *ParallelPatriciaHashed) Variant() TrieVariant { return VariantParallelHexPatricia }

// RootHash returns the root hash published by Process. If Process has not
// completed (or no updates were applied), it falls back to the template's
// current root — this matches the "no updates" path on a fresh trie.
func (p *ParallelPatriciaHashed) RootHash() ([]byte, error) {
	if r := p.rootHash.Load(); r != nil {
		src := *r
		out := make([]byte, len(src))
		copy(out, src)
		return out, nil
	}
	if p.template == nil {
		return nil, nil
	}
	return p.template.RootHash()
}

// Process is the entry point for parallel commitment computation. It expects
// updates.mode == ModeParallel with a populated parallelUpdate.
//
// Each worker folds its subtree, deposits a cell at every ancestor split-point
// it crosses, and either exits (a sibling still has work) or — as the last
// sibling to arrive — continues folding upward through the merged grid; the
// topmost finisher computes the root hash. A multi-bucket batch with no
// split-point is rejected: independent workers would each fold to root with
// only their own bucket touched and produce inconsistent answers.
func (p *ParallelPatriciaHashed) Process(
	ctx context.Context,
	updates *Updates,
	logPrefix string,
	onProgress func(*CommitProgress),
	warmup WarmupConfig,
) (rootHash []byte, err error) {
	if updates == nil || updates.mode != ModeParallel || updates.parallel == nil {
		return nil, errors.New("ParallelPatriciaHashed.Process requires Updates in ModeParallel")
	}
	if p.trieCtxFactory == nil {
		return nil, errors.New("ParallelPatriciaHashed.Process requires a TrieContextFactory")
	}
	if p.template == nil {
		return nil, errors.New("ParallelPatriciaHashed.Process called after Release")
	}

	p.rootHash.Store(nil)
	p.pendingRoot.Store(nil)

	pu := updates.parallel
	// Empty update set: no touches occurred. Fall back to the template's
	// existing root; matches the sequential no-op path.
	if pu.trie == nil || pu.trie.root == nil || pu.trie.root.subtreeCount == 0 {
		rh, rerr := p.template.RootHash()
		if rerr != nil {
			return nil, rerr
		}
		return rh, nil
	}

	// Warmup setup mirrors HexPatriciaHashed.Process: it is optional and the
	// trie functions without it. The warmuper threads its own context internally.
	// When the warmup cache is enabled, expose it on the template so every
	// worker inherits it during runNibbleBucket — otherwise the warmuper's
	// prefetches are wasted and workers re-read every branch from the DB.
	var warmuper *Warmuper
	if warmup.Enabled {
		if warmup.CtxFactory == nil {
			warmup.CtxFactory = p.trieCtxFactory
		}
		warmup.EnableWarmupCache = p.template.enableWarmupCache
		warmuper = NewWarmuper(ctx, warmup)
		warmuper.Start()
		defer warmuper.CloseAndWait()
		if warmup.EnableWarmupCache {
			p.template.cache = warmuper.Cache()
			defer func() { p.template.cache = nil }()
		}
	}

	// Prepare: DFS the prefix trie to emit split-points and leaf tasks. We use
	// the template's PatriciaContext (production callers always set it via
	// ResetContext) or, if absent, a transient context from the factory.
	prepareCtx, prepareCleanup := p.acquirePrepareContext()
	if prepareCleanup != nil {
		defer prepareCleanup()
	}
	pu.minSplitKeys = p.minSplitKeys
	if err := pu.Prepare(prepareCtx); err != nil {
		return nil, fmt.Errorf("[%s] parallel commitment prepare: %w", logPrefix, err)
	}

	// Reject multi-bucket no-split configurations: without any split-point,
	// independent workers would each fold to root with only their own bucket
	// touched, producing M mutually-inconsistent root states. The barrier
	// protocol relies on at least one shared split-point to merge them.
	if len(pu.splitPoints) == 0 && len(pu.leafQueue) > 1 {
		return nil, fmt.Errorf("[%s] ParallelPatriciaHashed: %d leafTasks emerged with no split-points; multi-bucket merging without a shared split-point is not supported", logPrefix, len(pu.leafQueue))
	}

	if warmuper != nil {
		p.warmupSplitAncestors(pu, warmuper)
	}

	// Group leafTasks by their root nibble. The ETL collector for a nibble
	// bucket can only be scanned once, so each bucket runs in a single
	// orchestrator goroutine that scans once and dispatches the keys to the
	// (potentially many) workers participating in that bucket.
	tasksByNibble := groupLeafTasksByNibble(pu.leafQueue)
	for _, task := range pu.leafQueue {
		if len(task.prefix) == 0 {
			return nil, errors.New("ParallelPatriciaHashed: leafTask emitted without a routing prefix")
		}
	}

	// foldSem is a global semaphore shared by every active bucket's fold-phase
	// goroutines. Without it, each bucket's inner errgroup would independently
	// cap at p.numWorkers, so the total fold concurrency could grow to
	// p.numWorkers * active_buckets (up to p.numWorkers * 16). With it, the
	// total fold-phase goroutine count across all buckets is bounded by
	// p.numWorkers regardless of how many buckets are active simultaneously.
	foldSem := make(chan struct{}, p.numWorkers)

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(p.numWorkers)

	// Iterate deterministically so failures reproduce regardless of map order.
	for nib := range 16 {
		tasks := tasksByNibble[byte(nib)]
		if len(tasks) == 0 {
			continue
		}
		nib := byte(nib)
		g.Go(func() error {
			return p.runNibbleBucket(gctx, updates, nib, tasks, foldSem, onProgress)
		})
	}

	if waitErr := g.Wait(); waitErr != nil {
		// Workers may have already appended partial deferred updates and one
		// may have staged a pendingRoot via CAS before another sibling failed.
		// Discard both: a retry that reused pu would re-apply stale deferred
		// branches against the previous-attempt prev values, and an
		// intermediate RootHash() read would surface a root that was never
		// persisted. Return pooled entries before truncating so they do not
		// leak from deferredUpdatePool.
		pu.deferredMu.Lock()
		for _, upd := range pu.deferredCombined {
			putDeferredUpdate(upd)
		}
		pu.deferredCombined = pu.deferredCombined[:0]
		pu.deferredMu.Unlock()
		p.pendingRoot.Store(nil)
		return nil, waitErr
	}

	if err := p.applyDeferredUpdates(pu); err != nil {
		// Deferred branch apply failed: discard the staged root so neither
		// p.rootHash nor p.template.root surface a state that was never
		// persisted to the DB. The pending pointer is dropped on the next
		// Process call too, but clearing here guards intermediate RootHash
		// reads between the failure and the next Process invocation.
		p.pendingRoot.Store(nil)
		return nil, err
	}

	// Promote the worker-staged root now that deferred updates are durable.
	// All fields must move together: rootHash drives RootHash() on this
	// instance, and template.root + the three root flags are what
	// RootTrie().EncodeCurrentState serializes for persistence. SetState on
	// restore replays both the cell and the flags; without the flag promotion
	// the restored trie would see rootChecked/rootTouched/rootPresent == false
	// and treat the persisted root cell as "not present" during the next
	// unfold/fold cycle.
	if pending := p.pendingRoot.Swap(nil); pending != nil {
		if p.template != nil {
			p.template.root = pending.cell
			p.template.rootChecked = pending.rootChecked
			p.template.rootTouched = pending.rootTouched
			p.template.rootPresent = pending.rootPresent
		}
		p.rootHash.Store(&pending.hash)
	}

	// The topmost finisher publishes the root hash via rootHash; with a single
	// leafTask and no split-points the lone worker publishes it too.
	if rh := p.rootHash.Load(); rh != nil {
		out := make([]byte, len(*rh))
		copy(out, *rh)
		if warmuper != nil {
			warmuper.DrainPending()
		}
		return out, nil
	}

	// No worker reached activeRows==0. This should be impossible: every batch
	// has at least one leafTask (we returned early on empty Updates), and the
	// barrier protocol guarantees the last-finisher of the topmost split-point
	// folds to root.
	return nil, fmt.Errorf("[%s] ParallelPatriciaHashed: no worker published the root hash", logPrefix)
}

// acquirePrepareContext returns a PatriciaContext to use during Prepare. The
// template's existing ctx (set via ResetContext by production callers) is
// preferred; otherwise a fresh context is borrowed from the factory and
// returned to it by the returned cleanup.
func (p *ParallelPatriciaHashed) acquirePrepareContext() (PatriciaContext, func()) {
	if p.template != nil && p.template.ctx != nil {
		return p.template.ctx, nil
	}
	if p.trieCtxFactory == nil {
		return nil, nil
	}
	return p.trieCtxFactory()
}

// runNibbleBucket processes every leafTask under a single root nibble bucket.
// The ETL collector backing the bucket can only be scanned once, so this
// orchestrator owns the scan and dispatches each key to the matching worker.
// After the scan completes, every worker drains its fold-back path through
// the barrier protocol concurrently (via a sub-errgroup that shares the
// outer worker-count budget through gctx cancellation).
//
// trieCtx lifecycle: one shared trieCtx is acquired for the entire dispatch
// phase (safe because dispatch is single-pass and sequential through fn).
// Each fold goroutine acquires its own trieCtx after passing through foldSem,
// so simultaneous trieCtx (and MDBX reader-slot) usage per bucket is bounded
// by 1 + min(p.numWorkers, len(tasks)) instead of len(tasks).
//
// Lifecycle per worker:
//   - acquire hph from the pool, bind it to the shared dispatch ctx, enable
//     deferred branch updates;
//   - receive its share of the bucket's keys via followAndUpdate during the
//     scan dispatch;
//   - inside the fold goroutine, swap in a fresh per-worker trieCtx and fold
//     its grid through every ancestor split-point via foldDrainWithBarrier,
//     either exiting at a barrier or — as the topmost finisher — publishing
//     the root hash;
//   - on return, the worker's deferred updates have been folded into
//     pu.deferredCombined and the worker has been put back into the pool;
//     the per-worker trieCtx is released via its cleanup defer.
//
// Errors from either the dispatch or any worker's drain cancel the whole
// bucket via gctx.
func (p *ParallelPatriciaHashed) runNibbleBucket(
	ctx context.Context,
	updates *Updates,
	nib byte,
	tasks []leafTask,
	foldSem chan struct{},
	_ func(*CommitProgress),
) (retErr error) {
	if len(tasks) == 0 {
		return nil
	}

	collector := updates.nibbles[nib]
	if collector == nil {
		return fmt.Errorf("ParallelPatriciaHashed: nibbles[%x] collector is nil", nib)
	}

	workers := make([]*HexPatriciaHashed, len(tasks))
	// Defensive cleanup on early-return paths. Successful runs hand each
	// worker off to foldDrainWithBarrier which is responsible for returning
	// it to the pool; we set workers[i]=nil before handoff so this loop
	// becomes a no-op for handed-off slots.
	defer func() {
		if retErr == nil {
			return
		}
		for _, hph := range workers {
			if hph == nil {
				continue
			}
			hph.resetForReuse()
			p.workerPool.Put(hph)
		}
	}()

	// Dispatch is single-pass and sequential through fn, so only one hph reads
	// from the DB at any moment. Share a single trieCtx across every hph for
	// dispatch; per-worker trieCtxes are acquired lazily inside the fold
	// goroutines (after foldSem) and released when the fold returns. This
	// bounds simultaneous trieCtx (and MDBX reader-slot) usage per bucket to
	// 1 + min(p.numWorkers, len(tasks)) instead of len(tasks).
	dispatchCtx, dispatchCleanup := p.trieCtxFactory()
	dispatchReleased := false
	releaseDispatchCtx := func() {
		if dispatchReleased {
			return
		}
		dispatchReleased = true
		if dispatchCleanup != nil {
			dispatchCleanup()
		}
	}
	defer releaseDispatchCtx()

	for i := range tasks {
		hph := p.workerPool.Get().(*HexPatriciaHashed)
		hph.resetForReuse()
		hph.ResetContext(dispatchCtx)
		hph.branchEncoder.setDeferUpdates(true)
		hph.SetLeaveDeferredForCaller(true)

		if p.template != nil {
			hph.trace = p.template.trace
			hph.traceDomain = p.template.traceDomain
			hph.enableWarmupCache = p.template.enableWarmupCache
			if p.template.cache != nil {
				hph.cache = p.template.cache
				hph.branchEncoder.SetCache(p.template.cache)
			}
		}

		workers[i] = hph
	}

	// Single scan of the bucket. dispatchLeafKeys silently skips keys with no
	// matching prefix; this is a defensive guard since every key Touch-Plain-Key
	// routed through Updates landed in the bucket because some leafTask under
	// it claimed it.
	if err := dispatchLeafKeys(ctx, collector, tasks, func(idx int, hk, pk []byte) error {
		if err := workers[idx].followAndUpdate(hk, pk, nil); err != nil {
			return fmt.Errorf("followAndUpdate (nibble=%x task[%d]): %w", nib, idx, err)
		}
		return nil
	}); err != nil {
		return err
	}
	// Dispatch is complete; the shared roTx is no longer needed. Releasing it
	// here keeps the bucket from holding two reader slots once the first fold
	// goroutine acquires its own trieCtx below.
	releaseDispatchCtx()

	// All workers fold back concurrently. They synchronise via splitMap
	// deposits; deadlock is impossible because sp.arrived's last decrement
	// triggers the last-finisher path even if the other workers exited
	// already.
	//
	// foldSem is a process-wide semaphore (sized p.numWorkers) shared by every
	// active bucket so the total fold-phase goroutine count across all buckets
	// is bounded by p.numWorkers — not p.numWorkers per bucket, which would
	// fan out to p.numWorkers * 16 with all buckets active. Workers that exit
	// at a sub-barrier release their slot immediately; the last-finisher holds
	// its slot while folding upward, bounded by the split-point chain depth.
	// Per-worker trieCtxes are acquired inside foldSem so concurrent trieCtx
	// (and MDBX reader-slot) usage tracks the fold concurrency cap, not
	// len(tasks).
	sg, sgctx := errgroup.WithContext(ctx)
	for i := range workers {
		taskIdx := i
		taskPrefix := tasks[i].prefix
		// Hand off ownership: foldDrainWithBarrier resets and re-pools the
		// worker, so the deferred cleanup above must skip this slot.
		hph := workers[i]
		workers[i] = nil
		sg.Go(func() error {
			select {
			case foldSem <- struct{}{}:
			case <-sgctx.Done():
				p.releaseHandedOffWorker(hph)
				return sgctx.Err()
			}
			defer func() { <-foldSem }()

			workerCtx, cleanup := p.trieCtxFactory()
			if cleanup != nil {
				defer cleanup()
			}
			hph.ResetContext(workerCtx)

			err := p.foldDrainWithBarrier(sgctx, hph, updates, taskPrefix)
			if err != nil {
				return fmt.Errorf("foldDrainWithBarrier (nibble=%x task[%d]): %w", nib, taskIdx, err)
			}
			return nil
		})
	}
	if err := sg.Wait(); err != nil {
		// foldDrainWithBarrier (or the deferred cleanup func above) has
		// returned each hph to the pool and released its per-worker trieCtx.
		return err
	}
	return nil
}

// releaseHandedOffWorker returns a worker to the pool when the fold goroutine
// has already taken ownership (so cleanupAll's hph!=nil guard skips it) but a
// foldSem-acquire context cancellation aborts before foldDrainWithBarrier
// runs. The per-worker trieCtx cleanup is still owned by runNibbleBucket's
// workers[i].cleanup and runs via cleanupAll on the error path.
func (p *ParallelPatriciaHashed) releaseHandedOffWorker(hph *HexPatriciaHashed) {
	if hph == nil {
		return
	}
	hph.resetForReuse()
	p.workerPool.Put(hph)
}

// foldDrainWithBarrier folds the worker's grid all the way to the root cell,
// then deposits the resulting cell at the worker's enclosing split-point. The
// worker either exits (a sibling is still pending) or — as the last sibling
// to arrive — rebuilds the split-point's grid row from every deposit and
// folds upward, repeating the process for each further-enclosing split-point
// in the chain. The topmost finisher publishes the root hash.
//
// Design note: an earlier attempt tried to detect split-point crossings
// during the fold loop itself (one fold step at a time, querying splitMap
// against the current `currentKey[:depths[deepest]-1]`). That doesn't work
// in this codebase: the HPH trie has no row at the split-point's child depth
// for workers that touched only one sibling — the trie structure is dense by
// trie-shape, not by split-point shape. We instead match the
// ConcurrentPatriciaHashed PoC: fold fully, deposit the worker's compressed
// root cell, and let the last-finisher synthesise the merged grid row from
// the deposits. The PoC handles a single depth-1 split-point; here we extend
// it to arbitrary-depth split-points and chains.
//
// The function always returns the worker to p.workerPool. Deferred branch
// updates collected by the worker are appended to the shared accumulator
// regardless of whether the worker exited at a barrier or at the root.
func (p *ParallelPatriciaHashed) foldDrainWithBarrier(
	ctx context.Context,
	hph *HexPatriciaHashed,
	updates *Updates,
	leafTaskPrefix []byte,
) (retErr error) {
	pu := updates.parallel
	defer func() {
		deferred := hph.TakeDeferredUpdates()
		if len(deferred) > 0 {
			pu.appendDeferred(deferred)
		}
		hph.resetForReuse()
		p.workerPool.Put(hph)
	}()

	// Stage 1: fold the worker's grid down toward the first enclosing
	// split-point's depth. Two outcomes:
	//
	//   - No enclosing split-point: fold all the way to activeRows == 0;
	//     hph.root carries the worker's entire subtree cell.
	//
	//   - Enclosing split-point at depth D = len(sp.prefix): fold while
	//     depths[deepest] > D+1 so the worker's deepest active row settles
	//     exactly at the split-point's child depth. The cell at
	//     grid[deepest][childNibble] is the deposit target — folding past it
	//     would incorrectly absorb the shared root branch into the worker's
	//     hph.root and overwrite sibling workers' contributions during
	//     deferred-update apply (the multi-phase failure mode this fix
	//     addresses). Workers in phase 1 (empty DB) typically have rows only
	//     at depths far below D+1 and naturally collapse to activeRows == 0,
	//     so the existing hph.root deposit path still fires.
	//
	// snapshot* captures currentKey + depths[0] only when Stage 1 still has
	// row 0 to fold and that fold would write extLen > 64 (deep storage
	// subtree case). depositRootIntoSplitPoint uses the snapshot to
	// reconstruct the trimmed extension when cell.extension was truncated by
	// the fold's silent [64]byte clamp.
	firstSP := findEnclosingSplitPoint(pu, leafTaskPrefix)
	stopDepth := int16(0)
	if firstSP != nil {
		stopDepth = int16(len(firstSP.prefix)) + 1
	}

	var rowZeroSnapKey []byte
	var rowZeroSnapDepth int16
	for hph.activeRows > 1 && hph.depths[hph.activeRows-1] > stopDepth {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := hph.fold(); err != nil {
			return fmt.Errorf("worker[%x] stage-1 fold: %w", leafTaskPrefix[0], err)
		}
	}
	if hph.activeRows == 1 && hph.depths[0] > stopDepth {
		rowZeroSnapDepth = hph.depths[0]
		rowZeroSnapKey = make([]byte, hph.currentKeyLen)
		copy(rowZeroSnapKey, hph.currentKey[:hph.currentKeyLen])
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := hph.fold(); err != nil {
			return fmt.Errorf("worker[%x] stage-1 final fold: %w", leafTaskPrefix[0], err)
		}
	}

	// Stage 2: walk up the chain of enclosing split-points. The first
	// iteration's deposit may come from grid[activeRows-1][childNibble] when
	// the worker stopped folding mid-stack (multi-phase path); subsequent
	// iterations always deposit hph.root because rebuildWorkerFromSplitPoint
	// drives activeRows back to 0.
	currentPrefix := leafTaskPrefix
	firstIteration := true
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		sp := findEnclosingSplitPoint(pu, currentPrefix)
		if sp == nil {
			// Stage 3: no further enclosing split-point. Publish the root.
			return p.publishRootFromWorker(hph)
		}

		childNibble := int(currentPrefix[len(sp.prefix)])
		if firstIteration && hph.activeRows > 0 && hph.depths[hph.activeRows-1] == int16(len(sp.prefix))+1 {
			if err := depositGridCellIntoSplitPoint(hph, sp, childNibble); err != nil {
				return fmt.Errorf("worker[%x] depositGridCellIntoSplitPoint(depth=%d, nib=%x): %w", leafTaskPrefix[0], len(sp.prefix), childNibble, err)
			}
		} else if err := depositRootIntoSplitPoint(hph, sp, childNibble, rowZeroSnapKey, rowZeroSnapDepth); err != nil {
			return fmt.Errorf("worker[%x] depositRootIntoSplitPoint(depth=%d, nib=%x): %w", leafTaskPrefix[0], len(sp.prefix), childNibble, err)
		}
		firstIteration = false

		remaining := sp.arrived.Add(-1)
		if remaining > 0 {
			// Sibling still pending: the last-finisher will pick up sp.cells.
			return nil
		}
		// Last-finisher: rebuild grid[0] from sp.cells, fold once to produce
		// the merged cell into hph.root, then continue the loop with the
		// split-point's prefix as the new "current" position.
		//
		// rebuildWorkerFromSplitPoint's fold writes upCell.extLen =
		// len(sp.prefix). For sp.prefix lengths <= 64 (covering all our
		// currently-exercised split-point depths: root and account-boundary
		// at depth 64) this fits cleanly in cell.extension[64]byte. We
		// refresh the snapshot so the next iteration's depositRoot can
		// recover if a deeper outer split-point ever requires a longer
		// extension.
		rowZeroSnapDepth = int16(len(sp.prefix)) + 1
		rowZeroSnapKey = make([]byte, len(sp.prefix))
		copy(rowZeroSnapKey, sp.prefix)
		if err := rebuildWorkerFromSplitPoint(hph, sp); err != nil {
			return fmt.Errorf("worker[%x] rebuildWorkerFromSplitPoint(depth=%d): %w", leafTaskPrefix[0], len(sp.prefix), err)
		}
		currentPrefix = sp.prefix
	}
}

// publishRootFromWorker computes the worker's hph.RootHash and stages it on
// p.pendingRoot together with the worker's final root cell and root flags.
// CAS detects orchestration bugs that let multiple workers reach this
// terminal state.
//
// Staging (rather than writing rootHash / template.root directly) lets the
// main Process goroutine promote the result only after applyDeferredUpdates
// has succeeded. If the deferred apply fails, the staged pending pointer is
// dropped and the trie does not surface a root that was never persisted.
//
// The captured hph.root cell and rootChecked/rootTouched/rootPresent flags
// are mirrored into the template on promotion. Workers never write to the
// template during Process, so without this copy the template stays at its
// initial (empty) state and downstream paths that depend on template's root
// state return the wrong value:
//
//   - RootHash() on this instance falls back to template.RootHash() when
//     p.rootHash is nil (e.g. on a fresh instance after restart);
//   - Process()'s zero-update fast-path returns template.RootHash() directly;
//   - commitmentdb.encodeCommitmentState serializes template via
//     RootTrie().EncodeCurrentState, which writes the three root flags
//     alongside the root cell; restorePatriciaState replays both via
//     SetState. Without the flag mirror the persisted state restores with
//     rootChecked/rootTouched/rootPresent == false, and the next unfold
//     treats the persisted root cell as "not present" in the fold/unfold
//     cycle.
//
// Safe to read hph.root and the flags without locks: CAS guarantees this
// branch runs in only one worker per Process call, and the worker is the
// sole owner of its pooled hph at this point.
func (p *ParallelPatriciaHashed) publishRootFromWorker(hph *HexPatriciaHashed) error {
	rh, err := hph.RootHash()
	if err != nil {
		return fmt.Errorf("RootHash: %w", err)
	}
	rhCopy := make([]byte, len(rh))
	copy(rhCopy, rh)
	pending := &publishedRoot{
		hash:        rhCopy,
		cell:        hph.root,
		rootChecked: hph.rootChecked,
		rootTouched: hph.rootTouched,
		rootPresent: hph.rootPresent,
	}
	if !p.pendingRoot.CompareAndSwap(nil, pending) {
		return errors.New("ParallelPatriciaHashed: another worker already published the root hash — split-point coverage is incomplete")
	}
	return nil
}

// findEnclosingSplitPoint returns the deepest split-point whose prefix is a
// strict prefix of leafTaskPrefix, or nil if none encloses it.
func findEnclosingSplitPoint(pu *parallelUpdate, leafTaskPrefix []byte) *splitPoint {
	// Walk the prefix from longest-to-shortest. Stop at length 0 (root
	// split-point) inclusive.
	for L := len(leafTaskPrefix) - 1; L >= 0; L-- {
		if sp, hit := pu.splitMap.Get(leafTaskPrefix[:L]); hit {
			return sp
		}
	}
	return nil
}

// depositRootIntoSplitPoint copies hph.root (the worker's compressed leafTask
// subtree cell) into sp.cells[childNibble], trimming the leading nibbles that
// are implicit in the slot index. The extension nibbles at the start of
// hph.root represent the path from depth 0 down to where the touched keys
// begin; the split-point's child slot already encodes the first L+1 of those
// nibbles (the prefix path through the split-point plus the child nibble), so
// they must be stripped before deposit.
//
// Two paths cover the common cases and the deep-storage-overflow case:
//
//   - extLen ≤ 64 ("normal"): computeCellHash is called first to memoise the
//     cell's hash, since cellEncodeData does not carry the Balance / Storage
//     / loaded-flag state that the last-finisher's hashRow would otherwise
//     need to recompute. Then the leading depositDepth nibbles are trimmed
//     from extension and hashedExtension.
//
//   - extLen > 64 ("deep storage"): the fold that produced hph.root silently
//     truncated cell.extension because the destination [64]byte cannot hold
//     the full path. We avoid computeCellHash entirely (it would panic on
//     cell.extension[:extLen]) and instead reconstruct the trimmed extension
//     from snapKey/snapDepth, which captured the worker's currentKey before
//     the final fold. After trimming, the residual extension fits within the
//     [64]byte capacity for every split-point ≤ depth 65 (the worker's row 0
//     depth minus depositDepth is bounded by 64 - 1 - depositDepth swing).
//     The cell.hash is already the branch hash written by foldBranch via
//     keccak2.Read, so the last-finisher's computeCellHash will correctly
//     compute extensionHash(trimmed_extension, hash).
//
// snapKey / snapDepth may be empty / zero when the worker's leafTask had no
// active rows to fold (single-touched-key paths) — that's safe because such
// a worker cannot have produced extLen > 0 either.
func depositRootIntoSplitPoint(hph *HexPatriciaHashed, sp *splitPoint, childNibble int, snapKey []byte, snapDepth int16) error {
	depositDepth := int16(len(sp.prefix)) + 1

	if hph.root.extLen > 64 {
		c := hph.root // shallow copy
		newExtLen := c.extLen - depositDepth
		if newExtLen > 64 {
			return fmt.Errorf("deposit extension would still overflow after trim: depositDepth=%d origExtLen=%d", depositDepth, c.extLen)
		}
		if int(depositDepth)+int(newExtLen) > len(snapKey) {
			return fmt.Errorf("snapshot key too short for deep-extension recovery: have %d need %d (origExtLen=%d snapDepth=%d)", len(snapKey), int(depositDepth)+int(newExtLen), c.extLen, snapDepth)
		}
		if newExtLen > 0 {
			copy(c.extension[:newExtLen], snapKey[depositDepth:depositDepth+newExtLen])
		}
		c.extLen = newExtLen
		// hashedExtension is [128]byte so the copy did not truncate. Trim it
		// in-place; for foldBranch's output it mirrors extension and the
		// last-finisher does not consult it for pure-branch cells, so the
		// exact contents do not affect correctness — only the length matters.
		if c.hashedExtLen > depositDepth {
			newLen := c.hashedExtLen - depositDepth
			if newLen <= int16(len(c.hashedExtension)) && int(depositDepth)+int(newLen) <= len(c.hashedExtension) {
				copy(c.hashedExtension[:newLen], c.hashedExtension[depositDepth:depositDepth+newLen])
				c.hashedExtLen = newLen
			} else {
				c.hashedExtLen = 0
			}
		} else {
			c.hashedExtLen = 0
		}
		sp.cells[childNibble] = cellEncodeDataFromCell(&c)
		return nil
	}

	// Normal path: the cell's hash is computed at the depth where it lives in
	// the merged trie — that's len(sp.prefix)+1 (one nibble below the split
	// point).
	if _, err := hph.computeCellHash(&hph.root, depositDepth, hph.hashAuxBuffer[:0]); err != nil {
		return fmt.Errorf("computeCellHash(depth=%d): %w", depositDepth, err)
	}

	c := hph.root // shallow copy
	trim := depositDepth
	if c.extLen > 0 {
		t := min(trim, c.extLen)
		c.extLen -= t
		copy(c.extension[:c.extLen], c.extension[t:t+c.extLen])
	}
	if c.hashedExtLen > 0 {
		t := min(trim, c.hashedExtLen)
		c.hashedExtLen -= t
		copy(c.hashedExtension[:c.hashedExtLen], c.hashedExtension[t:t+c.hashedExtLen])
	}

	sp.cells[childNibble] = cellEncodeDataFromCell(&c)
	return nil
}

// depositGridCellIntoSplitPoint deposits the grid cell at the deepest
// active row directly into sp.cells[childNibble]. This is the multi-phase
// path: Stage 1 stopped folding once the deepest row settled at the
// split-point's child depth, and the row's grid cell at childNibble already
// represents the worker's contribution to that slot. Depositing here avoids
// folding up through (and corrupting) the shared parent branch.
//
// The cell may have been loaded from DB without setting cellLoadAccount /
// cellLoadStorage flags, so its memoized stateHash may be empty. We call
// computeCellHash to populate stateHash before extraction; the last-finisher
// reads stateHash directly via the short-circuit path in computeCellHash.
func depositGridCellIntoSplitPoint(hph *HexPatriciaHashed, sp *splitPoint, childNibble int) error {
	row := hph.activeRows - 1
	if row < 0 {
		return errors.New("depositGridCellIntoSplitPoint: no active rows")
	}
	depositDepth := int16(len(sp.prefix)) + 1
	cellPtr := &hph.grid[row][childNibble]
	if _, err := hph.computeCellHash(cellPtr, depositDepth, hph.hashAuxBuffer[:0]); err != nil {
		return fmt.Errorf("computeCellHash(depth=%d, nib=%x): %w", depositDepth, childNibble, err)
	}
	sp.cells[childNibble] = cellEncodeDataFromCell(cellPtr)
	return nil
}

// rebuildWorkerFromSplitPoint repurposes the worker's hph as the
// last-finisher for sp. It zeroes grid[0], loads every deposited / DB cell
// from sp.cells into row 0 at depth = len(sp.prefix)+1, then folds row 0 to
// produce the merged cell into hph.root. After this call:
//
//   - hph.root holds the cell representing the entire sp.prefix subtree;
//   - activeRows == 0;
//   - the worker is ready to either deposit again at a further-enclosing
//     split-point or publish the root hash.
func rebuildWorkerFromSplitPoint(hph *HexPatriciaHashed, sp *splitPoint) error {
	// Reset the worker's trie state so stale grid entries from the leafTask
	// processing cannot leak in.
	hph.Reset()

	// Populate row 0 with the split-point's child cells. depth=len(sp.prefix)+1.
	depth := int16(len(sp.prefix)) + 1
	loadSiblingsIntoGrid(hph, sp, 0)
	hph.depths[0] = depth
	hph.activeRows = 1

	// currentKey carries the path from root down to the row's parent. The
	// path through the split-point is sp.prefix; the row's column selector
	// (currentKey[depth-1]) is the implicit nibble at position depth-1, which
	// is len(sp.prefix). foldBranch reads currentKey[upDepth:currentKeyLen]
	// to compute extension nibbles, so we set currentKeyLen = depth-1 and
	// fill currentKey[:depth-1] = sp.prefix. The byte at currentKey[depth-1]
	// is filled by foldBranch's own logic when row==0 it doesn't matter, but
	// to keep depthsToTxNum / extension construction sane we leave it zero.
	if len(sp.prefix) > 0 {
		copy(hph.currentKey[:len(sp.prefix)], sp.prefix)
	}
	hph.currentKeyLen = depth - 1

	// rootTouched/rootPresent get set by foldBranch based on whether the
	// merged branch carries any touchedBits. afterMap is what determines the
	// updateKind. For row 0, the fold writes upCell into &hph.root.
	if err := hph.fold(); err != nil {
		return fmt.Errorf("fold row 0: %w", err)
	}
	// After fold: activeRows=0, hph.root holds the merged sp.prefix subtree.
	return nil
}

// loadSiblingsIntoGrid is called by the last-finisher after all siblings have
// deposited at the split-point. It overwrites the deepest grid row with the
// 16 child cells (workers' deposits + DB pre-population from Prepare) and
// reconstructs the touchMap / afterMap / branchBefore state that hph.fold()
// expects to find. The row's depth was already set by the worker's unfold
// history; rows above the split-point retain the worker's state and are
// guaranteed consistent because every leafTask under the split-point shares
// the same ancestor path.
func loadSiblingsIntoGrid(hph *HexPatriciaHashed, sp *splitPoint, row int) {
	// Zero the row before reloading so stale slots from the worker's own
	// processing (which only touched grid[row][childNibble]) cannot leak
	// into the branch hash.
	for n := range 16 {
		hph.grid[row][n].reset()
	}

	afterMap := uint16(0)
	for bm := sp.touchedBitmap | sp.dbBitmap; bm != 0; {
		n := bits.TrailingZeros16(bm)
		bm &^= uint16(1) << uint16(n)
		src := &sp.cells[n]
		if cellEncodeDataIsEmpty(src) {
			// Touched-and-deleted slot: workers deposit an empty cell when a
			// delete propagates upward. Mark it absent in afterMap so the
			// branch encoder reflects the deletion.
			continue
		}
		dst := &hph.grid[row][n]
		dst.hashLen = src.hashLen
		copy(dst.hash[:src.hashLen], src.hash[:src.hashLen])
		dst.stateHashLen = src.stateHashLen
		copy(dst.stateHash[:src.stateHashLen], src.stateHash[:src.stateHashLen])
		dst.extLen = src.extLen
		copy(dst.extension[:src.extLen], src.extension[:src.extLen])
		dst.accountAddrLen = src.accountAddrLen
		copy(dst.accountAddr[:src.accountAddrLen], src.accountAddr[:src.accountAddrLen])
		dst.storageAddrLen = src.storageAddrLen
		copy(dst.storageAddr[:src.storageAddrLen], src.storageAddr[:src.storageAddrLen])
		afterMap |= uint16(1) << uint16(n)
	}
	hph.touchMap[row] = sp.touchedBitmap
	hph.afterMap[row] = afterMap
	hph.branchBefore[row] = sp.branchBefore
}

// cellEncodeDataIsEmpty reports whether a cellEncodeData carries no payload —
// i.e. an empty slot (no DB cell, no worker deposit, or a touched-and-deleted
// cell). Identical structurally to the zero cellEncodeData.
func cellEncodeDataIsEmpty(c *cellEncodeData) bool {
	return c.hashLen == 0 && c.stateHashLen == 0 && c.extLen == 0 &&
		c.accountAddrLen == 0 && c.storageAddrLen == 0
}

// applyDeferredUpdates merges every worker's deferred branch updates and
// applies them via a single PatriciaContext acquired from the factory.
// Entries are returned to deferredUpdatePool whether the apply succeeds or
// fails so the pool's allocation-reuse contract is preserved.
func (p *ParallelPatriciaHashed) applyDeferredUpdates(pu *parallelUpdate) error {
	pu.deferredMu.Lock()
	deferred := pu.deferredCombined
	pu.deferredCombined = nil
	pu.deferredMu.Unlock()

	if len(deferred) == 0 {
		return nil
	}
	defer func() {
		for _, upd := range deferred {
			putDeferredUpdate(upd)
		}
	}()

	applyCtx, cleanup := p.trieCtxFactory()
	if cleanup != nil {
		defer cleanup()
	}
	if applyCtx == nil {
		return errors.New("ParallelPatriciaHashed: trieCtxFactory returned nil context for deferred apply")
	}

	if _, err := ApplyDeferredBranchUpdates(deferred, p.numWorkers, applyCtx.PutBranch); err != nil {
		return fmt.Errorf("apply deferred branch updates: %w", err)
	}
	return nil
}

// warmupSplitAncestors enqueues every split-point prefix and its ancestor
// chain into the warmuper so the MDBX page cache for the eventual fold path
// is preloaded while workers process leaves.
//
// For each split-point at prefix P[0..d], we emit warmup work for P[0..1],
// P[0..2], ..., P[0..d]. The warmuper deduplicates internally via its work
// channel — duplicates from sibling split-points sharing an ancestor are
// harmless.
func (p *ParallelPatriciaHashed) warmupSplitAncestors(pu *parallelUpdate, warmuper *Warmuper) {
	if warmuper == nil || pu == nil {
		return
	}
	seen := make(map[string]struct{})
	for _, sp := range pu.splitPoints {
		if sp == nil || len(sp.prefix) == 0 {
			continue
		}
		key := string(sp.prefix)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		// The Warmuper accepts a hashed-key + startDepth and walks downward
		// branch-by-branch, so feeding the full split-point prefix once is
		// equivalent to enqueueing every ancestor depth individually.
		hashedKey := make([]byte, len(sp.prefix))
		copy(hashedKey, sp.prefix)
		warmuper.WarmKey(hashedKey, 0)
	}
}

// dispatchLeafKeys scans the given ETL collector exactly once and routes each
// hashed key to the leafTask whose prefix is the longest match. fn receives
// the leafTask index (into tasks) and the hashed/plain key pair. Keys that
// match none of the supplied tasks' prefixes are skipped silently: this lets
// multiple workers share a nibble bucket, each invoking dispatchLeafKeys with
// its own leafTask and filtering out keys owned by sibling workers.
//
// tasks may have any cardinality:
//   - 1 task with a broad prefix covering the bucket: every key matches; fn
//     is called for every entry.
//   - 1 task within a multi-task bucket: fn fires only on keys whose hashed
//     prefix matches the task's; sibling tasks' keys are skipped.
//   - >1 tasks: the longest matching prefix wins. Tasks with disjoint
//     prefixes are routed to distinct fn invocations.
//
// fn must not retain hk or pk beyond the call — they are backed by the ETL
// collector's reusable buffers.
func dispatchLeafKeys(
	ctx context.Context,
	collector *etl.Collector,
	tasks []leafTask,
	fn func(taskIdx int, hashedKey, plainKey []byte) error,
) error {
	if len(tasks) == 0 {
		return nil
	}
	// Build the routing table sorted by descending prefix length so the
	// longest-matching prefix wins. Each entry retains its original task
	// index so callers can identify which task a key was routed to.
	type entry struct {
		prefix []byte
		idx    int
	}
	routing := make([]entry, len(tasks))
	for i, t := range tasks {
		routing[i] = entry{prefix: t.prefix, idx: i}
	}
	sort.SliceStable(routing, func(i, j int) bool {
		return len(routing[i].prefix) > len(routing[j].prefix)
	})

	return collector.Load(nil, "", func(hk, pk []byte, _ etl.CurrentTableReader, _ etl.LoadNextFunc) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		matched := -1
		for _, e := range routing {
			if bytes.HasPrefix(hk, e.prefix) {
				matched = e.idx
				break
			}
		}
		if matched < 0 {
			// Key belongs to a sibling worker's leafTask. Skip silently.
			return nil
		}
		return fn(matched, hk, pk)
	}, etl.TransformArgs{Quit: ctx.Done()})
}

// groupLeafTasksByNibble bins leafTasks by their root nibble (prefix[0]). The
// returned map is keyed by nibble (0..15). Entries with empty prefix are
// dropped — Prepare guarantees non-root leafTasks have a non-empty prefix.
func groupLeafTasksByNibble(leafQueue []leafTask) map[byte][]leafTask {
	if len(leafQueue) == 0 {
		return nil
	}
	out := make(map[byte][]leafTask, 16)
	for _, t := range leafQueue {
		if len(t.prefix) == 0 {
			continue
		}
		out[t.prefix[0]] = append(out[t.prefix[0]], t)
	}
	return out
}
