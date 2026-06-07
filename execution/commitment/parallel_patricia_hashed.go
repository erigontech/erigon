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
	"os"
	"runtime"
	"sync"
	"sync/atomic"

	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon/common/dbg"
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

	// leaveDeferredForCaller makes Process skip the inline applyDeferredUpdates
	// and hand the worker-accumulated deferred branch updates to the caller
	// instead — the deferred-commitment (fork validation / parallel apply) path.
	leaveDeferredForCaller bool
	deferredForCaller      []*DeferredBranchUpdate
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

// SetLeaveDeferredForCaller makes Process leave the worker-accumulated deferred
// branch updates for the caller to flush instead of applying them inline.
// Mirrors HexPatriciaHashed; used by the deferred-commitment (fork validation /
// parallel apply) path.
func (p *ParallelPatriciaHashed) SetLeaveDeferredForCaller(leave bool) {
	p.leaveDeferredForCaller = leave
}

// HasPendingDeferredUpdates reports whether Process left deferred branch updates
// for the caller to flush.
func (p *ParallelPatriciaHashed) HasPendingDeferredUpdates() bool {
	return len(p.deferredForCaller) > 0
}

// TakeDeferredUpdates returns the deferred branch updates staged for the caller
// and clears them; the caller takes ownership and returns them to the pool after
// flushing (via PendingCommitmentUpdate.Clear).
func (p *ParallelPatriciaHashed) TakeDeferredUpdates() []*DeferredBranchUpdate {
	d := p.deferredForCaller
	p.deferredForCaller = nil
	return d
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
	if warmup.Enabled && dbg.EnvWarmupParallelProcess {
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
	// Coarsen the split threshold so a wide, uniform trie is not shattered into
	// tens of thousands of leaf tasks (each acquires a worker grid from the
	// pool — at that volume the pool stops amortizing and allocations blow up).
	// Aim for ~numWorkers*4 tasks; only ever raises the threshold, so small and
	// clustered batches keep p.minSplitKeys.
	if total := pu.trie.root.subtreeCount; total > 0 && p.numWorkers > 0 {
		if adaptive := total / uint32(p.numWorkers*4); adaptive > pu.minSplitKeys {
			pu.minSplitKeys = adaptive
		}
	}
	if err := pu.Prepare(prepareCtx); err != nil {
		return nil, fmt.Errorf("[%s] parallel commitment prepare: %w", logPrefix, err)
	}

	if os.Getenv("ERIGON_CMT_MOUNT") == "1" {
		rh, mErr := p.processMounted(ctx, updates)
		if mErr != nil {
			pu.deferredMu.Lock()
			for _, upd := range pu.deferredCombined {
				putDeferredUpdate(upd)
			}
			pu.deferredCombined = pu.deferredCombined[:0]
			pu.deferredMu.Unlock()
			return nil, mErr
		}
		if p.leaveDeferredForCaller {
			pu.deferredMu.Lock()
			p.deferredForCaller = pu.deferredCombined
			pu.deferredCombined = nil
			pu.deferredMu.Unlock()
		} else if aErr := p.applyDeferredUpdates(pu); aErr != nil {
			return nil, aErr
		}
		out := make([]byte, len(rh))
		copy(out, rh)
		p.rootHash.Store(&out)
		if warmuper != nil {
			warmuper.DrainPending()
		}
		return out, nil
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

	for _, task := range pu.leafQueue {
		if len(task.prefix) == 0 {
			return nil, errors.New("ParallelPatriciaHashed: leafTask emitted without a routing prefix")
		}
		if task.node == nil {
			return nil, errors.New("ParallelPatriciaHashed: leafTask emitted without a subtree node")
		}
	}

	// Each leafTask owns a disjoint trie subtree, so a flat errgroup capped at
	// numWorkers runs build+fold per task with no per-bucket serialization.
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(p.numWorkers)

	for i := range pu.leafQueue {
		task := pu.leafQueue[i]
		g.Go(func() error {
			return p.runLeafTask(gctx, updates, task)
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

	if p.leaveDeferredForCaller {
		// Deferred-commitment mode: stage the worker-accumulated deferred branch
		// updates for the caller to flush into the correct block's changeset
		// instead of applying them inline. The staged root is still promoted
		// below; the root hash comes from the in-memory fold and does not depend
		// on the branch apply.
		pu.deferredMu.Lock()
		p.deferredForCaller = pu.deferredCombined
		pu.deferredCombined = nil
		pu.deferredMu.Unlock()
	} else if err := p.applyDeferredUpdates(pu); err != nil {
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

// runLeafTask materialises one leafTask by DFS-walking its trie subtree, applies
// each key via followAndUpdate, then folds the worker back through the barrier.
// foldDrainWithBarrier owns returning the worker to the pool on its paths. See
// docs/design/parallel-patricia-hashed.md (Phase 3/4) for the phase model.
func (p *ParallelPatriciaHashed) runLeafTask(ctx context.Context, updates *Updates, task leafTask) error {
	hph := p.workerPool.Get().(*HexPatriciaHashed)
	hph.resetForReuse()
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

	workerCtx, cleanup := p.trieCtxFactory()
	if cleanup != nil {
		defer cleanup()
	}
	hph.ResetContext(workerCtx)

	path := make([]byte, 0, 144)
	path = append(path, task.prefix...)
	if err := dfsSubtree(task.node, path, func(hk, pk []byte, upd *Update) error {
		return hph.followAndUpdate(hk, pk, upd)
	}); err != nil {
		hph.resetForReuse()
		p.workerPool.Put(hph)
		return fmt.Errorf("leafTask[%x] build: %w", task.prefix[0], err)
	}

	if err := p.foldDrainWithBarrier(ctx, hph, updates, task.prefix); err != nil {
		return fmt.Errorf("leafTask[%x] fold: %w", task.prefix[0], err)
	}
	return nil
}

// dfsSubtree visits node's subtree in nibble order, calling fn(hashedKey, plainKey,
// update) at every terminating node; hashedKey is path, grown/truncated in place
// down the walk (fn must not retain it). A node emits its key BEFORE descending,
// so an account precedes its storage.
func dfsSubtree(node *prefixNode, path []byte, fn func(hashedKey, plainKey []byte, update *Update) error) error {
	if node == nil {
		return nil
	}
	if node.plainKey != nil {
		if err := fn(path, node.plainKey, node.update); err != nil {
			return err
		}
	} else if node.bitmap == 0 {
		return errors.New("ParallelPatriciaHashed: trie leaf without a plainKey")
	}
	childIdx := 0
	for bm := node.bitmap; bm != 0; {
		nib := byte(bits.TrailingZeros16(bm))
		child := node.children[childIdx]
		base := len(path)
		path = append(path, nib)
		path = append(path, child.ext...)
		if err := dfsSubtree(child, path, fn); err != nil {
			return err
		}
		path = path[:base]
		childIdx++
		bm &^= uint16(1) << nib
	}
	return nil
}

// foldDrainWithBarrier folds the worker's grid to its enclosing split-point,
// deposits the resulting cell there, and exits — unless it is the last sibling
// to arrive, in which case it rebuilds the merged row from every deposit, folds
// upward, and repeats for each further-enclosing split-point; the topmost
// finisher publishes the root. The worker is always returned to the pool and
// its deferred updates appended to the shared accumulator, on every path. See
// docs/design/parallel-patricia-hashed.md (Phase 4) for the protocol.
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

	// Stage 1: fold down to the enclosing split-point's child depth
	// (len(sp.prefix)+1), or all the way to activeRows==0 when none encloses
	// this worker. Folding past the child depth would absorb the shared parent
	// branch into hph.root and overwrite siblings' deposits at apply time.
	// snapshot* preserves currentKey/depth before a final row-0 fold so a
	// deep-storage extension (>64 nibbles, silently clamped by the fold) can be
	// reconstructed at deposit time.
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
		// Last finisher: rebuild grid[0] from sp.cells, fold to merge into
		// hph.root, and continue from sp.prefix. Refresh the snapshot so the
		// next iteration's deposit can rebuild a long extension if needed.
		rowZeroSnapDepth = int16(len(sp.prefix)) + 1
		rowZeroSnapKey = make([]byte, len(sp.prefix))
		copy(rowZeroSnapKey, sp.prefix)
		if err := rebuildWorkerFromSplitPoint(hph, sp); err != nil {
			return fmt.Errorf("worker[%x] rebuildWorkerFromSplitPoint(depth=%d): %w", leafTaskPrefix[0], len(sp.prefix), err)
		}
		currentPrefix = sp.prefix
	}
}

// publishRootFromWorker stages the worker's root hash, root cell, and root
// flags on p.pendingRoot (CAS-guarded, so only one worker can publish).
// Staging rather than writing rootHash/template directly lets Process promote
// the result only after applyDeferredUpdates succeeds, so a failed apply never
// surfaces an unpersisted root. The flags must travel with the cell: they are
// what EncodeCurrentState/SetState round-trip, and a fresh instance's
// RootHash() falls back to the template. Lock-free: CAS plus sole ownership of
// the pooled hph.
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

// depositRootIntoSplitPoint copies the worker's folded root cell into
// sp.cells[childNibble], stripping the leading nibbles already implied by the
// slot's position (the split-point prefix plus child nibble). The normal path
// memoises the cell hash first, since cellEncodeData drops the state the
// last-finisher would otherwise need to recompute it. The deep-storage path
// (extLen>64, where the fold silently clamped cell.extension) instead rebuilds
// the trimmed extension from the pre-fold snapshot. snapKey/snapDepth may be
// empty for single-key workers, which cannot have produced extLen>0. See
// docs/design/parallel-patricia-hashed.md.
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
	// Only a hash-only sub-branch carries the split-point path in its extension; a
	// leaf's extension is a key tail whose leading nibbles are not implied by the
	// slot position, so trimming it would corrupt the stored branch.
	if c.extLen > 0 && c.accountAddrLen == 0 && c.storageAddrLen == 0 {
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

// depositGridCellIntoSplitPoint deposits the grid cell at the deepest active
// row directly into sp.cells[childNibble] — the path taken when Stage 1 stopped
// folding at the split-point's child depth, so folding further would corrupt the
// shared parent branch. computeCellHash populates stateHash first (a DB-loaded
// cell may have it empty) for the last-finisher's hashRow.
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

	// foldBranch reads currentKey[upDepth:currentKeyLen] for the extension
	// nibbles, so set currentKeyLen = depth-1 and currentKey[:depth-1] =
	// sp.prefix (the path from root down to the row's parent).
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

// loadSiblingsIntoGrid overwrites the deepest grid row with the 16 child cells
// (worker deposits + DB pre-population) and reconstructs the touchMap/afterMap/
// branchBefore state hph.fold() expects. Rows above the split-point retain the
// worker's state, consistent because every leafTask under the split-point
// shares the ancestor path.
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

