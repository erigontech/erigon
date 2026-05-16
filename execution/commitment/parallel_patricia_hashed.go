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
//
// Task 6 wires Process with single-worker / no-splitPoint scope: each leafTask
// emerging from Prepare gets a freshly-pooled worker, the worker scans its
// nibble bucket once, applies keys via followAndUpdate, folds to the root and
// publishes the root hash. Multi-worker correctness (the barrier protocol
// that lets siblings deposit cells at split-points) is added in Task 7 — and
// only then is it safe to exercise updates that produce more than one
// leafTask under any nibble bucket.
type ParallelPatriciaHashed struct {
	template       *HexPatriciaHashed
	trieCtxFactory TrieContextFactory
	workerPool     sync.Pool

	accountKeyLen int16
	numWorkers    int
	minSplitKeys  uint32

	// rootHash holds the bytes published by the last-finisher of the topmost
	// split-point. Nil until Process completes successfully.
	rootHash atomic.Pointer[[]byte]
}

// NewParallelPatriciaHashed constructs a fresh ParallelPatriciaHashed. The
// returned instance is usable for configuration immediately; Process requires
// a non-nil ctxFactory.
func NewParallelPatriciaHashed(ctxFactory TrieContextFactory, accountKeyLen int16) *ParallelPatriciaHashed {
	p := &ParallelPatriciaHashed{
		template:       NewHexPatriciaHashed(accountKeyLen, nil),
		trieCtxFactory: ctxFactory,
		accountKeyLen:  accountKeyLen,
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
	p.workerPool = sync.Pool{
		New: func() any {
			return NewHexPatriciaHashed(akl, nil)
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
// Task 6 scope: tests exercise NumWorkers=1 with no split-points (typically by
// raising MinSplitKeys above the touched-key count). In that regime each
// nibble bucket yields at most one leafTask, the single worker scans the
// bucket end-to-end, folds to the root and publishes its hph.RootHash().
// Multi-leafTask-per-bucket configurations are detected and rejected here
// until Task 7 wires the barrier protocol.
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
	var warmuper *Warmuper
	if warmup.Enabled {
		if warmup.CtxFactory == nil {
			warmup.CtxFactory = p.trieCtxFactory
		}
		warmuper = NewWarmuper(ctx, warmup)
		warmuper.Start()
		defer warmuper.CloseAndWait()
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
		return nil, fmt.Errorf("parallel commitment prepare: %w", err)
	}

	if warmuper != nil {
		p.warmupSplitAncestors(pu, warmuper)
	}

	// Group leafTasks by their root nibble so each nibble's ETL collector is
	// scanned at most once. In Task 6 scope every group contains a single
	// leafTask; the dispatcher tolerates multi-task groups but processing
	// them requires the barrier (Task 7).
	tasksByNibble := groupLeafTasksByNibble(pu.leafQueue)

	var (
		rootMu    sync.Mutex
		rootHphs  []*HexPatriciaHashed
		rootCount int
	)

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(p.numWorkers)

	// Iterate deterministically over nibbles so test failures are reproducible
	// regardless of Go's map randomization.
	for nib := range 16 {
		tasks := tasksByNibble[byte(nib)]
		if len(tasks) == 0 {
			continue
		}
		if len(tasks) > 1 {
			// Cancels in-flight workers and triggers errgroup return.
			_ = g.Wait()
			return nil, fmt.Errorf("ParallelPatriciaHashed: nibble %x has %d leafTasks; multi-task per nibble requires the Task 7 barrier protocol", nib, len(tasks))
		}
		task := tasks[0]
		nib := byte(nib)
		g.Go(func() error {
			hph, err := p.runLeafTask(gctx, updates, nib, task, onProgress)
			if err != nil {
				return err
			}
			rootMu.Lock()
			rootHphs = append(rootHphs, hph)
			rootCount++
			rootMu.Unlock()
			return nil
		})
	}

	if waitErr := g.Wait(); waitErr != nil {
		// Return all surviving workers to the pool.
		for _, h := range rootHphs {
			h.Reset()
			p.workerPool.Put(h)
		}
		return nil, waitErr
	}

	if err := p.applyDeferredUpdates(pu); err != nil {
		for _, h := range rootHphs {
			h.Reset()
			p.workerPool.Put(h)
		}
		return nil, err
	}

	// Task 6 scope: there is exactly one surviving worker (since we reject
	// multi-leafTask configurations above). Extract the root hash from it.
	if len(rootHphs) == 0 {
		rh, rerr := p.template.RootHash()
		if rerr != nil {
			return nil, rerr
		}
		return rh, nil
	}
	if len(rootHphs) > 1 {
		// Defensive: with Task 6's single-leafTask constraint this branch
		// should never trigger. If it does, the resulting "root" is the
		// fold-product of one worker, not the merged trie.
		for _, h := range rootHphs[1:] {
			h.Reset()
			p.workerPool.Put(h)
		}
		rootHphs = rootHphs[:1]
		return nil, errors.New("ParallelPatriciaHashed: multiple leafTask workers survived; the barrier protocol (Task 7) is required to merge their roots")
	}

	rootWorker := rootHphs[0]
	defer func() {
		rootWorker.Reset()
		p.workerPool.Put(rootWorker)
	}()

	rh, rerr := rootWorker.RootHash()
	if rerr != nil {
		return nil, fmt.Errorf("parallel commitment root hash: %w", rerr)
	}
	rhCopy := make([]byte, len(rh))
	copy(rhCopy, rh)
	p.rootHash.Store(&rhCopy)

	if warmuper != nil {
		warmuper.DrainPending()
	}
	_ = logPrefix
	return rh, nil
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

// runLeafTask processes one leafTask end-to-end on a freshly-pooled worker
// and returns the worker (still holding root state) for the caller to extract
// its RootHash. The caller is responsible for resetting and re-pooling.
//
// Lifecycle steps:
//  1. acquire a worker hph from the pool and bind it to a per-worker context;
//  2. scan the nibbles[nib] ETL collector exactly once, dispatching each key
//     to the matching leafTask (single-task fast path in Task 6 scope);
//  3. invoke followAndUpdate per key — the existing fold/unfold machinery
//     handles trie navigation;
//  4. fold to root (Task 6 has no barrier — workers fold all the way up);
//  5. drain deferred branch updates into the shared accumulator.
func (p *ParallelPatriciaHashed) runLeafTask(
	ctx context.Context,
	updates *Updates,
	nib byte,
	task leafTask,
	_ func(*CommitProgress),
) (*HexPatriciaHashed, error) {
	hph := p.workerPool.Get().(*HexPatriciaHashed)
	hph.Reset()

	workerCtx, cleanup := p.trieCtxFactory()
	if cleanup != nil {
		defer cleanup()
	}
	hph.ResetContext(workerCtx)
	hph.branchEncoder.SetDeferUpdates(true)
	hph.SetLeaveDeferredForCaller(true)

	// Defensive copy: the trace/capture/warmup-cache settings on the template
	// are propagated to the worker so per-batch debug flags are honoured.
	if p.template != nil {
		hph.trace = p.template.trace
		hph.traceDomain = p.template.traceDomain
		hph.enableWarmupCache = p.template.enableWarmupCache
	}

	collector := updates.nibbles[nib]
	if collector == nil {
		// Worker is still safe to return — the caller will reset/repool it.
		return nil, fmt.Errorf("ParallelPatriciaHashed: nibbles[%x] collector is nil", nib)
	}

	tasks := []leafTask{task}
	if err := dispatchLeafKeys(ctx, collector, tasks, func(_ int, hk, pk []byte) error {
		if err := hph.followAndUpdate(hk, pk, nil); err != nil {
			return fmt.Errorf("followAndUpdate: %w", err)
		}
		return nil
	}); err != nil {
		return nil, err
	}

	for hph.activeRows > 0 {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := hph.fold(); err != nil {
			return nil, fmt.Errorf("worker[%x] final fold: %w", nib, err)
		}
	}

	deferred := hph.TakeDeferredUpdates()
	updates.parallel.appendDeferred(deferred)

	return hph, nil
}

// applyDeferredUpdates merges every worker's deferred branch updates and
// applies them via a single PatriciaContext acquired from the factory.
func (p *ParallelPatriciaHashed) applyDeferredUpdates(pu *parallelUpdate) error {
	pu.deferredMu.Lock()
	deferred := pu.deferredCombined
	pu.deferredCombined = nil
	pu.deferredMu.Unlock()

	if len(deferred) == 0 {
		return nil
	}

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
// the leafTask index (into tasks) and the hashed/plain key pair.
//
// tasks may have any cardinality:
//   - 1 task: every key matches; fn is called for every entry.
//   - >1 tasks (Task 7 scope): the longest matching prefix wins. Tasks with
//     disjoint prefixes are routed to distinct fn invocations.
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
			return fmt.Errorf("dispatchLeafKeys: hashedKey %x matches no leafTask prefix", hk)
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
