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

	"github.com/erigontech/erigon/common/dbg"
)

// ParallelPatriciaHashed is the trie-side of the parallel commitment pipeline.
// It owns:
//
//   - a template *HexPatriciaHashed that exposes ctx/cache/metrics/trace
//     configuration to callers and serves as the mount base during Process: it
//     is unfolded to the root branch, the per-nibble workers' folded cells are
//     dropped into its row, and it folds the merged root.
//   - a worker pool of fresh *HexPatriciaHashed instances mounted per touched
//     root nibble in Process.
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

	// rootHash holds the root hash produced by the mount fold. Nil until
	// Process completes successfully.
	rootHash atomic.Pointer[[]byte]

	// leaveDeferredForCaller makes Process skip the inline applyDeferredUpdates
	// and hand the worker-accumulated deferred branch updates to the caller
	// instead — the deferred-commitment (fork validation / parallel apply) path.
	leaveDeferredForCaller bool
	deferredForCaller      []*DeferredBranchUpdate
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
// updates.mode == ModeParallel with a populated parallelUpdate. It delegates to
// processMounted, which unfolds the root branch, mounts a worker per touched
// child nibble, folds each subtree into a cell, and folds the merged root.
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
		// Deferred-commitment mode: hand the worker-accumulated deferred branch
		// updates to the caller to flush into the correct block's changeset
		// instead of applying them inline. The root hash comes from the in-memory
		// fold and does not depend on the branch apply.
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
