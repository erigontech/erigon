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
)

// ParallelPatriciaHashed is the trie-side of the parallel commitment pipeline.
type ParallelPatriciaHashed struct {
	template       *HexPatriciaHashed
	trieCtxFactory TrieContextFactory
	workerPool     sync.Pool
	cfg            TrieConfig

	accountKeyLen int16
	numWorkers    int

	rootHash atomic.Pointer[[]byte]

	leaveDeferredForCaller bool
	deferredForCaller      []*DeferredBranchUpdate

	streaming *StreamingCommitter
}

// NewParallelPatriciaHashed constructs a fresh ParallelPatriciaHashed.
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

func (p *ParallelPatriciaHashed) resetPool() {
	akl := p.accountKeyLen
	cfg := p.cfg
	p.workerPool = sync.Pool{
		New: func() any {
			return NewHexPatriciaHashed(akl, nil, cfg)
		},
	}
}

// SetNumWorkers overrides the worker count for the next Process call; n <= 0 falls back to runtime.NumCPU.
func (p *ParallelPatriciaHashed) SetNumWorkers(n int) {
	if n <= 0 {
		n = runtime.NumCPU()
	}
	p.numWorkers = n
	if p.streaming != nil {
		p.streaming.SetNumWorkers(n)
	}
}

// SetLeaveDeferredForCaller makes Process leave the deferred branch updates for the caller to flush instead of applying them inline.
func (p *ParallelPatriciaHashed) SetLeaveDeferredForCaller(leave bool) {
	p.leaveDeferredForCaller = leave
	if p.streaming != nil {
		p.streaming.SetLeaveDeferredForCaller(leave)
	}
}

// HasPendingDeferredUpdates reports whether Process left deferred branch updates for the caller to flush.
func (p *ParallelPatriciaHashed) HasPendingDeferredUpdates() bool {
	return len(p.deferredForCaller) > 0
}

// TakeDeferredUpdates returns the deferred branch updates staged for the caller and clears them; the caller takes ownership.
func (p *ParallelPatriciaHashed) TakeDeferredUpdates() []*DeferredBranchUpdate {
	d := p.deferredForCaller
	p.deferredForCaller = nil
	return d
}

// RootTrie exposes the configuration template only; it must not be used as live root state.
func (p *ParallelPatriciaHashed) RootTrie() *HexPatriciaHashed {
	return p.template
}

// Reset clears the published root hash, drops pooled workers, and resets the template so the instance can be reused.
func (p *ParallelPatriciaHashed) Reset() {
	if p.template != nil {
		p.template.Reset()
	}
	p.rootHash.Store(nil)
	p.resetPool()
	if p.streaming != nil {
		p.streaming.Reset()
	}
}

// Release frees the template and worker pool; the instance must not be used afterwards. Repeat calls are no-ops.
func (p *ParallelPatriciaHashed) Release() {
	if p.template != nil {
		p.template.Release()
		p.template = nil
	}
	p.rootHash.Store(nil)
	p.resetPool()
	if p.streaming != nil {
		p.streaming.Release()
		p.streaming = nil
	}
}

// ResetContext propagates a new PatriciaContext to the template; per-worker contexts come from trieCtxFactory.
func (p *ParallelPatriciaHashed) ResetContext(ctx PatriciaContext) {
	if p.template != nil {
		p.template.ResetContext(ctx)
	}
}

// SetTrieContextFactory replaces the per-worker context factory.
func (p *ParallelPatriciaHashed) SetTrieContextFactory(f TrieContextFactory) {
	p.trieCtxFactory = f
	if p.streaming != nil {
		p.streaming.SetTrieContextFactory(f)
	}
}

// SetStreamingCommitter switches Process to the streaming path; the same committer must also be wired to the Updates buffer.
func (p *ParallelPatriciaHashed) SetStreamingCommitter(sc *StreamingCommitter) {
	p.streaming = sc
	if sc != nil && p.trieCtxFactory != nil {
		sc.SetTrieContextFactory(p.trieCtxFactory)
	}
}

func (p *ParallelPatriciaHashed) SetTrace(b bool) {
	if p.template != nil {
		p.template.SetTrace(b)
	}
	if p.streaming != nil {
		p.streaming.SetTrace(b)
	}
}

func (p *ParallelPatriciaHashed) SetTraceDomain(b bool) {
	if p.template != nil {
		p.template.SetTraceDomain(b)
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

func (p *ParallelPatriciaHashed) Variant() TrieVariant {
	if p.streaming != nil {
		return VariantStreamingHexPatricia
	}
	return VariantParallelHexPatricia
}

// RootHash returns the root hash published by Process, falling back to the template's current root if none was published.
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

// processStreaming delegates Process to the attached StreamingCommitter and republishes the root.
func (p *ParallelPatriciaHashed) processStreaming(ctx context.Context) ([]byte, error) {
	rh, err := p.streaming.Process(ctx)
	if err != nil {
		return nil, err
	}
	if p.leaveDeferredForCaller {
		p.deferredForCaller = p.streaming.TakeDeferredUpdates()
	}
	// Promote the root into the template so EncodeCurrentState serializes a root SetState can restore.
	p.streaming.PromoteRootInto(p.template)
	out := make([]byte, len(rh))
	copy(out, rh)
	p.rootHash.Store(&out)
	return out, nil
}

// Process is the entry point for parallel commitment computation; it requires updates.mode == ModeParallel.
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

	if p.streaming != nil {
		return p.processStreaming(ctx)
	}

	pu := updates.parallel
	if pu.trie == nil || pu.trie.root == nil || pu.trie.root.subtreeCount == 0 {
		rh, rerr := p.template.RootHash()
		if rerr != nil {
			return nil, rerr
		}
		return rh, nil
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
	flushTrieStateRates()
	return out, nil
}

// dfsSubtree visits the subtree in nibble order, emitting each node before its children; the hashedKey passed to fn is mutated in place and must not be retained.
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

// applyDeferredUpdates applies the merged deferred branch updates, returning every entry to the pool on success or failure.
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
