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

// AdoptRootTrie replaces the template with a trie that already carries state
// (e.g. restored before the variant upgrade); the previous trie must not be used after.
func (p *ParallelPatriciaHashed) AdoptRootTrie(root *HexPatriciaHashed) {
	p.template = root
}

// RootTrie returns the template trie, which carries the live root state.
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

// syncWriter serializes concurrent trace writes from the root template and the
// streaming/mount fold workers onto one underlying io.Writer.
type syncWriter struct {
	mu sync.Mutex
	w  io.Writer
}

// NewSyncWriter wraps w so concurrent trace writes are serialized. It is
// idempotent (a *syncWriter is returned unchanged) and returns nil for nil,
// so the same guarded writer can be shared across the template and streaming
// committer without stacking mutexes.
func NewSyncWriter(w io.Writer) io.Writer {
	if w == nil {
		return nil
	}
	if _, ok := w.(*syncWriter); ok {
		return w
	}
	return &syncWriter{w: w}
}

func (sw *syncWriter) Write(p []byte) (int, error) {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	return sw.w.Write(p)
}

// prefixWriter tags each line with a fixed prefix and forwards the whole tagged
// chunk to w in one Write, so a worker's trace lines stay attributable and never
// interleave mid-line when multiplexed onto a shared writer.
type prefixWriter struct {
	w      io.Writer
	prefix []byte
}

func (pw *prefixWriter) Write(p []byte) (int, error) {
	buf := make([]byte, 0, len(p)+len(pw.prefix)*2)
	buf = append(buf, pw.prefix...)
	for i := 0; i < len(p); i++ {
		buf = append(buf, p[i])
		if p[i] == '\n' && i != len(p)-1 { // re-tag interior lines, not a trailing newline
			buf = append(buf, pw.prefix...)
		}
	}
	n, err := pw.w.Write(buf)
	if err != nil {
		return 0, err
	}
	if n < len(buf) {
		return 0, io.ErrShortWrite
	}
	return len(p), nil
}

// tracePrefix wraps w so a fold worker's trace lines are tagged with prefix.
// Returns nil (tracing disabled) when w is nil, so callers keep the cheap
// `traceW != nil` guard. w should be the shared syncWriter.
func tracePrefix(w io.Writer, prefix string) io.Writer {
	if w == nil {
		return nil
	}
	return &prefixWriter{w: w, prefix: []byte(prefix)}
}

// SetTraceWriter routes trace output to w (nil disables tracing). The writer is
// wrapped once in a mutex-guarded syncWriter shared with the template and the
// streaming committer, whose fold workers fan it out to their per-goroutine tries.
func (p *ParallelPatriciaHashed) SetTraceWriter(w io.Writer) {
	tw := NewSyncWriter(w)
	if p.template != nil {
		p.template.SetTraceWriter(tw)
	}
	if p.streaming != nil {
		p.streaming.SetTraceWriter(tw)
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

	pu := updates.parallel
	if pu.trie == nil || pu.trie.root == nil || pu.trie.root.subtreeCount == 0 {
		// A consumed (or never-touched) collection must return the carried root; folding
		// an empty streaming base would publish the empty-trie root instead.
		rh, rerr := p.template.RootHash()
		if rerr != nil {
			return nil, rerr
		}
		return rh, nil
	}

	if p.streaming != nil {
		rh, sErr := p.processStreaming(ctx)
		if sErr == nil {
			updates.consumeParallel()
		}
		return rh, sErr
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

	updates.consumeParallel()

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
