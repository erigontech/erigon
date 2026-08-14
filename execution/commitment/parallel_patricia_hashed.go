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

func (p *ParallelPatriciaHashed) SetNumWorkers(n int) {
	if n <= 0 {
		n = runtime.NumCPU()
	}
	p.numWorkers = n
	if p.streaming != nil {
		p.streaming.SetNumWorkers(n)
	}
}

func (p *ParallelPatriciaHashed) SetLeaveDeferredForCaller(leave bool) {
	p.leaveDeferredForCaller = leave
	if p.streaming != nil {
		p.streaming.SetLeaveDeferredForCaller(leave)
	}
}

func (p *ParallelPatriciaHashed) HasPendingDeferredUpdates() bool {
	return len(p.deferredForCaller) > 0
}

func (p *ParallelPatriciaHashed) TakeDeferredUpdates() []*DeferredBranchUpdate {
	d := p.deferredForCaller
	p.deferredForCaller = nil
	return d
}

// Previous trie must not be used after adoption.
func (p *ParallelPatriciaHashed) AdoptRootTrie(root *HexPatriciaHashed) {
	p.template = root
}

func (p *ParallelPatriciaHashed) RootTrie() *HexPatriciaHashed {
	return p.template
}

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

func (p *ParallelPatriciaHashed) ResetContext(ctx PatriciaContext) {
	if p.template != nil {
		p.template.ResetContext(ctx)
	}
}

func (p *ParallelPatriciaHashed) SetTrieContextFactory(f TrieContextFactory) {
	p.trieCtxFactory = f
	if p.streaming != nil {
		p.streaming.SetTrieContextFactory(f)
	}
}

func (p *ParallelPatriciaHashed) SetStreamingCommitter(sc *StreamingCommitter) {
	p.streaming = sc
	if sc != nil && p.trieCtxFactory != nil {
		sc.SetTrieContextFactory(p.trieCtxFactory)
	}
}

type syncWriter struct {
	mu sync.Mutex
	w  io.Writer
}

// Returns nil for nil and is idempotent to allow sharing without stacking mutexes.
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

type prefixWriter struct {
	w      io.Writer
	prefix []byte
}

func (pw *prefixWriter) Write(p []byte) (int, error) {
	buf := make([]byte, 0, len(p)+len(pw.prefix)*2)
	buf = append(buf, pw.prefix...)
	for i := range p {
		buf = append(buf, p[i])
		if p[i] == '\n' && i != len(p)-1 {
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

func tracePrefix(w io.Writer, prefix string) io.Writer {
	if w == nil {
		return nil
	}
	return &prefixWriter{w: w, prefix: []byte(prefix)}
}

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

func (p *ParallelPatriciaHashed) processStreaming(ctx context.Context) ([]byte, error) {
	// The template root is the restore target of SetState, so it seeds the committer's base;
	// PromoteRootInto below keeps the two in sync after every fold.
	p.streaming.SeedRootFrom(p.template)
	rh, err := p.streaming.Process(ctx)
	if err != nil {
		return nil, err
	}
	if p.leaveDeferredForCaller {
		p.deferredForCaller = p.streaming.TakeDeferredUpdates()
	}
	p.streaming.PromoteRootInto(p.template)
	out := make([]byte, len(rh))
	copy(out, rh)
	p.rootHash.Store(&out)
	return out, nil
}

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
	} else if aErr := p.applyDeferredUpdates(ctx, pu); aErr != nil {
		return nil, aErr
	}

	updates.consumeParallel()

	out := make([]byte, len(rh))
	copy(out, rh)
	p.rootHash.Store(&out)
	flushTrieStateRates()
	return out, nil
}

// Path is mutated in place; do not retain it.
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

func (p *ParallelPatriciaHashed) applyDeferredUpdates(ctx context.Context, pu *parallelUpdate) error {
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

	applyCtx, cleanup := p.trieCtxFactory(ctx)
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
