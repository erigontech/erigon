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

	accountKeyLen int16
	cfg           TrieConfig
	numWorkers    int

	rootHash atomic.Pointer[[]byte]

	deepLocalFolds atomic.Uint64

	leaveDeferredForCaller bool
	deferredForCaller      []*DeferredBranchUpdate
}

func (p *ParallelPatriciaHashed) DeepLocalFolds() uint64 { return p.deepLocalFolds.Load() }

func NewParallelPatriciaHashed(ctxFactory TrieContextFactory, accountKeyLen int16, cfg TrieConfig) *ParallelPatriciaHashed {
	p := &ParallelPatriciaHashed{
		template:       NewHexPatriciaHashed(accountKeyLen, nil, cfg),
		trieCtxFactory: ctxFactory,
		accountKeyLen:  accountKeyLen,
		numWorkers:     runtime.NumCPU(),
		cfg:            cfg,
	}
	return p
}

func (p *ParallelPatriciaHashed) SetNumWorkers(n int) {
	if n <= 0 {
		n = runtime.NumCPU()
	}
	p.numWorkers = n
}

func (p *ParallelPatriciaHashed) SetLeaveDeferredForCaller(leave bool) {
	p.leaveDeferredForCaller = leave
}

func (p *ParallelPatriciaHashed) HasPendingDeferredUpdates() bool {
	return len(p.deferredForCaller) > 0
}

func (p *ParallelPatriciaHashed) TakeDeferredUpdates() []*DeferredBranchUpdate {
	d := p.deferredForCaller
	p.deferredForCaller = nil
	return d
}

func (p *ParallelPatriciaHashed) AdoptRootTrie(root *HexPatriciaHashed) {
	p.template = root
}

func (p *ParallelPatriciaHashed) RootTrie() *HexPatriciaHashed {
	return p.template
}

// EncodeCurrentState and SetState delegate to the template trie, which is where
// the live root state lives; they make the parallel trie a StatefulTrie.
func (p *ParallelPatriciaHashed) EncodeCurrentState(buf []byte) ([]byte, error) {
	return p.template.EncodeCurrentState(buf)
}

// A restore moves the root, so a root published by an earlier Process no longer
// describes the trie and RootHash has to fall back to the template.
func (p *ParallelPatriciaHashed) SetState(buf []byte) error {
	p.rootHash.Store(nil)
	return p.template.SetState(buf)
}

// Reset clears the published root hash and resets the template so the instance
// can be reused; pooled workers stay cached for the next Process call.
func (p *ParallelPatriciaHashed) Reset() {
	if p.template != nil {
		p.template.Reset()
	}
	p.rootHash.Store(nil)
	p.deepLocalFolds.Store(0)
}

func (p *ParallelPatriciaHashed) Release() {
	if p.template != nil {
		p.template.Release()
		p.template = nil
	}
	p.rootHash.Store(nil)
}

func (p *ParallelPatriciaHashed) ResetContext(ctx PatriciaContext) {
	if p.template != nil {
		p.template.ResetContext(ctx)
	}
}

func (p *ParallelPatriciaHashed) SetTrieContextFactory(f TrieContextFactory) {
	p.trieCtxFactory = f
}

type syncWriter struct {
	mu sync.Mutex
	w  io.Writer
}

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
}

func (p *ParallelPatriciaHashed) EnableCsvMetrics(filePathPrefix string) {
	if p.template != nil {
		p.template.EnableCsvMetrics(filePathPrefix)
	}
}

func (p *ParallelPatriciaHashed) Variant() TrieVariant {
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
	p.deepLocalFolds.Store(0)

	pu := updates.parallel
	if pu.trie == nil || pu.trie.root == nil || pu.trie.root.subtreeCount == 0 {
		// A consumed (or never-touched) collection must return the carried root; folding
		// an empty base would publish the empty-trie root instead.
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

// hashedKey passed to fn is mutated in place and must not be retained.
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
