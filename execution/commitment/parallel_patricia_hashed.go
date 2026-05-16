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
	"runtime"
	"sync"
	"sync/atomic"
)

// ParallelPatriciaHashed is the trie-side of the parallel commitment pipeline.
// It owns:
//
//   - a template *HexPatriciaHashed used ONLY to expose ctx/cache/metrics/trace
//     configuration to callers. Workers never write to it and it does not hold
//     live root state during Process.
//   - a worker pool of fresh *HexPatriciaHashed instances acquired per leafTask
//     in Process (wired in Task 6).
//   - a TrieContextFactory that yields per-worker PatriciaContext instances so
//     DB reads run concurrently.
//   - an atomic root-hash pointer published by the last-finisher of the
//     topmost split-point at the end of Process.
//
// The skeleton in this file implements lifecycle and configuration plumbing
// only. Process and the worker fold-time barrier protocol are added in Tasks
// 6 and 7.
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
// returned instance is usable for configuration immediately; Process (Task 6)
// requires a non-nil ctxFactory.
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
// contexts are produced from trieCtxFactory by Process (Task 6).
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
