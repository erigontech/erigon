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

import "github.com/erigontech/erigon/execution/cache"

// BranchReadView binds BranchCache access to one durable database state and
// files view. Concurrent publication turns a read into a miss, while fill
// admission prevents an old snapshot from entering a new generation.
type BranchReadView struct {
	c          *BranchCache
	generation cache.GenerationView
}

// View returns an inert handle unless the requested generation is currently
// published.
func (c *BranchCache) View(generation cache.Generation) BranchReadView {
	if c == nil {
		return BranchReadView{}
	}
	return BranchReadView{c: c, generation: c.generation.View(generation)}
}

func (v BranchReadView) current() bool {
	return v.c != nil && v.generation.Current()
}

// Get returns a branch only while the view remains current.
func (v BranchReadView) Get(prefix []byte) ([]byte, uint64, bool) {
	return cache.ReadCurrentWithStep(v.generation, func() ([]byte, uint64, bool) {
		return v.c.Get(prefix)
	})
}

// Fill admits a branch read from the view's database snapshot.
func (v BranchReadView) Fill(prefix, value []byte, step uint64) {
	if !v.current() || len(value) == 0 {
		return
	}
	v.generation.Admit(func() {
		v.c.Put(prefix, value, step)
	})
}

// BranchUpdate is one committed commitment-domain value. Step is returned with
// cache hits for bounded reads. TxNum records process write coverage for
// detecting files downloaded outside this publication path.
type BranchUpdate struct {
	Key   []byte
	Value []byte
	Step  uint64
	TxNum uint64
}

type canonicalPublisher = cache.CanonicalPublisher

// BranchPublisher is the canonical mutation handle for BranchCache.
type BranchPublisher struct {
	canonicalPublisher
	c *BranchCache
}

// Publisher returns a handle that can publish durable branch generations.
func (c *BranchCache) Publisher() BranchPublisher {
	if c == nil {
		return BranchPublisher{}
	}
	return BranchPublisher{
		canonicalPublisher: cache.NewCanonicalPublisher(&c.generation, c.resetProvenanceAndClear),
		c:                  c,
	}
}

// BranchPublication is one pending durable branch transition.
type BranchPublication struct {
	lifecycle *cache.CanonicalPublication
	c         *BranchCache
}

func (p BranchPublisher) Begin() *BranchPublication {
	lifecycle := p.canonicalPublisher.Begin()
	if lifecycle == nil {
		return nil
	}
	return &BranchPublication{lifecycle: lifecycle, c: p.c}
}

func (p *BranchPublication) Abort() {
	if p == nil || p.c == nil {
		return
	}
	p.lifecycle.Abort()
	p.c = nil
}

// Publish applies staged pin changes and committed branch updates. Forward
// commits retain unchanged branches. A lineage replacement sets clear because
// its updates do not enumerate every branch from the discarded state.
func (p *BranchPublication) Publish(generation cache.Generation, updates []BranchUpdate, clear bool, adaptive *AdaptivePinPlan) {
	if p == nil || p.c == nil {
		return
	}
	p.lifecycle.Publish(generation, clear, func(publication *cache.GenerationPublication) {
		adaptive.apply(publication)
		for i := range updates {
			update := &updates[i]
			if committedEnd := update.TxNum + 1; committedEnd > p.c.committedTxNumEnd {
				p.c.committedTxNumEnd = committedEnd
			}
			if len(update.Value) == 0 {
				p.c.Invalidate(update.Key)
				continue
			}
			p.c.Put(update.Key, update.Value, update.Step)
		}
	})
	p.c = nil
}
