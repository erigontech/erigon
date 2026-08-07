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

// View returns an inert handle unless generation is currently published.
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
	if !v.current() {
		return nil, 0, false
	}
	value, step, ok := v.c.Get(prefix)
	if !v.current() {
		return nil, 0, false
	}
	return value, step, ok
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

// BranchUpdate is one committed commitment-domain value. TxNum records process
// write coverage for detecting files downloaded outside this publication path.
type BranchUpdate struct {
	Key   []byte
	Value []byte
	Step  uint64
	TxNum uint64
}

// BranchPublisher is the canonical mutation handle for BranchCache.
type BranchPublisher struct {
	c          *BranchCache
	generation cache.GenerationPublisher
}

// Publisher returns a handle that can publish durable branch generations.
func (c *BranchCache) Publisher() BranchPublisher {
	if c == nil {
		return BranchPublisher{}
	}
	return BranchPublisher{c: c, generation: c.generation.Publisher()}
}

func (p BranchPublisher) Enabled() bool {
	return p.c != nil && p.generation.Enabled()
}

// Initialize binds an empty or previously published cache to generation.
func (p BranchPublisher) Initialize(generation cache.Generation) {
	if p.c == nil {
		return
	}
	p.generation.Initialize(generation, p.c.Clear)
}

// BranchPublication represents one pending durable branch transition.
type BranchPublication struct {
	c          *BranchCache
	generation *cache.GenerationPublication
}

// Begin revokes current BranchReadViews without changing branch entries.
func (p BranchPublisher) Begin() *BranchPublication {
	if p.c == nil {
		return nil
	}
	return &BranchPublication{c: p.c, generation: p.generation.Begin()}
}

// Abort restores the previous branch generation after database rollback.
func (p *BranchPublication) Abort() {
	if p == nil || p.c == nil {
		return
	}
	p.generation.Abort()
	p.c = nil
}

// Publish applies staged pin changes and committed branch updates before it
// exposes generation. clear is required after canonical unwind because its
// diffset is not a complete list of branches from the discarded fork.
func (p *BranchPublication) Publish(generation cache.Generation, updates []BranchUpdate, clear bool, adaptive *AdaptivePinPlan) {
	if p == nil || p.c == nil {
		return
	}
	p.generation.Publish(generation, func() {
		if clear {
			p.c.Clear()
		}
		adaptive.apply()
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
