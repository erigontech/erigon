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

// BranchReadView binds BranchCache access to one durable PlainStateVersion.
// Publication concurrent with a read turns the result into a miss, while fills
// are serialized so a value from an old database snapshot cannot enter a new
// generation.
type BranchReadView struct {
	c       *BranchCache
	version cache.PlainStateVersionView
}

// View returns an inert handle unless stateVersion is currently published.
func (c *BranchCache) View(stateVersion uint64) BranchReadView {
	if c == nil {
		return BranchReadView{}
	}
	version := c.version.View(stateVersion)
	if !version.Current() {
		return BranchReadView{}
	}
	return BranchReadView{c: c, version: version}
}

func (v BranchReadView) current() bool {
	return v.c != nil && v.version.Current()
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
	v.version.Admit(func() {
		v.c.Put(prefix, value, step)
	})
}

// BranchUpdate is one committed commitment-domain value.
type BranchUpdate struct {
	Key   []byte
	Value []byte
	Step  uint64
}

// BranchPublisher is the canonical mutation handle for BranchCache.
type BranchPublisher struct {
	c       *BranchCache
	version cache.PlainStateVersionPublisher
}

// Publisher returns a handle that can publish durable branch generations.
func (c *BranchCache) Publisher() BranchPublisher {
	if c == nil {
		return BranchPublisher{}
	}
	return BranchPublisher{c: c, version: c.version.Publisher()}
}

func (p BranchPublisher) Enabled() bool {
	return p.c != nil && p.version.Enabled()
}

// Initialize binds an empty or previously published cache to stateVersion.
func (p BranchPublisher) Initialize(stateVersion uint64) {
	if p.c == nil {
		return
	}
	p.version.Initialize(stateVersion, p.c.Clear)
}

// BranchPublication represents one pending durable branch transition.
type BranchPublication struct {
	c       *BranchCache
	version *cache.PlainStateVersionPublication
}

// Begin revokes current BranchReadViews without changing branch entries.
func (p BranchPublisher) Begin() *BranchPublication {
	if p.c == nil {
		return nil
	}
	return &BranchPublication{c: p.c, version: p.version.Begin()}
}

// Abort restores the previous branch generation after database rollback.
func (p *BranchPublication) Abort() {
	if p == nil || p.c == nil {
		return
	}
	p.version.Abort()
	p.c = nil
}

// Publish applies staged pin changes and committed branch updates before it
// exposes stateVersion. clear is required after canonical unwind because its
// diffset is not a complete list of branches from the discarded fork.
func (p *BranchPublication) Publish(stateVersion uint64, updates []BranchUpdate, clear bool, adaptive *AdaptivePinPlan) {
	if p == nil || p.c == nil {
		return
	}
	p.version.Publish(stateVersion, func() {
		if clear {
			p.c.Clear()
		}
		adaptive.apply()
		for i := range updates {
			update := &updates[i]
			if len(update.Value) == 0 {
				p.c.Invalidate(update.Key)
				continue
			}
			p.c.Put(update.Key, update.Value, update.Step)
		}
	})
	p.c = nil
}
