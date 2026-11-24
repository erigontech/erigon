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

package accounts

import (
	"runtime"
	"sync"
	"sync/atomic"
	"weak"

	"github.com/puzpuzpuz/xsync/v4"
)

// canonTable gives one canonical pointer per value. A new value goes into a strong generation, so interning it
// costs no weak.Make or cleanup; a full generation is handed to the weak table in the background with the same
// pointers, so identity holds and memory stays bounded.
type canonTable[T comparable] struct {
	mu      sync.RWMutex // read-held while inserting into cur; write-held to rotate
	cur     atomic.Pointer[xsync.Map[T, *T]]
	prev    atomic.Pointer[xsync.Map[T, *T]]
	n       atomic.Int64
	limit   int64
	weak    *xsync.Map[T, weak.Pointer[T]]
	migrate sync.Mutex
}

func newCanonTable[T comparable](limit int64) *canonTable[T] {
	c := &canonTable[T]{limit: limit, weak: xsync.NewMap[T, weak.Pointer[T]]()}
	c.cur.Store(xsync.NewMap[T, *T]())
	return c
}

func (c *canonTable[T]) lookup(v T) *T {
	if p, ok := c.cur.Load().Load(v); ok {
		return p
	}
	if prev := c.prev.Load(); prev != nil {
		if p, ok := prev.Load(v); ok {
			return p
		}
	}
	if wp, ok := c.weak.Load(v); ok {
		return wp.Value()
	}
	return nil
}

func (c *canonTable[T]) intern(v T) *T {
	if p := c.lookup(v); p != nil {
		return p
	}
	c.mu.RLock()
	// Re-check under the read lock: a rotation or a migration may have moved v since lookup.
	if prev := c.prev.Load(); prev != nil {
		if p, ok := prev.Load(v); ok {
			c.mu.RUnlock()
			return p
		}
	}
	if wp, ok := c.weak.Load(v); ok {
		if p := wp.Value(); p != nil {
			c.mu.RUnlock()
			return p
		}
	}
	p, loaded := c.cur.Load().LoadOrCompute(v, func() (*T, bool) { p := new(T); *p = v; return p, false })
	c.mu.RUnlock()
	if !loaded && c.n.Add(1) >= c.limit {
		c.rotate()
	}
	return p
}

// rotate makes the current generation the previous one and migrates it to the weak table in the background.
func (c *canonTable[T]) rotate() {
	if !c.migrate.TryLock() {
		return
	}
	c.mu.Lock()
	old := c.cur.Load()
	c.cur.Store(xsync.NewMap[T, *T]())
	c.prev.Store(old)
	c.n.Store(0)
	c.mu.Unlock()
	go func() {
		defer c.migrate.Unlock()
		c.toWeak(old)
	}()
}

func (c *canonTable[T]) toWeak(old *xsync.Map[T, *T]) {
	old.Range(func(v T, p *T) bool {
		wp := weak.Make(p)
		c.weak.Store(v, wp)
		runtime.AddCleanup(p, func(v T) { c.release(v, wp) }, v)
		return true
	})
	c.mu.Lock()
	c.prev.Store(nil)
	c.mu.Unlock()
}

// release deletes v only while it still maps to wp: a later intern may have replaced the dead entry.
func (c *canonTable[T]) release(v T, wp weak.Pointer[T]) {
	c.weak.Compute(v, func(old weak.Pointer[T], loaded bool) (weak.Pointer[T], xsync.ComputeOp) {
		if loaded && old == wp {
			return old, xsync.DeleteOp
		}
		return old, xsync.CancelOp
	})
}

// flush migrates every strong generation to the weak table and waits for it; tests use it.
func (c *canonTable[T]) flush() {
	c.migrate.Lock()
	c.mu.Lock()
	old := c.cur.Load()
	c.cur.Store(xsync.NewMap[T, *T]())
	c.prev.Store(old)
	c.n.Store(0)
	c.mu.Unlock()
	c.toWeak(old)
	c.migrate.Unlock()
}
