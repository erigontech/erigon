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
	"cmp"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"unsafe"
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
	if p := c.lookup(v); p != nil {
		c.mu.RUnlock()
		return p
	}
	p, loaded := c.cur.Load().LoadOrCompute(v, func() (*T, bool) { p := new(T); *p = v; return p, false })
	full := !loaded && c.n.Add(1) >= c.limit // counted under the read lock, so swap cannot reset n between the insert and its count
	c.mu.RUnlock()
	if full {
		c.rotate()
	}
	return p
}

// rotate makes the current generation the previous one and migrates it to the weak table in the background.
// While a migration runs, an insert that fills cur waits for it, so each strong generation holds at most limit values.
func (c *canonTable[T]) rotate() {
	c.migrate.Lock()
	if c.n.Load() < c.limit {
		c.migrate.Unlock()
		return
	}
	old := c.swap()
	go func() {
		defer c.migrate.Unlock()
		c.toWeak(old)
	}()
}

func (c *canonTable[T]) swap() *xsync.Map[T, *T] {
	c.mu.Lock()
	defer c.mu.Unlock()
	old := c.cur.Load()
	c.cur.Store(xsync.NewMap[T, *T]())
	c.prev.Store(old)
	c.n.Store(0)
	return old
}

func (c *canonTable[T]) toWeak(old *xsync.Map[T, *T]) {
	// weak.Make and AddCleanup insert into a per-span list of specials; in address order each insert is near the last one.
	ptrs := make([]*T, 0, old.Size())
	old.Range(func(_ T, p *T) bool {
		ptrs = append(ptrs, p)
		return true
	})
	slices.SortFunc(ptrs, func(a, b *T) int { return cmp.Compare(uintptr(unsafe.Pointer(a)), uintptr(unsafe.Pointer(b))) })
	for _, p := range ptrs {
		v := *p
		wp := weak.Make(p)
		c.weak.Store(v, wp)
		runtime.AddCleanup(p, func(v T) { c.release(v, wp) }, v)
	}
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
	c.toWeak(c.swap())
	c.migrate.Unlock()
}
