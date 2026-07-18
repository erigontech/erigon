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

// Package cachebudget bounds the total resident memory of the process-wide
// application caches (state, code) to a fraction of the memory actually
// available — system RAM, cgroup limit, or GOMEMLIMIT, whichever is lowest.
//
// Caches do not pre-commit their full configured size. They start small and
// grow in steps, reserving each step's bytes from one shared envelope; a step
// that would overflow the envelope is refused, so the cache stops growing and
// evicts within its current size instead. A cache with a small working set (a
// test fixture) therefore stays small regardless of its configured budget,
// while a busy production cache grows into it — and the sum across every cache
// instance in the process stays within the envelope. Release returns a cache's
// reserved bytes when it is torn down or cleared. No cache is ever disabled and
// there is no test-specific sizing.
package cachebudget

import (
	"sync/atomic"

	"github.com/erigontech/erigon/common/estimate"
)

// Divisor sets the single shared envelope — covering every application cache
// (state, code, and the commitment-branch LRU tail) — to this fraction of total
// available memory. It is the one knob governing aggregate cache residency:
// larger (e.g. 16) buys hit-rate on big nodes; 32 keeps a constrained 16GB CI
// runner well within bounds even under the race detector's memory multiplier.
const Divisor = 32

// Budget is a shared byte allowance drawn down by Reserve and returned by
// Release. Safe for concurrent use.
type Budget struct {
	limit int64
	used  atomic.Int64
}

func New(limit int64) *Budget { return &Budget{limit: limit} }

// Global is the process-wide application-cache envelope.
var Global = New(int64(estimate.TotalMemory() / Divisor))

// Reserve takes exactly n bytes if the envelope has room, returning true; it
// takes nothing and returns false when full. A grow step calls this and stops
// growing on false.
func (b *Budget) Reserve(n int64) bool {
	if n <= 0 {
		return true
	}
	for {
		used := b.used.Load()
		if used+n > b.limit {
			return false
		}
		if b.used.CompareAndSwap(used, used+n) {
			return true
		}
	}
}

// Take reserves n bytes unconditionally (may push used past limit). Used for a
// cache's initial small allocation, which must always succeed so no cache is
// born disabled.
func (b *Budget) Take(n int64) {
	if n > 0 {
		b.used.Add(n)
	}
}

// Release returns n bytes to the envelope.
func (b *Budget) Release(n int64) {
	if n > 0 {
		b.used.Add(-n)
	}
}

func (b *Budget) Limit() int64 { return b.limit }
func (b *Budget) Used() int64  { return b.used.Load() }
