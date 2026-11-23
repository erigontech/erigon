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

// Package internbench compares storage-key interning schemes under a concurrent GC. Run with INTERNBENCH=1.
package internbench

import (
	"fmt"
	"math/bits"
	"os"
	"runtime"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unique"
	"unsafe"
	"weak"

	"github.com/puzpuzpuz/xsync/v4"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types/accounts"
)

type interner struct {
	name  string
	make  func(common.Hash) unsafe.Pointer // the canonical pointer; the caller keeps it, so the handle stays live
	reset func()
}

// weakTable is an xsync map of weak pointers; weak.Make and AddCleanup run outside the map lock.
type weakTable struct {
	m *xsync.Map[common.Hash, weak.Pointer[common.Hash]]
}

func (t *weakTable) intern(k common.Hash) *common.Hash {
	if wp, ok := t.m.Load(k); ok {
		if p := wp.Value(); p != nil {
			return p
		}
	}
	p := new(common.Hash)
	*p = k
	wp := weak.Make(p)
	var live *common.Hash
	t.m.Compute(k, func(old weak.Pointer[common.Hash], loaded bool) (weak.Pointer[common.Hash], xsync.ComputeOp) {
		if loaded {
			if live = old.Value(); live != nil {
				return old, xsync.CancelOp
			}
		}
		return wp, xsync.UpdateOp
	})
	if live != nil {
		return live
	}
	m := t.m
	runtime.AddCleanup(p, func(k common.Hash) {
		m.Compute(k, func(old weak.Pointer[common.Hash], loaded bool) (weak.Pointer[common.Hash], xsync.ComputeOp) {
			if loaded && old == wp {
				return old, xsync.DeleteOp
			}
			return old, xsync.CancelOp
		})
	}, k)
	return p
}

type shard struct {
	mu sync.Mutex
	m  map[common.Hash]*common.Hash
	_  [40]byte
}

type shardedTable struct{ s [64]shard }

func (t *shardedTable) intern(k common.Hash) *common.Hash {
	s := &t.s[k[0]&63]
	s.mu.Lock()
	p, ok := s.m[k]
	if !ok {
		p = new(common.Hash)
		*p = k
		s.m[k] = p
	}
	s.mu.Unlock()
	return p
}

func newShardedTable() *shardedTable {
	t := &shardedTable{}
	for i := range t.s {
		t.s[i].m = map[common.Hash]*common.Hash{}
	}
	return t
}

var valueSink atomic.Uint64

func interners() []interner {
	wt := &weakTable{m: xsync.NewMap[common.Hash, weak.Pointer[common.Hash]]()}
	sm := xsync.NewMap[common.Hash, *common.Hash]()
	st := newShardedTable()
	return []interner{
		{name: "unique.Make", make: func(k common.Hash) unsafe.Pointer { h := unique.Make(k); return *(*unsafe.Pointer)(unsafe.Pointer(&h)) }},
		{name: "main InternKey (1M front table + unique)", make: func(k common.Hash) unsafe.Pointer {
			h := accounts.InternKey(k)
			return *(*unsafe.Pointer)(unsafe.Pointer(&h))
		}},
		{
			name: "#26404 xsync+weak", make: func(k common.Hash) unsafe.Pointer { return unsafe.Pointer(wt.intern(k)) },
			reset: func() { wt.m.Clear() },
		},
		{name: "xsync strong", make: func(k common.Hash) unsafe.Pointer {
			p, _ := sm.LoadOrCompute(k, func() (*common.Hash, bool) { p := new(common.Hash); *p = k; return p, false })
			return unsafe.Pointer(p)
		}, reset: func() { sm.Clear() }},
		{
			name: "64-shard map+mutex", make: func(k common.Hash) unsafe.Pointer { return unsafe.Pointer(st.intern(k)) },
			reset: func() {
				for i := range st.s {
					st.s[i].mu.Lock()
					clear(st.s[i].m)
					st.s[i].mu.Unlock()
				}
			},
		},
		{name: "no interning ([32]byte)", make: func(k common.Hash) unsafe.Pointer {
			valueSink.Add(uint64(k[0]))
			return nil
		}},
	}
}

func fresh(g, i uint64) common.Hash {
	var h common.Hash
	x := g<<40 | i
	for j := range 4 {
		x = x*0x9e3779b97f4a7c15 + 1
		*(*uint64)(unsafe.Pointer(&h[j*8])) = x
	}
	return h
}

// hist is a log-linear latency histogram: 8 sub-buckets per power of two.
type hist struct {
	b        [64 * 8]uint64
	n, sum   uint64
	maxNanos uint64
}

func (h *hist) add(d time.Duration) {
	ns := uint64(d)
	h.n++
	h.sum += ns
	h.maxNanos = max(h.maxNanos, ns)
	e := bits.Len64(ns)
	sub := uint64(0)
	if e > 3 {
		sub = (ns >> (e - 4)) & 7
	}
	h.b[e*8+int(sub)]++
}

func (h *hist) merge(o *hist) {
	for i := range h.b {
		h.b[i] += o.b[i]
	}
	h.n, h.sum, h.maxNanos = h.n+o.n, h.sum+o.sum, max(h.maxNanos, o.maxNanos)
}

func (h *hist) quantile(q float64) time.Duration {
	target, acc := uint64(float64(h.n)*q), uint64(0)
	for i := range h.b {
		c := h.b[i]
		if acc += c; acc > target {
			e, sub := i/8, uint64(i%8)
			if e <= 3 {
				return time.Duration(uint64(1) << e)
			}
			return time.Duration((8 + sub + 1) << (e - 4))
		}
	}
	return time.Duration(h.maxNanos)
}

type result struct {
	nsPerOp, p99, max time.Duration
}

// runArm runs n goroutines for d: half the ops intern a fresh key (kept live for the last `live` ops, like a block-lived key),
// half a key from a repeated set of 4096. With gc, one more goroutine loops runtime.GC().
func runArm(it interner, n int, gc bool, d time.Duration) result {
	if it.reset != nil {
		it.reset()
	}
	runtime.GC()
	var stop atomic.Bool
	var gcwg sync.WaitGroup
	if gc {
		gcwg.Go(func() {
			for !stop.Load() {
				runtime.GC()
			}
		})
	}
	const live = 100_000
	hs := make([]hist, n)
	var wg sync.WaitGroup
	for g := range n {
		wg.Go(func() {
			keep := make([]unsafe.Pointer, live)
			h := &hs[g]
			deadline := time.Now().Add(d)
			var i uint64
			for ; ; i++ {
				if i&1023 == 0 && time.Now().After(deadline) {
					break
				}
				var k common.Hash
				if i&1 == 0 {
					k = fresh(uint64(g+1), i)
				} else {
					k = fresh(0, (i*2654435761)&4095)
				}
				t := time.Now()
				keep[i%live] = it.make(k)
				h.add(time.Since(t))
			}
			runtime.KeepAlive(keep)
		})
	}
	wg.Wait()
	stop.Store(true)
	gcwg.Wait()
	var all hist
	for i := range hs {
		all.merge(&hs[i])
	}
	return result{nsPerOp: time.Duration(all.sum / all.n), p99: all.quantile(0.99), max: time.Duration(all.maxNanos)}
}

func TestInternUnderGC(t *testing.T) {
	if os.Getenv("INTERNBENCH") == "" {
		return
	}
	d := 3 * time.Second
	if s := os.Getenv("INTERNBENCH_SECS"); s != "" {
		v, _ := strconv.Atoi(s)
		d = time.Duration(v) * time.Second
	}
	fmt.Printf("arm | goroutines | GC loop | ns/op | p99 | max\n")
	for _, it := range interners() {
		for _, n := range []int{1, 4, 16} {
			for _, gc := range []bool{false, true} {
				r := runArm(it, n, gc, d)
				fmt.Printf("%s | %d | %v | %d | %v | %v\n", it.name, n, gc, r.nsPerOp.Nanoseconds(), r.p99, r.max)
			}
		}
	}
}

// TestGCMarkVsLiveHandles times a forced GC with L live handles of each arm.
func TestGCMarkVsLiveHandles(t *testing.T) {
	if os.Getenv("INTERNBENCH") == "" {
		return
	}
	fmt.Printf("arm | live handles | runtime.GC ms (median of 5) | STW pause µs\n")
	for _, L := range []int{1_000_000, 10_000_000} {
		for _, it := range interners() {
			if it.name == "main InternKey (1M front table + unique)" {
				continue
			}
			var keep []unsafe.Pointer
			var vals []common.Hash
			if it.name == "no interning ([32]byte)" {
				vals = make([]common.Hash, L)
				for i := range L {
					vals[i] = fresh(7, uint64(i))
				}
			} else {
				keep = make([]unsafe.Pointer, L)
				for i := range L {
					keep[i] = it.make(fresh(7, uint64(i)))
				}
			}
			var ms []float64
			var ps runtime.MemStats
			for range 5 {
				t0 := time.Now()
				runtime.GC()
				ms = append(ms, float64(time.Since(t0).Microseconds())/1000)
			}
			runtime.ReadMemStats(&ps)
			slices.Sort(ms)
			fmt.Printf("%s | %d | %.1f | %d\n", it.name, L, ms[2], ps.PauseNs[(ps.NumGC+255)%256]/1000)
			runtime.KeepAlive(keep)
			runtime.KeepAlive(vals)
			if it.reset != nil {
				it.reset()
			}
		}
	}
}
