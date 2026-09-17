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
	"sync"
	"sync/atomic"
	"testing"

	"github.com/erigontech/erigon/diagnostics/metrics"
)

func TestPublishCounterDeltaDoesNotDoubleCount(t *testing.T) {
	var cur, last atomic.Uint64
	m := metrics.GetOrCreateCounter("test_branchcache_delta_total")
	base := m.GetValue()

	cur.Store(7)
	publishCounterDelta(&cur, &last, m)
	if got := m.GetValue() - base; got != 7 {
		t.Fatalf("first publish: counter delta = %v, want 7", got)
	}

	publishCounterDelta(&cur, &last, m)
	if got := m.GetValue() - base; got != 7 {
		t.Fatalf("republish with no change: counter delta = %v, want 7", got)
	}

	cur.Store(10)
	publishCounterDelta(&cur, &last, m)
	if got := m.GetValue() - base; got != 10 {
		t.Fatalf("after +3: counter delta = %v, want 10", got)
	}
}

func TestPublishCounterDeltaConcurrentPublishers(t *testing.T) {
	const publishers, rounds = 4, 2000
	var cur, last atomic.Uint64
	m := metrics.GetOrCreateCounter("test_branchcache_delta_concurrent_total")
	base := m.GetValue()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < rounds; i++ {
			cur.Add(1)
		}
	}()
	for p := 0; p < publishers; p++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < rounds; i++ {
				publishCounterDelta(&cur, &last, m)
			}
		}()
	}
	wg.Wait()
	publishCounterDelta(&cur, &last, m)

	if got, want := m.GetValue()-base, float64(cur.Load()); got != want {
		t.Fatalf("concurrent publish: counter delta = %v, want %v (source counter)", got, want)
	}
}

func TestPublishMetricsWiresEachTierToItsOwnCounter(t *testing.T) {
	c := &BranchCache{}
	tiers := []struct {
		name string
		src  *atomic.Uint64
		m    metrics.Counter
		bump uint64
	}{
		{"root_hits", &c.rootHits, mxRootHits, 1},
		{"root_misses", &c.rootMisses, mxRootMisses, 2},
		{"trunk_hits", &c.trunkHits, mxTrunkHits, 3},
		{"trunk_misses", &c.trunkMisses, mxTrunkMisses, 4},
		{"pinned_hits", &c.pinnedHits, mxPinnedHits, 5},
		{"pinned_misses", &c.pinnedMisses, mxPinnedMisses, 6},
		{"tail_hits", &c.tailHits, mxTailHits, 7},
		{"tail_misses", &c.tailMisses, mxTailMisses, 8},
		{"stale_evicted", &c.staleEvicted, mxStaleEvicted, 9},
	}

	base := make([]float64, len(tiers))
	for i, tr := range tiers {
		base[i] = tr.m.GetValue()
		tr.src.Store(tr.bump)
	}

	c.PublishMetrics()

	for i, tr := range tiers {
		if got := tr.m.GetValue() - base[i]; got != float64(tr.bump) {
			t.Errorf("%s: published %v, want %d — tier wired to the wrong metric or shadow", tr.name, got, tr.bump)
		}
	}
}
