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
	"sync/atomic"
	"time"

	"github.com/erigontech/erigon/diagnostics/metrics"
)

// No per-contract labels (Prometheus cardinality); detail in [adaptive-pin] log.
var (
	mxPinnedHits    = metrics.GetOrCreateCounter("commitment_branchcache_pinned_hits_total")
	mxPinnedMisses  = metrics.GetOrCreateCounter("commitment_branchcache_pinned_misses_total")
	mxPinnedEntries = metrics.GetOrCreateGauge("commitment_branchcache_pinned_entries")

	mxRootHits     = metrics.GetOrCreateCounter("commitment_branchcache_root_hits_total")
	mxRootMisses   = metrics.GetOrCreateCounter("commitment_branchcache_root_misses_total")
	mxTrunkHits    = metrics.GetOrCreateCounter("commitment_branchcache_trunk_hits_total")
	mxTrunkMisses  = metrics.GetOrCreateCounter("commitment_branchcache_trunk_misses_total")
	mxTailHits     = metrics.GetOrCreateCounter("commitment_branchcache_tail_hits_total")
	mxTailMisses   = metrics.GetOrCreateCounter("commitment_branchcache_tail_misses_total")
	mxStaleEvicted = metrics.GetOrCreateCounter("commitment_branchcache_stale_evicted_total")

	mxAdaptivePromoted = metrics.GetOrCreateCounter("commitment_adaptive_pin_promoted_total")
	mxAdaptiveExtended = metrics.GetOrCreateCounter("commitment_adaptive_pin_extended_total")
	mxAdaptiveDemoted  = metrics.GetOrCreateCounter("commitment_adaptive_pin_demoted_total")
	mxAdaptiveActive   = metrics.GetOrCreateGauge("commitment_adaptive_pin_active_contracts")

	mxPreloadDurationSecondsTotal = metrics.GetOrCreateCounter("commitment_trunk_preload_duration_seconds_total")
	mxPreloadBytesTotal           = metrics.GetOrCreateCounter("commitment_trunk_preload_bytes_total")
)

func recordPreload(started time.Time, bytesPinned int) {
	mxPreloadDurationSecondsTotal.Add(time.Since(started).Seconds())
	if bytesPinned > 0 {
		mxPreloadBytesTotal.AddInt(bytesPinned)
	}
}

func publishCounterDelta(cur, last *atomic.Uint64, m metrics.Counter) {
	v := cur.Load()
	for {
		prev := last.Load()
		if v <= prev {
			return
		}
		if last.CompareAndSwap(prev, v) {
			m.AddUint64(v - prev)
			return
		}
	}
}

func (c *BranchCache) PublishMetrics() {
	publishCounterDelta(&c.rootHits, &c.lastPublishedRootHits, mxRootHits)
	publishCounterDelta(&c.rootMisses, &c.lastPublishedRootMisses, mxRootMisses)
	publishCounterDelta(&c.trunkHits, &c.lastPublishedTrunkHits, mxTrunkHits)
	publishCounterDelta(&c.trunkMisses, &c.lastPublishedTrunkMisses, mxTrunkMisses)
	publishCounterDelta(&c.pinnedHits, &c.lastPublishedPinnedHits, mxPinnedHits)
	publishCounterDelta(&c.pinnedMisses, &c.lastPublishedPinnedMisses, mxPinnedMisses)
	publishCounterDelta(&c.tailHits, &c.lastPublishedTailHits, mxTailHits)
	publishCounterDelta(&c.tailMisses, &c.lastPublishedTailMisses, mxTailMisses)
	publishCounterDelta(&c.staleEvicted, &c.lastPublishedStaleEvicted, mxStaleEvicted)
	mxPinnedEntries.SetUint64(uint64(c.pinnedEntries.Load()))
}
