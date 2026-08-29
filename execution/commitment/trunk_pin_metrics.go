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
	"time"

	"github.com/erigontech/erigon/diagnostics/metrics"
)

// No per-contract labels (Prometheus cardinality); detail in [adaptive-pin] log.
var (
	mxPinnedHits    = metrics.GetOrCreateCounter("commitment_branchcache_pinned_hits_total")
	mxPinnedMisses  = metrics.GetOrCreateCounter("commitment_branchcache_pinned_misses_total")
	mxPinnedEntries = metrics.GetOrCreateGauge("commitment_branchcache_pinned_entries")

	mxAdaptivePromoted = metrics.GetOrCreateCounter("commitment_adaptive_pin_promoted_total")
	mxAdaptiveExtended = metrics.GetOrCreateCounter("commitment_adaptive_pin_extended_total")
	mxAdaptiveDemoted  = metrics.GetOrCreateCounter("commitment_adaptive_pin_demoted_total")
	mxAdaptiveActive   = metrics.GetOrCreateGauge("commitment_adaptive_pin_active_contracts")

	mxPreloadDurationSecondsTotal = metrics.GetOrCreateCounter("commitment_trunk_preload_duration_seconds_total")
	mxPreloadBytesTotal           = metrics.GetOrCreateCounter("commitment_trunk_preload_bytes_total")

	// Gauges of the cumulative tier counters: the cache owns the totals, so
	// publishing them straight avoids a per-tier last-published field.
	mxRootHits     = metrics.GetOrCreateGauge("commitment_branchcache_root_hits")
	mxRootMisses   = metrics.GetOrCreateGauge("commitment_branchcache_root_misses")
	mxTrunkHits    = metrics.GetOrCreateGauge("commitment_branchcache_trunk_hits")
	mxTrunkMisses  = metrics.GetOrCreateGauge("commitment_branchcache_trunk_misses")
	mxTailHits     = metrics.GetOrCreateGauge("commitment_branchcache_tail_hits")
	mxTailMisses   = metrics.GetOrCreateGauge("commitment_branchcache_tail_misses")
	mxStaleEvicted = metrics.GetOrCreateGauge("commitment_branchcache_stale_evicted")
)

func recordPreload(started time.Time, bytesPinned int) {
	mxPreloadDurationSecondsTotal.Add(time.Since(started).Seconds())
	if bytesPinned > 0 {
		mxPreloadBytesTotal.AddInt(bytesPinned)
	}
}

// publishEvery is a power of two so the sampling test is a mask. Publishing is
// otherwise driven only by the adaptive-pin controller, which does not run on
// the engine-API path, leaving the tier counters unpublished there.
const publishEvery = 1 << 13

func (c *BranchCache) maybePublishMetrics() {
	if c.publishTick.Add(1)&(publishEvery-1) == 0 {
		c.PublishMetrics()
	}
}

func (c *BranchCache) PublishMetrics() {
	hits := c.pinnedHits.Load()
	misses := c.pinnedMisses.Load()
	if delta := hits - c.lastPublishedPinnedHits.Swap(hits); delta > 0 {
		mxPinnedHits.AddUint64(delta)
	}
	if delta := misses - c.lastPublishedPinnedMisses.Swap(misses); delta > 0 {
		mxPinnedMisses.AddUint64(delta)
	}
	mxPinnedEntries.SetUint64(uint64(c.pinnedEntries.Load()))

	mxRootHits.SetUint64(c.rootHits.Load())
	mxRootMisses.SetUint64(c.rootMisses.Load())
	mxTrunkHits.SetUint64(c.trunkHits.Load())
	mxTrunkMisses.SetUint64(c.trunkMisses.Load())
	mxTailHits.SetUint64(c.tailHits.Load())
	mxTailMisses.SetUint64(c.tailMisses.Load())
	mxStaleEvicted.SetUint64(c.staleEvicted.Load())
}
