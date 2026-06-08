// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

package commitment

import (
	"github.com/erigontech/erigon/diagnostics/metrics"
)

// Per-contract labels omitted to keep cardinality bounded; per-contract
// detail is in the [adaptive-pin] structured log line.

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
)

// PublishMetrics emits counter deltas (last-published tracked internally) and
// sets gauges absolute. Call once per SD.Flush — once-per-batch avoids hot-path cost.
func (c *BranchCache) PublishMetrics() {
	hits := c.pinnedHits.Load()
	misses := c.pinnedMisses.Load()
	if delta := hits - c.lastPublishedPinnedHits.Swap(hits); delta > 0 {
		mxPinnedHits.AddUint64(delta)
	}
	if delta := misses - c.lastPublishedPinnedMisses.Swap(misses); delta > 0 {
		mxPinnedMisses.AddUint64(delta)
	}
	mxPinnedEntries.SetUint64(uint64(c.pinned.Len()))
}
