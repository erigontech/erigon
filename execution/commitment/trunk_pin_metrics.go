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

// Prometheus metrics for the storage-trunk pin layer. Per-contract
// labels intentionally omitted — cardinality risk is real (mainnet
// could see hundreds of promoted contracts over a process lifetime)
// and the structured [adaptive-pin] log line gives per-contract
// detail when needed for debugging.

var (
	// BranchCache pinned-tier counters. Surface the existing
	// PinnedStats hits/misses/entries so dashboards can reason about
	// pin effectiveness without scraping logs.
	mxPinnedHits    = metrics.GetOrCreateCounter("commitment_branchcache_pinned_hits_total")
	mxPinnedMisses  = metrics.GetOrCreateCounter("commitment_branchcache_pinned_misses_total")
	mxPinnedEntries = metrics.GetOrCreateGauge("commitment_branchcache_pinned_entries")

	// AdaptivePinController lifecycle counters.
	mxAdaptivePromoted = metrics.GetOrCreateCounter("commitment_adaptive_pin_promoted_total")
	mxAdaptiveExtended = metrics.GetOrCreateCounter("commitment_adaptive_pin_extended_total")
	mxAdaptiveDemoted  = metrics.GetOrCreateCounter("commitment_adaptive_pin_demoted_total")
	mxAdaptiveActive   = metrics.GetOrCreateGauge("commitment_adaptive_pin_active_contracts")

	// Preload-side counters for diagnostics. Per-contract preload
	// duration is too high-cardinality for a histogram label; total
	// duration spent in preload is the aggregate proxy.
	mxPreloadDurationSecondsTotal = metrics.GetOrCreateCounter("commitment_trunk_preload_duration_seconds_total")
	mxPreloadBytesTotal           = metrics.GetOrCreateCounter("commitment_trunk_preload_bytes_total")
)

// publishPinnedGauge updates the pinned-entries gauge from the cache's
// current size. Called from PublishMetrics; cheap so safe to call
// frequently.
func (c *BranchCache) publishPinnedGauge() {
	mxPinnedEntries.SetUint64(uint64(c.pinned.Len()))
}

// PublishMetrics snapshots the cache's current counters into the
// Prometheus registry. Call periodically from the host (e.g. once per
// SD.Flush) to keep metrics fresh — atomic counter loads are cheap
// but not free, so a one-call-per-batch cadence avoids hot-path cost.
func (c *BranchCache) PublishMetrics() {
	mxPinnedHits.SetUint64(c.pinnedHits.Load())
	mxPinnedMisses.SetUint64(c.pinnedMisses.Load())
	c.publishPinnedGauge()
}
