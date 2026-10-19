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
package state

import (
	"sync/atomic"
	"time"

	"github.com/erigontech/erigon/diagnostics/metrics"
)

// slowProbe separates disk-bound file probes from page-cache hits: an HDD seek takes milliseconds, an NVMe read
// about 0.1 ms, a cached page microseconds.
const slowProbe = time.Millisecond

const (
	tierSSD = iota
	tierHDD
)

var tierNames = [2]string{"ssd", "hdd"}

var (
	mxTierProbes        = tierGauges("tiering_file_probes_total")
	mxTierSlowProbes    = tierGauges("tiering_file_slow_probes_total")
	mxTierExistenceFP   = tierGauges("tiering_existence_false_positives_total")
	mxTierLookups       = metrics.GetOrCreateGauge("tiering_lookups_total")
	mxTierHDDLookups    = metrics.GetOrCreateGauge("tiering_hdd_lookups_total")
	mxTierProbeSeconds  = tierHistograms("tiering_file_probe_seconds", []float64{1e-6, 2.5e-6, 5e-6, 1e-5, 2.5e-5, 5e-5, 1e-4, 2.5e-4, 5e-4, 1e-3, 2.5e-3, 5e-3, 1e-2, 2.5e-2, 5e-2, 0.1, 0.25, 0.5, 1})
	mxTierProbesPerLook = tierHistograms("tiering_probes_per_lookup", []float64{0, 1, 2, 3, 4, 6, 8, 16, 32, 64})

	mxDemotionPausedSeconds   = metrics.GetOrCreateGauge("tiering_demotion_paused_seconds_total")
	mxDemotionInFlight        = metrics.GetOrCreateGauge("tiering_demotion_in_flight")
	mxDemotionFile            = metrics.GetOrCreateGaugeVec("tiering_demotion_file", []string{"file"})
	mxDemotions               = metrics.GetOrCreateGaugeVec("tiering_demotions_total", []string{"domain"})
	mxDemotedBytes            = metrics.GetOrCreateGaugeVec("tiering_demoted_bytes_total", []string{"domain"})
	mxDemotionSeconds         = metrics.GetOrCreateGauge("tiering_demotion_seconds")
	mxDemotionCopyBytesPerSec = metrics.GetOrCreateGauge("tiering_demotion_copy_bytes_per_sec")

	tierProbes, tierSlowProbes, tierExistenceFP [2]atomic.Uint64
	tierLookups, tierHDDLookups                 atomic.Uint64
)

func tierGauges(name string) (g [2]metrics.Gauge) {
	for i, tier := range tierNames {
		g[i] = metrics.GetOrCreateGauge(name + `{tier="` + tier + `"}`)
	}
	return g
}

func tierHistograms(name string, buckets []float64) (h [2]metrics.Histogram) {
	for i, tier := range tierNames {
		h[i] = metrics.NewHistogram(name+`{tier="`+tier+`"}`, buckets)
	}
	return h
}

func tierOf(cold bool) int {
	if cold {
		return tierHDD
	}
	return tierSSD
}

// lookupProbes counts the file probes of one commitment key lookup and publishes them once, in done. A clock read
// costs about as much as a cached probe, so SSD probes are timed only in a sample of lookups; HDD probes always are.
type lookupProbes struct {
	n, slow, fp [2]uint64
	timeSSD     bool
}

func newLookupProbes(keyHash uint64) *lookupProbes {
	return &lookupProbes{timeSSD: keyHash&15 == 0}
}

func (p *lookupProbes) timed(tier int) bool { return tier == tierHDD || p.timeSSD }

func (p *lookupProbes) probe(tier int, start time.Time, found bool, passedExistence bool) {
	p.n[tier]++
	if passedExistence && !found {
		p.fp[tier]++
	}
	if !p.timed(tier) {
		return
	}
	d := time.Since(start)
	if d >= slowProbe {
		p.slow[tier]++
	}
	mxTierProbeSeconds[tier].Observe(d.Seconds())
}

func (p *lookupProbes) done() {
	tierLookups.Add(1)
	if p.n[tierHDD] > 0 {
		tierHDDLookups.Add(1)
		mxTierProbesPerLook[tierHDD].Observe(float64(p.n[tierHDD]))
	}
	if p.timeSSD {
		mxTierProbesPerLook[tierSSD].Observe(float64(p.n[tierSSD]))
	}
	for tier := range p.n {
		addNonZero(&tierProbes[tier], p.n[tier])
		addNonZero(&tierSlowProbes[tier], p.slow[tier])
		addNonZero(&tierExistenceFP[tier], p.fp[tier])
	}
}

func addNonZero(c *atomic.Uint64, n uint64) {
	if n > 0 {
		c.Add(n)
	}
}

// TierProbeCounts are process totals of commitment file probes, for per-block deltas.
type TierProbeCounts struct {
	SSD, HDD, HDDSlow, Lookups, HDDLookups uint64
}

func ReadTierProbeCounts() TierProbeCounts {
	return TierProbeCounts{
		SSD:        tierProbes[tierSSD].Load(),
		HDD:        tierProbes[tierHDD].Load(),
		HDDSlow:    tierSlowProbes[tierHDD].Load(),
		Lookups:    tierLookups.Load(),
		HDDLookups: tierHDDLookups.Load(),
	}
}

func (c TierProbeCounts) Sub(o TierProbeCounts) TierProbeCounts {
	return TierProbeCounts{SSD: c.SSD - o.SSD, HDD: c.HDD - o.HDD, HDDSlow: c.HDDSlow - o.HDDSlow, Lookups: c.Lookups - o.Lookups, HDDLookups: c.HDDLookups - o.HDDLookups}
}

// PublishTierProbeCounts copies the probe totals into their gauges; the hot path only touches atomics.
func PublishTierProbeCounts() {
	for tier := range tierNames {
		mxTierProbes[tier].SetUint64(tierProbes[tier].Load())
		mxTierSlowProbes[tier].SetUint64(tierSlowProbes[tier].Load())
		mxTierExistenceFP[tier].SetUint64(tierExistenceFP[tier].Load())
	}
	mxTierLookups.SetUint64(tierLookups.Load())
	mxTierHDDLookups.SetUint64(tierHDDLookups.Load())
}
