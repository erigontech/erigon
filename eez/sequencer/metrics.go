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

package sequencer

import (
	"sync"

	"github.com/erigontech/erigon/diagnostics/metrics"
)

type sequencerMetrics struct {
	produced      [3]metrics.Counter
	head          metrics.Gauge
	offGridSkips  metrics.Counter
	triggerFailed metrics.Counter
	witnessFailed metrics.Counter
	headsSigned   metrics.Counter
	headsSkipped  metrics.Counter
	headsDropped  metrics.Counter
}

var (
	metricsOnce sync.Once
	sm          *sequencerMetrics
)

// loadMetrics registers the sequencer's metrics with its first instance, so
// that a node without one exports none.
func loadMetrics() *sequencerMetrics {
	metricsOnce.Do(func() {
		sm = &sequencerMetrics{
			produced: [3]metrics.Counter{
				Live:   metrics.GetOrCreateCounter(`eez_sequencer_blocks_total{kind="live"}`),
				Future: metrics.GetOrCreateCounter(`eez_sequencer_blocks_total{kind="future"}`),
				Sync:   metrics.GetOrCreateCounter(`eez_sequencer_blocks_total{kind="sync"}`),
			},
			head:          metrics.GetOrCreateGauge("eez_sequencer_head"),
			offGridSkips:  metrics.GetOrCreateCounter("eez_sequencer_off_grid_skips_total"),
			triggerFailed: metrics.GetOrCreateCounter("eez_sequencer_trigger_failures_total"),
			witnessFailed: metrics.GetOrCreateCounter("eez_sequencer_witness_failures_total"),
			headsSigned:   metrics.GetOrCreateCounter("eez_sequencer_heads_signed_total"),
			headsSkipped:  metrics.GetOrCreateCounter("eez_sequencer_heads_skipped_total"),
			headsDropped:  metrics.GetOrCreateCounter("eez_sequencer_heads_dropped_total"),
		}
	})
	return sm
}
