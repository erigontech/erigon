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

package follower

import (
	"sync"

	"github.com/erigontech/erigon/diagnostics/metrics"
)

type followerMetrics struct {
	l1Head, l1Scanned, l1Finalized, l1Lag metrics.Gauge
	l2Cursor, l2Safe, l2Finalized         metrics.Gauge
	reorgs, divergences                   metrics.Counter
	sync                                  metrics.Histogram
}

var (
	metricsOnce sync.Once
	fm          *followerMetrics
)

// loadMetrics registers the follower's metrics with its first instance, so
// that a node without one exports none.
func loadMetrics() *followerMetrics {
	metricsOnce.Do(func() {
		fm = &followerMetrics{
			l1Head:      metrics.GetOrCreateGauge("eez_l1_head"),
			l1Scanned:   metrics.GetOrCreateGauge("eez_l1_scanned"),
			l1Finalized: metrics.GetOrCreateGauge("eez_l1_finalized"),
			l1Lag:       metrics.GetOrCreateGauge("eez_l1_lag_blocks"),
			l2Cursor:    metrics.GetOrCreateGauge("eez_l2_cursor"),
			l2Safe:      metrics.GetOrCreateGauge("eez_l2_safe"),
			l2Finalized: metrics.GetOrCreateGauge("eez_l2_finalized"),
			reorgs:      metrics.GetOrCreateCounter("eez_l1_reorgs_total"),
			divergences: metrics.GetOrCreateCounter("eez_divergences_total"),
			sync:        metrics.GetOrCreateHistogram("eez_sync_seconds"),
		}
	})
	return fm
}
