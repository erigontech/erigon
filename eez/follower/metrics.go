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
	"fmt"
	"sync"

	"github.com/erigontech/erigon/diagnostics/metrics"
)

// phases are the values of Phase.
var phases = []Phase{PhaseBooting, PhaseWaitingForFinality, PhaseSyncing, PhaseWaitingForPeers, PhaseDeriving, PhaseFollowing}

type followerMetrics struct {
	l1Head, l1Scanned, l1Finalized, l1Lag metrics.Gauge
	l2Cursor, l2Safe, l2Finalized         metrics.Gauge
	l2Peers                               metrics.Gauge
	reorgs, divergences                   metrics.Counter
	sync                                  metrics.Histogram
	// phase is 1 for the current phase, 0 for the others.
	phase map[Phase]metrics.Gauge
	// The signed-head fork choice: moves of the head that keep the previous
	// head canonical and that drop it, decisions held or waiting for
	// derivation, failed fetches, and heads not followed, once per head and
	// reason.
	headAdopted, headRewound, headHeld, headAhead, headFetchFailed, headRejected metrics.Counter
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
			l2Peers:     metrics.GetOrCreateGauge("eez_l2_peers"),
			reorgs:      metrics.GetOrCreateCounter("eez_l1_reorgs_total"),
			divergences: metrics.GetOrCreateCounter("eez_divergences_total"),
			sync:        metrics.GetOrCreateHistogram("eez_sync_seconds"),
			phase:       make(map[Phase]metrics.Gauge, len(phases)),

			headAdopted:     metrics.GetOrCreateCounter("eez_head_adopted_total"),
			headRewound:     metrics.GetOrCreateCounter("eez_head_rewound_total"),
			headHeld:        metrics.GetOrCreateCounter("eez_head_held_total"),
			headAhead:       metrics.GetOrCreateCounter("eez_head_ahead_total"),
			headFetchFailed: metrics.GetOrCreateCounter("eez_head_fetch_failures_total"),
			headRejected:    metrics.GetOrCreateCounter("eez_head_rejected_total"),
		}
		for _, p := range phases {
			fm.phase[p] = metrics.GetOrCreateGauge(fmt.Sprintf("eez_sync_phase{phase=%q}", p))
		}
	})
	return fm
}
