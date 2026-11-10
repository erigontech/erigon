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

package overlay

import (
	"sync"

	"github.com/erigontech/erigon/diagnostics/metrics"
)

type overlayMetrics struct {
	// The messages from peers, and their verdicts.
	received, accepted, ignored, rejected metrics.Counter
	// dropped counts the accepted heads that found Heads full.
	dropped metrics.Counter
	peers   metrics.Gauge
}

var (
	metricsOnce sync.Once
	om          *overlayMetrics
)

// loadMetrics registers the overlay's metrics with its first node, so that
// a node without one exports none.
func loadMetrics() *overlayMetrics {
	metricsOnce.Do(func() {
		om = &overlayMetrics{
			received: metrics.GetOrCreateCounter("eez_p2p_message_received_total"),
			accepted: metrics.GetOrCreateCounter("eez_p2p_message_accepted_total"),
			ignored:  metrics.GetOrCreateCounter("eez_p2p_message_ignored_total"),
			rejected: metrics.GetOrCreateCounter("eez_p2p_message_rejected_total"),
			dropped:  metrics.GetOrCreateCounter("eez_p2p_inbound_dropped_total"),
			peers:    metrics.GetOrCreateGauge("eez_p2p_peers"),
		}
	})
	return om
}
