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

package deriver

import (
	"sync"

	"github.com/erigontech/erigon/diagnostics/metrics"
)

type deriverMetrics struct {
	blocks metrics.Counter
}

var (
	metricsOnce sync.Once
	dm          *deriverMetrics
)

// loadMetrics registers the deriver's metrics on first use, so that a node
// that derives nothing exports none.
func loadMetrics() *deriverMetrics {
	metricsOnce.Do(func() {
		dm = &deriverMetrics{blocks: metrics.GetOrCreateCounter("eez_l2_blocks_derived_total")}
	})
	return dm
}
