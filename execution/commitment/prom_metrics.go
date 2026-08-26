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

// Round-level commitment metrics. Counters and a histogram rather than gauges
// holding pre-divided rates: rate() belongs in the query, so the window stays
// the reader's choice and a missed scrape costs a sample instead of an interval.
var (
	mxRounds = metrics.GetOrCreateCounter("commitment_rounds_total")

	// Buckets span a fast incremental block through a whale fold.
	mxRoundDuration = metrics.NewHistogram("commitment_round_duration_seconds",
		[]float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60})
)

// ObserveRound records one finished commitment round.
func ObserveRound(start time.Time) {
	mxRounds.Inc()
	mxRoundDuration.ObserveDuration(start)
}
