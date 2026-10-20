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

package forkchoice

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/diagnostics/metrics"
)

// block_importing_latency must be recorded for a block imported in its own
// (current) slot, and skipped for older blocks (e.g. backfill), so the gauge
// reflects head-arrival latency rather than block age.
func TestCollectOnBlockLatency(t *testing.T) {
	ctrl := gomock.NewController(t)
	clock := eth_clock.NewMockEthereumClock(ctrl)
	g := metrics.GetOrCreateGauge("block_importing_latency")

	// A non-current-slot block (e.g. backfill) must not update the metric.
	g.Set(-1)
	collectOnBlockLatencyToUnixTime(clock, 99, 100)
	require.Equal(t, float64(-1), g.GetValue(), "non-current-slot block must not update the metric")

	// A current-slot block must record a non-negative latency.
	clock.EXPECT().GetSlotTime(uint64(100)).Return(time.Now().Add(-1500 * time.Millisecond))
	collectOnBlockLatencyToUnixTime(clock, 100, 100)
	v := g.GetValue()
	require.NotEqual(t, float64(-1), v, "current-slot block must update the metric")
	require.GreaterOrEqual(t, v, float64(0))
}
