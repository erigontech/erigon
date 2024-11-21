package shuffling_metrics

import (
	"time"

	"github.com/erigontech/erigon-lib/metrics"
)

var (
	// shuffling metrics
	computeShuffledIndicies = metrics.GetOrCreateGauge("compute_shuffled_indices")
)

// ObserveComputeShuffledIndiciesTime sets computeShuffledIndicies time
func ObserveComputeShuffledIndiciesTime(startTime time.Time) {
	computeShuffledIndicies.Set(float64(time.Since(startTime).Microseconds()))
}
