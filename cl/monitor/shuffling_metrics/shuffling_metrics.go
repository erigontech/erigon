package shuffling_metrics

import (
	"time"

	"github.com/erigontech/erigon/diagnostics/metrics"
)

// shuffling metrics
var computeShuffledIndicies = metrics.GetOrCreateGauge("compute_shuffled_indices")

// ActiveValidatorsIndicesComputed counts full registry scans of GetActiveValidatorsIndices.
var ActiveValidatorsIndicesComputed = metrics.GetOrCreateCounter("caplin_active_validators_indices_computed_total")

// ObserveComputeShuffledIndiciesTime sets computeShuffledIndicies time
func ObserveComputeShuffledIndiciesTime(startTime time.Time) {
	computeShuffledIndicies.Set(float64(time.Since(startTime).Microseconds()))
}
