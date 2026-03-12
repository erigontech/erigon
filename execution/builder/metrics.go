package builder

import "github.com/erigontech/erigon/diagnostics/metrics"

var (
	mxBlockBuildDuration = metrics.GetOrCreateSummary("block_build_duration_seconds")
)
