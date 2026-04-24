package cmd

import (
	"github.com/erigontech/erigon/cmd/pgwatch/internal/cluster"
	"github.com/erigontech/erigon/cmd/pgwatch/internal/metrics"
	"github.com/erigontech/erigon/cmd/pgwatch/internal/mincore"
	"github.com/erigontech/erigon/cmd/pgwatch/internal/pattern"
	"github.com/erigontech/erigon/cmd/pgwatch/internal/report"
	"github.com/erigontech/erigon/cmd/pgwatch/internal/sampler"
)

// buildResult derives a FileResult from a residency bitmap and optional snapshots.
func buildResult(path string, fileSize int64, residency []bool, sampled bool, snaps []sampler.Snapshot) report.FileResult {
	ps := mincore.PageSize()
	m := metrics.Compute(residency, ps)
	clusters := cluster.Detect(residency, ps, cluster.DefaultGapThreshold)
	gaps := cluster.InterGaps(clusters, ps)
	pat := pattern.Classify(m, clusters, snaps)

	return report.FileResult{
		Path:      path,
		FileSize:  fileSize,
		Sampled:   sampled,
		Residency: residency,
		Metrics:   m,
		Clusters:  clusters,
		Gaps:      gaps,
		Pattern:   pat,
		Snapshots: snaps,
	}
}

// deltaResidency returns a residency bitmap of pages newly loaded since before.
// A page is counted only if it is in after but not in before.
func deltaResidency(before, after []bool) []bool {
	n := len(after)
	if len(before) < n {
		n = len(before)
	}
	delta := make([]bool, len(after))
	for i := 0; i < n; i++ {
		delta[i] = after[i] && !before[i]
	}
	// Pages beyond before's range that appear in after are also new.
	for i := n; i < len(after); i++ {
		delta[i] = after[i]
	}
	return delta
}
