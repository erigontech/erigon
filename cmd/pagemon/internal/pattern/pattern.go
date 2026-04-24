package pattern

import (
	"time"

	"github.com/erigontech/erigon/cmd/pagemon/internal/cluster"
	"github.com/erigontech/erigon/cmd/pagemon/internal/metrics"
	"github.com/erigontech/erigon/cmd/pagemon/internal/sampler"
)

// Pattern is the access pattern tag emitted by the tool.
type Pattern string

const (
	Sequential           Pattern = "SEQUENTIAL"
	RandomScattered      Pattern = "RANDOM_SCATTERED"
	IndexLookupScattered Pattern = "INDEX_LOOKUP_SCATTERED"
	HotColdMixed         Pattern = "HOT_COLD_MIXED"
	Bursty               Pattern = "BURSTY"
	Unknown              Pattern = "UNKNOWN"
)

// Classify derives a Pattern from deterministic metrics, clusters, and temporal snapshots.
// snaps may be nil when temporal data is unavailable (snapshot/measure without watch).
func Classify(m metrics.Metrics, clusters []cluster.Cluster, snaps []sampler.Snapshot) Pattern {
	if m.PagesLoaded == 0 {
		return Unknown
	}

	nClusters := len(clusters)
	bursty := isBursty(snaps)

	switch {
	case m.Density > 0.7 && m.ScatterScore < 10:
		return Sequential

	case nClusters >= 2 && nClusters <= 8 && avgClusterPages(clusters) < 500 && bursty:
		return IndexLookupScattered

	case m.Density >= 0.2 && m.Density < 0.6 && nClusters > 1:
		return HotColdMixed

	case m.Density < 0.2 && m.ScatterScore > 100:
		return RandomScattered

	case bursty:
		return Bursty

	default:
		return Unknown
	}
}

func avgClusterPages(clusters []cluster.Cluster) float64 {
	if len(clusters) == 0 {
		return 0
	}
	var total int64
	for _, c := range clusters {
		total += c.PageCount
	}
	return float64(total) / float64(len(clusters))
}

// isBursty returns true if there are temporal gaps > 500ms between loading phases.
func isBursty(snaps []sampler.Snapshot) bool {
	if len(snaps) < 2 {
		return false
	}
	const burstGap = 500 * time.Millisecond
	for i := 1; i < len(snaps); i++ {
		if snaps[i].At-snaps[i-1].At > burstGap {
			return true
		}
	}
	return false
}
