package cluster

// Cluster is a contiguous run of loaded pages separated from its neighbours by
// at most GapThreshold unloaded pages.
type Cluster struct {
	StartPage int64
	EndPage   int64 // inclusive
	PageCount int64
	SizeBytes int64
}

const DefaultGapThreshold = 32

// Detect groups loaded pages into clusters. Consecutive loaded pages are merged
// if the unloaded gap between them is < gapThreshold pages.
func Detect(residency []bool, pageSize int64, gapThreshold int) []Cluster {
	if gapThreshold <= 0 {
		gapThreshold = DefaultGapThreshold
	}
	var clusters []Cluster
	inCluster := false
	var start, lastLoaded int64

	for i, loaded := range residency {
		idx := int64(i)
		if loaded {
			if !inCluster {
				start = idx
				inCluster = true
			}
			lastLoaded = idx
		} else if inCluster {
			gap := idx - lastLoaded - 1
			if gap >= int64(gapThreshold) {
				clusters = append(clusters, newCluster(start, lastLoaded, pageSize))
				inCluster = false
			}
		}
	}
	if inCluster {
		clusters = append(clusters, newCluster(start, lastLoaded, pageSize))
	}
	return clusters
}

func newCluster(start, end, pageSize int64) Cluster {
	count := end - start + 1
	return Cluster{
		StartPage: start,
		EndPage:   end,
		PageCount: count,
		SizeBytes: count * pageSize,
	}
}

// InterGaps returns the byte-distance between each pair of consecutive clusters.
func InterGaps(clusters []Cluster, pageSize int64) []int64 {
	if len(clusters) < 2 {
		return nil
	}
	gaps := make([]int64, len(clusters)-1)
	for i := 1; i < len(clusters); i++ {
		gaps[i-1] = (clusters[i].StartPage - clusters[i-1].EndPage - 1) * pageSize
	}
	return gaps
}
