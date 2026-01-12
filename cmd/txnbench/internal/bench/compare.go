package bench

import (
	"fmt"
	"slices"
	"strings"
)

type agg struct {
	Block uint64

	Cold []float64 // first latencies
	Warm []float64 // averages (per-tx)
}

// CompareResults groups all blocks into 10 clusters and compares averaged cold & warm latencies.
func CompareResults(old BenchOutput, newer BenchOutput) string {
	const clusters = 10 // number of clusters for grouping

	oldAgg := aggregateByBlock(old)
	newAgg := aggregateByBlock(newer)

	// collect common block numbers
	keys := make([]uint64, 0, len(oldAgg))
	for k := range oldAgg {
		if _, ok := newAgg[k]; ok {
			keys = append(keys, k)
		}
	}
	if len(keys) == 0 {
		return "no overlapping blocks\n"
	}
	slices.Sort(keys)

	clusterSize := len(keys) / clusters
	if clusterSize == 0 {
		clusterSize = 1
	}

	type cluster struct {
		start, end           uint64
		count                int
		oldCold, newCold     float64
		oldWarm, newWarm     float64
		deltaCold, deltaWarm float64
	}
	results := make([]cluster, 0, clusters)

	for i := 0; i < clusters; i++ {
		startIdx := i * clusterSize
		endIdx := startIdx + clusterSize
		if i == clusters-1 || endIdx > len(keys) {
			endIdx = len(keys)
		}
		sub := keys[startIdx:endIdx]
		if len(sub) == 0 {
			continue
		}

		var oldColdSum, newColdSum, oldWarmSum, newWarmSum float64
		var cnt int
		for _, b := range sub {
			oc, nc := oldAgg[b], newAgg[b]
			if len(oc.Cold) == 0 || len(nc.Cold) == 0 {
				continue
			}
			oldColdSum += mean(oc.Cold)
			newColdSum += mean(nc.Cold)
			if len(oc.Warm) > 0 && len(nc.Warm) > 0 {
				oldWarmSum += mean(oc.Warm)
				newWarmSum += mean(nc.Warm)
			}
			cnt++
		}
		if cnt == 0 {
			continue
		}
		oldColdAvg := oldColdSum / float64(cnt)
		newColdAvg := newColdSum / float64(cnt)
		oldWarmAvg := oldWarmSum / float64(cnt)
		newWarmAvg := newWarmSum / float64(cnt)

		results = append(results, cluster{
			start:     sub[0],
			end:       sub[len(sub)-1],
			count:     cnt,
			oldCold:   oldColdAvg,
			newCold:   newColdAvg,
			oldWarm:   oldWarmAvg,
			newWarm:   newWarmAvg,
			deltaCold: pctDelta(oldColdAvg, newColdAvg),
			deltaWarm: pctDelta(oldWarmAvg, newWarmAvg),
		})
	}

	var out strings.Builder
	out.WriteString("LATENCY COMPARISON (averaged over clusters)\n")
	out.WriteString("cluster_range\tblocks\told_cold_ms\tnew_cold_ms\tdelta_cold\told_warm_ms\tnew_warm_ms\tdelta_warm\n")

	for i, c := range results {
		out.WriteString(fmt.Sprintf(
			"%2d: %d–%d\t%d\t%.3f\t%.3f\t%+.1f%%\t%.3f\t%.3f\t%+.1f%%\n",
			i+1, c.start, c.end, c.count,
			c.oldCold, c.newCold, c.deltaCold,
			c.oldWarm, c.newWarm, c.deltaWarm,
		))
	}

	return out.String()
}

func aggregateByBlock(out BenchOutput) map[uint64]agg {
	m := make(map[uint64]agg)
	for _, r := range out.Results {
		a := m[r.BlockNumber]
		a.Block = r.BlockNumber
		a.Cold = append(a.Cold, r.FirstLatencyMs)
		a.Warm = append(a.Warm, r.AvgLatencyMs)
		m[r.BlockNumber] = a
	}
	return m
}

func mean(xs []float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	var s float64
	for _, v := range xs {
		s += v
	}
	return s / float64(len(xs))
}

func pctDelta(old, new float64) float64 {
	if old == 0 {
		if new == 0 {
			return 0
		}
		return 100
	}
	return (new/old - 1.0) * 100.0
}
