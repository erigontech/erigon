package bench

import (
	"fmt"
	"slices"
)

type agg struct {
	Block uint64

	Cold []float64 // first latencies
	Warm []float64 // averages (per-tx)
}

func CompareResults(old BenchOutput, newer BenchOutput) string {
	oldAgg := aggregateByBlock(old)
	newAgg := aggregateByBlock(newer)

	keys := make(map[uint64]struct{})
	for k := range oldAgg {
		keys[k] = struct{}{}
	}
	for k := range newAgg {
		keys[k] = struct{}{}
	}
	blocks := make([]uint64, 0, len(keys))
	for k := range keys {
		blocks = append(blocks, k)
	}
	slices.Sort(blocks)

	out := ""
	out += "FIRST-LATENCY (cold)\n"
	out += "name\told avg ms\tnew avg ms\tdelta\n"
	for _, b := range blocks {
		oc, ok1 := oldAgg[b]
		nc, ok2 := newAgg[b]
		if !ok1 || !ok2 || len(oc.Cold) == 0 || len(nc.Cold) == 0 {
			continue
		}
		om := mean(oc.Cold)
		nm := mean(nc.Cold)
		delta := pctDelta(om, nm)
		out += fmt.Sprintf("block_%d\t%.3f\t%.3f\t%+.1f%%\n", b, om, nm, delta)
	}

	out += "\nWARM-LATENCY (avg of repeats)\n"
	out += "name\told avg ms\tnew avg ms\tdelta\n"
	for _, b := range blocks {
		ow, ok1 := oldAgg[b]
		nw, ok2 := newAgg[b]
		if !ok1 || !ok2 || len(ow.Warm) == 0 || len(nw.Warm) == 0 {
			continue
		}
		om := mean(ow.Warm)
		nm := mean(nw.Warm)
		delta := pctDelta(om, nm)
		out += fmt.Sprintf("block_%d\t%.3f\t%.3f\t%+.1f%%\n", b, om, nm, delta)
	}

	return out
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
