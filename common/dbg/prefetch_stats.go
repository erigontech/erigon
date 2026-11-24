package dbg

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
)

// PrefetchStats accumulates the cost of the PrefetchLatest calls made under one context.
type PrefetchStats struct {
	Calls, KeysIn, KeysProbed, Waves, MaxWaves, Pages, Incomplete atomic.Int64
	Touches, Mincores, MincoreNs, FilterNs, ProbeNs, ReadNs       atomic.Int64
	WaveHist                                                      [65]atomic.Int64 // calls by wave count
}

var WarmBALStatsInfo = EnvBool("WARMBAL_STATS", false)

type prefetchStatsKey struct{}

func WithPrefetchStats(ctx context.Context, s *PrefetchStats) context.Context {
	return context.WithValue(ctx, prefetchStatsKey{}, s)
}

func PrefetchStatsFrom(ctx context.Context) *PrefetchStats {
	s, _ := ctx.Value(prefetchStatsKey{}).(*PrefetchStats)
	return s
}

func (s *PrefetchStats) ObserveWaves(waves int) {
	s.Waves.Add(int64(waves))
	s.WaveHist[min(waves, len(s.WaveHist)-1)].Add(1)
	for {
		m := s.MaxWaves.Load()
		if int64(waves) <= m || s.MaxWaves.CompareAndSwap(m, int64(waves)) {
			return
		}
	}
}

func (s *PrefetchStats) String() string {
	var hist strings.Builder
	for i := range s.WaveHist {
		if n := s.WaveHist[i].Load(); n > 0 {
			fmt.Fprintf(&hist, "%d:%d,", i, n)
		}
	}
	ms := func(ns int64) float64 { return float64(ns) / 1e6 }
	return fmt.Sprintf("calls=%d keys=%d probed=%d waves=%d maxWaves=%d waveHist=%s pages=%d incomplete=%d touches=%d mincores=%d mincoreMs=%.3f filterMs=%.3f probeMs=%.3f readMs=%.3f",
		s.Calls.Load(), s.KeysIn.Load(), s.KeysProbed.Load(), s.Waves.Load(), s.MaxWaves.Load(), strings.TrimSuffix(hist.String(), ","),
		s.Pages.Load(), s.Incomplete.Load(), s.Touches.Load(), s.Mincores.Load(), ms(s.MincoreNs.Load()),
		ms(s.FilterNs.Load()), ms(s.ProbeNs.Load()), ms(s.ReadNs.Load()))
}
