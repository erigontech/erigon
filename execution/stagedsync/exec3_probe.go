package stagedsync

import (
	"context"
	"runtime/metrics"
	"sync/atomic"
	"time"
)

var peakGoroutines, peakStackBytes atomic.Int64

// startPeakSampler records the peak goroutine count and stack memory (dbg probe).
func startPeakSampler(ctx context.Context) {
	go func() {
		s := []metrics.Sample{{Name: "/sched/goroutines:goroutines"}, {Name: "/memory/classes/heap/stacks:bytes"}}
		t := time.NewTicker(2 * time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				metrics.Read(s)
				storeMax(&peakGoroutines, int64(s[0].Value.Uint64()))
				storeMax(&peakStackBytes, int64(s[1].Value.Uint64()))
			}
		}
	}()
}

func storeMax(a *atomic.Int64, v int64) {
	for {
		o := a.Load()
		if v <= o || a.CompareAndSwap(o, v) {
			return
		}
	}
}

const (
	rerunSpine = iota
	rerunDep
	rerunInvalid
	rerunRecheck
	rerunErr
	rerunCauses
)

type rerunProbe struct {
	runs, runNs                       atomic.Int64
	reruns, wasteNs                   [rerunCauses]atomic.Int64
	parks, parkNs, parked, peakParked atomic.Int64
}
