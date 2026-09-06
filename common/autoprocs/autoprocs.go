// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

// Package autoprocs sizes GOMAXPROCS for a process that reads state through
// mmap. An mmap page fault is a CPU trap, not a syscall, so the runtime never
// learns the thread blocked and the P stays held for the whole device
// round-trip. Concurrent faults are therefore capped at GOMAXPROCS, and a
// machine whose CPU count matches its core count cannot keep an NVMe device
// busy.
//
// Faults arrive in bursts far shorter than a second, so the mean queue depth
// says nothing: it stays near zero while bursts still saturate every P. The
// controller samples the fault rate fast enough to see a burst and sizes
// GOMAXPROCS from the burst, not from the average.
package autoprocs

import (
	"context"
	"runtime"
	"slices"
	"time"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
)

var (
	enabled  = dbg.EnvBool("AUTO_GOMAXPROCS", false)
	interval = time.Duration(max(1, dbg.EnvInt("AUTO_GOMAXPROCS_SEC", 5))) * time.Second
	maxRatio = max(1, dbg.EnvInt("AUTO_GOMAXPROCS_MAX_RATIO", 8))
	// Device time to overlap per fault, the knob that carries the storage
	// geometry. Little's law turns the burst fault rate into a queue depth,
	// but the raw NVMe service time of ~70us undershoots: a burst shorter
	// than one sample is averaged down inside its window, and demand measured
	// under the current cap cannot show what a larger cap would absorb.
	overlapMicros = max(1, dbg.EnvInt("AUTO_GOMAXPROCS_OVERLAP_US", 280))
)

const samplePeriod = 10 * time.Millisecond

// Start runs the controller until ctx is done. It is a no-op unless
// AUTO_GOMAXPROCS is set, and on platforms with no major-fault counter.
func Start(ctx context.Context, logger log.Logger) {
	if !enabled {
		return
	}
	if _, ok := majorFaults(); !ok {
		logger.Info("[autoprocs] unsupported on this platform")
		return
	}
	go loop(ctx, runtime.GOMAXPROCS(0), logger)
}

func loop(ctx context.Context, base int, logger log.Logger) {
	t := time.NewTicker(samplePeriod)
	defer t.Stop()
	rates := make([]float64, 0, int(interval/samplePeriod))
	prev, _ := majorFaults()
	prevAt := time.Now()
	cur := base
	for {
		select {
		case <-ctx.Done():
			runtime.GOMAXPROCS(base)
			return
		case now := <-t.C:
			n, ok := majorFaults()
			if !ok {
				continue
			}
			if dt := now.Sub(prevAt).Seconds(); dt > 0 {
				rates = append(rates, float64(n-prev)/dt)
			}
			prev, prevAt = n, now
			if len(rates) < cap(rates) {
				continue
			}
			// burstDepth sorts in place, so the window must be dropped after.
			cur = resize(cur, base, burstDepth(rates), logger)
			rates = rates[:0]
		}
	}
}

// burstDepth is the number of faults in flight while a burst is running. The
// high percentile, not the mean, is what has to fit inside GOMAXPROCS.
func burstDepth(rates []float64) int {
	if len(rates) == 0 {
		return 0
	}
	slices.Sort(rates)
	return int(rates[len(rates)*9/10] * float64(overlapMicros) / 1e6)
}

func resize(cur, base, depth int, logger log.Logger) int {
	want := min(base+depth, base*maxRatio)
	// GOMAXPROCS stops the world, so ignore single-slot jitter.
	if want >= cur-1 && want <= cur+1 {
		return cur
	}
	runtime.GOMAXPROCS(want)
	logger.Info("[autoprocs] resized", "from", cur, "to", want, "burstDepth", depth)
	return want
}
