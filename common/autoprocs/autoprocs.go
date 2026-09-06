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
	enabled = dbg.EnvBool("AUTO_GOMAXPROCS", false)
	// Device time to overlap per fault, the knob that carries the storage
	// geometry. Little's law turns the burst fault rate into a queue depth,
	// but the raw NVMe service time of ~70us undershoots: a burst shorter
	// than one sample is averaged down inside its window, and demand measured
	// under the current cap cannot show what a larger cap would absorb.
	// Too small only under-raises, so slower storage degrades toward off; too
	// large over-raises, which costs throughput on CPU-bound work.
	overlapMicros = max(1, dbg.EnvInt("AUTO_GOMAXPROCS_OVERLAP_US", 280))
)

const (
	samplePeriod = 10 * time.Millisecond
	interval     = 5 * time.Second
	maxRatio     = 8
	// Consecutive windows of lower demand before giving a slot back. Releasing
	// eagerly would undo the raise that produced the demand and start a cycle.
	decayPeriods = 3
)

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
	go loop(ctx, logger)
}

func loop(ctx context.Context, logger log.Logger) {
	base := runtime.GOMAXPROCS(0)
	t := time.NewTicker(samplePeriod)
	defer t.Stop()
	rates := make([]float64, 0, int(interval/samplePeriod))
	prev, _ := majorFaults()
	prevAt := time.Now()
	cur, lowFor := base, 0
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			n, _ := majorFaults()
			if dt := now.Sub(prevAt).Seconds(); dt > 0 {
				rates = append(rates, float64(n-prev)/dt)
			}
			prev, prevAt = n, now
			if len(rates) < cap(rates) {
				continue
			}
			want, low := nextSize(cur, base, burstDepth(rates), lowFor)
			lowFor = low
			if want != cur {
				runtime.GOMAXPROCS(want)
				logger.Info("[autoprocs] resized", "from", cur, "to", want)
				cur = want
			}
			rates = rates[:0]
		}
	}
}

// burstDepth is the number of faults in flight while a burst is running. The
// high percentile, not the mean, is what has to fit inside GOMAXPROCS.
func burstDepth(rates []float64) int {
	slices.Sort(rates)
	return int(rates[len(rates)*9/10] * float64(overlapMicros) / 1e6)
}

// nextSize moves cur towards the demand implied by depth. Raising GOMAXPROCS
// raises the fault rate that measured the demand in the first place, so acting
// on each sample makes the loop ring: only ratchet up, and step down a slot at a
// time once demand has stayed low for a while.
func nextSize(cur, base, depth, lowFor int) (int, int) {
	want := min(base+depth, base*maxRatio)
	switch {
	case want > cur:
		return want, 0
	case want < cur:
		if lowFor+1 < decayPeriods {
			return cur, lowFor + 1
		}
		return cur - 1, 0
	default: // demand matches the current size, which is not low demand
		return cur, 0
	}
}
