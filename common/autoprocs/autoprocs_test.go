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

package autoprocs

import (
	"testing"

	"github.com/erigontech/erigon/common/log/v3"
)

func TestBurstDepthIgnoresIdleSamples(t *testing.T) {
	// A burst covering a tenth of the window still has to fit in GOMAXPROCS,
	// even though the mean rate over the window is near zero.
	rates := make([]float64, 100)
	for i := 90; i < 100; i++ {
		rates[i] = 100_000 // 100k faults/s at the 280us default overlap is 28
	}
	if got := burstDepth(rates); got != 28 {
		t.Fatalf("burstDepth = %d, want 28", got)
	}
	if got := burstDepth(make([]float64, 100)); got != 0 {
		t.Fatalf("idle burstDepth = %d, want 0", got)
	}
}

// A measured run cycled 14 -> 12 -> 16 -> 14 indefinitely, each step a
// stop-the-world, because raising GOMAXPROCS raises the fault rate that
// measured the demand. Alternating demand has to settle instead.
func TestResizeDoesNotOscillate(t *testing.T) {
	const base = 6
	cur, lowFor := base, 0
	settled := map[int]int{}
	for i := range 40 {
		depth := 8 // wants base+8 = 14
		if i%2 == 1 {
			depth = 6 // wants base+6 = 12
		}
		cur, lowFor = resize(cur, base, depth, lowFor, log.New())
		if i >= 20 {
			settled[cur]++
		}
	}
	if len(settled) != 1 {
		t.Fatalf("still oscillating over the last 20 windows: %v", settled)
	}
}

func TestResizeDecaysOnlyAfterSustainedLowDemand(t *testing.T) {
	const base = 6
	cur, lowFor := base, 0
	cur, lowFor = resize(cur, base, 8, lowFor, log.New())
	if cur != 14 {
		t.Fatalf("cur = %d, want 14", cur)
	}
	for range decayPeriods - 1 {
		cur, lowFor = resize(cur, base, 0, lowFor, log.New())
		if cur != 14 {
			t.Fatalf("gave a slot back too early: cur = %d, want 14", cur)
		}
	}
	if cur, _ = resize(cur, base, 0, lowFor, log.New()); cur != 13 {
		t.Fatalf("cur = %d, want 13 after sustained low demand", cur)
	}
}
