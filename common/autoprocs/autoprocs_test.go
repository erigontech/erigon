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

func TestResizeHoldsOnJitter(t *testing.T) {
	if got := resize(12, 6, 6, log.New()); got != 12 {
		t.Fatalf("resize returned %d, want 12 held without a stop-the-world", got)
	}
}
