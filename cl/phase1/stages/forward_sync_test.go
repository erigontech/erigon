// Copyright 2024 The Erigon Authors
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

package stages

import (
	"testing"
	"time"
)

// currentSlot can overshoot the captured chainTipSlot; the distance/ETA math
// must not underflow into a ~2^64 slot count and overflow time.Duration.
func TestForwardSyncProgress_CurrentSlotPastChainTip(t *testing.T) {
	dist, eta := forwardSyncProgress(1_000_000, 1_000_050, 999_900, 12, 30)
	if dist != 0 {
		t.Fatalf("distFromChainTip = %s, want 0 when current slot is past the tip", dist)
	}
	if eta < 0 {
		t.Fatalf("ETA must not be negative, got %s", eta)
	}
}

// A reorg can drop currentSlot below prevProgress; the rate denominator must not
// underflow and drive the ETA to a garbage value.
func TestForwardSyncProgress_ReorgBelowPrevProgress(t *testing.T) {
	dist, eta := forwardSyncProgress(1_000_000, 900_000, 950_000, 12, 30)
	if dist < 0 {
		t.Fatalf("distFromChainTip must not be negative, got %s", dist)
	}
	if want := 999 * time.Hour; eta != want {
		t.Fatalf("ETA = %s, want %s (default when no forward progress)", eta, want)
	}
}

// Normal case: distance is slots-remaining × seconds-per-slot, ETA scales with rate.
func TestForwardSyncProgress_Normal(t *testing.T) {
	dist, eta := forwardSyncProgress(1_000_000, 900_000, 899_700, 12, 30)
	if want := 100_000 * 12 * time.Second; dist != want {
		t.Fatalf("distFromChainTip = %s, want %s", dist, want)
	}
	// rate = (900000-899700)/30 = 10 slots/s; eta = 100000/10 = 10000s.
	if want := 10_000 * time.Second; eta != want {
		t.Fatalf("ETA = %s, want %s", eta, want)
	}
}

func TestBoundedDuration(t *testing.T) {
	if got := boundedDuration(-5); got != 0 {
		t.Fatalf("boundedDuration(negative) = %s, want 0", got)
	}
	if got := boundedDuration(0); got != 0 {
		t.Fatalf("boundedDuration(0) = %s, want 0", got)
	}
	if got := boundedDuration(42); got != 42*time.Second {
		t.Fatalf("boundedDuration(42) = %s, want 42s", got)
	}
	// Absurdly large second count must saturate, not wrap negative.
	if got := boundedDuration(1e18); got != time.Duration(1<<63-1) {
		t.Fatalf("boundedDuration(1e18) = %s, want max duration", got)
	}
}
