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
	"math"
	"testing"
	"time"
)

// The EL head (lowestBlockToReach) can advance past the highestBlockSeen frozen
// at download start; the unsigned progress math must not underflow into a ~2^64
// total and a garbage ETA.
func TestHistoryDownloadProgress_ELHeadPastFrozenTip(t *testing.T) {
	const (
		highestBlockSeen   = uint64(23_000_000) // EL block number frozen at download start
		lowestBlockToReach = uint64(23_123_953) // live EL head, 123953 blocks higher
		currentBlock       = uint64(22_983_559) // downloader 16441 below the frozen top
	)
	const speed = 12.9

	processed, toprocess, eta := historyDownloadProgress(highestBlockSeen, lowestBlockToReach, currentBlock, speed)

	if toprocess > highestBlockSeen {
		t.Fatalf("toprocess underflowed: got %d, want <= %d", toprocess, highestBlockSeen)
	}
	if processed > toprocess {
		t.Fatalf("processed (%d) must not exceed toprocess (%d)", processed, toprocess)
	}
	if eta < 0 {
		t.Fatalf("ETA must not be negative, got %s", eta)
	}
}

// clampProgress must never report a total below processed nor underflow, even
// when the floor sits above the frozen top or the current position is out of range.
func TestClampProgress(t *testing.T) {
	cases := []struct {
		name                     string
		highest, floor, current  uint64
		wantProcessed, wantTotal uint64
	}{
		{"normal", 100, 20, 60, 40, 80},
		{"floor above top", 100, 150, 60, 0, 0},
		{"current above top", 100, 20, 200, 0, 80},
		{"current below floor", 100, 20, 5, 80, 80},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			processed, total := clampProgress(tc.highest, tc.floor, tc.current)
			if processed != tc.wantProcessed || total != tc.wantTotal {
				t.Fatalf("clampProgress(%d,%d,%d) = (%d,%d), want (%d,%d)",
					tc.highest, tc.floor, tc.current, processed, total, tc.wantProcessed, tc.wantTotal)
			}
			if processed > total {
				t.Fatalf("processed (%d) exceeds total (%d)", processed, total)
			}
		})
	}
}

// A very slow download must not overflow time.Duration into a negative ETA.
func TestHistoryDownloadProgress_SlowSpeedNoETAOverflow(t *testing.T) {
	const (
		highestBlockSeen   = uint64(23_000_000)
		lowestBlockToReach = uint64(1)
		currentBlock       = uint64(23_000_000)
	)
	const speed = 0.0001

	_, _, eta := historyDownloadProgress(highestBlockSeen, lowestBlockToReach, currentBlock, speed)
	if eta < 0 {
		t.Fatalf("ETA must not be negative on slow speed, got %s", eta)
	}
}

// Normal case: EL head below the frozen top, downloader descending toward it.
func TestHistoryDownloadProgress_Normal(t *testing.T) {
	const (
		highestBlockSeen   = uint64(23_000_000)
		lowestBlockToReach = uint64(22_000_000)
		currentBlock       = uint64(22_500_000)
	)
	const speed = 10.0

	processed, toprocess, eta := historyDownloadProgress(highestBlockSeen, lowestBlockToReach, currentBlock, speed)
	if toprocess != 1_000_000 {
		t.Fatalf("toprocess = %d, want 1000000", toprocess)
	}
	if processed != 500_000 {
		t.Fatalf("processed = %d, want 500000", processed)
	}
	if want := 50_000 * time.Second; eta != want {
		t.Fatalf("eta = %s, want %s", eta, want)
	}
}

// Post-merge the EL block number exceeds the beacon slot, so a snapshot-gap
// floor must be compared against EL block progress, not the slot.
func TestELBackfillFinished_GapUsesBlockNotSlot(t *testing.T) {
	const (
		bellatrixSlot = uint64(4_636_672) // a real beacon-slot floor
		frozenBlock   = uint64(25_073_000)
		headSlot      = uint64(14_460_640)
		headBlock     = uint64(25_224_522)
	)
	destBlock := frozenBlock - 1

	if elBackfillFinished(headSlot, headBlock, bellatrixSlot, destBlock) {
		t.Fatalf("backfill reported finished at the tip (slot=%d block=%d) while gap down to block %d is unfilled",
			headSlot, headBlock, destBlock)
	}

	// Once EL block progress has descended to the frozen tip, it is finished.
	if !elBackfillFinished(headSlot-150_000, destBlock, bellatrixSlot, destBlock) {
		t.Fatalf("backfill should be finished once EL block progress reaches the frozen tip (block %d)", destBlock)
	}
}

// Without a snapshot gap, the EL block floor is unset and completion is driven
// purely by the beacon-slot floor (normal Deneb backfill toward the merge).
func TestELBackfillFinished_NoGapUsesSlotFloor(t *testing.T) {
	const bellatrixSlot = uint64(4_636_672)
	noBlockFloor := uint64(math.MaxUint64)

	if elBackfillFinished(bellatrixSlot+1, 20_000_000, bellatrixSlot, noBlockFloor) {
		t.Fatal("backfill must continue while still above the beacon-slot floor")
	}
	if !elBackfillFinished(bellatrixSlot, 20_000_000, bellatrixSlot, noBlockFloor) {
		t.Fatal("backfill must finish once the beacon-slot floor is reached")
	}
}
