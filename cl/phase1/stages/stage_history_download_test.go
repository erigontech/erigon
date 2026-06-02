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
)

// On any post-merge chain the EL block number is far larger than the beacon
// slot (e.g. mainnet block ~25.2M vs slot ~14.46M). When a snapshot gap sets
// an EL block-number floor, the backfill-finished decision must compare EL
// block progress against that floor — comparing the beacon slot instead trips
// trivially true and stops the backfill at the chain tip, leaving the gap
// unfilled (manifesting downstream as "parent's total difficulty not found").
func TestELBackfillFinished_GapUsesBlockNotSlot(t *testing.T) {
	const (
		bellatrixSlot = uint64(4_636_672) // a real beacon-slot floor
		frozenBlock   = uint64(25_073_000)
		headSlot      = uint64(14_460_640)
		headBlock     = uint64(25_224_522)
	)
	destBlock := frozenBlock - 1

	// At the chain tip, with a gap still to fill down to the frozen tip, the
	// backfill must NOT be considered finished.
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
