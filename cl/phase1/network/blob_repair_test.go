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

package network

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDrainBlobGapsFillsEverythingUnderTheLimit(t *testing.T) {
	var seen []uint64
	filled, attempted := drainBlobGaps([]uint64{1, 2, 3}, 10, func(slot uint64) bool {
		seen = append(seen, slot)
		return true
	})

	require.Equal(t, 3, filled)
	require.Equal(t, 3, attempted)
	require.Equal(t, []uint64{1, 2, 3}, seen)
}

// A backlog of thousands against a per-request timeout would occupy one pass for hours.
// The drain has to stop and leave the rest for the next pass instead of blocking.
func TestDrainBlobGapsStopsAtTheLimit(t *testing.T) {
	var seen []uint64
	filled, attempted := drainBlobGaps([]uint64{1, 2, 3, 4, 5}, 2, func(slot uint64) bool {
		seen = append(seen, slot)
		return true
	})

	require.Equal(t, 2, filled)
	require.Equal(t, 2, attempted)
	require.Equal(t, []uint64{1, 2}, seen, "must take the oldest slots first, not a random subset")
}

// A slot no endpoint can serve must not consume the whole budget's worth of progress
// reporting: it is attempted but not filled, and the drain carries on.
func TestDrainBlobGapsCountsOnlyFilledSlots(t *testing.T) {
	filled, attempted := drainBlobGaps([]uint64{1, 2, 3, 4}, 10, func(slot uint64) bool {
		return slot%2 == 0
	})

	require.Equal(t, 2, filled)
	require.Equal(t, 4, attempted)
}

func TestDrainBlobGapsIsInertWithoutBudgetOrWork(t *testing.T) {
	filled, attempted := drainBlobGaps([]uint64{1, 2}, 0, func(uint64) bool {
		t.Fatal("repair must not run without budget")
		return false
	})
	require.Zero(t, filled)
	require.Zero(t, attempted)

	filled, attempted = drainBlobGaps(nil, 10, func(uint64) bool {
		t.Fatal("repair must not run without work")
		return false
	})
	require.Zero(t, filled)
	require.Zero(t, attempted)
}

// The repair runs on its own goroutine because downloadOnce holds run()'s goroutine for
// the length of a full walk. Both it and the end-of-pass call can therefore fire at once,
// and two concurrent drains would fetch the same slots twice.
func TestRepairIsSingleFlight(t *testing.T) {
	b := &BlobHistoryDownloader{}

	require.True(t, b.tryBeginRepair(), "first caller must win")
	require.False(t, b.tryBeginRepair(), "second caller must be refused while one is running")

	b.endRepair()
	require.True(t, b.tryBeginRepair(), "must be reusable once the first finishes")
}
