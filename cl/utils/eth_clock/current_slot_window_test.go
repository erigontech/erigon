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

package eth_clock

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/common"
)

const (
	testGenesisTime    = 1_000_000
	testSecondsPerSlot = 12
	testSlot           = 1000
)

func testSlotStart(slot uint64) time.Time {
	return time.Unix(int64(testGenesisTime+testSecondsPerSlot*slot), 0)
}

func clockAt(now time.Time) EthereumClock {
	clock := NewEthereumClock(testGenesisTime, common.Hash{}, &clparams.BeaconChainConfig{SecondsPerSlot: testSecondsPerSlot, SlotsPerEpoch: 32})
	clock.(*ethereumClockImpl).now = func() time.Time { return now }
	return clock
}

// TestIsSlotCurrentSlotWithMaximumClockDisparityFollowsSpecWindow pins the spec's is_current_slot:
// a slot is current while the time is within the slot, widened by the clock disparity on both
// ends, so the next slot is current only shortly before it starts and the previous slot only
// shortly after it ends.
func TestIsSlotCurrentSlotWithMaximumClockDisparityFollowsSpecWindow(t *testing.T) {
	start, next := testSlotStart(testSlot), testSlotStart(testSlot+1)
	for _, tc := range []struct {
		name    string
		now     time.Time
		slot    uint64
		current bool
	}{
		{"the slot itself, mid-slot", start.Add(6 * time.Second), testSlot, true},
		{"the next slot, early in the slot", start.Add(time.Second), testSlot + 1, false},
		{"the next slot, exactly one disparity before it starts", next.Add(-maximumClockDisparity), testSlot + 1, true},
		{"the next slot, just more than one disparity before it starts", next.Add(-maximumClockDisparity - time.Millisecond), testSlot + 1, false},
		{"the previous slot, exactly one disparity after it ends", start.Add(maximumClockDisparity), testSlot - 1, true},
		{"the previous slot, just more than one disparity after it ends", start.Add(maximumClockDisparity + time.Millisecond), testSlot - 1, false},
		{"two slots ahead", start.Add(11 * time.Second), testSlot + 2, false},
		{"two slots behind", start.Add(100 * time.Millisecond), testSlot - 2, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.current, clockAt(tc.now).IsSlotCurrentSlotWithMaximumClockDisparity(tc.slot))
		})
	}
}

// TestIsSlotCurrentSlotWithMaximumClockDisparityRejectsSlotsThatAliasToNow proves a slot whose
// start time overflows into the current slot is not treated as current. With 12-second slots the
// time arithmetic repeats every 2^62 slots.
func TestIsSlotCurrentSlotWithMaximumClockDisparityRejectsSlotsThatAliasToNow(t *testing.T) {
	clock := clockAt(testSlotStart(testSlot).Add(6 * time.Second))
	for k := uint64(1); k <= 3; k++ {
		require.False(t, clock.IsSlotCurrentSlotWithMaximumClockDisparity(testSlot+k<<62), "slot %d + %d<<62", testSlot, k)
	}
	require.False(t, clock.IsSlotCurrentSlotWithMaximumClockDisparity(math.MaxUint64))
}

func TestIsSlotCurrentSlotWithMaximumClockDisparityAtGenesis(t *testing.T) {
	genesis := testSlotStart(0)
	require.True(t, clockAt(genesis.Add(-maximumClockDisparity)).IsSlotCurrentSlotWithMaximumClockDisparity(0))
	require.False(t, clockAt(genesis.Add(-maximumClockDisparity-time.Millisecond)).IsSlotCurrentSlotWithMaximumClockDisparity(0))
}

func TestGetCurrentEpochUsesClockTime(t *testing.T) {
	require.Equal(t, uint64(0), clockAt(testSlotStart(0).Add(-time.Hour)).GetCurrentEpoch())
	require.Equal(t, uint64(testSlot/32), clockAt(testSlotStart(testSlot)).GetCurrentEpoch())
}
