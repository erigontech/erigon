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

package execmodule

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBuildDuration(t *testing.T) {
	const ethereum, gnosis = uint64(12), uint64(5)
	slotStart := time.Unix(1_700_000_000, 0)
	payloadTimestamp := uint64(slotStart.Unix())

	for _, tc := range []struct {
		name           string
		secondsPerSlot uint64
		sentAt         time.Time
		want           time.Duration
	}{
		// A consensus layer sends payload attributes ahead of the slot and then calls getPayload
		// without refreshing, so the builder has to survive until that slot however early it was asked.
		{"attributes well before the slot", ethereum, slotStart.Add(-8 * time.Second), 12 * time.Second},
		{"attributes shortly before the slot", ethereum, slotStart.Add(-4 * time.Second), 8 * time.Second},
		{"attributes at production time", ethereum, slotStart.Add(400 * time.Millisecond), 3600 * time.Millisecond},

		// A request arriving too late to leave a useful window still gets the old fixed budget,
		// so late proposals are no worse off than before.
		{"very late request floors", ethereum, slotStart.Add(3900 * time.Millisecond), 3 * time.Second},

		// Bounds scale with the chain rather than assuming 12s slots.
		{"short slots, attributes before the slot", gnosis, slotStart.Add(-4 * time.Second), 4*time.Second + 5*time.Second/3},
		{"short slots, late request floors", gnosis, slotStart.Add(2 * time.Second), 1250 * time.Millisecond},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, buildDuration(payloadTimestamp, tc.sentAt, tc.secondsPerSlot))
		})
	}
}

func TestBuildDurationCapsAbsurdTimestamp(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	farFuture := uint64(now.Add(time.Hour).Unix())

	// A bogus timestamp must not pin a builder, and its resources, indefinitely.
	require.Equal(t, 24*time.Second, buildDuration(farFuture, now, 12))
}

func TestBuildDurationCapsOverflowingTimestamp(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)

	// Beyond int64 seconds the conversion wraps into the past, which would collapse the budget to
	// the floor instead of the cap.
	require.Equal(t, 24*time.Second, buildDuration(math.MaxUint64, now, 12))
}
