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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGCDueBeforeTheEarliestPacerTrigger(t *testing.T) {
	const gib = uint64(1) << 30
	live, goal := 10*gib, 20*gib
	earliestTrigger := live + (goal-live)/64*45

	require.False(t, gcDue(live, live, goal, gib), "fresh heap, two slots of allocation stay far below the trigger")
	require.False(t, gcDue(earliestTrigger-2*gib-1, live, goal, gib), "one byte short of the trigger after two slots")
	require.True(t, gcDue(earliestTrigger-2*gib, live, goal, gib), "two slots of allocation reach the trigger")
	require.True(t, gcDue(earliestTrigger, live, goal, 0), "already at the trigger")
	require.True(t, gcDue(live, live, live, 0), "goal not above the live heap")
}
