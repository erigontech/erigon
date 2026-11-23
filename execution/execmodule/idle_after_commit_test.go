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

func TestGCDueBetweenHalfRunwayAndTheEarliestPacerTrigger(t *testing.T) {
	const gib = uint64(1) << 30
	live, goal := 10*gib, 20*gib
	half := live + (goal-live)/2
	earliestTrigger := live + (goal-live)/64*45

	require.False(t, gcDue(half-1, live, goal, false), "less than half the runway used")
	require.True(t, gcDue(half, live, goal, false), "half the runway used")
	require.True(t, gcDue(earliestTrigger-1, live, goal, false), "just below the earliest pacer trigger")
	require.False(t, gcDue(earliestTrigger, live, goal, false), "a natural cycle may already run, and runtime.GC would add a second one")
	require.False(t, gcDue(half, live, goal, true), "a cycle finished since the last commit")
	require.False(t, gcDue(goal, live, live, false), "goal not above live: the runtime already collects without a break")
}
