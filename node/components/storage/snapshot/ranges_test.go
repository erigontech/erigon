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

package snapshot

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStepRange(t *testing.T) {
	r := StepRange{10, 20}
	require.Equal(t, uint64(10), r.Len())
	require.True(t, r.Contains(10))
	require.True(t, r.Contains(19))
	require.False(t, r.Contains(20))
	require.False(t, r.Contains(9))
}

func TestStepRangeOverlaps(t *testing.T) {
	a := StepRange{10, 20}
	require.True(t, a.Overlaps(StepRange{15, 25}))
	require.True(t, a.Overlaps(StepRange{5, 15}))
	require.True(t, a.Overlaps(StepRange{12, 18}))
	require.False(t, a.Overlaps(StepRange{20, 30}))
	require.False(t, a.Overlaps(StepRange{0, 10}))
}

func TestStepRangeCovers(t *testing.T) {
	a := StepRange{10, 30}
	require.True(t, a.Covers(StepRange{10, 30}))
	require.True(t, a.Covers(StepRange{15, 25}))
	require.False(t, a.Covers(StepRange{5, 15}))
	require.False(t, a.Covers(StepRange{25, 35}))
}

func TestNormalize(t *testing.T) {
	// Overlapping ranges merge.
	rs := StepRanges{{10, 20}, {15, 30}, {5, 12}}
	norm := rs.Normalize()
	require.Equal(t, StepRanges{{5, 30}}, norm)

	// Adjacent ranges merge.
	rs = StepRanges{{10, 20}, {20, 30}}
	norm = rs.Normalize()
	require.Equal(t, StepRanges{{10, 30}}, norm)

	// Non-overlapping stay separate.
	rs = StepRanges{{10, 20}, {30, 40}}
	norm = rs.Normalize()
	require.Equal(t, StepRanges{{10, 20}, {30, 40}}, norm)
}

func TestCoverage(t *testing.T) {
	rs := StepRanges{{0, 100}, {200, 300}}
	require.Equal(t, uint64(200), rs.Coverage())
}

func TestIsComplete(t *testing.T) {
	rs := StepRanges{{0, 50}, {50, 100}}
	require.True(t, rs.IsComplete(0, 100))
	require.False(t, rs.IsComplete(0, 150))

	// With gap.
	rs = StepRanges{{0, 50}, {60, 100}}
	require.False(t, rs.IsComplete(0, 100))
	require.True(t, rs.IsComplete(0, 50))
}

func TestGaps(t *testing.T) {
	rs := StepRanges{{0, 50}, {80, 100}}
	gaps := rs.Gaps(0, 100)
	require.Equal(t, StepRanges{{50, 80}}, gaps)

	// Gap at start.
	rs = StepRanges{{20, 50}}
	gaps = rs.Gaps(0, 50)
	require.Equal(t, StepRanges{{0, 20}}, gaps)

	// Gap at end.
	rs = StepRanges{{0, 50}}
	gaps = rs.Gaps(0, 100)
	require.Equal(t, StepRanges{{50, 100}}, gaps)

	// No gaps.
	rs = StepRanges{{0, 100}}
	gaps = rs.Gaps(0, 100)
	require.Empty(t, gaps)
}

func TestGapsAgainst(t *testing.T) {
	local := StepRanges{{0, 50}}
	remote := StepRanges{{0, 100}}
	gaps := local.GapsAgainst(remote)
	require.Equal(t, StepRanges{{50, 100}}, gaps)

	// Remote has disjoint ranges.
	local = StepRanges{{0, 50}}
	remote = StepRanges{{0, 30}, {60, 100}}
	gaps = local.GapsAgainst(remote)
	require.Equal(t, StepRanges{{60, 100}}, gaps)

	// Remote has nothing we're missing.
	local = StepRanges{{0, 100}}
	remote = StepRanges{{0, 80}}
	gaps = local.GapsAgainst(remote)
	require.Empty(t, gaps)
}

func TestUnion(t *testing.T) {
	a := StepRanges{{0, 50}}
	b := StepRanges{{40, 100}}
	u := a.Union(b)
	require.Equal(t, StepRanges{{0, 100}}, u)
}
