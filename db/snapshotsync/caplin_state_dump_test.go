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

package snapshotsync

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func firstJobFrom(jobs []caplinStateDumpJob, name string) (uint64, bool) {
	for _, j := range jobs {
		if j.name == name {
			return j.from, true
		}
	}
	return 0, false
}

func countJobs(jobs []caplinStateDumpJob, name string) uint64 {
	var n uint64
	for _, j := range jobs {
		if j.name == name {
			n++
		}
	}
	return n
}

// overlapsCoverage reports whether any job lands inside an existing covered range.
func overlapsCoverage(jobs []caplinStateDumpJob, name string, covered []Range) bool {
	for _, j := range jobs {
		if j.name != name {
			continue
		}
		for _, r := range covered {
			if j.from < r.to && r.from < j.to {
				return true
			}
		}
	}
	return false
}

// A new state-snapshot type added to an already-populated datadir must not drag
// the dump back to genesis for types that are already caught up: only missing
// ranges are scheduled, so a full base file is never re-sliced into overlaps.
func TestPlanStateDumpResumesPerType(t *testing.T) {
	const blocksPerFile = 50_000
	coverage := map[string][]Range{
		"Covered": {{from: 0, to: 10_400_000}}, // already at head
		"Lagging": {{from: 0, to: 2_700_000}},  // newly added type, behind
		"Empty":   nil,                         // brand-new type, nothing on disk
	}

	jobs := planStateDump(coverage, 10_400_000, blocksPerFile)

	for _, j := range jobs {
		require.Equal(t, blocksPerFile, int(j.to-j.from), "every job must be exactly one full file")
	}
	for name, cov := range coverage {
		require.False(t, overlapsCoverage(jobs, name, cov), "type %s scheduled a job overlapping existing coverage", name)
	}

	require.Zero(t, countJobs(jobs, "Covered"), "caught-up type must not be re-dumped")

	laggingFrom, ok := firstJobFrom(jobs, "Lagging")
	require.True(t, ok, "lagging type must be dumped")
	require.Equal(t, uint64(2_700_000), laggingFrom, "lagging type must resume at its coverage end")
	require.Equal(t, uint64((10_400_000-2_700_000)/blocksPerFile), countJobs(jobs, "Lagging"))

	emptyFrom, ok := firstJobFrom(jobs, "Empty")
	require.True(t, ok, "empty type must be dumped from genesis")
	require.Equal(t, uint64(0), emptyFrom)
	require.Equal(t, uint64(10_400_000/blocksPerFile), countJobs(jobs, "Empty"))
}

// A gap between existing segments must be filled, without re-dumping (and thus
// overlapping) a larger already-present segment that follows the gap.
func TestPlanStateDumpFillsGapWithoutOverlap(t *testing.T) {
	const blocksPerFile = 50_000
	covered := []Range{
		{from: 0, to: 7_150_000},
		{from: 7_200_000, to: 10_400_000}, // 7.15M..7.2M missing; tail is one large segment
	}
	jobs := planStateDump(map[string][]Range{"X": covered}, 10_400_000, blocksPerFile)

	require.Equal(t, uint64(1), countJobs(jobs, "X"), "only the single missing file should be scheduled")
	from, _ := firstJobFrom(jobs, "X")
	require.Equal(t, uint64(7_150_000), from)
	require.False(t, overlapsCoverage(jobs, "X", covered))
}

// An unaligned coverage tail must resume exactly at its end, never flooring to a
// file boundary below it (which would overlap the existing tail).
func TestPlanStateDumpUnalignedResumeHasNoOverlap(t *testing.T) {
	const blocksPerFile = 50_000
	covered := []Range{{from: 0, to: 2_725_000}}
	jobs := planStateDump(map[string][]Range{"X": covered}, 2_825_000, blocksPerFile)

	from, ok := firstJobFrom(jobs, "X")
	require.True(t, ok)
	require.Equal(t, uint64(2_725_000), from, "must resume at coverage end, not floor below it")
	require.False(t, overlapsCoverage(jobs, "X", covered))
}

func TestMissingRanges(t *testing.T) {
	require.Equal(t, []Range(nil), missingRanges([]Range{{from: 0, to: 100}}, 100), "fully covered → nothing missing")
	require.Equal(t, []Range{{from: 0, to: 100}}, missingRanges(nil, 100), "empty → all missing")
	require.Equal(t, []Range{{from: 70, to: 100}}, missingRanges([]Range{{from: 0, to: 70}}, 100), "trailing tail")
	require.Equal(t, []Range{{from: 50, to: 60}}, missingRanges([]Range{{from: 0, to: 50}, {from: 60, to: 100}}, 100), "interior gap only")
	require.Equal(t, []Range(nil), missingRanges([]Range{{from: 0, to: 200}}, 100), "coverage beyond toSlot → nothing missing")
}
