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

// A new state-snapshot type added to an already-populated datadir must not drag
// the dump back to genesis for the types that are already caught up: each type
// resumes from its own coverage, so a full base file is never re-sliced into
// overlapping sub-files.
func TestPlanStateDumpResumesPerType(t *testing.T) {
	const blocksPerFile = 50_000
	availability := map[string]uint64{
		"Covered": 10_400_000, // already at head (e.g. one 0..7.15M base + increments)
		"Lagging": 2_700_000,  // newly added type, behind
		"Empty":   0,          // brand-new type, nothing on disk
	}

	jobs := planStateDump(availability, 10_400_000, blocksPerFile)

	for _, j := range jobs {
		require.NotEqual(t, "Covered", j.name, "already-covered type must not be re-dumped: %+v", j)
		require.GreaterOrEqual(t, j.from, availability[j.name],
			"type %s dumped below its own availability (would overlap existing files): from=%d avail=%d", j.name, j.from, availability[j.name])
		require.Equal(t, blocksPerFile, int(j.to-j.from), "every job must be exactly one full file")
	}

	laggingFrom, ok := firstJobFrom(jobs, "Lagging")
	require.True(t, ok, "lagging type must be dumped")
	require.Equal(t, uint64(2_700_000), laggingFrom, "lagging type must resume at its own availability")
	require.Equal(t, uint64((10_400_000-2_700_000)/blocksPerFile), countJobs(jobs, "Lagging"))

	emptyFrom, ok := firstJobFrom(jobs, "Empty")
	require.True(t, ok, "empty type must be dumped from genesis")
	require.Equal(t, uint64(0), emptyFrom)
	require.Equal(t, uint64(10_400_000/blocksPerFile), countJobs(jobs, "Empty"))
}

// A gap in a type's visible segments must cap availability at the contiguous
// prefix, so planStateDump re-dumps the missing range instead of skipping it.
func TestBlocksAvailableForTypeStopsAtGap(t *testing.T) {
	s := &CaplinStateSnapshots{}
	s.visible.Store("Gapped", []*VisibleSegment{
		{Range: Range{from: 0, to: 7_150_000}},
		{Range: Range{from: 7_200_000, to: 7_250_000}}, // 7.15M..7.2M missing
	})
	require.Equal(t, uint64(7_150_000), s.blocksAvailableForType("Gapped"))
}

// Resuming must start exactly at the coverage end, never below it — flooring an
// unaligned tail to a file boundary would overlap the existing files.
func TestPlanStateDumpUnalignedResumeHasNoOverlap(t *testing.T) {
	const blocksPerFile = 50_000
	jobs := planStateDump(map[string]uint64{"X": 2_725_000}, 2_825_000, blocksPerFile)

	from, ok := firstJobFrom(jobs, "X")
	require.True(t, ok)
	require.Equal(t, uint64(2_725_000), from, "must resume at availability, not floor below it")
	for _, j := range jobs {
		require.GreaterOrEqual(t, j.from, uint64(2_725_000), "no job may start before existing coverage")
	}
}

func TestBlocksAvailableForType(t *testing.T) {
	s := &CaplinStateSnapshots{}
	s.visible.Store("A", []*VisibleSegment{
		{Range: Range{from: 0, to: 7_150_000}},
		{Range: Range{from: 7_150_000, to: 7_200_000}},
	})
	s.visible.Store("Empty", []*VisibleSegment{})

	require.Equal(t, uint64(7_200_000), s.blocksAvailableForType("A"))
	require.Equal(t, uint64(0), s.blocksAvailableForType("Empty"))
	require.Equal(t, uint64(0), s.blocksAvailableForType("missing"))
}
