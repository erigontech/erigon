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

func TestCanonicalLayoutPowerOf2(t *testing.T) {
	// Power-of-2 steps produce a single file.
	layout := CanonicalLayout(8, 0)
	require.Equal(t, StepRanges{{0, 8}}, layout)

	layout = CanonicalLayout(4096, 0)
	require.Equal(t, StepRanges{{0, 4096}}, layout)
}

func TestCanonicalLayoutNonPowerOf2(t *testing.T) {
	// 12 = 8 + 4
	layout := CanonicalLayout(12, 0)
	require.Equal(t, StepRanges{{0, 8}, {8, 12}}, layout)

	// 100 = 64 + 32 + 4
	layout = CanonicalLayout(100, 0)
	require.Equal(t, StepRanges{{0, 64}, {64, 96}, {96, 100}}, layout)
}

func TestCanonicalLayoutWithMaxMergeSize(t *testing.T) {
	// At step 8192 with different merge caps.
	layout := CanonicalLayout(8192, 8192)
	require.Equal(t, StepRanges{{0, 8192}}, layout)

	layout = CanonicalLayout(8192, 4096)
	require.Equal(t, StepRanges{{0, 4096}, {4096, 8192}}, layout)

	layout = CanonicalLayout(8192, 2048)
	require.Equal(t, StepRanges{{0, 2048}, {2048, 4096}, {4096, 6144}, {6144, 8192}}, layout)

	layout = CanonicalLayout(8192, 1024)
	require.Len(t, layout, 8)
	require.True(t, layout.IsComplete(0, 8192))
}

func TestCanonicalLayoutProgressiveReplacement(t *testing.T) {
	// A node at step 8192 publishes [0-4096) [4096-8192) with maxMerge=4096.
	shallow := CanonicalLayout(8192, 4096)
	require.Equal(t, StepRanges{{0, 4096}, {4096, 8192}}, shallow)

	// Later, deep merge completes: [0-8192).
	deep := CanonicalLayout(8192, 8192)
	require.Equal(t, StepRanges{{0, 8192}}, deep)

	// Both are valid canonical layouts.
	require.True(t, IsCanonical(shallow, 8192))
	require.True(t, IsCanonical(deep, 8192))

	// The deep layout replaces the shallow one.
	missing := MissingMerges(shallow, deep)
	require.Equal(t, StepRanges{{0, 8192}}, missing)
}

func TestCanonicalLayoutTail(t *testing.T) {
	// Step 4100: deep merge for 4096, then small tail.
	layout := CanonicalLayout(4100, 0)
	require.Equal(t, StepRanges{{0, 4096}, {4096, 4100}}, layout)

	// Step 6144: two files.
	layout = CanonicalLayout(6144, 0)
	require.Equal(t, StepRanges{{0, 4096}, {4096, 6144}}, layout)
}

func TestIsCanonical(t *testing.T) {
	// Valid canonical layouts.
	require.True(t, IsCanonical(StepRanges{{0, 8192}}, 8192))
	require.True(t, IsCanonical(StepRanges{{0, 4096}, {4096, 8192}}, 8192))
	require.True(t, IsCanonical(StepRanges{{0, 2048}, {2048, 4096}, {4096, 6144}, {6144, 8192}}, 8192))

	// Invalid: gap.
	require.False(t, IsCanonical(StepRanges{{0, 4096}, {5000, 8192}}, 8192))

	// Invalid: non-power-of-2 size.
	require.False(t, IsCanonical(StepRanges{{0, 3000}, {3000, 8192}}, 8192))

	// Invalid: misaligned.
	require.False(t, IsCanonical(StepRanges{{0, 4096}, {4096, 6144}, {6144, 8000}, {8000, 8192}}, 8192))
}

func TestMissingMerges(t *testing.T) {
	current := StepRanges{{0, 1024}, {1024, 2048}, {2048, 3072}, {3072, 4096}}
	target := StepRanges{{0, 4096}}

	missing := MissingMerges(current, target)
	require.Equal(t, StepRanges{{0, 4096}}, missing)
}

func TestMissingMergesPartial(t *testing.T) {
	current := StepRanges{{0, 4096}, {4096, 5120}, {5120, 6144}, {6144, 7168}, {7168, 8192}}
	target := StepRanges{{0, 4096}, {4096, 8192}}

	missing := MissingMerges(current, target)
	require.Equal(t, StepRanges{{4096, 8192}}, missing)
}

func TestCanonicalLevel(t *testing.T) {
	layout := CanonicalLayout(8192, 4096)
	require.Equal(t, uint64(4096), CanonicalLevel(layout))

	layout = CanonicalLayout(8192, 0)
	require.Equal(t, uint64(8192), CanonicalLevel(layout))
}

func TestCanonicalLayoutEmpty(t *testing.T) {
	layout := CanonicalLayout(0, 0)
	require.Nil(t, layout)
}

func TestAllCanonicalLevelsAreValid(t *testing.T) {
	// For step 8192, every merge level produces a valid canonical layout.
	for _, maxMerge := range []uint64{1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192} {
		layout := CanonicalLayout(8192, maxMerge)
		require.True(t, IsCanonical(layout, 8192), "maxMerge=%d layout=%v", maxMerge, layout)
		require.True(t, layout.IsComplete(0, 8192), "maxMerge=%d", maxMerge)
	}
}
