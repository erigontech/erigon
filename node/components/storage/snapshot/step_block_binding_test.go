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

func TestStepBlockBoundary_RegisterAndQuery(t *testing.T) {
	inv := NewInventory()

	// Register a few boundaries.
	inv.RegisterStepBlockBoundary(256, 100_000)
	inv.RegisterStepBlockBoundary(512, 250_000)
	inv.RegisterStepBlockBoundary(768, 400_000)

	// Block 50_000 falls before any boundary's recorded block, so
	// step 256 (the smallest boundary >= 50_000) covers it.
	step, ok := inv.BlockToStep(50_000)
	require.True(t, ok)
	require.Equal(t, uint64(256), step,
		"step 256 covers blocks up to 100_000")

	// Block 100_000 sits at the boundary of step 256.
	step, ok = inv.BlockToStep(100_000)
	require.True(t, ok)
	require.Equal(t, uint64(256), step)

	// Block 200_000 falls within step 512's range (100_000, 250_000].
	step, ok = inv.BlockToStep(200_000)
	require.True(t, ok)
	require.Equal(t, uint64(512), step)

	// Block 500_000 is past every boundary; no step covers it.
	step, ok = inv.BlockToStep(500_000)
	require.False(t, ok)
	require.Equal(t, uint64(0), step)
}

func TestStepBlockBoundary_OverwriteOnReregister(t *testing.T) {
	// Re-registering the same step with a different block (e.g.
	// after a retire-merge replaces the commitment file) overwrites.
	inv := NewInventory()
	inv.RegisterStepBlockBoundary(256, 100_000)
	inv.RegisterStepBlockBoundary(256, 105_000)

	bs := inv.StepBlockBoundaries()
	require.Len(t, bs, 1)
	require.Equal(t, uint64(105_000), bs[0].Block)
}

func TestStepBlockBoundaries_SortedAscendingByStep(t *testing.T) {
	inv := NewInventory()
	inv.RegisterStepBlockBoundary(768, 400_000)
	inv.RegisterStepBlockBoundary(256, 100_000)
	inv.RegisterStepBlockBoundary(512, 250_000)

	bs := inv.StepBlockBoundaries()
	require.Len(t, bs, 3)
	require.Equal(t, uint64(256), bs[0].ToStep)
	require.Equal(t, uint64(512), bs[1].ToStep)
	require.Equal(t, uint64(768), bs[2].ToStep)
}

func TestBlockToStep_EmptyInventoryReturnsFalse(t *testing.T) {
	inv := NewInventory()
	step, ok := inv.BlockToStep(100_000)
	require.False(t, ok)
	require.Equal(t, uint64(0), step)
}
