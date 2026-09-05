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

package raw

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
)

func TestCopyIntoSetsPhase0VersionOnReusedDestination(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	source := New(&cfg)
	require.NoError(t, source.SetSlot(3))
	destination := New(&cfg)
	destination.SetVersion(clparams.GloasVersion)
	require.NoError(t, destination.SetSlot(99))

	require.NoError(t, source.CopyInto(destination))

	require.Equal(t, clparams.Phase0Version, destination.Version())
	expectedRoot, err := source.HashSSZ()
	require.NoError(t, err)
	actualRoot, err := destination.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, expectedRoot, actualRoot)
}
