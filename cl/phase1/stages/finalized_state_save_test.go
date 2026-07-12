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

package stages

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/antiquary/tests"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/checkpoint_sync"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/mock_services"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/datadir"
)

func TestWriteFinalizedStateFile_RoundTrip(t *testing.T) {
	_, st, _ := tests.GetPhase0Random()
	dirs := datadir.New(t.TempDir())

	require.NoError(t, writeFinalizedStateFile(dirs, st))

	got, err := checkpoint_sync.ReadLocalFinalizedState(dirs, &clparams.MainnetBeaconConfig)
	require.NoError(t, err)
	require.NotNil(t, got)

	wantRoot, err := st.HashSSZ()
	require.NoError(t, err)
	gotRoot, err := got.HashSSZ()
	require.NoError(t, err)
	assert.Equal(t, wantRoot, gotRoot)
}

func TestSaveFinalizedStateOnDisk(t *testing.T) {
	_, st, _ := tests.GetPhase0Random()
	beaconCfg := &clparams.MainnetBeaconConfig
	finalizedRoot := common.Hash{0xfa}

	t.Run("persists finalized state on cadence", func(t *testing.T) {
		dirs := datadir.New(t.TempDir())
		fc := mock_services.NewForkChoiceStorageMock(t)
		fc.FinalizedCheckpointVal = solid.Checkpoint{Epoch: 3, Root: finalizedRoot}
		fc.StateAtBlockRootVal[finalizedRoot] = st

		require.NoError(t, saveFinalizedStateOnDiskIfNeeded(fc, beaconCfg, dirs, 0))

		got, err := checkpoint_sync.ReadLocalFinalizedState(dirs, beaconCfg)
		require.NoError(t, err)
		wantRoot, err := st.HashSSZ()
		require.NoError(t, err)
		gotRoot, err := got.HashSSZ()
		require.NoError(t, err)
		assert.Equal(t, wantRoot, gotRoot)
	})

	t.Run("off-cadence writes nothing", func(t *testing.T) {
		dirs := datadir.New(t.TempDir())
		fc := mock_services.NewForkChoiceStorageMock(t)
		fc.FinalizedCheckpointVal = solid.Checkpoint{Epoch: 3, Root: finalizedRoot}
		fc.StateAtBlockRootVal[finalizedRoot] = st

		require.NoError(t, saveFinalizedStateOnDiskIfNeeded(fc, beaconCfg, dirs, 1))

		_, err := checkpoint_sync.ReadLocalFinalizedState(dirs, beaconCfg)
		require.Error(t, err)
	})

	t.Run("missing finalized state is non-fatal", func(t *testing.T) {
		dirs := datadir.New(t.TempDir())
		fc := mock_services.NewForkChoiceStorageMock(t)
		fc.FinalizedCheckpointVal = solid.Checkpoint{Epoch: 3, Root: common.Hash{0xbb}}

		require.NoError(t, saveFinalizedStateOnDiskIfNeeded(fc, beaconCfg, dirs, 0))

		_, err := checkpoint_sync.ReadLocalFinalizedState(dirs, beaconCfg)
		require.Error(t, err)
	})
}
