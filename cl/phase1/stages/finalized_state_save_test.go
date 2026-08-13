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
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/antiquary/tests"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/checkpoint_sync"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/mock_services"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/spf13/afero"
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

func TestWriteFinalizedStateFile_RestoresGloasStateRoot(t *testing.T) {
	beaconCfg := clparams.MainnetBeaconConfig
	beaconCfg.AltairForkEpoch = 0
	beaconCfg.BellatrixForkEpoch = 0
	beaconCfg.CapellaForkEpoch = 0
	beaconCfg.DenebForkEpoch = 0
	beaconCfg.ElectraForkEpoch = 0
	beaconCfg.FuluForkEpoch = 0
	beaconCfg.GloasForkEpoch = 0
	st := state.New(&beaconCfg)
	st.SetVersion(clparams.GloasVersion)
	st.SetSlot(64)
	st.SetLatestBlockHeader(&cltypes.BeaconBlockHeader{Slot: 64})
	authoritativeRoot := common.Hash{0xaa}
	st.SetPreviousStateRoot(authoritativeRoot)
	wantBlockRoot, err := st.BlockRoot()
	require.NoError(t, err)
	dirs := datadir.New(t.TempDir())

	require.NoError(t, writeFinalizedStateFile(dirs, st))
	got, err := checkpoint_sync.ReadLocalFinalizedState(dirs, &beaconCfg)
	require.NoError(t, err)
	require.Equal(t, authoritativeRoot, got.PeekPreviousStateRoot())
	gotBlockRoot, err := got.BlockRoot()
	require.NoError(t, err)
	require.Equal(t, wantBlockRoot, gotBlockRoot)
}

func TestReadFinalizedGloasStateWithoutRootRejectsUnsafeResume(t *testing.T) {
	beaconCfg := clparams.MainnetBeaconConfig
	beaconCfg.AltairForkEpoch = 0
	beaconCfg.BellatrixForkEpoch = 0
	beaconCfg.CapellaForkEpoch = 0
	beaconCfg.DenebForkEpoch = 0
	beaconCfg.ElectraForkEpoch = 0
	beaconCfg.FuluForkEpoch = 0
	beaconCfg.GloasForkEpoch = 0
	st := state.New(&beaconCfg)
	st.SetVersion(clparams.GloasVersion)
	st.SetSlot(64)
	st.SetLatestBlockHeader(&cltypes.BeaconBlockHeader{Slot: 64})
	dirs := datadir.New(t.TempDir())
	require.NoError(t, os.MkdirAll(dirs.CaplinLatest, 0o755))
	encoded, err := utils.EncodeSSZSnappy(st)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dirs.CaplinLatest, clparams.LatestFinalizedStateFileName), encoded, 0o644))

	genesis := state.New(&beaconCfg)
	got, err := checkpoint_sync.NewLocalCheckpointSyncer(genesis, afero.NewBasePathFs(afero.NewOsFs(), dirs.CaplinLatest)).GetLatestBeaconState(t.Context())
	require.NoError(t, err)
	require.Equal(t, genesis.Slot(), got.Slot())
}

func TestReadFinalizedGloasStateRejectsCorruptRootRecord(t *testing.T) {
	beaconCfg := clparams.MainnetBeaconConfig
	dirs := datadir.New(t.TempDir())
	_, st, _ := tests.GetPhase0Random()
	require.NoError(t, writeFinalizedStateFile(dirs, st))
	encoded, err := os.ReadFile(filepath.Join(dirs.CaplinLatest, clparams.LatestFinalizedStateFileName))
	require.NoError(t, err)
	rootPath := filepath.Join(dirs.CaplinLatest, checkpoint_sync.FinalizedStateRootFileName(encoded))
	record, err := os.ReadFile(rootPath)
	require.NoError(t, err)
	record[0] ^= 1
	require.NoError(t, os.WriteFile(rootPath, record, 0o644))

	_, err = checkpoint_sync.ReadLocalFinalizedState(dirs, &beaconCfg)
	require.ErrorContains(t, err, "invalid finalized state root checksum")
}

func TestReadFinalizedStateRejectsRootRecordFromDifferentState(t *testing.T) {
	beaconCfg := clparams.MainnetBeaconConfig
	dirsA := datadir.New(t.TempDir())
	dirsB := datadir.New(t.TempDir())
	_, stateA, _ := tests.GetPhase0Random()
	stateB, err := stateA.Copy()
	require.NoError(t, err)
	stateB.SetSlot(stateA.Slot() + 1)
	stateA.SetPreviousStateRoot(common.Hash{1})
	stateB.SetPreviousStateRoot(common.Hash{2})
	require.NoError(t, writeFinalizedStateFile(dirsA, stateA))
	require.NoError(t, writeFinalizedStateFile(dirsB, stateB))
	encodedA, err := os.ReadFile(filepath.Join(dirsA.CaplinLatest, clparams.LatestFinalizedStateFileName))
	require.NoError(t, err)
	encodedB, err := os.ReadFile(filepath.Join(dirsB.CaplinLatest, clparams.LatestFinalizedStateFileName))
	require.NoError(t, err)
	recordB, err := os.ReadFile(filepath.Join(dirsB.CaplinLatest, checkpoint_sync.FinalizedStateRootFileName(encodedB)))
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dirsA.CaplinLatest, checkpoint_sync.FinalizedStateRootFileName(encodedA)), recordB, 0o644))

	_, err = checkpoint_sync.ReadLocalFinalizedState(dirsA, &beaconCfg)
	require.ErrorContains(t, err, "invalid finalized state root checksum")
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

	t.Run("persists on a non-zero cadence boundary", func(t *testing.T) {
		dirs := datadir.New(t.TempDir())
		fc := mock_services.NewForkChoiceStorageMock(t)
		fc.FinalizedCheckpointVal = solid.Checkpoint{Epoch: 3, Root: finalizedRoot}
		fc.StateAtBlockRootVal[finalizedRoot] = st

		onCadence := beaconCfg.SlotsPerEpoch * 5
		require.NoError(t, saveFinalizedStateOnDiskIfNeeded(fc, beaconCfg, dirs, onCadence))

		_, err := checkpoint_sync.ReadLocalFinalizedState(dirs, beaconCfg)
		require.NoError(t, err)
	})

	t.Run("off-cadence writes nothing", func(t *testing.T) {
		dirs := datadir.New(t.TempDir())
		fc := mock_services.NewForkChoiceStorageMock(t)
		fc.FinalizedCheckpointVal = solid.Checkpoint{Epoch: 3, Root: finalizedRoot}
		fc.StateAtBlockRootVal[finalizedRoot] = st

		offCadence := beaconCfg.SlotsPerEpoch*5 + 1
		require.NoError(t, saveFinalizedStateOnDiskIfNeeded(fc, beaconCfg, dirs, offCadence))

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
