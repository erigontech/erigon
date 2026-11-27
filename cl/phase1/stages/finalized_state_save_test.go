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
	"errors"
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
	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/spf13/afero"
)

func TestWriteFinalizedStateFile_RoundTrip(t *testing.T) {
	_, st, _ := tests.GetPhase0Random()
	dirs := datadir.New(t.TempDir())

	require.NoError(t, writeFinalizedStateFile(dirs, st))
	rootFiles, err := filepath.Glob(filepath.Join(dirs.CaplinLatest, ".finalized-state-root-*"))
	require.NoError(t, err)
	require.Empty(t, rootFiles)

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
	beaconCfg, st := newGloasFinalizedState(t)
	authoritativeRoot := common.Hash{0xaa}
	st.SetPreviousStateRoot(authoritativeRoot)
	wantBlockRoot, err := st.BlockRoot()
	require.NoError(t, err)
	dirs := datadir.New(t.TempDir())

	require.NoError(t, writeFinalizedStateFile(dirs, st))
	got, err := checkpoint_sync.ReadLocalFinalizedState(dirs, beaconCfg)
	require.NoError(t, err)
	require.Equal(t, authoritativeRoot, got.PeekPreviousStateRoot())
	gotBlockRoot, err := got.BlockRoot()
	require.NoError(t, err)
	require.Equal(t, wantBlockRoot, gotBlockRoot)
}

func TestReadFinalizedGloasStateWithoutRootRejectsUnsafeResume(t *testing.T) {
	beaconCfg, st := newGloasFinalizedState(t)
	dirs := datadir.New(t.TempDir())
	require.NoError(t, os.MkdirAll(dirs.CaplinLatest, 0o755))
	encoded, err := utils.EncodeSSZSnappy(st)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dirs.CaplinLatest, clparams.LatestFinalizedStateFileName), encoded, 0o644))

	genesis := state.New(beaconCfg)
	got, err := checkpoint_sync.NewLocalCheckpointSyncer(genesis, afero.NewBasePathFs(afero.NewOsFs(), dirs.CaplinLatest)).GetLatestBeaconState(t.Context())
	require.NoError(t, err)
	require.Equal(t, genesis.Slot(), got.Slot())
}

func TestReadFinalizedGloasStateRejectsCorruptRootRecord(t *testing.T) {
	beaconCfg, st := newGloasFinalizedState(t)
	dirs := datadir.New(t.TempDir())
	require.NoError(t, writeFinalizedStateFile(dirs, st))
	encoded, err := os.ReadFile(filepath.Join(dirs.CaplinLatest, clparams.LatestFinalizedStateFileName))
	require.NoError(t, err)
	rootPath := filepath.Join(dirs.CaplinLatest, checkpoint_sync.FinalizedStateRootFileName(encoded))
	record, err := os.ReadFile(rootPath)
	require.NoError(t, err)
	record[0] ^= 1
	require.NoError(t, os.WriteFile(rootPath, record, 0o644))

	_, err = checkpoint_sync.ReadLocalFinalizedState(dirs, beaconCfg)
	require.ErrorContains(t, err, "invalid finalized state root checksum")
}

func TestReadFinalizedStateRejectsRootRecordFromDifferentState(t *testing.T) {
	beaconCfg, stateA := newGloasFinalizedState(t)
	dirsA := datadir.New(t.TempDir())
	dirsB := datadir.New(t.TempDir())
	stateB, err := stateA.Copy()
	require.NoError(t, err)
	require.NoError(t, stateB.SetSlot(stateA.Slot()+1))
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

	_, err = checkpoint_sync.ReadLocalFinalizedState(dirsA, beaconCfg)
	require.ErrorContains(t, err, "invalid finalized state root checksum")
}

func newGloasFinalizedState(t *testing.T) (*clparams.BeaconChainConfig, *state.CachingBeaconState) {
	t.Helper()
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
	require.NoError(t, st.SetSlot(64))
	st.SetLatestBlockHeader(&cltypes.BeaconBlockHeader{Slot: 64})
	return &beaconCfg, st
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
		var savedRoot common.Hash

		require.NoError(t, saveFinalizedStateOnDiskIfNeeded(fc, beaconCfg, dirs, 0, &savedRoot))

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
		var savedRoot common.Hash

		onCadence := beaconCfg.SlotsPerEpoch * 5
		require.NoError(t, saveFinalizedStateOnDiskIfNeeded(fc, beaconCfg, dirs, onCadence, &savedRoot))

		_, err := checkpoint_sync.ReadLocalFinalizedState(dirs, beaconCfg)
		require.NoError(t, err)
	})

	t.Run("off-cadence writes nothing", func(t *testing.T) {
		dirs := datadir.New(t.TempDir())
		fc := mock_services.NewForkChoiceStorageMock(t)
		fc.FinalizedCheckpointVal = solid.Checkpoint{Epoch: 3, Root: finalizedRoot}
		fc.StateAtBlockRootVal[finalizedRoot] = st
		var savedRoot common.Hash

		offCadence := beaconCfg.SlotsPerEpoch*5 + 1
		require.NoError(t, saveFinalizedStateOnDiskIfNeeded(fc, beaconCfg, dirs, offCadence, &savedRoot))

		_, err := checkpoint_sync.ReadLocalFinalizedState(dirs, beaconCfg)
		require.Error(t, err)
	})

	t.Run("missing finalized state is non-fatal", func(t *testing.T) {
		dirs := datadir.New(t.TempDir())
		fc := mock_services.NewForkChoiceStorageMock(t)
		fc.FinalizedCheckpointVal = solid.Checkpoint{Epoch: 3, Root: common.Hash{0xbb}}
		var savedRoot common.Hash

		require.NoError(t, saveFinalizedStateOnDiskIfNeeded(fc, beaconCfg, dirs, 0, &savedRoot))

		_, err := checkpoint_sync.ReadLocalFinalizedState(dirs, beaconCfg)
		require.Error(t, err)
	})
}

func finalizedStateFilePath(dirs datadir.Dirs) string {
	return filepath.Join(dirs.CaplinLatest, clparams.LatestFinalizedStateFileName)
}

func savedFinalizedStateSlot(t *testing.T, dirs datadir.Dirs, beaconCfg *clparams.BeaconChainConfig) uint64 {
	t.Helper()
	got, err := checkpoint_sync.ReadLocalFinalizedState(dirs, beaconCfg)
	require.NoError(t, err)
	return got.Slot()
}

func requireNoFinalizedStateFile(t *testing.T, dirs datadir.Dirs) {
	t.Helper()
	_, err := os.Stat(finalizedStateFilePath(dirs))
	require.ErrorIs(t, err, os.ErrNotExist)
}

func newDistinctPhase0States(t *testing.T) (first, second *state.CachingBeaconState) {
	t.Helper()
	_, first, _ = tests.GetPhase0Random()
	second, err := first.Copy()
	require.NoError(t, err)
	require.NoError(t, second.SetSlot(first.Slot()+1))
	return first, second
}

type finalizedCheckpointSequence struct {
	*mock_services.ForkChoiceStorageMock
	roots []common.Hash
	reads int
}

func (f *finalizedCheckpointSequence) FinalizedCheckpoint() solid.Checkpoint {
	root := f.roots[min(f.reads, len(f.roots)-1)]
	f.reads++
	return solid.Checkpoint{Root: root}
}

func TestSaveFinalizedStateSkipsUnchangedFinalizedRoot(t *testing.T) {
	_, st, _ := tests.GetPhase0Random()
	beaconCfg := &clparams.MainnetBeaconConfig
	dirs := datadir.New(t.TempDir())
	fc := mock_services.NewForkChoiceStorageMock(t)
	fc.FinalizedCheckpointVal = solid.Checkpoint{Epoch: 3, Root: common.Hash{0xfa}}
	fetches := 0
	fc.GetStateAtBlockRootFn = func(common.Hash) (*state.CachingBeaconState, error) {
		fetches++
		return st.Copy()
	}
	var savedRoot common.Hash

	require.NoError(t, saveFinalizedStateOnDiskIfNeeded(fc, beaconCfg, dirs, 0, &savedRoot))
	require.NoError(t, dir.RemoveFile(finalizedStateFilePath(dirs)))
	require.NoError(t, saveFinalizedStateOnDiskIfNeeded(fc, beaconCfg, dirs, 0, &savedRoot))

	require.Equal(t, 1, fetches)
	requireNoFinalizedStateFile(t, dirs)
}

func TestSaveFinalizedStateSavesWhenFinalizedRootChanges(t *testing.T) {
	first, second := newDistinctPhase0States(t)
	beaconCfg := &clparams.MainnetBeaconConfig
	root1, root2 := common.Hash{0x01}, common.Hash{0x02}
	dirs := datadir.New(t.TempDir())
	fc := mock_services.NewForkChoiceStorageMock(t)
	fc.StateAtBlockRootVal[root1] = first
	fc.StateAtBlockRootVal[root2] = second
	var savedRoot common.Hash

	fc.FinalizedCheckpointVal = solid.Checkpoint{Epoch: 3, Root: root1}
	require.NoError(t, saveFinalizedStateOnDiskIfNeeded(fc, beaconCfg, dirs, 0, &savedRoot))
	require.Equal(t, first.Slot(), savedFinalizedStateSlot(t, dirs, beaconCfg))
	require.NoError(t, dir.RemoveFile(finalizedStateFilePath(dirs)))

	fc.FinalizedCheckpointVal = solid.Checkpoint{Epoch: 4, Root: root2}
	require.NoError(t, saveFinalizedStateOnDiskIfNeeded(fc, beaconCfg, dirs, 0, &savedRoot))
	require.Equal(t, second.Slot(), savedFinalizedStateSlot(t, dirs, beaconCfg))
}

func TestSaveFinalizedStateRetriesAfterWriteError(t *testing.T) {
	_, st, _ := tests.GetPhase0Random()
	beaconCfg := &clparams.MainnetBeaconConfig
	finalizedRoot := common.Hash{0xfa}
	dirs := datadir.New(t.TempDir())
	fc := mock_services.NewForkChoiceStorageMock(t)
	fc.FinalizedCheckpointVal = solid.Checkpoint{Epoch: 3, Root: finalizedRoot}
	fc.StateAtBlockRootVal[finalizedRoot] = st
	var savedRoot common.Hash

	require.NoError(t, dir.RemoveAll(dirs.CaplinLatest))
	require.NoError(t, os.WriteFile(dirs.CaplinLatest, nil, 0o644))
	require.Error(t, saveFinalizedStateOnDiskIfNeeded(fc, beaconCfg, dirs, 0, &savedRoot))
	require.Equal(t, common.Hash{}, savedRoot)

	require.NoError(t, dir.RemoveFile(dirs.CaplinLatest))
	require.NoError(t, saveFinalizedStateOnDiskIfNeeded(fc, beaconCfg, dirs, 0, &savedRoot))
	require.Equal(t, st.Slot(), savedFinalizedStateSlot(t, dirs, beaconCfg))
	require.Equal(t, finalizedRoot, savedRoot)
}

func TestSaveFinalizedStateRetriesAfterFailedLookup(t *testing.T) {
	for _, test := range []struct {
		name      string
		lookupErr error
	}{
		{name: "missing state"},
		{name: "lookup error", lookupErr: errors.New("lookup failed")},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, st, _ := tests.GetPhase0Random()
			beaconCfg := &clparams.MainnetBeaconConfig
			finalizedRoot := common.Hash{0xfa}
			dirs := datadir.New(t.TempDir())
			fc := mock_services.NewForkChoiceStorageMock(t)
			fc.FinalizedCheckpointVal = solid.Checkpoint{Epoch: 3, Root: finalizedRoot}
			fc.GetStateAtBlockRootFn = func(common.Hash) (*state.CachingBeaconState, error) {
				return nil, test.lookupErr
			}
			var savedRoot common.Hash

			require.NoError(t, saveFinalizedStateOnDiskIfNeeded(fc, beaconCfg, dirs, 0, &savedRoot))
			requireNoFinalizedStateFile(t, dirs)
			require.Equal(t, common.Hash{}, savedRoot)

			fc.GetStateAtBlockRootFn = nil
			fc.StateAtBlockRootVal[finalizedRoot] = st
			require.NoError(t, saveFinalizedStateOnDiskIfNeeded(fc, beaconCfg, dirs, 0, &savedRoot))
			require.Equal(t, st.Slot(), savedFinalizedStateSlot(t, dirs, beaconCfg))
		})
	}
}

func TestSaveFinalizedStateReadsFinalizedRootOnce(t *testing.T) {
	first, second := newDistinctPhase0States(t)
	beaconCfg := &clparams.MainnetBeaconConfig
	root1, root2 := common.Hash{0x01}, common.Hash{0x02}
	dirs := datadir.New(t.TempDir())
	mock := mock_services.NewForkChoiceStorageMock(t)
	mock.StateAtBlockRootVal[root1] = first
	mock.StateAtBlockRootVal[root2] = second
	fc := &finalizedCheckpointSequence{ForkChoiceStorageMock: mock, roots: []common.Hash{root1, root2}}
	var savedRoot common.Hash

	require.NoError(t, saveFinalizedStateOnDiskIfNeeded(fc, beaconCfg, dirs, 0, &savedRoot))
	require.Equal(t, 1, fc.reads)
	require.Equal(t, first.Slot(), savedFinalizedStateSlot(t, dirs, beaconCfg))
	require.Equal(t, root1, savedRoot)

	require.NoError(t, saveFinalizedStateOnDiskIfNeeded(fc, beaconCfg, dirs, 0, &savedRoot))
	require.Equal(t, 2, fc.reads)
	require.Equal(t, second.Slot(), savedFinalizedStateSlot(t, dirs, beaconCfg))
	require.Equal(t, root2, savedRoot)
}
