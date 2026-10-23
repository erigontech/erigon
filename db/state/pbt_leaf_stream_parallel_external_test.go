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

package state_test

import (
	"bytes"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
)

func TestForEachPBinLeafWithWorkersMatchesSerial(t *testing.T) {
	selectPBinLeafStreamHash(t)
	db, agg := pbinLeafStreamDatadir(t)
	serial := pbinLeafStreamLeaves(t, db, agg, true)
	serialRoot, err := state.PBinRootFromStream(eip8297.SelectedHash(), func(emit func(state.PBinLeaf) error) error {
		for _, leaf := range serial {
			if err := emit(leaf); err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err)
	for _, workers := range []int{1, 4, 16} {
		at := agg.BeginFilesRo()
		runs := new(atomic.Int32)
		var parallel []state.PBinLeaf
		err := state.ForEachPBinLeafWithWorkers(at, nil, true, workers, runs, func(leaf state.PBinLeaf) error {
			parallel = append(parallel, state.PBinLeaf{Key: bytes.Clone(leaf.Key), Value: bytes.Clone(leaf.Value), Stamp: leaf.Stamp})
			return nil
		})
		at.Close()
		require.NoError(t, err)
		require.Equal(t, serial, parallel, "workers=%d", workers)
		parallelRoot, err := state.PBinRootFromStream(eip8297.SelectedHash(), func(emit func(state.PBinLeaf) error) error {
			for _, leaf := range parallel {
				if emitErr := emit(leaf); emitErr != nil {
					return emitErr
				}
			}
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, serialRoot, parallelRoot, "workers=%d", workers)
		if workers > 1 {
			require.Greater(t, runs.Load(), int32(1), "workers=%d", workers)
		}
	}
}
