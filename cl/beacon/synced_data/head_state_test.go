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

package synced_data

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/common"
)

func TestHeadUpdateKeepsPreviousHead(t *testing.T) {
	manager := NewSyncedDataManager(&clparams.MainnetBeaconConfig, true)
	var headRoot [32]byte
	for i := range 3 {
		published := state.New(&clparams.MainnetBeaconConfig)
		require.NoError(t, published.SetSlot(uint64(100+i)))
		prevRoot := headRoot
		var err error
		headRoot, err = published.BlockRoot()
		require.NoError(t, err)
		require.NoError(t, manager.OnHeadStateWithBlockRoot(published, headRoot))

		var head, prev *state.CachingBeaconState
		require.NoError(t, manager.ViewHeadState(func(s *state.CachingBeaconState) error {
			head = s
			return nil
		}))
		prevErr := manager.ViewPreviousHeadState(func(s *state.CachingBeaconState) error {
			prev = s
			return nil
		})
		require.NotSame(t, published, head)
		require.Equal(t, published.Slot(), head.Slot())
		if i == 0 {
			require.ErrorIs(t, prevErr, ErrPreviousStateNotAvailable)
			continue
		}
		require.NoError(t, prevErr)
		require.NotSame(t, head, prev)
		root, err := prev.BlockRoot()
		require.NoError(t, err)
		require.Equal(t, prevRoot, root)
	}
}

func TestFailedHeadUpdateKeepsHead(t *testing.T) {
	broken := state.New(&clparams.MainnetBeaconConfig)
	require.NoError(t, broken.SetSlot(100))
	broken.AddPreviousEpochAttestation(&solid.PendingAttestation{
		AggregationBits: solid.NewBitList(0, 2048),
		Data:            &solid.AttestationData{Slot: 200},
	})
	manager := NewSyncedDataManager(&clparams.MainnetBeaconConfig, true)
	for i := range 2 {
		good := state.New(&clparams.MainnetBeaconConfig)
		require.NoError(t, good.SetSlot(uint64(10+i)))
		require.NoError(t, manager.OnHeadStateWithBlockRoot(good, common.Hash{}))
		require.Error(t, manager.OnHeadStateWithBlockRoot(broken, common.Hash{}))
		require.ErrorIs(t, manager.ViewPreviousHeadState(func(*state.CachingBeaconState) error { return nil }), ErrPreviousStateNotAvailable)

		var slot uint64
		require.NoError(t, manager.ViewHeadState(func(s *state.CachingBeaconState) error {
			slot = s.Slot()
			return nil
		}))
		require.Equal(t, good.Slot(), slot)
	}
}
