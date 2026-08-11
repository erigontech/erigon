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
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/common"
)

func TestSelectedHeadLifecycle(t *testing.T) {
	manager := NewSyncedDataManager(&clparams.MainnetBeaconConfig, true)

	_, _, ok := manager.SelectedHead()
	require.False(t, ok)

	root100 := common.Hash{0x10}
	manager.OnSelectedHead(root100, 100)
	root, slot, ok := manager.SelectedHead()
	require.True(t, ok)
	require.Equal(t, root100, root)
	require.Equal(t, uint64(100), slot)

	root99 := common.Hash{0x09}
	manager.OnSelectedHead(root99, 99)
	root, slot, ok = manager.SelectedHead()
	require.True(t, ok)
	require.Equal(t, root99, root)
	require.Equal(t, uint64(99), slot)

	manager.UnsetHeadState()
	_, _, ok = manager.SelectedHead()
	require.False(t, ok)
}

func TestSelectedHeadDisabled(t *testing.T) {
	manager := NewSyncedDataManager(&clparams.MainnetBeaconConfig, false)
	manager.OnSelectedHead(common.Hash{0x10}, 100)

	_, _, ok := manager.SelectedHead()
	require.False(t, ok)
}

func TestSelectedHeadRootAndSlotStayInOneGeneration(t *testing.T) {
	manager := NewSyncedDataManager(&clparams.MainnetBeaconConfig, true)
	rootA := common.Hash{0xaa}
	rootB := common.Hash{0xbb}
	manager.OnSelectedHead(rootA, 100)

	var writers sync.WaitGroup
	writers.Go(func() {
		for range 10_000 {
			manager.OnSelectedHead(rootB, 99)
			manager.OnSelectedHead(rootA, 100)
		}
	})

	for range 10_000 {
		root, slot, ok := manager.SelectedHead()
		require.True(t, ok)
		require.True(t, root == rootA && slot == 100 || root == rootB && slot == 99)
	}
	writers.Wait()
}

func TestStateHeadRootAndSlotStayInOneGeneration(t *testing.T) {
	manager := NewSyncedDataManager(&clparams.MainnetBeaconConfig, true)
	rootA := common.Hash{0xaa}
	rootB := common.Hash{0xbb}
	manager.stateHead.Store(&headIdentity{root: rootA, slot: 100})

	var writers sync.WaitGroup
	writers.Go(func() {
		for range 10_000 {
			manager.stateHead.Store(&headIdentity{root: rootB, slot: 99})
			manager.stateHead.Store(&headIdentity{root: rootA, slot: 100})
		}
	})

	for range 10_000 {
		root, slot, ok := manager.StateHead()
		require.True(t, ok)
		require.True(t, root == rootA && slot == 100 || root == rootB && slot == 99)
	}
	writers.Wait()
}

func TestViewHeadStateWithIdentityStaysInOneGeneration(t *testing.T) {
	manager := NewSyncedDataManager(&clparams.MainnetBeaconConfig, true)
	manager.headState = state.New(&clparams.MainnetBeaconConfig)
	manager.stateHead.Store(&headIdentity{root: common.Hash{0xaa}, slot: 100})
	manager.headState.SetSlot(100)

	var writers sync.WaitGroup
	writers.Go(func() {
		for range 10_000 {
			manager.mu.Lock()
			manager.headState.SetSlot(99)
			manager.stateHead.Store(&headIdentity{root: common.Hash{0xbb}, slot: 99})
			manager.mu.Unlock()

			manager.mu.Lock()
			manager.headState.SetSlot(100)
			manager.stateHead.Store(&headIdentity{root: common.Hash{0xaa}, slot: 100})
			manager.mu.Unlock()
		}
	})

	for range 10_000 {
		require.NoError(t, manager.ViewHeadStateWithIdentity(func(headState *state.CachingBeaconState, root common.Hash, slot uint64) error {
			require.Equal(t, slot, headState.Slot())
			require.True(t, root == (common.Hash{0xaa}) && slot == 100 || root == (common.Hash{0xbb}) && slot == 99)
			return nil
		}))
	}
	writers.Wait()
}
