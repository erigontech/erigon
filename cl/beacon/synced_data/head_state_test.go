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
	"sync/atomic"
	"testing"
	"time"

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
		goodRoot := common.Hash{byte(1 + i)}
		require.NoError(t, manager.OnHeadStateWithBlockRoot(good, goodRoot))

		rootBefore, slotBefore, okBefore := manager.StateHead()
		require.True(t, okBefore)

		require.Error(t, manager.OnHeadStateWithBlockRoot(broken, common.Hash{0xff}))
		require.ErrorIs(t, manager.ViewPreviousHeadState(func(*state.CachingBeaconState) error { return nil }), ErrPreviousStateNotAvailable)

		rootAfter, slotAfter, okAfter := manager.StateHead()
		require.True(t, okAfter)
		require.Equal(t, rootBefore, rootAfter)
		require.Equal(t, slotBefore, slotAfter)
		require.Equal(t, goodRoot, rootAfter)
		require.Equal(t, good.Slot(), slotAfter)
		require.Equal(t, goodRoot, manager.HeadRoot())
		require.Equal(t, good.Slot(), manager.HeadSlot())

		var slot uint64
		require.NoError(t, manager.ViewHeadState(func(s *state.CachingBeaconState) error {
			slot = s.Slot()
			return nil
		}))
		require.Equal(t, good.Slot(), slot)
	}
}

func TestWriterWaitsForHeadStateView(t *testing.T) {
	manager := NewSyncedDataManager(&clparams.MainnetBeaconConfig, true)
	published := state.New(&clparams.MainnetBeaconConfig)
	require.NoError(t, published.SetSlot(100))
	require.NoError(t, manager.OnHeadStateWithBlockRoot(published, common.Hash{0x1}))

	next := state.New(&clparams.MainnetBeaconConfig)
	require.NoError(t, next.SetSlot(101))

	inView := make(chan struct{})
	writerDone := make(chan error, 1)
	var writerFinished atomic.Bool
	go func() {
		<-inView
		err := manager.OnHeadStateWithBlockRoot(next, common.Hash{0x2})
		writerFinished.Store(true)
		writerDone <- err
	}()

	require.NoError(t, manager.ViewHeadState(func(s *state.CachingBeaconState) error {
		close(inView)
		time.Sleep(100 * time.Millisecond)
		require.False(t, writerFinished.Load(), "writer recycled the head buffer while a view still owned it")
		require.Equal(t, uint64(100), s.Slot())
		return nil
	}))

	require.NoError(t, <-writerDone)
	require.Equal(t, common.Hash{0x2}, manager.HeadRoot())
	require.Equal(t, uint64(101), manager.HeadSlot())
}

func TestWriterWaitsForPreviousHeadStateView(t *testing.T) {
	manager := NewSyncedDataManager(&clparams.MainnetBeaconConfig, true)
	first := state.New(&clparams.MainnetBeaconConfig)
	require.NoError(t, first.SetSlot(100))
	require.NoError(t, manager.OnHeadStateWithBlockRoot(first, common.Hash{0x1}))
	second := state.New(&clparams.MainnetBeaconConfig)
	require.NoError(t, second.SetSlot(101))
	require.NoError(t, manager.OnHeadStateWithBlockRoot(second, common.Hash{0x2}))

	third := state.New(&clparams.MainnetBeaconConfig)
	require.NoError(t, third.SetSlot(102))

	inView := make(chan struct{})
	writerDone := make(chan error, 1)
	var writerFinished atomic.Bool
	go func() {
		<-inView
		err := manager.OnHeadStateWithBlockRoot(third, common.Hash{0x3})
		writerFinished.Store(true)
		writerDone <- err
	}()

	require.NoError(t, manager.ViewPreviousHeadState(func(s *state.CachingBeaconState) error {
		close(inView)
		time.Sleep(100 * time.Millisecond)
		require.False(t, writerFinished.Load(), "writer recycled the previous-head buffer while a view still owned it")
		require.Equal(t, uint64(100), s.Slot())
		return nil
	}))

	require.NoError(t, <-writerDone)
	require.Equal(t, common.Hash{0x3}, manager.HeadRoot())
	require.Equal(t, uint64(102), manager.HeadSlot())
}

func TestHeadIdentityMatchesPublishedState(t *testing.T) {
	manager := NewSyncedDataManager(&clparams.MainnetBeaconConfig, true)
	published := state.New(&clparams.MainnetBeaconConfig)
	require.NoError(t, published.SetSlot(128))
	root := common.Hash{0xab}
	require.NoError(t, manager.OnHeadStateWithBlockRoot(published, root))

	var seenRoot common.Hash
	var seenSlot, stateSlot uint64
	require.NoError(t, manager.ViewHeadStateWithIdentity(func(s *state.CachingBeaconState, r common.Hash, slot uint64) error {
		seenRoot, seenSlot, stateSlot = r, slot, s.Slot()
		return nil
	}))
	require.Equal(t, root, seenRoot)
	require.Equal(t, published.Slot(), seenSlot)
	require.Equal(t, published.Slot(), stateSlot)

	headRoot, headSlot, ok := manager.StateHead()
	require.True(t, ok)
	require.Equal(t, root, headRoot)
	require.Equal(t, published.Slot(), headSlot)
	require.Equal(t, root, manager.HeadRoot())
	require.Equal(t, published.Slot(), manager.HeadSlot())
	require.False(t, manager.Syncing())

	manager.UnsetHeadState()
	require.True(t, manager.Syncing())
	require.ErrorIs(t, manager.ViewHeadState(func(*state.CachingBeaconState) error { return nil }), ErrNotSynced)
	require.ErrorIs(t, manager.ViewPreviousHeadState(func(*state.CachingBeaconState) error { return nil }), ErrPreviousStateNotAvailable)
	_, _, ok = manager.StateHead()
	require.False(t, ok)
}

func TestDisabledHeadUpdateIsNoop(t *testing.T) {
	manager := NewSyncedDataManager(&clparams.MainnetBeaconConfig, false)
	require.NotPanics(t, func() { require.NoError(t, manager.OnHeadState(nil)) })
}
