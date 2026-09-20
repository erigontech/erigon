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
	"encoding/binary"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/common"
)

// bigValidatorState builds a state with n validators, large enough to make
// CachingBeaconState.CopyInto take tens of milliseconds, so a test can
// observe whether a lock is held for that duration.
func bigValidatorState(t *testing.T, n int) *state.CachingBeaconState {
	t.Helper()
	s := state.New(&clparams.MainnetBeaconConfig)
	for i := range n {
		var pk [48]byte
		binary.BigEndian.PutUint64(pk[:], uint64(i))
		v := solid.NewValidator()
		v.SetActivationEpoch(0)
		v.SetExitEpoch(^uint64(0))
		v.SetPublicKey(pk)
		v.SetEffectiveBalance(32_000_000_000)
		require.NoError(t, s.AddValidator(v, 32_000_000_000))
	}
	return s
}

// TestViewHeadStateDoesNotWaitForHeadStateCopy proves that a ViewHeadState
// reader running concurrently with OnHeadStateWithBlockRoot is not made to
// wait for the incoming state to finish copying: readers should only ever be
// blocked for the swap itself, regardless of how large the new state is.
func TestViewHeadStateDoesNotWaitForHeadStateCopy(t *testing.T) {
	manager := NewSyncedDataManager(&clparams.MainnetBeaconConfig, true)

	seed := bigValidatorState(t, 1)
	require.NoError(t, manager.OnHeadStateWithBlockRoot(seed, common.Hash{0x01}))

	big := bigValidatorState(t, 500_000)

	writerDone := make(chan struct{})
	var writeErr error
	go func() {
		defer close(writerDone)
		writeErr = manager.OnHeadStateWithBlockRoot(big, common.Hash{0x02})
	}()

	var maxReadLatency time.Duration
	var readCount int
	for {
		select {
		case <-writerDone:
			require.NoError(t, writeErr)
			require.GreaterOrEqual(t, readCount, 100,
				"too few ViewHeadState calls overlapped the writer for this assertion to be meaningful")
			require.Less(t, maxReadLatency, 5*time.Millisecond,
				"a ViewHeadState call blocked for close to the full head-state copy duration")
			return
		default:
		}
		start := time.Now()
		require.NoError(t, manager.ViewHeadState(func(*state.CachingBeaconState) error { return nil }))
		readCount++
		if elapsed := time.Since(start); elapsed > maxReadLatency {
			maxReadLatency = elapsed
		}
	}
}

// TestOnHeadStateWithBlockRootColdStart verifies that the first update
// populates the head state and leaves no previous state, since there is
// nothing to demote yet.
func TestOnHeadStateWithBlockRootColdStart(t *testing.T) {
	manager := NewSyncedDataManager(&clparams.MainnetBeaconConfig, true)
	root := common.Hash{0x01}

	require.NoError(t, manager.OnHeadStateWithBlockRoot(bigValidatorState(t, 1), root))

	require.NoError(t, manager.ViewHeadState(func(headState *state.CachingBeaconState) error {
		require.Equal(t, uint64(0), headState.Slot())
		return nil
	}))
	require.ErrorIs(t, manager.ViewPreviousHeadState(func(*state.CachingBeaconState) error {
		return nil
	}), ErrPreviousStateNotAvailable)
}

// TestOnHeadStateWithBlockRootDemotesPriorHeadToPrevious verifies that after a
// second update, ViewPreviousHeadState observes the state that was head right
// before the update, matching the pre-refactor copy-based semantics.
func TestOnHeadStateWithBlockRootDemotesPriorHeadToPrevious(t *testing.T) {
	manager := NewSyncedDataManager(&clparams.MainnetBeaconConfig, true)

	first := bigValidatorState(t, 1)
	require.NoError(t, first.SetSlot(100))
	require.NoError(t, manager.OnHeadStateWithBlockRoot(first, common.Hash{0x01}))

	second := bigValidatorState(t, 1)
	require.NoError(t, second.SetSlot(200))
	require.NoError(t, manager.OnHeadStateWithBlockRoot(second, common.Hash{0x02}))

	require.NoError(t, manager.ViewHeadState(func(headState *state.CachingBeaconState) error {
		require.Equal(t, uint64(200), headState.Slot())
		return nil
	}))
	require.NoError(t, manager.ViewPreviousHeadState(func(headState *state.CachingBeaconState) error {
		require.Equal(t, uint64(100), headState.Slot())
		return nil
	}))
}

// TestOnHeadStateWithBlockRootSerializesConcurrentWriters verifies that a
// writer copying a large state cannot be overtaken and overwritten by a
// writer that starts later but copies a smaller, faster state. Copying
// outside the reader lock must not let arrival order at the swap depend on
// copy duration.
func TestOnHeadStateWithBlockRootSerializesConcurrentWriters(t *testing.T) {
	manager := NewSyncedDataManager(&clparams.MainnetBeaconConfig, true)
	require.NoError(t, manager.OnHeadStateWithBlockRoot(bigValidatorState(t, 1), common.Hash{0x00}))

	slow := bigValidatorState(t, 500_000)
	require.NoError(t, slow.SetSlot(100))
	fast := bigValidatorState(t, 1)
	require.NoError(t, fast.SetSlot(200))

	var wg sync.WaitGroup
	var slowErr, fastErr error
	slowStarted := make(chan struct{})
	wg.Go(func() {
		close(slowStarted)
		slowErr = manager.OnHeadStateWithBlockRoot(slow, common.Hash{0xaa})
	})
	<-slowStarted
	time.Sleep(time.Millisecond) // let the slow writer start its copy first
	wg.Go(func() {
		fastErr = manager.OnHeadStateWithBlockRoot(fast, common.Hash{0xbb})
	})
	wg.Wait()
	require.NoError(t, slowErr)
	require.NoError(t, fastErr)

	require.NoError(t, manager.ViewHeadState(func(headState *state.CachingBeaconState) error {
		require.Equal(t, uint64(200), headState.Slot(), "the writer that started later must not be overwritten by the slower writer that started first")
		return nil
	}))
}

// TestOnHeadStateWithBlockRootConcurrentReadWrite races real writers against
// real readers through the public API (as opposed to the direct field pokes
// in selected_head_test.go) to catch data races introduced by swapping
// pointers instead of copying into shared buffers. Run with -race.
func TestOnHeadStateWithBlockRootConcurrentReadWrite(t *testing.T) {
	manager := NewSyncedDataManager(&clparams.MainnetBeaconConfig, true)
	require.NoError(t, manager.OnHeadStateWithBlockRoot(bigValidatorState(t, 1), common.Hash{0x00}))

	var wg sync.WaitGroup
	var writeErr error
	wg.Go(func() {
		for i := range 200 {
			s := bigValidatorState(t, 1)
			if err := s.SetSlot(uint64(i)); err != nil {
				writeErr = err
				return
			}
			if err := manager.OnHeadStateWithBlockRoot(s, common.Hash{byte(i)}); err != nil {
				writeErr = err
				return
			}
		}
	})

	for range 200 {
		require.NoError(t, manager.ViewHeadState(func(*state.CachingBeaconState) error { return nil }))
		if err := manager.ViewPreviousHeadState(func(*state.CachingBeaconState) error { return nil }); err != nil {
			require.ErrorIs(t, err, ErrPreviousStateNotAvailable)
		}
	}
	wg.Wait()
	require.NoError(t, writeErr)
}
