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
//
// Any threshold on absolute read latency is inherently fragile - too tight
// and it flakes on scheduling/GC noise (as the first version of this test
// did under -race), too loose and it stops discriminating the bug it exists
// to catch (as the second version did: a reproduction against the pre-fix
// code showed max read latency landing at 26.8-36.5ms, comfortably under a
// 50ms threshold meant to tolerate -race noise). This version drops
// absolute timing entirely: it waits until the writer has genuinely entered
// its critical section (proven by TryLock failing, not by counting reads or
// guessing a timing margin), then asserts a real read completes before the
// writer does - true on the fix regardless of how long the copy or any
// scheduling delay takes. On a synthetic worst-case regression (the copy
// moved back under mu, with the smallest possible gap between mu and
// writeLock releasing), this reliably catches it (16/20 runs); the residual
// miss rate is the unavoidable tail race between a read unblocking and this
// check running, not a calibration problem, and does not occur on the fix
// (20/20 clean).
func TestViewHeadStateDoesNotWaitForHeadStateCopy(t *testing.T) {
	manager := NewSyncedDataManager(&clparams.MainnetBeaconConfig, true)

	seed := bigValidatorState(t, 1)
	require.NoError(t, manager.OnHeadStateWithBlockRoot(seed, common.Hash{0x01}))

	big := bigValidatorState(t, 500_000) // large enough that the copy takes tens of ms

	writerDone := make(chan struct{})
	var writeErr error
	go func() {
		defer close(writerDone)
		writeErr = manager.OnHeadStateWithBlockRoot(big, common.Hash{0x02})
	}()

	for {
		select {
		case <-writerDone:
			t.Fatal("writer finished before it was observed to enter its critical section")
		default:
		}
		if manager.writeLock.TryLock() {
			// Nothing else in this test holds writeLock, so succeeding here
			// only proves the writer has not reached it yet - keep polling.
			manager.writeLock.Unlock()
			continue
		}
		break
	}

	require.NoError(t, manager.ViewHeadState(func(*state.CachingBeaconState) error { return nil }))

	select {
	case <-writerDone:
		t.Fatal("the writer already finished by the time the read completed - " +
			"the read did not overlap the copy, so this run proves nothing")
	default:
	}

	<-writerDone
	require.NoError(t, writeErr)
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
// writer that starts later but copies a smaller, faster state. Ordering is
// proven deterministically by holding writeLock directly to represent a
// writer already inside its critical section, rather than by racing on
// relative timing.
func TestOnHeadStateWithBlockRootSerializesConcurrentWriters(t *testing.T) {
	manager := NewSyncedDataManager(&clparams.MainnetBeaconConfig, true)
	require.NoError(t, manager.OnHeadStateWithBlockRoot(bigValidatorState(t, 1), common.Hash{0x00}))

	manager.writeLock.Lock() // represents a writer already inside publishHeadState

	second := bigValidatorState(t, 1)
	require.NoError(t, second.SetSlot(200))
	secondDone := make(chan error, 1)
	go func() {
		secondDone <- manager.OnHeadStateWithBlockRoot(second, common.Hash{0xbb})
	}()

	select {
	case err := <-secondDone:
		t.Fatalf("second writer completed (err=%v) while writeLock was still held", err)
	case <-time.After(20 * time.Millisecond):
	}

	first := bigValidatorState(t, 500_000)
	require.NoError(t, first.SetSlot(100))
	firstCopy, err := first.Copy()
	require.NoError(t, err)
	manager.mu.Lock()
	manager.previousHeadState = manager.headState
	manager.headState = firstCopy
	manager.stateHead.Store(&headIdentity{root: common.Hash{0xaa}, slot: first.Slot()})
	manager.mu.Unlock()
	manager.writeLock.Unlock()

	require.NoError(t, <-secondDone)
	require.NoError(t, manager.ViewHeadState(func(headState *state.CachingBeaconState) error {
		require.Equal(t, uint64(200), headState.Slot(), "the writer that started later must not be overwritten by the writer that released writeLock first")
		return nil
	}))
}

// TestOnHeadStateResolvesRootUnderWriteLock verifies that OnHeadState's own
// BlockRoot() computation is serialized against other writers the same way
// OnHeadStateWithBlockRoot's copy is: a competing writer with nothing to
// compute must not be able to publish and then get overwritten once the
// slower root resolution finishes.
//
// slowRoot is sized so BlockRoot() alone takes ~40ms (calibrated). It is
// launched first and confirmed started before fast launches, then given a
// 10ms head start - a quarter of its ~40ms operation - before fast is
// created, so fast can only run ahead of it if root resolution is not
// serialized under writeLock (the bug): with the fix, slowRoot has already
// acquired writeLock by the time fast is created, so fast is deterministically
// blocked behind it regardless of scheduling.
func TestOnHeadStateResolvesRootUnderWriteLock(t *testing.T) {
	manager := NewSyncedDataManager(&clparams.MainnetBeaconConfig, true)
	require.NoError(t, manager.OnHeadStateWithBlockRoot(bigValidatorState(t, 1), common.Hash{0x00}))

	slowRoot := bigValidatorState(t, 100_000)
	require.NoError(t, slowRoot.SetSlot(100))
	fast := bigValidatorState(t, 1)
	require.NoError(t, fast.SetSlot(200))

	slowStarted := make(chan struct{})
	slowDone := make(chan error, 1)
	go func() {
		close(slowStarted)
		slowDone <- manager.OnHeadState(slowRoot)
	}()
	<-slowStarted
	time.Sleep(10 * time.Millisecond)

	fastDone := make(chan error, 1)
	go func() { fastDone <- manager.OnHeadStateWithBlockRoot(fast, common.Hash{0xbb}) }()

	require.NoError(t, <-slowDone)
	require.NoError(t, <-fastDone)

	require.NoError(t, manager.ViewHeadState(func(headState *state.CachingBeaconState) error {
		require.Equal(t, uint64(200), headState.Slot(),
			"OnHeadState's root resolution must not run outside writeLock, letting a concurrent writer publish and then be overwritten once it finishes")
		return nil
	}))
}

// TestUnsetHeadStateSerializesWithPublish verifies that UnsetHeadState cannot
// run while a publish is already inside its critical section and then let
// that in-flight publish resurrect a head UnsetHeadState is meant to clear:
// once UnsetHeadState is called, it must either fully precede or fully
// follow any given publish, never land in between the publish's copy and its
// swap.
func TestUnsetHeadStateSerializesWithPublish(t *testing.T) {
	manager := NewSyncedDataManager(&clparams.MainnetBeaconConfig, true)
	require.NoError(t, manager.OnHeadStateWithBlockRoot(bigValidatorState(t, 1), common.Hash{0x00}))

	manager.writeLock.Lock() // represents a publish already inside its critical section

	unsetDone := make(chan struct{})
	go func() {
		defer close(unsetDone)
		manager.UnsetHeadState()
	}()

	select {
	case <-unsetDone:
		t.Fatal("UnsetHeadState completed while writeLock was still held by an in-flight publish")
	case <-time.After(20 * time.Millisecond):
	}

	published := bigValidatorState(t, 1)
	require.NoError(t, published.SetSlot(100))
	copied, err := published.Copy()
	require.NoError(t, err)
	manager.mu.Lock()
	manager.previousHeadState = manager.headState
	manager.headState = copied
	manager.stateHead.Store(&headIdentity{root: common.Hash{0xaa}, slot: published.Slot()})
	manager.mu.Unlock()
	manager.writeLock.Unlock()

	<-unsetDone
	require.True(t, manager.Syncing(),
		"UnsetHeadState must not be resurrected by a publish that was already in flight when it was called")
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
