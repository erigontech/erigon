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
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/core/state/raw"
	"github.com/erigontech/erigon/common"
)

// bigValidatorState builds a state with n validators, large enough to make
// CachingBeaconState.Copy take tens of milliseconds.
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
// reader is not blocked for the duration of a concurrent writer's copy -
// only for the swap. Uses copyHookForTest to pause the writer at an exact
// point inside its critical section, so the read is proven to overlap the
// copy rather than merely hoped to.
func TestViewHeadStateDoesNotWaitForHeadStateCopy(t *testing.T) {
	manager := NewSyncedDataManager(&clparams.MainnetBeaconConfig, true)
	require.NoError(t, manager.OnHeadStateWithBlockRoot(bigValidatorState(t, 1), common.Hash{0x01}))

	writerPaused := make(chan struct{})
	releaseWriter := make(chan struct{})
	copyHookForTest = func() {
		close(writerPaused)
		<-releaseWriter
	}
	t.Cleanup(func() { copyHookForTest = nil })

	writerDone := make(chan struct{})
	var writeErr error
	next := bigValidatorState(t, 1)
	go func() {
		defer close(writerDone)
		writeErr = manager.OnHeadStateWithBlockRoot(next, common.Hash{0x02})
	}()
	<-writerPaused

	readDone := make(chan error, 1)
	go func() {
		readDone <- manager.ViewHeadState(func(*state.CachingBeaconState) error { return nil })
	}()

	select {
	case err := <-readDone:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("ViewHeadState did not complete while the writer was paused before its copy")
	}

	close(releaseWriter)
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

// TestOnHeadStateWithBlockRootDemotesPriorHeadToPrevious verifies that after
// a second update, ViewPreviousHeadState observes the state that was head
// right before the update.
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

func TestPublishCopiesIntoTheStateDroppedByTheLastPublish(t *testing.T) {
	manager := NewSyncedDataManager(&clparams.MainnetBeaconConfig, true)
	var heads []*state.CachingBeaconState
	for i := range 4 {
		s := bigValidatorState(t, 1)
		require.NoError(t, s.SetSlot(uint64(100+i)))
		require.NoError(t, manager.OnHeadStateWithBlockRoot(s, common.Hash{byte(1 + i)}))
		require.NoError(t, manager.ViewHeadState(func(headState *state.CachingBeaconState) error {
			heads = append(heads, headState)
			return nil
		}))
	}

	require.Same(t, heads[0], heads[3])
	require.Equal(t, uint64(103), heads[3].Slot())
	require.NoError(t, manager.ViewPreviousHeadState(func(previous *state.CachingBeaconState) error {
		require.Same(t, heads[2], previous)
		require.Equal(t, uint64(102), previous.Slot())
		return nil
	}))
}

func TestFailedHeadUpdateKeepsHead(t *testing.T) {
	manager := NewSyncedDataManager(&clparams.MainnetBeaconConfig, true)
	for i := range 3 {
		good := state.New(&clparams.MainnetBeaconConfig)
		require.NoError(t, good.SetSlot(uint64(100+i)))
		require.NoError(t, manager.OnHeadStateWithBlockRoot(good, common.Hash{byte(1 + i)}))
	}

	broken := state.New(&clparams.MainnetBeaconConfig)
	require.NoError(t, broken.SetSlot(150))
	broken.AddPreviousEpochAttestation(&solid.PendingAttestation{
		AggregationBits: solid.NewBitList(0, 2048),
		Data:            &solid.AttestationData{Slot: 200},
	})
	require.ErrorIs(t, manager.OnHeadStateWithBlockRoot(broken, common.Hash{0xff}), raw.ErrGetBlockRootAtSlotFuture)

	root, slot, ok := manager.StateHead()
	require.True(t, ok)
	require.Equal(t, common.Hash{0x03}, root)
	require.Equal(t, uint64(102), slot)
	require.NoError(t, manager.ViewHeadState(func(headState *state.CachingBeaconState) error {
		require.Equal(t, uint64(102), headState.Slot())
		return nil
	}))
	require.NoError(t, manager.ViewPreviousHeadState(func(headState *state.CachingBeaconState) error {
		require.Equal(t, uint64(101), headState.Slot())
		return nil
	}))
}

// TestOnHeadStateWithBlockRootSerializesConcurrentWriters verifies that a
// writer cannot be overtaken and overwritten by a writer that starts later.
// Deterministic: holds writeLock directly to represent a writer already
// inside its critical section, rather than racing on timing.
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

	first := bigValidatorState(t, 1)
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

type blockingHashVector struct {
	solid.HashVectorSSZ
	started chan<- struct{}
	release <-chan struct{}
}

func (v *blockingHashVector) HashSSZ() ([32]byte, error) {
	close(v.started)
	<-v.release
	return v.HashVectorSSZ.HashSSZ()
}

// Root resolution must hold writeLock so a known-root update cannot publish
// first and then be overwritten when the earlier root resolution finishes.
func TestOnHeadStateSerializesAgainstOnHeadStateWithBlockRoot(t *testing.T) {
	manager := NewSyncedDataManager(&clparams.MainnetBeaconConfig, true)

	first := bigValidatorState(t, 1)
	require.NoError(t, first.SetSlot(100))
	second := bigValidatorState(t, 1)
	require.NoError(t, second.SetSlot(200))

	rootStarted := make(chan struct{})
	releaseRoot := make(chan struct{})
	// BlockRoot hashes this vector. Pausing its HashSSZ call keeps root resolution
	// in progress while we check the lock, regardless of state size or scheduling.
	first.SetBlockRoots(&blockingHashVector{
		HashVectorSSZ: first.BlockRoots(),
		started:       rootStarted,
		release:       releaseRoot,
	})
	resumeRoot := sync.OnceFunc(func() { close(releaseRoot) })
	var writers sync.WaitGroup
	// An assertion failure must release the paused hash before joining the writers.
	t.Cleanup(func() {
		resumeRoot()
		writers.Wait()
	})

	var firstErr, secondErr error
	writers.Go(func() { firstErr = manager.OnHeadState(first) })
	<-rootStarted
	// The second writer has not started, so only OnHeadState can own writeLock.
	if manager.writeLock.TryLock() {
		manager.writeLock.Unlock()
		t.Fatal("OnHeadState must hold writeLock while resolving BlockRoot")
	}

	writers.Go(func() { secondErr = manager.OnHeadStateWithBlockRoot(second, common.Hash{0xbb}) })
	resumeRoot()
	writers.Wait()
	require.NoError(t, firstErr)
	require.NoError(t, secondErr)

	require.NoError(t, manager.ViewHeadState(func(headState *state.CachingBeaconState) error {
		require.Equal(t, uint64(200), headState.Slot())
		return nil
	}))
	require.NoError(t, manager.ViewPreviousHeadState(func(headState *state.CachingBeaconState) error {
		require.Equal(t, uint64(100), headState.Slot())
		return nil
	}))
}

// TestUnsetHeadStateSerializesWithPublish verifies that UnsetHeadState
// cannot land in the middle of an in-flight publish and then be resurrected
// by it: it must either fully precede or fully follow any given publish.
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
// real readers through the public API to catch data races introduced by
// swapping pointers instead of copying into shared buffers. Run with -race.
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

func TestOnOwnedHeadStateWithBlockRootColdStart(t *testing.T) {
	manager := NewSyncedDataManager(&clparams.MainnetBeaconConfig, true)
	owned := state.New(&clparams.MainnetBeaconConfig)
	require.NoError(t, owned.SetSlot(42))
	root := common.Hash{0x42}
	spare := state.New(&clparams.MainnetBeaconConfig)
	manager.spareHeadState = spare

	require.NoError(t, manager.OnOwnedHeadStateWithBlockRoot(owned, root))

	require.NoError(t, manager.ViewHeadState(func(head *state.CachingBeaconState) error {
		require.Same(t, owned, head)
		return nil
	}))
	require.ErrorIs(t, manager.ViewPreviousHeadState(func(*state.CachingBeaconState) error {
		return nil
	}), ErrPreviousStateNotAvailable)
	actualRoot, actualSlot, ok := manager.StateHead()
	require.True(t, ok)
	require.Equal(t, root, actualRoot)
	require.Equal(t, uint64(42), actualSlot)
	require.Same(t, spare, manager.spareHeadState)
}

func TestOnOwnedHeadStateWithBlockRootCopyOwnedCopyRotation(t *testing.T) {
	manager := NewSyncedDataManager(&clparams.MainnetBeaconConfig, true)
	for i := range 3 {
		require.NoError(t, manager.OnHeadStateWithBlockRoot(state.New(&clparams.MainnetBeaconConfig), common.Hash{byte(i + 1)}))
	}
	formerHead := manager.headState
	spare := manager.spareHeadState
	require.NotNil(t, spare)

	owned := state.New(&clparams.MainnetBeaconConfig)
	require.NoError(t, manager.OnOwnedHeadStateWithBlockRoot(owned, common.Hash{0x04}))
	require.Same(t, owned, manager.headState)
	require.Same(t, formerHead, manager.previousHeadState)
	require.Same(t, spare, manager.spareHeadState)

	copyInput := state.New(&clparams.MainnetBeaconConfig)
	require.NoError(t, manager.OnHeadStateWithBlockRoot(copyInput, common.Hash{0x05}))
	require.NotSame(t, copyInput, manager.headState)
	require.Same(t, owned, manager.previousHeadState)
	require.Same(t, formerHead, manager.spareHeadState)
}

func TestOnOwnedHeadStateWithBlockRootDemotesOwnedHead(t *testing.T) {
	manager := NewSyncedDataManager(&clparams.MainnetBeaconConfig, true)
	first := state.New(&clparams.MainnetBeaconConfig)
	second := state.New(&clparams.MainnetBeaconConfig)

	require.NoError(t, manager.OnOwnedHeadStateWithBlockRoot(first, common.Hash{0x01}))
	require.NoError(t, manager.OnOwnedHeadStateWithBlockRoot(second, common.Hash{0x02}))

	require.Same(t, second, manager.headState)
	require.Same(t, first, manager.previousHeadState)
}

func TestOnOwnedHeadStateWithBlockRootRejectsManagedStates(t *testing.T) {
	manager := NewSyncedDataManager(&clparams.MainnetBeaconConfig, true)
	for i := range 3 {
		s := state.New(&clparams.MainnetBeaconConfig)
		require.NoError(t, s.SetSlot(uint64(i+1)))
		require.NoError(t, manager.OnHeadStateWithBlockRoot(s, common.Hash{byte(i + 1)}))
	}

	head := manager.headState
	previous := manager.previousHeadState
	spare := manager.spareHeadState
	root, slot, ok := manager.StateHead()

	for name, rejected := range map[string]*state.CachingBeaconState{
		"nil":      nil,
		"head":     head,
		"previous": previous,
		"spare":    spare,
	} {
		t.Run(name, func(t *testing.T) {
			require.Error(t, manager.OnOwnedHeadStateWithBlockRoot(rejected, common.Hash{0xff}))
			require.Same(t, head, manager.headState)
			require.Same(t, previous, manager.previousHeadState)
			require.Same(t, spare, manager.spareHeadState)
			actualRoot, actualSlot, actualOK := manager.StateHead()
			require.Equal(t, root, actualRoot)
			require.Equal(t, slot, actualSlot)
			require.Equal(t, ok, actualOK)
		})
	}
}

func TestOnOwnedHeadStateWithBlockRootDisabled(t *testing.T) {
	manager := NewSyncedDataManager(&clparams.MainnetBeaconConfig, false)
	require.NoError(t, manager.OnOwnedHeadStateWithBlockRoot(nil, common.Hash{0x01}))
	require.Nil(t, manager.headState)
}

func TestOnOwnedHeadStateWithBlockRootConcurrentAccess(t *testing.T) {
	manager := NewSyncedDataManager(&clparams.MainnetBeaconConfig, true)
	require.NoError(t, manager.OnHeadStateWithBlockRoot(state.New(&clparams.MainnetBeaconConfig), common.Hash{}))

	const iterations = 50
	errs := make(chan error, iterations*2)
	var wg sync.WaitGroup
	wg.Go(func() {
		for i := range iterations {
			s := state.New(&clparams.MainnetBeaconConfig)
			if err := s.SetSlot(uint64(i)); err != nil {
				errs <- err
				return
			}
			if err := manager.OnHeadStateWithBlockRoot(s, common.Hash{byte(i)}); err != nil {
				errs <- err
				return
			}
		}
	})
	wg.Go(func() {
		for i := range iterations {
			s := state.New(&clparams.MainnetBeaconConfig)
			if err := s.SetSlot(uint64(i)); err != nil {
				errs <- err
				return
			}
			if err := manager.OnOwnedHeadStateWithBlockRoot(s, common.Hash{byte(i), 0x01}); err != nil {
				errs <- err
				return
			}
		}
	})
	wg.Go(func() {
		for range iterations {
			manager.UnsetHeadState()
		}
	})
	wg.Go(func() {
		for range iterations {
			if err := manager.ViewHeadState(func(*state.CachingBeaconState) error { return nil }); err != nil && !errors.Is(err, ErrNotSynced) {
				errs <- fmt.Errorf("view head state: %w", err)
				return
			}
			if err := manager.ViewPreviousHeadState(func(*state.CachingBeaconState) error { return nil }); err != nil && !errors.Is(err, ErrPreviousStateNotAvailable) {
				errs <- fmt.Errorf("view previous head state: %w", err)
				return
			}
			manager.CommitteeCount(0)
		}
	})
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
}
