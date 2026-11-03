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

package services

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/erigontech/erigon/cl/antiquary/tests"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	forkchoice_mock "github.com/erigontech/erigon/cl/phase1/forkchoice/mock_services"
	"github.com/erigontech/erigon/cl/transition"
	"github.com/erigontech/erigon/common"
	"github.com/stretchr/testify/require"
)

var testParentRootForProposer = common.Hash{1}

// fakeHead serves state as the head state of root, or reports not synced without a state. It implements only what the
// expected-proposer reader uses.
type fakeHead struct {
	synced_data.SyncedData
	root  common.Hash
	state *state.CachingBeaconState
	views atomic.Int32
}

func (h *fakeHead) ViewHeadStateWithIdentity(fn synced_data.ViewHeadStateWithIdentityFn) error {
	h.views.Add(1)
	if h.state == nil {
		return synced_data.ErrNotSynced
	}
	return fn(h.state, h.root, h.state.Slot())
}

// proposerAfterAdvancing is the proposer of slot as the spec computes it: from a copy of the parent advanced to slot.
func proposerAfterAdvancing(t *testing.T, parentState *state.CachingBeaconState, slot uint64) uint64 {
	t.Helper()
	advanced, err := parentState.Copy()
	require.NoError(t, err)
	require.NoError(t, transition.DefaultMachine.ProcessSlots(advanced, slot))
	proposer, err := advanced.GetBeaconProposerIndexForSlot(slot)
	require.NoError(t, err)
	return proposer
}

// A parent that is the head is read from the head state in place, and the answer is reused for every sidecar of the block.
func TestExpectedProposerIsReadFromTheHeldParentStateOnce(t *testing.T) {
	_, _, parentState := tests.GetCapellaRandom()
	cfg := parentState.BeaconConfig()
	slot := (state.Epoch(parentState)+1)*cfg.SlotsPerEpoch - 1
	want, err := parentState.GetBeaconProposerIndexForSlot(slot)
	require.NoError(t, err)
	fc := forkchoice_mock.NewForkChoiceStorageMock(t)
	head := &fakeHead{root: testParentRootForProposer, state: parentState}
	fc.GetStateAtBlockRootFn = func(common.Hash) (*state.CachingBeaconState, error) {
		t.Error("parent state rebuilt although it is the head state")
		return nil, nil
	}
	proposers := newExpectedProposers(t.Context(), cfg, fc, head)

	for range 2 {
		got, err := proposers.get(t.Context(), testParentRootForProposer, parentState.Slot(), slot)
		require.NoError(t, err)
		require.Equal(t, want, got)
	}
	require.Equal(t, int32(1), head.views.Load())
}

// From Fulu on, a parent in the previous epoch holds the proposers of the next one in its lookahead, so they are read
// without advancing or rebuilding the parent.
func TestExpectedProposerOfTheNextEpochIsReadFromTheFuluLookahead(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.AltairForkEpoch, cfg.BellatrixForkEpoch, cfg.CapellaForkEpoch, cfg.DenebForkEpoch, cfg.ElectraForkEpoch, cfg.FuluForkEpoch = 0, 0, 0, 0, 0, 0
	parentEpoch := uint64(3)
	parentSlot := (parentEpoch+1)*cfg.SlotsPerEpoch - 1
	parentState := state.New(&cfg)
	require.NoError(t, parentState.SetSlot(parentSlot))
	parentState.SetVersion(clparams.FuluVersion)
	lookahead := solid.NewUint64VectorSSZ(int((cfg.MinSeedLookahead + 1) * cfg.SlotsPerEpoch))
	for i := range lookahead.Length() {
		lookahead.Set(i, uint64(1000+i))
	}
	parentState.SetProposerLookahead(lookahead)
	fc := forkchoice_mock.NewForkChoiceStorageMock(t)
	head := &fakeHead{root: testParentRootForProposer, state: parentState}
	fc.GetStateAtBlockRootFn = func(common.Hash) (*state.CachingBeaconState, error) {
		t.Error("parent state rebuilt although its lookahead holds the proposer")
		return nil, nil
	}

	got, err := newExpectedProposers(t.Context(), &cfg, fc, head).get(t.Context(), testParentRootForProposer, parentSlot, parentSlot+3)

	require.NoError(t, err)
	require.Equal(t, uint64(1000+cfg.SlotsPerEpoch+2), got)
	require.Equal(t, parentSlot, parentState.Slot())
}

// Before Fulu the parent state does not hold the proposers of a later epoch, so the proposer is computed from the
// parent advanced to the slot, not ignored.
func TestExpectedProposerOfALaterEpochIsComputedFromTheAdvancedParent(t *testing.T) {
	_, _, parentState := tests.GetCapellaRandom()
	cfg := parentState.BeaconConfig()
	slot := (state.Epoch(parentState) + 2) * cfg.SlotsPerEpoch
	want := proposerAfterAdvancing(t, parentState, slot)
	fc := forkchoice_mock.NewForkChoiceStorageMock(t)
	fc.StateAtBlockRootVal[testParentRootForProposer] = parentState
	parentSlot := parentState.Slot()

	got, err := newExpectedProposers(t.Context(), cfg, fc, &fakeHead{}).get(t.Context(), testParentRootForProposer, parentSlot, slot)

	require.NoError(t, err)
	require.Equal(t, want, got)
	require.Equal(t, parentSlot, parentState.Slot(), "the parent state was advanced")
}

// The first Fulu block has an Electra parent: its proposer comes from the parent advanced across the Fulu upgrade.
func TestExpectedProposerAcrossTheElectraFuluBoundary(t *testing.T) {
	_, _, fixtureState := tests.GetElectraRandom()
	encoded, err := fixtureState.EncodeSSZ(nil)
	require.NoError(t, err)
	cfg := *fixtureState.BeaconConfig()
	cfg.AltairForkEpoch, cfg.BellatrixForkEpoch, cfg.CapellaForkEpoch, cfg.DenebForkEpoch, cfg.ElectraForkEpoch = 0, 0, 0, 0, 0
	cfg.FuluForkEpoch = state.Epoch(fixtureState) + 1
	cfg.GloasForkEpoch = cfg.FarFutureEpoch
	parentState := state.New(&cfg)
	require.NoError(t, parentState.DecodeSSZ(encoded, int(clparams.ElectraVersion)))
	slot := cfg.FuluForkEpoch * cfg.SlotsPerEpoch
	require.Equal(t, clparams.ElectraVersion, parentState.Version())
	advanced, err := parentState.Copy()
	require.NoError(t, err)
	require.NoError(t, transition.DefaultMachine.ProcessSlots(advanced, slot))
	require.Equal(t, clparams.FuluVersion, advanced.Version())
	want, err := advanced.GetBeaconProposerIndexForSlot(slot)
	require.NoError(t, err)
	fc := forkchoice_mock.NewForkChoiceStorageMock(t)
	fc.StateAtBlockRootVal[testParentRootForProposer] = parentState

	got, err := newExpectedProposers(t.Context(), &cfg, fc, &fakeHead{}).get(t.Context(), testParentRootForProposer, parentState.Slot(), slot)

	require.NoError(t, err)
	require.Equal(t, want, got)
}

func TestExpectedProposerWithoutAParentStateIsIgnoredAndNotCached(t *testing.T) {
	_, _, parentState := tests.GetCapellaRandom()
	cfg := parentState.BeaconConfig()
	slot := (state.Epoch(parentState) + 2) * cfg.SlotsPerEpoch
	fc := forkchoice_mock.NewForkChoiceStorageMock(t)
	fc.GetStateAtBlockRootFn = func(common.Hash) (*state.CachingBeaconState, error) {
		return nil, errors.New("state not found")
	}
	proposers := newExpectedProposers(t.Context(), cfg, fc, &fakeHead{})

	_, err := proposers.get(t.Context(), testParentRootForProposer, parentState.Slot(), slot)
	require.ErrorIs(t, err, ErrIgnore)

	fc.GetStateAtBlockRootFn = nil
	fc.StateAtBlockRootVal[testParentRootForProposer] = parentState
	_, err = proposers.get(t.Context(), testParentRootForProposer, parentState.Slot(), slot)
	require.NoError(t, err, "a failed read is not cached")
}

// Rebuilds of parent states fork choice does not hold are bounded across parents and slots. A read over the budget
// waits for a rebuild to finish instead of dropping the sidecar, and gives up after a slot. A parent that is the head is
// read meanwhile.
func TestExpectedProposerStateRebuildsAreBounded(t *testing.T) {
	_, _, parentState := tests.GetCapellaRandom()
	cfg := *parentState.BeaconConfig()
	cfg.SecondsPerSlot = 1
	firstSlot := state.Epoch(parentState) * cfg.SlotsPerEpoch
	started, release := make(chan struct{}), make(chan struct{})
	releaseRebuilds := sync.OnceFunc(func() { close(release) })
	t.Cleanup(releaseRebuilds)
	var rebuilds atomic.Int32
	fc := forkchoice_mock.NewForkChoiceStorageMock(t)
	held := common.Hash{0xfe}
	head := &fakeHead{root: held, state: parentState}
	fc.ViewStateAtBlockRootFn = func(_ common.Hash, fn func(*state.CachingBeaconState) error) error {
		rebuilds.Add(1)
		started <- struct{}{}
		<-release
		return fn(parentState)
	}
	proposers := newExpectedProposers(t.Context(), &cfg, fc, head)
	get := func(root common.Hash) <-chan error {
		done := make(chan error, 1)
		go func() {
			_, err := proposers.get(t.Context(), root, parentState.Slot(), firstSlot)
			done <- err
		}()
		return done
	}

	running := make([]<-chan error, 0, maxConcurrentSidecarProposerStateRebuilds)
	for i := range maxConcurrentSidecarProposerStateRebuilds {
		running = append(running, get(common.Hash{byte(i + 1)}))
		<-started
	}
	_, err := proposers.get(t.Context(), held, parentState.Slot(), firstSlot)
	require.NoError(t, err, "the head parent was not read while the rebuild budget was used up")

	start := time.Now()
	select {
	case err := <-get(common.Hash{0xa0}):
		require.ErrorIs(t, err, ErrIgnore)
	case <-started:
		t.Fatal("a parent state was rebuilt past the budget")
	case <-time.After(5 * time.Second):
		t.Fatal("a read over the budget did not give up after a slot")
	}
	require.GreaterOrEqual(t, time.Since(start), time.Second, "a read over the budget gave up before a slot passed")
	require.Equal(t, int32(maxConcurrentSidecarProposerStateRebuilds), rebuilds.Load())

	waiting := get(common.Hash{0xa1})
	// Gives the read time to reach the budget; if it has not, it only starts after the release and passes anyway.
	time.Sleep(100 * time.Millisecond)
	releaseRebuilds()
	go func() {
		for range started {
		}
	}()
	require.NoError(t, <-waiting, "a read over the budget did not wait for a rebuild to finish")
	for _, done := range running {
		require.NoError(t, <-done)
	}
}

// Every sidecar of a block waits for the same shared read, each under its own context: a canceled validation stops
// waiting at once, and the others, the first one's included, still get the proposer.
func TestExpectedProposerWaitersFollowTheirOwnContext(t *testing.T) {
	_, _, parentState := tests.GetCapellaRandom()
	cfg := parentState.BeaconConfig()
	slot := (state.Epoch(parentState)+1)*cfg.SlotsPerEpoch - 1
	want, err := parentState.GetBeaconProposerIndexForSlot(slot)
	require.NoError(t, err)
	started, release := make(chan struct{}), make(chan struct{})
	releaseRead := sync.OnceFunc(func() { close(release) })
	t.Cleanup(releaseRead)
	fc := forkchoice_mock.NewForkChoiceStorageMock(t)
	fc.ViewStateAtBlockRootFn = func(_ common.Hash, fn func(*state.CachingBeaconState) error) error {
		close(started)
		<-release
		return fn(parentState)
	}
	proposers := newExpectedProposers(t.Context(), cfg, fc, &fakeHead{})
	get := func(ctx context.Context) <-chan error {
		done := make(chan error, 1)
		go func() {
			got, err := proposers.get(ctx, testParentRootForProposer, parentState.Slot(), slot)
			if err == nil && got != want {
				err = errors.New("wrong proposer")
			}
			done <- err
		}()
		return done
	}

	firstCtx, cancelFirst := context.WithCancel(t.Context())
	first := get(firstCtx)
	<-started
	secondCtx, cancelSecond := context.WithCancel(t.Context())
	second := get(secondCtx)
	third := get(t.Context())

	cancelSecond()
	select {
	case err := <-second:
		require.ErrorIs(t, err, ErrIgnore)
	case <-time.After(5 * time.Second):
		t.Fatal("a canceled validation kept waiting for the shared read")
	}
	cancelFirst()
	select {
	case err := <-first:
		require.ErrorIs(t, err, ErrIgnore)
	case <-time.After(5 * time.Second):
		t.Fatal("the canceled first validation kept waiting for the shared read")
	}

	releaseRead()
	require.NoError(t, <-third, "the first validation's cancellation failed the other waiters")
	got, err := proposers.get(t.Context(), testParentRootForProposer, parentState.Slot(), slot)
	require.NoError(t, err)
	require.Equal(t, want, got, "the shared read was not cached")
}

// A shared read that waits for a rebuild permit is not stopped when the validation that started it is canceled: the
// other sidecars of the block still get the proposer once a permit frees.
func TestExpectedProposerSharedReadOutlivesTheFirstWaitersCancellation(t *testing.T) {
	_, _, parentState := tests.GetCapellaRandom()
	cfg := parentState.BeaconConfig()
	slot := (state.Epoch(parentState)+1)*cfg.SlotsPerEpoch - 1
	want, err := parentState.GetBeaconProposerIndexForSlot(slot)
	require.NoError(t, err)
	holding, release := make(chan struct{}, maxConcurrentSidecarProposerStateRebuilds), make(chan struct{})
	releaseRebuilds := sync.OnceFunc(func() { close(release) })
	t.Cleanup(releaseRebuilds)
	shared := common.Hash{0xaa}
	fc := forkchoice_mock.NewForkChoiceStorageMock(t)
	fc.ViewStateAtBlockRootFn = func(root common.Hash, fn func(*state.CachingBeaconState) error) error {
		if root != shared {
			holding <- struct{}{}
			<-release
		}
		return fn(parentState)
	}
	proposers := newExpectedProposers(t.Context(), cfg, fc, &fakeHead{})
	get := func(ctx context.Context, root common.Hash) <-chan error {
		done := make(chan error, 1)
		go func() {
			got, err := proposers.get(ctx, root, parentState.Slot(), slot)
			if err == nil && got != want {
				err = errors.New("wrong proposer")
			}
			done <- err
		}()
		return done
	}
	for i := range maxConcurrentSidecarProposerStateRebuilds {
		get(t.Context(), common.Hash{byte(i + 1)})
		<-holding
	}

	firstCtx, cancelFirst := context.WithCancel(t.Context())
	first := get(firstCtx, shared)
	// Lets the first read reach the permit wait and the second join it; if they have not, the test passes anyway.
	time.Sleep(50 * time.Millisecond)
	second := get(t.Context(), shared)
	time.Sleep(50 * time.Millisecond)
	cancelFirst()
	require.ErrorIs(t, <-first, ErrIgnore)

	releaseRebuilds()
	select {
	case err := <-second:
		require.NoError(t, err, "canceling the first validation stopped the shared read")
	case <-time.After(5 * time.Second):
		t.Fatal("the shared read did not finish")
	}
}

// A panic in the shared read runs outside the gossip validator's recovery, so the read recovers it: every waiter gets
// IGNORE, the rebuild permit is returned, and nothing is cached.
func TestExpectedProposerReadPanicIsIgnoredNotFatal(t *testing.T) {
	_, _, parentState := tests.GetCapellaRandom()
	cfg := parentState.BeaconConfig()
	slot := (state.Epoch(parentState)+1)*cfg.SlotsPerEpoch - 1
	started, release := make(chan struct{}), make(chan struct{})
	releaseRead := sync.OnceFunc(func() { close(release) })
	t.Cleanup(releaseRead)
	var panicking atomic.Bool
	panicking.Store(true)
	fc := forkchoice_mock.NewForkChoiceStorageMock(t)
	fc.ViewStateAtBlockRootFn = func(_ common.Hash, fn func(*state.CachingBeaconState) error) error {
		if panicking.Load() {
			close(started)
			<-release
			panic("replay failed")
		}
		return fn(parentState)
	}
	proposers := newExpectedProposers(t.Context(), cfg, fc, &fakeHead{})
	get := func() <-chan error {
		done := make(chan error, 1)
		go func() {
			_, err := proposers.get(t.Context(), testParentRootForProposer, parentState.Slot(), slot)
			done <- err
		}()
		return done
	}

	first := get()
	<-started
	second := get()
	releaseRead()
	for _, done := range []<-chan error{first, second} {
		select {
		case err := <-done:
			require.ErrorIs(t, err, ErrIgnore)
		case <-time.After(5 * time.Second):
			t.Fatal("a waiter was not released after the read panicked")
		}
	}
	require.Zero(t, len(proposers.rebuilds), "the panicking read kept its rebuild permit")

	panicking.Store(false)
	_, err := proposers.get(t.Context(), testParentRootForProposer, parentState.Slot(), slot)
	require.NoError(t, err, "the panicked read was cached")
}

// Reads waiting for a rebuild are bounded too: with the rebuild budget used up and the queue full, a read for another
// parent is ignored at once instead of holding its gossip validation, and the queued reads still finish.
func TestExpectedProposerReadsBeyondTheRebuildQueueAreIgnoredAtOnce(t *testing.T) {
	_, _, parentState := tests.GetCapellaRandom()
	cfg := parentState.BeaconConfig()
	firstSlot := state.Epoch(parentState) * cfg.SlotsPerEpoch
	started, release := make(chan struct{}, maxConcurrentSidecarProposerStateRebuilds+maxQueuedSidecarProposerStateRebuilds), make(chan struct{})
	releaseRebuilds := sync.OnceFunc(func() { close(release) })
	t.Cleanup(releaseRebuilds)
	held := common.Hash{0xfe}
	fc := forkchoice_mock.NewForkChoiceStorageMock(t)
	head := &fakeHead{root: held, state: parentState}
	fc.ViewStateAtBlockRootFn = func(_ common.Hash, fn func(*state.CachingBeaconState) error) error {
		started <- struct{}{}
		<-release
		return fn(parentState)
	}
	proposers := newExpectedProposers(t.Context(), cfg, fc, head)
	get := func(root common.Hash) <-chan error {
		done := make(chan error, 1)
		go func() {
			_, err := proposers.get(t.Context(), root, parentState.Slot(), firstSlot)
			done <- err
		}()
		return done
	}

	admitted := make([]<-chan error, 0, maxConcurrentSidecarProposerStateRebuilds+maxQueuedSidecarProposerStateRebuilds)
	for i := range maxConcurrentSidecarProposerStateRebuilds {
		admitted = append(admitted, get(common.Hash{byte(i + 1)}))
		<-started
	}
	for i := range maxQueuedSidecarProposerStateRebuilds {
		admitted = append(admitted, get(common.Hash{byte(0x80 + i)}))
	}
	require.Eventually(t, func() bool {
		return len(proposers.admitted) == maxConcurrentSidecarProposerStateRebuilds+maxQueuedSidecarProposerStateRebuilds
	}, 5*time.Second, time.Millisecond, "the queued reads were not admitted")

	select {
	case err := <-get(common.Hash{0xff}):
		require.ErrorIs(t, err, ErrIgnore)
	case <-time.After(time.Second):
		t.Fatal("a read beyond the queue waited")
	}
	require.Len(t, started, 0, "a read beyond the queue started a rebuild")
	_, err := proposers.get(t.Context(), held, parentState.Slot(), firstSlot)
	require.NoError(t, err, "the head parent was not read while the rebuild queue was full")

	releaseRebuilds()
	for _, done := range admitted {
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Fatal("a queued read did not finish")
		}
	}
}

// Callers that join a shared read wait for it holding their gossip validations, so their number is bounded too: past
// the limit a caller is ignored at once instead of joining.
func TestExpectedProposerWaitersOfASharedReadAreBounded(t *testing.T) {
	_, _, parentState := tests.GetCapellaRandom()
	cfg := parentState.BeaconConfig()
	slot := (state.Epoch(parentState)+1)*cfg.SlotsPerEpoch - 1
	started, release := make(chan struct{}), make(chan struct{})
	releaseRead := sync.OnceFunc(func() { close(release) })
	t.Cleanup(releaseRead)
	fc := forkchoice_mock.NewForkChoiceStorageMock(t)
	fc.ViewStateAtBlockRootFn = func(_ common.Hash, fn func(*state.CachingBeaconState) error) error {
		close(started)
		<-release
		return fn(parentState)
	}
	proposers := newExpectedProposers(t.Context(), cfg, fc, &fakeHead{})
	get := func() <-chan error {
		done := make(chan error, 1)
		go func() {
			_, err := proposers.get(t.Context(), testParentRootForProposer, parentState.Slot(), slot)
			done <- err
		}()
		return done
	}

	waiters := make([]<-chan error, 0, proposers.maxWaiting)
	waiters = append(waiters, get())
	<-started
	for int64(len(waiters)) < proposers.maxWaiting {
		waiters = append(waiters, get())
	}
	require.Eventually(t, func() bool { return proposers.waiting.Load() == proposers.maxWaiting }, 5*time.Second, time.Millisecond)

	select {
	case err := <-get():
		require.ErrorIs(t, err, ErrIgnore)
	case <-time.After(time.Second):
		t.Fatal("a caller beyond the waiter limit joined the shared read")
	}

	releaseRead()
	for _, done := range waiters {
		require.NoError(t, <-done)
	}
}

// The waiter limit bounds callers waiting for rebuilds only: with it filled by followers of a slow rebuild, a sidecar
// whose parent is the head still gets its proposer.
func TestExpectedProposerHeldParentIsReadWhileWaitersAreFull(t *testing.T) {
	_, _, parentState := tests.GetCapellaRandom()
	cfg := parentState.BeaconConfig()
	slot := (state.Epoch(parentState)+1)*cfg.SlotsPerEpoch - 1
	want, err := parentState.GetBeaconProposerIndexForSlot(slot)
	require.NoError(t, err)
	started, release := make(chan struct{}), make(chan struct{})
	releaseRead := sync.OnceFunc(func() { close(release) })
	t.Cleanup(releaseRead)
	held := common.Hash{0xfe}
	fc := forkchoice_mock.NewForkChoiceStorageMock(t)
	head := &fakeHead{root: held, state: parentState}
	fc.ViewStateAtBlockRootFn = func(_ common.Hash, fn func(*state.CachingBeaconState) error) error {
		close(started)
		<-release
		return fn(parentState)
	}
	proposers := newExpectedProposers(t.Context(), cfg, fc, head)
	waiters := make([]<-chan error, 0, proposers.maxWaiting)
	for range proposers.maxWaiting {
		done := make(chan error, 1)
		go func() {
			_, err := proposers.get(t.Context(), testParentRootForProposer, parentState.Slot(), slot)
			done <- err
		}()
		waiters = append(waiters, done)
	}
	<-started
	require.Eventually(t, func() bool { return proposers.waiting.Load() == proposers.maxWaiting }, 5*time.Second, time.Millisecond)

	got, err := proposers.get(t.Context(), held, parentState.Slot(), slot)
	require.NoError(t, err, "a sidecar whose parent is the head was ignored while the waiter limit was full")
	require.Equal(t, want, got)

	releaseRead()
	for _, done := range waiters {
		require.NoError(t, <-done)
	}
}

// OnBlock holds the fork-choice lock for a whole state transition. A sidecar whose parent is the head is read from the
// head state, which needs no fork-choice lock, so it does not wait for an import.
func TestExpectedProposerHeadParentIsReadWithoutTheForkChoiceLock(t *testing.T) {
	_, _, parentState := tests.GetCapellaRandom()
	cfg := parentState.BeaconConfig()
	slot := (state.Epoch(parentState)+1)*cfg.SlotsPerEpoch - 1
	want, err := parentState.GetBeaconProposerIndexForSlot(slot)
	require.NoError(t, err)
	importing := make(chan struct{})
	t.Cleanup(func() { close(importing) })
	fc := forkchoice_mock.NewForkChoiceStorageMock(t)
	// Every fork-choice state access waits, as it would behind OnBlock's write lock.
	fc.ViewCurrentStateAtBlockRootFn = func(common.Hash, func(*state.CachingBeaconState) error) error {
		<-importing
		return nil
	}
	fc.ViewStateAtBlockRootFn = fc.ViewCurrentStateAtBlockRootFn
	fc.GetStateAtBlockRootFn = func(common.Hash) (*state.CachingBeaconState, error) {
		<-importing
		return nil, nil
	}
	head := &fakeHead{root: testParentRootForProposer, state: parentState}
	proposers := newExpectedProposers(t.Context(), cfg, fc, head)

	done := make(chan error, 1)
	var got uint64
	go func() {
		var err error
		got, err = proposers.get(t.Context(), testParentRootForProposer, parentState.Slot(), slot)
		done <- err
	}()
	select {
	case err := <-done:
		require.NoError(t, err)
		require.Equal(t, want, got)
	case <-time.After(5 * time.Second):
		t.Fatal("a sidecar whose parent is the head waited for the fork-choice lock")
	}
}
