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
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/erigontech/erigon/cl/antiquary/tests"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	forkchoice_mock "github.com/erigontech/erigon/cl/phase1/forkchoice/mock_services"
	"github.com/erigontech/erigon/cl/transition"
	"github.com/erigontech/erigon/common"
	"github.com/stretchr/testify/require"
)

var testParentRootForProposer = common.Hash{1}

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

// The parent state fork choice holds is read in place, and the answer is reused for every sidecar of the block.
func TestExpectedProposerIsReadFromTheHeldParentStateOnce(t *testing.T) {
	_, _, parentState := tests.GetCapellaRandom()
	cfg := parentState.BeaconConfig()
	slot := (state.Epoch(parentState)+1)*cfg.SlotsPerEpoch - 1
	want, err := parentState.GetBeaconProposerIndexForSlot(slot)
	require.NoError(t, err)
	fc := forkchoice_mock.NewForkChoiceStorageMock(t)
	var views atomic.Int32
	fc.ViewCurrentStateAtBlockRootFn = func(_ common.Hash, fn func(*state.CachingBeaconState) error) error {
		views.Add(1)
		return fn(parentState)
	}
	fc.GetStateAtBlockRootFn = func(common.Hash) (*state.CachingBeaconState, error) {
		t.Error("parent state rebuilt although fork choice holds it")
		return nil, nil
	}
	proposers := newExpectedProposers(cfg, fc)

	for range 2 {
		got, err := proposers.get(t.Context(), testParentRootForProposer, parentState.Slot(), slot)
		require.NoError(t, err)
		require.Equal(t, want, got)
	}
	require.Equal(t, int32(1), views.Load())
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
	fc.StateAtBlockRootVal[testParentRootForProposer] = parentState
	fc.GetStateAtBlockRootFn = func(common.Hash) (*state.CachingBeaconState, error) {
		t.Error("parent state rebuilt although its lookahead holds the proposer")
		return nil, nil
	}

	got, err := newExpectedProposers(&cfg, fc).get(t.Context(), testParentRootForProposer, parentSlot, parentSlot+3)

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

	got, err := newExpectedProposers(cfg, fc).get(t.Context(), testParentRootForProposer, parentSlot, slot)

	require.NoError(t, err)
	require.Equal(t, want, got)
	require.Equal(t, parentSlot, parentState.Slot(), "the held parent state was advanced")
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

	got, err := newExpectedProposers(&cfg, fc).get(t.Context(), testParentRootForProposer, parentState.Slot(), slot)

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
	proposers := newExpectedProposers(cfg, fc)

	_, err := proposers.get(t.Context(), testParentRootForProposer, parentState.Slot(), slot)
	require.ErrorIs(t, err, ErrIgnore)

	fc.GetStateAtBlockRootFn = nil
	fc.StateAtBlockRootVal[testParentRootForProposer] = parentState
	_, err = proposers.get(t.Context(), testParentRootForProposer, parentState.Slot(), slot)
	require.NoError(t, err, "a failed read is not cached")
}

// Rebuilds of parent states fork choice does not hold are bounded across parents and slots. A read over the budget
// waits for a rebuild to finish instead of dropping the sidecar, and gives up after a slot. A parent fork choice holds
// is read meanwhile.
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
	fc.ViewCurrentStateAtBlockRootFn = func(root common.Hash, fn func(*state.CachingBeaconState) error) error {
		if root == held {
			return fn(parentState)
		}
		return nil
	}
	fc.ViewStateAtBlockRootFn = func(_ common.Hash, fn func(*state.CachingBeaconState) error) error {
		rebuilds.Add(1)
		started <- struct{}{}
		<-release
		return fn(parentState)
	}
	proposers := newExpectedProposers(&cfg, fc)
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
	require.NoError(t, err, "a held parent state was not read while the rebuild budget was used up")

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
