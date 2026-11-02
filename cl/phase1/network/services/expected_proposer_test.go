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

	"github.com/erigontech/erigon/cl/antiquary/tests"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/common"
	"github.com/stretchr/testify/require"
)

// countingStateViewer serves current, the state fork choice holds without rebuilding it, and counts the rebuilds of
// state that any other parent needs. A rebuild waits for release when it is set.
type countingStateViewer struct {
	current *state.CachingBeaconState
	state   *state.CachingBeaconState
	err     error
	views   atomic.Int32
	started chan struct{}
	release chan struct{}
}

func (v *countingStateViewer) ViewCurrentStateAtBlockRoot(_ common.Hash, fn func(*state.CachingBeaconState) error) error {
	if v.current == nil {
		return nil
	}
	return fn(v.current)
}

func (v *countingStateViewer) ViewStateAtBlockRoot(_ common.Hash, fn func(*state.CachingBeaconState) error) error {
	v.views.Add(1)
	if v.started != nil {
		v.started <- struct{}{}
	}
	if v.release != nil {
		<-v.release
	}
	if v.err != nil {
		return v.err
	}
	return fn(v.state)
}

func TestExpectedProposerIsReadFromTheParentStateOnce(t *testing.T) {
	_, _, parentState := tests.GetCapellaRandom()
	cfg := parentState.BeaconConfig()
	slot := (state.Epoch(parentState)+1)*cfg.SlotsPerEpoch - 1
	want, err := parentState.GetBeaconProposerIndexForSlot(slot)
	require.NoError(t, err)
	viewer := &countingStateViewer{state: parentState}
	proposers := newExpectedProposers(cfg, viewer)

	for range 2 {
		got, err := proposers.get(common.Hash{1}, parentState.Slot(), slot)
		require.NoError(t, err)
		require.Equal(t, want, got)
	}
	require.Equal(t, int32(1), viewer.views.Load())
}

// Before Fulu the proposers of a later epoch are not in the parent state, so the sidecar is ignored, not rejected.
func TestExpectedProposerOfALaterEpochBeforeFuluIsIgnored(t *testing.T) {
	_, _, parentState := tests.GetCapellaRandom()
	cfg := parentState.BeaconConfig()
	viewer := &countingStateViewer{state: parentState}

	_, err := newExpectedProposers(cfg, viewer).get(common.Hash{1}, parentState.Slot(), (state.Epoch(parentState)+1)*cfg.SlotsPerEpoch)

	require.ErrorIs(t, err, ErrIgnore)
	require.Zero(t, viewer.views.Load())
}

func TestExpectedProposerWithoutAParentStateIsIgnored(t *testing.T) {
	_, _, parentState := tests.GetCapellaRandom()
	cfg := parentState.BeaconConfig()
	viewer := &countingStateViewer{err: errors.New("state not found")}
	proposers := newExpectedProposers(cfg, viewer)
	slot := (state.Epoch(parentState)+1)*cfg.SlotsPerEpoch - 1

	_, err := proposers.get(common.Hash{1}, parentState.Slot(), slot)
	require.ErrorIs(t, err, ErrIgnore)

	viewer.err = nil
	viewer.state = parentState
	_, err = proposers.get(common.Hash{1}, parentState.Slot(), slot)
	require.NoError(t, err, "a failed read is not cached")
}

// From Fulu on, a parent in the previous epoch holds the proposers of the next one in its lookahead, so they are read
// without advancing the parent.
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
	viewer := &countingStateViewer{current: parentState}

	got, err := newExpectedProposers(&cfg, viewer).get(common.Hash{1}, parentSlot, parentSlot+3)

	require.NoError(t, err)
	require.Equal(t, uint64(1000+cfg.SlotsPerEpoch+2), got)
	require.Equal(t, parentSlot, parentState.Slot())
	require.Zero(t, viewer.views.Load())
}

// Rebuilding the state of a parent fork choice does not hold is bounded across parents and slots: past the budget the
// sidecar is ignored, while a parent fork choice holds is still read.
func TestExpectedProposerStateRebuildsAreBounded(t *testing.T) {
	_, _, parentState := tests.GetCapellaRandom()
	cfg := parentState.BeaconConfig()
	firstSlot := state.Epoch(parentState) * cfg.SlotsPerEpoch
	viewer := &countingStateViewer{state: parentState, started: make(chan struct{}), release: make(chan struct{})}
	proposers := newExpectedProposers(cfg, viewer)

	done := make(chan error, maxConcurrentSidecarProposerStateRebuilds)
	for i := range maxConcurrentSidecarProposerStateRebuilds {
		go func() {
			_, err := proposers.get(common.Hash{byte(i + 1)}, parentState.Slot(), firstSlot)
			done <- err
		}()
		<-viewer.started
	}

	releaseRebuilds := sync.OnceFunc(func() { close(viewer.release) })
	t.Cleanup(releaseRebuilds)
	overBudget := make(chan error, 1)
	go func() {
		_, err := proposers.get(common.Hash{0xff}, parentState.Slot(), firstSlot)
		overBudget <- err
	}()
	select {
	case err := <-overBudget:
		require.ErrorIs(t, err, ErrIgnore)
	case <-viewer.started:
		t.Fatal("a parent state was rebuilt past the budget")
	}
	require.Equal(t, int32(maxConcurrentSidecarProposerStateRebuilds), viewer.views.Load())

	viewer.current = parentState
	_, err := proposers.get(common.Hash{0xfe}, parentState.Slot(), firstSlot)
	require.NoError(t, err)

	releaseRebuilds()
	for range maxConcurrentSidecarProposerStateRebuilds {
		require.NoError(t, <-done)
	}
}
