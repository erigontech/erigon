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
	"testing"

	"github.com/erigontech/erigon/cl/antiquary/tests"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/common"
	"github.com/stretchr/testify/require"
)

type countingStateViewer struct {
	state *state.CachingBeaconState
	err   error
	views int
}

func (v *countingStateViewer) ViewStateAtBlockRoot(_ common.Hash, fn func(*state.CachingBeaconState) error) error {
	v.views++
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
	require.Equal(t, 1, viewer.views)
}

// Before Fulu the proposers of a later epoch are not in the parent state, so the sidecar is ignored, not rejected.
func TestExpectedProposerOfALaterEpochBeforeFuluIsIgnored(t *testing.T) {
	_, _, parentState := tests.GetCapellaRandom()
	cfg := parentState.BeaconConfig()
	viewer := &countingStateViewer{state: parentState}

	_, err := newExpectedProposers(cfg, viewer).get(common.Hash{1}, parentState.Slot(), (state.Epoch(parentState)+1)*cfg.SlotsPerEpoch)

	require.ErrorIs(t, err, ErrIgnore)
	require.Zero(t, viewer.views)
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
