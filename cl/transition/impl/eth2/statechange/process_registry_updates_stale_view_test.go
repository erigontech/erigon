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

package statechange

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state"
)

// swappingRegistry installs a fresh copy of the registry before each setter, so views taken
// earlier stop reflecting later writes.
type swappingRegistry struct{ *state.CachingBeaconState }

func (w swappingRegistry) swap() {
	fresh := solid.NewValidatorSet(w.ValidatorSet().Cap())
	w.ValidatorSet().CopyTo(fresh)
	w.SetValidatorSet(fresh)
}

func (w swappingRegistry) SetActivationEligibilityEpochForValidatorAtIndex(i int, epoch uint64) error {
	w.swap()
	return w.CachingBeaconState.SetActivationEligibilityEpochForValidatorAtIndex(i, epoch)
}

func (w swappingRegistry) InitiateValidatorExit(i uint64) error {
	w.swap()
	return w.CachingBeaconState.InitiateValidatorExit(i)
}

func (w swappingRegistry) SetActivationEpochForValidatorAtIndex(i int, epoch uint64) error {
	w.swap()
	return w.CachingBeaconState.SetActivationEpochForValidatorAtIndex(i, epoch)
}

func registryState(t *testing.T) *state.CachingBeaconState {
	t.Helper()
	cfg := clparams.MainnetBeaconConfig
	s := state.New(&cfg)
	s.SetVersion(clparams.ElectraVersion)
	far := cfg.FarFutureEpoch
	// 0: eligible for the activation queue; 1: active, at the ejection balance; 2: plain active.
	require.NoError(t, s.AddValidator(solid.NewValidatorFromParameters([48]byte{1}, [32]byte{}, cfg.MinActivationBalance, false, far, far, far, far), cfg.MinActivationBalance))
	require.NoError(t, s.AddValidator(solid.NewValidatorFromParameters([48]byte{2}, [32]byte{}, cfg.EjectionBalance, false, 0, 0, far, far), cfg.EjectionBalance))
	require.NoError(t, s.AddValidator(solid.NewValidatorFromParameters([48]byte{3}, [32]byte{}, cfg.MinActivationBalance, false, 0, 0, far, far), cfg.MinActivationBalance))
	require.NoError(t, s.SetSlot(cfg.SlotsPerEpoch*20))
	require.NoError(t, s.InitBeaconState())
	return s
}

func TestSwappingRegistryMakesEarlierViewsStale(t *testing.T) {
	s := registryState(t)
	view := s.ValidatorSet().Get(0)
	require.NoError(t, swappingRegistry{s}.SetActivationEligibilityEpochForValidatorAtIndex(0, 21))
	require.Equal(t, s.BeaconConfig().FarFutureEpoch, view.ActivationEligibilityEpoch(), "seam must leave the old view stale")
	require.Equal(t, uint64(21), s.ValidatorSet().Get(0).ActivationEligibilityEpoch())
}

func TestProcessRegistryUpdatesWithStaleViews(t *testing.T) {
	plain, swapped := registryState(t), registryState(t)
	require.NoError(t, ProcessRegistryUpdates(plain))
	require.NoError(t, ProcessRegistryUpdates(swappingRegistry{swapped}))
	want, err := plain.HashSSZ()
	require.NoError(t, err)
	got, err := swapped.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, want, got)
	v, err := swapped.ValidatorForValidatorIndex(0)
	require.NoError(t, err)
	require.NotEqual(t, plain.BeaconConfig().FarFutureEpoch, v.ActivationEligibilityEpoch(), "eligibility must be set")
	exit, err := swapped.ValidatorExitEpoch(1)
	require.NoError(t, err)
	require.NotEqual(t, plain.BeaconConfig().FarFutureEpoch, exit, "ejection must start")
}
