// Copyright 2024 The Erigon Authors
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

package state_test

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state"
)

// TestComputeNextSyncCommittee_ZeroActiveValidators verifies that computing the
// next sync committee on a state with no active validators returns an error
// instead of panicking with an integer divide-by-zero (i % activeValidatorCount).
func TestComputeNextSyncCommittee_ZeroActiveValidators(t *testing.T) {
	s := state.New(&clparams.MainnetBeaconConfig)
	for i := range 100 {
		var pk [48]byte
		binary.BigEndian.PutUint64(pk[:], uint64(i))
		v := solid.NewValidator()
		v.SetActivationEpoch(0)
		v.SetExitEpoch(0) // already exited → not active at any epoch
		v.SetPublicKey(pk)
		v.SetEffectiveBalance(2000000000)
		require.NoError(t, s.AddValidator(v, 2000000000))
	}
	require.NoError(t, s.SetSlot(8160))

	_, err := s.ComputeNextSyncCommittee()
	require.Error(t, err)
}

func TestProposerIndexFromLookahead(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	stateEpoch := uint64(3)
	s := state.New(&cfg)
	require.NoError(t, s.SetSlot(stateEpoch*cfg.SlotsPerEpoch+5))

	lookahead := solid.NewUint64VectorSSZ(int((cfg.MinSeedLookahead + 1) * cfg.SlotsPerEpoch))
	for i := range lookahead.Length() {
		lookahead.Set(i, uint64(1000+i))
	}
	s.SetProposerLookahead(lookahead)

	s.SetVersion(clparams.ElectraVersion)
	_, ok := s.ProposerIndexFromLookahead(stateEpoch * cfg.SlotsPerEpoch)
	require.False(t, ok)

	s.SetVersion(clparams.FuluVersion)
	tests := []struct {
		name string
		slot uint64
		want uint64
		ok   bool
	}{
		{name: "first slot of state epoch", slot: stateEpoch * cfg.SlotsPerEpoch, want: 1000, ok: true},
		{name: "slot in state epoch", slot: stateEpoch*cfg.SlotsPerEpoch + 7, want: 1007, ok: true},
		{name: "first slot of last lookahead epoch", slot: (stateEpoch + cfg.MinSeedLookahead) * cfg.SlotsPerEpoch, want: 1000 + cfg.MinSeedLookahead*cfg.SlotsPerEpoch, ok: true},
		{name: "last slot of last lookahead epoch", slot: (stateEpoch+cfg.MinSeedLookahead+1)*cfg.SlotsPerEpoch - 1, want: 1000 + (cfg.MinSeedLookahead+1)*cfg.SlotsPerEpoch - 1, ok: true},
		{name: "after lookahead", slot: (stateEpoch + cfg.MinSeedLookahead + 1) * cfg.SlotsPerEpoch},
		{name: "before state epoch", slot: stateEpoch*cfg.SlotsPerEpoch - 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := s.ProposerIndexFromLookahead(tt.slot)
			require.Equal(t, tt.ok, ok)
			require.Equal(t, tt.want, got)
		})
	}
}
