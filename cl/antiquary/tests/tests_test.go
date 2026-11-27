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

package tests

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/common"
)

func TestStateFixturesAreIndependent(t *testing.T) {
	for name, load := range map[string]func() ([]*cltypes.SignedBeaconBlock, *state.CachingBeaconState, *state.CachingBeaconState){
		"phase0":    GetPhase0Random,
		"bellatrix": GetBellatrixRandom,
		"capella":   GetCapellaRandom,
		"electra":   GetElectraRandom,
	} {
		t.Run(name, func(t *testing.T) {
			_, first, _ := load()
			want, err := first.HashSSZ()
			require.NoError(t, err)
			_, second, _ := load()
			require.NoError(t, second.SetRandaoMixAt(0, common.Hash{0xff}))
			_, third, _ := load()
			for _, unchanged := range []*state.CachingBeaconState{first, third} {
				got, err := unchanged.HashSSZ()
				require.NoError(t, err)
				require.Equal(t, want, got)
			}
		})
	}
}
