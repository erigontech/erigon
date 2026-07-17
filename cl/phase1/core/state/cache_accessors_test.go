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
		s.AddValidator(v, 2000000000)
	}
	s.SetSlot(8160)

	_, err := s.ComputeNextSyncCommittee()
	require.Error(t, err)
}
