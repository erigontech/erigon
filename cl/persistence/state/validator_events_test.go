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

package state_accessors

import (
	"testing"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/stretchr/testify/require"
)

func TestStateEvents(t *testing.T) {
	events := NewStateEvents()
	events.AddValidator(0, solid.NewValidator())
	events.ChangeExitEpoch(1, 3)
	events.ChangeWithdrawableEpoch(1, 4)
	events.ChangeWithdrawalCredentials(1, [32]byte{2})
	events.ChangeActivationEpoch(1, 5)
	events.ChangeActivationEligibilityEpoch(1, 6)
	events.ChangeSlashed(1, true)
	// Make one for index 2
	events.AddValidator(2, solid.NewValidator())
	events.ChangeExitEpoch(2, 2)
	events.ChangeWithdrawableEpoch(2, 3)
	events.ChangeWithdrawalCredentials(2, [32]byte{1})
	events.ChangeActivationEpoch(2, 4)
	events.ChangeActivationEligibilityEpoch(2, 5)
	events.ChangeSlashed(2, true)
	// Ok now lets replay it.
	ReplayEvents(func(validatorIndex uint64, validator solid.Validator) error {
		require.Equal(t, validator, solid.NewValidator())
		return nil
	}, func(validatorIndex, exitEpoch uint64) error {
		if validatorIndex == 1 {
			require.Equal(t, exitEpoch, uint64(3))
		} else {
			require.Equal(t, exitEpoch, uint64(2))
		}
		return nil
	}, func(validatorIndex, withdrawableEpoch uint64) error {
		if validatorIndex == 1 {
			require.Equal(t, withdrawableEpoch, uint64(4))
		} else {
			require.Equal(t, withdrawableEpoch, uint64(3))
		}
		return nil
	}, func(validatorIndex uint64, withdrawalCredentials libcommon.Hash) error {
		if validatorIndex == 1 {
			require.Equal(t, withdrawalCredentials, libcommon.Hash([32]byte{2}))
		} else {
			require.Equal(t, withdrawalCredentials, libcommon.Hash([32]byte{1}))
		}
		return nil
	}, func(validatorIndex, activationEpoch uint64) error {
		if validatorIndex == 1 {
			require.Equal(t, activationEpoch, uint64(5))
		} else {
			require.Equal(t, activationEpoch, uint64(4))
		}
		return nil
	}, func(validatorIndex, activationEligibilityEpoch uint64) error {
		if validatorIndex == 1 {
			require.Equal(t, activationEligibilityEpoch, uint64(6))
		} else {
			require.Equal(t, activationEligibilityEpoch, uint64(5))
		}
		return nil
	}, func(validatorIndex uint64, slashed bool) error {
		require.Equal(t, slashed, true)
		return nil
	}, events)

}
