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

	"github.com/erigontech/erigon-lib/common"
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
			require.Equal(t, uint64(3), exitEpoch)
		} else {
			require.Equal(t, uint64(2), exitEpoch)
		}
		return nil
	}, func(validatorIndex, withdrawableEpoch uint64) error {
		if validatorIndex == 1 {
			require.Equal(t, uint64(4), withdrawableEpoch)
		} else {
			require.Equal(t, uint64(3), withdrawableEpoch)
		}
		return nil
	}, func(validatorIndex uint64, withdrawalCredentials common.Hash) error {
		if validatorIndex == 1 {
			require.Equal(t, withdrawalCredentials, common.Hash([32]byte{2}))
		} else {
			require.Equal(t, withdrawalCredentials, common.Hash([32]byte{1}))
		}
		return nil
	}, func(validatorIndex, activationEpoch uint64) error {
		if validatorIndex == 1 {
			require.Equal(t, uint64(5), activationEpoch)
		} else {
			require.Equal(t, uint64(4), activationEpoch)
		}
		return nil
	}, func(validatorIndex, activationEligibilityEpoch uint64) error {
		if validatorIndex == 1 {
			require.Equal(t, uint64(6), activationEligibilityEpoch)
		} else {
			require.Equal(t, uint64(5), activationEligibilityEpoch)
		}
		return nil
	}, func(validatorIndex uint64, slashed bool) error {
		require.True(t, slashed)
		return nil
	}, events)

}
