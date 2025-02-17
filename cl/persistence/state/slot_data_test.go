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
	"bytes"
	"testing"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/clparams/initial_state"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/stretchr/testify/require"
)

func TestSlotData(t *testing.T) {
	s, err := initial_state.GetGenesisState(clparams.MainnetNetwork)
	require.NoError(t, err)
	m := &SlotData{
		Version:                       clparams.ElectraVersion,
		Eth1Data:                      &cltypes.Eth1Data{},
		Eth1DepositIndex:              1,
		NextWithdrawalIndex:           2,
		NextWithdrawalValidatorIndex:  3,
		DepositRequestsStartIndex:     4,
		DepositBalanceToConsume:       5,
		ExitBalanceToConsume:          6,
		EarliestExitEpoch:             7,
		ConsolidationBalanceToConsume: 8,
		EarliestConsolidationEpoch:    9,
		PendingDeposits:               solid.NewPendingDepositList(s.BeaconConfig()),
		PendingPartialWithdrawals:     solid.NewPendingWithdrawalList(s.BeaconConfig()),
		PendingConsolidations:         solid.NewPendingConsolidationList(s.BeaconConfig()),
		Fork:                          &cltypes.Fork{Epoch: 12},
	}
	var b bytes.Buffer
	if err := m.WriteTo(&b); err != nil {
		t.Fatal(err)
	}
	m2 := &SlotData{}
	if err := m2.ReadFrom(&b, s.BeaconConfig()); err != nil {
		t.Fatal(err)
	}

	require.Equal(t, m, m2)
}
