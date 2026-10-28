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

package raw

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
)

func requireStateRootMatchesEncoding(t *testing.T, state *BeaconState) {
	t.Helper()
	actual, err := state.HashSSZ()
	require.NoError(t, err)
	encoded, err := state.EncodeSSZ(nil)
	require.NoError(t, err)
	fresh := New(state.BeaconConfig())
	require.NoError(t, fresh.DecodeSSZ(encoded, int(state.Version())))
	expected, err := fresh.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, expected, actual)
}

func TestGloasListRootsAfterCopyIntoMutation(t *testing.T) {
	mutations := []struct {
		name   string
		mutate func(*BeaconState)
	}{
		{
			name: "append pending deposit",
			mutate: func(state *BeaconState) {
				state.AppendPendingDeposit(&solid.PendingDeposit{Amount: 999_001, Slot: 999_002})
			},
		},
		{
			name: "append pending partial withdrawal",
			mutate: func(state *BeaconState) {
				state.AppendPendingPartialWithdrawal(&solid.PendingPartialWithdrawal{ValidatorIndex: 999_003, Amount: 999_004})
			},
		},
		{
			name: "append pending consolidation",
			mutate: func(state *BeaconState) {
				state.AppendPendingConsolidation(&solid.PendingConsolidation{SourceIndex: 999_005, TargetIndex: 999_006})
			},
		},
		{
			name: "replace pending deposits",
			mutate: func(state *BeaconState) {
				list := solid.NewPendingDepositList(state.BeaconConfig())
				list.Append(&solid.PendingDeposit{Amount: 999_007, Slot: 999_008})
				state.SetPendingDeposits(list)
			},
		},
		{
			name: "set builder",
			mutate: func(state *BeaconState) {
				builders := state.GetBuilders()
				builders.Set(0, &cltypes.Builder{Balance: 999_009})
				state.SetBuilders(builders)
			},
		},
		{
			name: "append builder pending withdrawal",
			mutate: func(state *BeaconState) {
				withdrawals := state.GetBuilderPendingWithdrawals()
				withdrawals.Append(&cltypes.BuilderPendingWithdrawal{BuilderIndex: 999_010, Amount: 999_011})
				state.SetBuilderPendingWithdrawals(withdrawals)
			},
		},
		{
			name: "replace payload expected withdrawals with shallow copy",
			mutate: func(state *BeaconState) {
				withdrawals := state.GetPayloadExpectedWithdrawals().ShallowCopy()
				withdrawals.Set(0, &cltypes.Withdrawal{Index: 999_012, Amount: 999_013})
				state.SetPayloadExpectedWithdrawals(withdrawals)
			},
		},
		{
			name: "cut pending partial withdrawals",
			mutate: func(state *BeaconState) {
				withdrawals := state.GetPendingPartialWithdrawals()
				withdrawals.Cut(1)
				state.SetPendingPartialWithdrawals(withdrawals)
			},
		},
	}

	for _, mutation := range mutations {
		for _, target := range []string{"source", "copy"} {
			t.Run(mutation.name+" on "+target, func(t *testing.T) {
				source := newStateWithQueues(clparams.GloasVersion)
				_, err := source.HashSSZ()
				require.NoError(t, err)
				dst := New(source.BeaconConfig())
				require.NoError(t, source.CopyInto(dst))

				if target == "source" {
					mutation.mutate(source)
				} else {
					mutation.mutate(dst)
				}

				requireStateRootMatchesEncoding(t, source)
				requireStateRootMatchesEncoding(t, dst)
			})
		}
	}
}
