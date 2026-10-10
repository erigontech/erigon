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

package misc_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/state/execctx/execctxapi"
	"github.com/erigontech/erigon/execution/protocol/misc"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func TestDequeueRequestsWithoutCode(t *testing.T) {
	for _, tc := range []struct {
		name    string
		address accounts.Address
		dequeue func(rules.SystemCall, *state.IntraBlockState, accounts.Address) (*types.FlatRequest, error)
	}{
		{"withdrawal", params.WithdrawalRequestAddress, misc.DequeueWithdrawalRequests7002},
		{"consolidation", params.ConsolidationRequestAddress, misc.DequeueConsolidationRequests7251},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, createAccount := range []bool{false, true} {
				name := "absent-account"
				if createAccount {
					name = "empty-code"
				}
				t.Run(name, func(t *testing.T) {
					db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
					tx, domains := temporaltest.NewTestTxSD(t, db)
					statedb := state.New(state.NewReaderV3(domains.AsStateGetter(tx, execctxapi.StateGetterOptions{})))
					defer statedb.Close()
					if createAccount {
						require.NoError(t, statedb.CreateAccount(tc.address, true))
					}
					called := false
					request, err := tc.dequeue(func(accounts.Address, []byte) ([]byte, error) {
						called = true
						return nil, nil
					}, statedb, tc.address)
					require.NoError(t, err)
					require.Nil(t, request)
					require.False(t, called)
				})
			}
		})
	}
}
