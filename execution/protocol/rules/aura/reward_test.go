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

package aura

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func TestApplyRewardsBlockAccessList(t *testing.T) {
	for _, tc := range []struct {
		name           string
		timestamp      uint64
		reward         uint64
		readBalance    bool
		readAfter      bool
		emptyExists    bool
		initialBalance uint64
		wantAccess     bool
	}{
		{name: "zero_reward", timestamp: 10},
		{name: "existing_empty_beneficiary", timestamp: 10, emptyExists: true},
		{name: "before_amsterdam", timestamp: 9, wantAccess: true},
		{name: "nonzero_reward", timestamp: 10, reward: 1, wantAccess: true},
		{name: "separate_balance_read", timestamp: 10, readBalance: true, wantAccess: true},
		{name: "later_balance_read", timestamp: 10, readAfter: true, wantAccess: true},
		{name: "later_balance_read_empty_account", timestamp: 10, emptyExists: true, readAfter: true, wantAccess: true},
		{name: "later_balance_read_funded_account", timestamp: 10, initialBalance: 7, readAfter: true, wantAccess: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			engine := &AuRa{cfg: AuthorityRoundParams{
				BlockReward: BlockRewardList{{amount: uint256.NewInt(tc.reward)}},
			}}
			beneficiary := common.HexToAddress("0x1111111111111111111111111111111111111111")
			addr := accounts.InternAddress(beneficiary)
			vm := state.NewVersionMap(nil)
			if tc.emptyExists || tc.initialBalance != 0 {
				account := accounts.NewAccount()
				account.Balance.SetUint64(tc.initialBalance)
				vm.WriteAddress(addr, state.Version{TxIndex: -1}, &account, true)
			}
			ibs := state.NewWithVersionMap(state.NewNoopReader(), vm)
			ibs.SetTxContext(1, 0)
			ibs.StartAccessRecording()
			t.Cleanup(ibs.Close)

			if tc.readBalance {
				_, err := ibs.GetBalance(addr)
				require.NoError(t, err)
			}
			amsterdam := uint64(10)
			err := engine.applyRewards(
				&chain.Config{AmsterdamTime: &amsterdam},
				&types.Header{Coinbase: beneficiary, Time: tc.timestamp},
				ibs,
				nil,
			)
			require.NoError(t, err)

			if tc.readAfter {
				_, err := ibs.GetBalance(addr)
				require.NoError(t, err)
			}

			writes, err := ibs.FinalizedWrites(&chain.Rules{IsSpuriousDragon: true, IsAura: true, IsAmsterdam: tc.timestamp >= amsterdam})
			require.NoError(t, err)
			ibs.FlushWritesToVersionMap(writes)
			_, exists := vm.View(1, state.NewNoopReader()).GetAccount(addr)
			require.Equal(t, tc.reward+tc.initialBalance != 0, exists)
			io := state.NewVersionedIO(1)
			ibs.MergeTxIOInto(io, writes)
			bal := io.AsBlockAccessList()
			if !tc.wantAccess {
				require.Empty(t, bal)
				return
			}
			require.Len(t, bal, 1)
			require.Equal(t, beneficiary, bal[0].Address)
			if tc.reward == 0 {
				require.Empty(t, bal[0].BalanceChanges)
			} else {
				require.Equal(t, []*types.BalanceChange{{Index: 1, Value: *uint256.NewInt(tc.reward)}}, bal[0].BalanceChanges)
			}
		})
	}
}
