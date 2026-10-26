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
		name        string
		timestamp   uint64
		reward      uint64
		readBalance bool
		wantAccess  bool
	}{
		{name: "zero_reward", timestamp: 10},
		{name: "before_amsterdam", timestamp: 9, wantAccess: true},
		{name: "nonzero_reward", timestamp: 10, reward: 1, wantAccess: true},
		{name: "separate_balance_read", timestamp: 10, readBalance: true, wantAccess: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			engine := &AuRa{cfg: AuthorityRoundParams{
				BlockReward: BlockRewardList{{amount: uint256.NewInt(tc.reward)}},
			}}
			ibs := state.NewWithVersionMap(state.NewNoopReader(), state.NewVersionMap(nil))
			ibs.SetTxContext(1, 0)
			ibs.StartAccessRecording()
			t.Cleanup(ibs.Close)

			beneficiary := common.HexToAddress("0x1111111111111111111111111111111111111111")
			if tc.readBalance {
				_, err := ibs.GetBalance(accounts.InternAddress(beneficiary))
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

			writes, err := ibs.FinalizedWrites(&chain.Rules{IsSpuriousDragon: true, IsAmsterdam: tc.timestamp >= amsterdam})
			require.NoError(t, err)
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
