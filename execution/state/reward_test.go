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

package state

import (
	"fmt"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func TestZeroRewardThenZeroWithdrawal(t *testing.T) {
	ibs := NewWithVersionMap(NewNoopReader(), NewVersionMap(nil))
	t.Cleanup(ibs.Close)
	ibs.SetTxContext(1, 0)
	ibs.StartAccessRecording()
	addr := accounts.InternAddress([20]byte{0x11})

	require.NoError(t, ibs.AddZeroReward(addr, tracing.BalanceIncreaseRewardMineBlock))
	require.NoError(t, ibs.AddBalance(addr, uint256.Int{}, tracing.BalanceIncreaseWithdrawal))

	writes, err := ibs.FinalizedWrites(&chain.Rules{IsSpuriousDragon: true, IsAmsterdam: true})
	require.NoError(t, err)
	io := NewVersionedIO(1)
	ibs.MergeTxIOInto(io, writes)
	bal := io.AsBlockAccessList()
	require.Len(t, bal, 1)
	require.Equal(t, addr.Value(), bal[0].Address)
	require.Empty(t, bal[0].BalanceChanges)
}

func TestZeroRewardMergedAccesses(t *testing.T) {
	for _, tc := range []struct {
		name       string
		rewards    []bool
		wantAccess bool
	}{
		{name: "rewards_only", rewards: []bool{true, true}},
		{name: "read_before_reward", rewards: []bool{false, true}, wantAccess: true},
		{name: "read_after_reward", rewards: []bool{true, false}, wantAccess: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			addr := accounts.InternAddress([20]byte{0x11})
			io := NewVersionedIO(1)
			for _, reward := range tc.rewards {
				ibs := NewWithVersionMap(NewNoopReader(), NewVersionMap(nil))
				t.Cleanup(ibs.Close)
				ibs.SetTxContext(1, 0)
				ibs.StartAccessRecording()
				if reward {
					require.NoError(t, ibs.AddZeroReward(addr, tracing.BalanceIncreaseRewardMineBlock))
				} else {
					_, err := ibs.GetBalance(addr)
					require.NoError(t, err)
				}
				writes, err := ibs.FinalizedWrites(&chain.Rules{IsSpuriousDragon: true, IsAmsterdam: true})
				require.NoError(t, err)
				ibs.MergeTxIOInto(io, writes)
			}
			bal := io.AsBlockAccessList()
			if tc.wantAccess {
				require.Len(t, bal, 1)
				require.Equal(t, addr.Value(), bal[0].Address)
				require.Empty(t, bal[0].BalanceChanges)
			} else {
				require.Empty(t, bal)
			}
		})
	}
}

func TestZeroRewardInternalRead(t *testing.T) {
	for _, readFirst := range []bool{true, false} {
		t.Run(fmt.Sprintf("read_first_%t", readFirst), func(t *testing.T) {
			ibs := NewWithVersionMap(NewNoopReader(), NewVersionMap(nil))
			t.Cleanup(ibs.Close)
			ibs.SetTxContext(1, 0)
			ibs.StartAccessRecording()
			addr := accounts.InternAddress([20]byte{0x11})
			read := func() {
				_, err := ibs.GetBalance(addr)
				require.NoError(t, err)
				ibs.MarkReadsInternal(addr)
			}
			if readFirst {
				read()
			}
			require.NoError(t, ibs.AddZeroReward(addr, tracing.BalanceIncreaseRewardMineBlock))
			if !readFirst {
				read()
			}
			writes, err := ibs.FinalizedWrites(&chain.Rules{IsSpuriousDragon: true, IsAmsterdam: true})
			require.NoError(t, err)
			io := NewVersionedIO(1)
			ibs.MergeTxIOInto(io, writes)
			require.Empty(t, io.AsBlockAccessList())
		})
	}
}
