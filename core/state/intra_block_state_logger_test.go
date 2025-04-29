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

package state

import (
	"reflect"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/kv/rawdbv3"
	"github.com/erigontech/erigon-lib/log/v3"
	stateLib "github.com/erigontech/erigon-lib/state"
	"github.com/erigontech/erigon/core/tracing"
)

func TestStateLogger(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name                    string
		run                     func(state *IntraBlockState)
		checker                 func(t *testing.T, state *IntraBlockState)
		wantBalanceChangeTraces []balanceChangeTrace
	}{
		{
			name: "multiple add balance",
			run: func(state *IntraBlockState) {
				state.AddBalance(common.Address{}, uint256.NewInt(2), tracing.BalanceChangeUnspecified)
				state.AddBalance(common.Address{}, uint256.NewInt(1), tracing.BalanceChangeUnspecified)
			},
			checker: func(t *testing.T, stateDB *IntraBlockState) {
				bi, ok := stateDB.balanceInc[common.Address{}]
				if !ok {
					t.Errorf("%s isn't present in balanceInc", common.Address{})
				}

				if !reflect.DeepEqual(&bi.increase, uint256.NewInt(3)) {
					t.Errorf("Incorrect BalanceInc for  %s expectedBalance: %s, got:%s", common.Address{}, uint256.NewInt(3), &bi.increase)
				}

				if bi.count != 2 {
					t.Errorf("Incorrect BalanceInc count for %s expected: %d, got:%d", common.Address{}, 2, bi.count)
				}

				if len(stateDB.journal.entries) != 2 {
					t.Errorf("Incorrect number of jounal entries expectedBalance: %d, got:%d", 2, len(stateDB.journal.entries))
				}
				for i := range stateDB.journal.entries {
					switch balanceInc := stateDB.journal.entries[i].(type) {
					case balanceIncrease:
						var expectedInc *uint256.Int
						if i == 0 {
							expectedInc = uint256.NewInt(2)
						} else {
							expectedInc = uint256.NewInt(1)
						}
						if !reflect.DeepEqual(&balanceInc.increase, expectedInc) {
							t.Errorf("Incorrect BalanceInc in jounal for  %s expectedBalance: %s, got:%s", common.Address{}, expectedInc, &balanceInc.increase)
						}
					default:
						t.Errorf("Invalid journal entry found:  %s", reflect.TypeOf(stateDB.journal.entries[i]))
					}
				}

				so, err := stateDB.GetOrNewStateObject(common.Address{})
				require.NoError(t, err)
				if !reflect.DeepEqual(so.Balance(), uint256.NewInt(3)) {
					t.Errorf("Incorrect Balance for  %s expectedBalance: %s, got:%s", common.Address{}, uint256.NewInt(3), so.Balance())
				}
			},
			wantBalanceChangeTraces: []balanceChangeTrace{
				{addr: common.Address{}, prev: *uint256.NewInt(0), new: *uint256.NewInt(2), reason: tracing.BalanceChangeUnspecified},
				{addr: common.Address{}, prev: *uint256.NewInt(2), new: *uint256.NewInt(3), reason: tracing.BalanceChangeUnspecified},
			},
		},
		{
			name: "sub balance",
			run: func(state *IntraBlockState) {
				state.AddBalance(common.Address{}, uint256.NewInt(2), tracing.BalanceChangeUnspecified)
				state.SubBalance(common.Address{}, uint256.NewInt(1), tracing.BalanceChangeUnspecified)
			},
			checker: func(t *testing.T, stateDB *IntraBlockState) {
				so, err := stateDB.GetOrNewStateObject(common.Address{})
				require.NoError(t, err)
				if !reflect.DeepEqual(so.Balance(), uint256.NewInt(1)) {
					t.Errorf("Incorrect Balance for  %s expectedBalance: %s, got:%s", common.Address{}, uint256.NewInt(1), so.Balance())
				}
			},
			wantBalanceChangeTraces: []balanceChangeTrace{
				{addr: common.Address{}, prev: *uint256.NewInt(0), new: *uint256.NewInt(2), reason: tracing.BalanceChangeUnspecified},
				{addr: common.Address{}, prev: *uint256.NewInt(2), new: *uint256.NewInt(1), reason: tracing.BalanceChangeUnspecified},
			},
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			_, tx, _ := NewTestTemporalDb(t)

			domains, err := stateLib.NewSharedDomains(tx, log.New())
			require.NoError(t, err)
			defer domains.Close()

			domains.SetTxNum(1)
			domains.SetBlockNum(1)
			err = rawdbv3.TxNums.Append(tx, 1, 1)
			require.NoError(t, err)

			mockCtl := gomock.NewController(t)
			defer mockCtl.Finish()
			mt := mockTracer{}
			state := New(NewReaderV3(domains))
			state.SetHooks(mt.Hooks())

			tt.run(state)
			tt.checker(t, state)
			require.Equal(t, tt.wantBalanceChangeTraces, mt.balanceChangeTraces)
		})
	}
}

type mockTracer struct {
	balanceChangeTraces []balanceChangeTrace
}

func (mt *mockTracer) Hooks() *tracing.Hooks {
	return &tracing.Hooks{
		OnBalanceChange: func(addr common.Address, prev, new *uint256.Int, reason tracing.BalanceChangeReason) {
			mt.balanceChangeTraces = append(mt.balanceChangeTraces, balanceChangeTrace{
				addr:   addr,
				prev:   *prev,
				new:    *new,
				reason: reason,
			})
		},
	}
}

type balanceChangeTrace struct {
	addr   common.Address
	prev   uint256.Int
	new    uint256.Int
	reason tracing.BalanceChangeReason
}
