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
	"github.com/erigontech/erigon/core/tracing"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
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
				state.AddBalance(common.Address{}, *uint256.NewInt(2), tracing.BalanceChangeUnspecified)
				state.AddBalance(common.Address{}, *uint256.NewInt(1), tracing.BalanceChangeUnspecified)
			},
			checker: func(t *testing.T, stateDB *IntraBlockState) {
				if len(stateDB.journal.entries) != 3 {
					t.Errorf("Incorrect number of jounal entries expectedBalance: %d, got:%d", 3, len(stateDB.journal.entries))
				}
				for i := range stateDB.journal.entries {
					switch balanceInc := stateDB.journal.entries[i].(type) {
					case balanceChange:
						var expectedPrev *uint256.Int
						if i == 1 {
							expectedPrev = uint256.NewInt(0)
						} else {
							expectedPrev = uint256.NewInt(2)
						}
						if !reflect.DeepEqual(&balanceInc.prev, expectedPrev) {
							t.Errorf("Incorrect BalanceInc in jounal for  %s expectedBalance: %s, got:%s", common.Address{}, expectedPrev, &balanceInc.prev)
						}
					case createObjectChange:
					default:
						t.Errorf("Invalid journal entry found:  %s", reflect.TypeOf(stateDB.journal.entries[i]))
					}
				}

				so, err := stateDB.GetOrNewStateObject(common.Address{})
				require.NoError(t, err)
				balance := so.Balance()
				if !reflect.DeepEqual(&balance, uint256.NewInt(3)) {
					t.Errorf("Incorrect Balance for  %s expectedBalance: %s, got:%s", common.Address{}, uint256.NewInt(3), &balance)
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
				state.AddBalance(common.Address{}, *uint256.NewInt(2), tracing.BalanceChangeUnspecified)
				state.SubBalance(common.Address{}, *uint256.NewInt(1), tracing.BalanceChangeUnspecified)
			},
			checker: func(t *testing.T, stateDB *IntraBlockState) {
				so, err := stateDB.GetOrNewStateObject(common.Address{})
				require.NoError(t, err)
				if !reflect.DeepEqual(so.Balance(), *uint256.NewInt(1)) {
					balance := so.Balance()
					t.Errorf("Incorrect Balance for  %s expectedBalance: %s, got:%s", common.Address{}, uint256.NewInt(1), &balance)
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

			err := rawdbv3.TxNums.Append(tx, 1, 1)
			require.NoError(t, err)

			mockCtl := gomock.NewController(t)
			defer mockCtl.Finish()
			mt := mockTracer{}
			state := New(NewReaderV3(tx))
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
		OnBalanceChange: func(addr common.Address, prev, new uint256.Int, reason tracing.BalanceChangeReason) {
			mt.balanceChangeTraces = append(mt.balanceChangeTraces, balanceChangeTrace{
				addr:   addr,
				prev:   prev,
				new:    new,
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
