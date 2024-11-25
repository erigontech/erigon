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

	libcommon "github.com/erigontech/erigon/erigon-lib/common"
	"github.com/erigontech/erigon/erigon-lib/kv/rawdbv3"
	"github.com/erigontech/erigon/erigon-lib/log/v3"
	stateLib "github.com/erigontech/erigon/erigon-lib/state"

	"github.com/erigontech/erigon/core/tracing"
	"github.com/erigontech/erigon/core/tracing/mocks"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
	gomock "go.uber.org/mock/gomock"
)

func TestStateLogger(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		prepare func(mockTracer *mocks.Mocktracer)
		run     func(state *IntraBlockState)
		checker func(t *testing.T, state *IntraBlockState)
	}{
		{
			name: "multiple add balance",
			prepare: func(mockTracer *mocks.Mocktracer) {
				mockTracer.EXPECT().BalanceChangeHook(libcommon.Address{}, uint256.NewInt(0), uint256.NewInt(2), tracing.BalanceChangeUnspecified)
				mockTracer.EXPECT().BalanceChangeHook(libcommon.Address{}, uint256.NewInt(2), uint256.NewInt(3), tracing.BalanceChangeUnspecified)
			},
			run: func(state *IntraBlockState) {
				state.AddBalance(libcommon.Address{}, uint256.NewInt(2), tracing.BalanceChangeUnspecified)
				state.AddBalance(libcommon.Address{}, uint256.NewInt(1), tracing.BalanceChangeUnspecified)
			},
			checker: func(t *testing.T, stateDB *IntraBlockState) {
				bi, ok := stateDB.balanceInc[libcommon.Address{}]
				if !ok {
					t.Errorf("%s isn't present in balanceInc", libcommon.Address{})
				}

				if !reflect.DeepEqual(&bi.increase, uint256.NewInt(3)) {
					t.Errorf("Incorrect BalanceInc for  %s expectedBalance: %s, got:%s", libcommon.Address{}, uint256.NewInt(3), &bi.increase)
				}

				if bi.count != 2 {
					t.Errorf("Incorrect BalanceInc count for %s expected: %d, got:%d", libcommon.Address{}, 2, bi.count)
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
							t.Errorf("Incorrect BalanceInc in jounal for  %s expectedBalance: %s, got:%s", libcommon.Address{}, expectedInc, &balanceInc.increase)
						}
					default:
						t.Errorf("Invalid journal entry found:  %s", reflect.TypeOf(stateDB.journal.entries[i]))
					}
				}

				so := stateDB.GetOrNewStateObject(libcommon.Address{})
				if !reflect.DeepEqual(so.Balance(), uint256.NewInt(3)) {
					t.Errorf("Incorrect Balance for  %s expectedBalance: %s, got:%s", libcommon.Address{}, uint256.NewInt(3), so.Balance())
				}
			},
		},
		{
			name: "sub balance",
			prepare: func(mockTracer *mocks.Mocktracer) {
				mockTracer.EXPECT().BalanceChangeHook(libcommon.Address{}, uint256.NewInt(0), uint256.NewInt(2), tracing.BalanceChangeUnspecified)
				mockTracer.EXPECT().BalanceChangeHook(libcommon.Address{}, uint256.NewInt(2), uint256.NewInt(1), tracing.BalanceChangeUnspecified)
			},
			run: func(state *IntraBlockState) {
				state.AddBalance(libcommon.Address{}, uint256.NewInt(2), tracing.BalanceChangeUnspecified)
				state.SubBalance(libcommon.Address{}, uint256.NewInt(1), tracing.BalanceChangeUnspecified)
			},
			checker: func(t *testing.T, stateDB *IntraBlockState) {
				so := stateDB.GetOrNewStateObject(libcommon.Address{})
				if !reflect.DeepEqual(so.Balance(), uint256.NewInt(1)) {
					t.Errorf("Incorrect Balance for  %s expectedBalance: %s, got:%s", libcommon.Address{}, uint256.NewInt(1), so.Balance())
				}
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
			mockTracer := mocks.NewMocktracer(mockCtl)

			state := New(NewReaderV3(domains))
			state.SetHooks(mockTracer.Hooks())

			tt.prepare(mockTracer)
			tt.run(state)
			tt.checker(t, state)
		})
	}
}
