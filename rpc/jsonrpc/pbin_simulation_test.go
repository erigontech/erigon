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

package jsonrpc

import (
	"context"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
	dbstate "github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/ethapi"
)

func TestPBinDualSimulation(t *testing.T) {
	_, m := pbinDualWitnessFixture(t)
	api := newEthApiForTest(newBaseApiForTest(m), m.DB, nil, nil)
	from := common.HexToAddress("0x71562b71999873db5b286df957af199ec94617f7")
	to := common.HexToAddress("0x1000000000000000000000000000000000000001")
	call := func(n uint64) SimulatedBlock {
		gas := hexutil.Uint64(2_000_000)
		time := hexutil.Uint64(n * 10)
		value := common.BigToHash(uint256.NewInt(n).ToBig())
		data := hexutil.Bytes(value[:])
		return SimulatedBlock{BlockOverrides: &ethapi.BlockOverrides{Time: &time}, Calls: []ethapi.CallArgs{{From: &from, To: &to, Gas: &gas, Value: (*hexutil.U256)(uint256.NewInt(1)), Input: &data}}}
	}
	for _, tc := range []struct {
		name   string
		base   uint64
		blocks []SimulatedBlock
	}{
		{"post_activation", 3, []SimulatedBlock{call(4)}},
		{"cross_activation", 1, []SimulatedBlock{call(2), call(3), call(4)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result, err := api.SimulateV1(t.Context(), SimulationRequest{BlockStateCalls: tc.blocks}, rpc.BlockNumberOrHashWithNumber(rpc.BlockNumber(tc.base)))
			require.NoError(t, err)
			require.Len(t, result, len(tc.blocks))
			require.NoError(t, m.DB.View(t.Context(), func(tx kv.Tx) error {
				for i, block := range result {
					calls := block["calls"].([]CallResult)
					require.Equal(t, hexutil.Uint64(1), calls[0].Status)
					require.Equal(t, rawdb.ReadHeaderByNumber(tx, tc.base+uint64(i)+1).Root, block["stateRoot"])
				}
				return nil
			}))
		})
	}
	t.Run("without_commitment_history", func(t *testing.T) {
		request := func() SimulationRequest {
			first := call(2)
			recipient := common.HexToAddress("0x2000000000000000000000000000000000000002")
			first.Calls[0].To = &recipient
			first.Calls[0].Input = nil
			return SimulationRequest{BlockStateCalls: []SimulatedBlock{first, call(3)}}
		}
		selector := rpc.BlockNumberOrHashWithNumber(rpc.BlockNumber(1))
		expected, err := api.SimulateV1(t.Context(), request(), selector)
		require.NoError(t, err)
		require.NoError(t, m.DB.Update(t.Context(), func(tx kv.RwTx) error { return rawdb.WriteDBCommitmentHistoryEnabled(tx, false) }))
		t.Cleanup(func() {
			require.NoError(t, m.DB.Update(context.Background(), func(tx kv.RwTx) error { return rawdb.WriteDBCommitmentHistoryEnabled(tx, true) }))
		})
		replayAPI := newEthApiForTest(newBaseApiForTest(m), m.DB, nil, nil)
		result, err := replayAPI.SimulateV1(t.Context(), SimulationRequest{BlockStateCalls: []SimulatedBlock{call(4)}}, rpc.BlockNumberOrHashWithNumber(rpc.BlockNumber(3)))
		require.NoError(t, err)
		require.NoError(t, m.DB.View(t.Context(), func(tx kv.Tx) error {
			require.Equal(t, rawdb.ReadHeaderByNumber(tx, 4).Root, result[0]["stateRoot"])
			return nil
		}))
		actual, err := replayAPI.SimulateV1(t.Context(), request(), selector)
		require.NoError(t, err)
		for i := range expected {
			require.Equal(t, expected[i]["stateRoot"], actual[i]["stateRoot"])
		}

	})

	t.Run("frozen_hex", func(t *testing.T) {
		cases := []struct {
			name   string
			base   rpc.BlockNumber
			blocks []SimulatedBlock
		}{
			{"latest", rpc.LatestBlockNumber, []SimulatedBlock{call(5)}},
			{"pre_activation", 1, []SimulatedBlock{call(2)}},
			{"cross_activation", 1, []SimulatedBlock{call(2), call(3), call(4)}},
		}
		before := make([]SimulationResult, len(cases))
		for i, tc := range cases {
			var err error
			before[i], err = api.SimulateV1(t.Context(), SimulationRequest{BlockStateCalls: tc.blocks}, rpc.BlockNumberOrHashWithNumber(tc.base))
			require.NoError(t, err)
		}
		_, state := readCommittedCommitmentState(t, t.Context(), m.DB)
		txNum, _ := commitmentdb.DecodeTxBlockNums(state)
		agg := m.DB.(dbstate.HasAgg).Agg().(*dbstate.Aggregator)
		require.NoError(t, agg.FreezeDomain(kv.CommitmentDomain, txNum))
		for i, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				after, err := api.SimulateV1(t.Context(), SimulationRequest{BlockStateCalls: tc.blocks}, rpc.BlockNumberOrHashWithNumber(tc.base))
				require.NoError(t, err)
				require.Equal(t, before[i], after)
			})
		}
		_, current := readCommittedCommitmentState(t, t.Context(), m.DB)
		require.Equal(t, state, current)
		frozenAt, frozen := agg.IsDomainFrozen(kv.CommitmentDomain)
		require.True(t, frozen)
		require.Equal(t, txNum, frozenAt)
	})
}
