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

package gasprice

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/types"
)

func TestFeeHistoryFrameFeeBudget(t *testing.T) {
	frame := &types.FrameTransaction{
		Frames: []types.Frame{{Mode: types.FrameVerify, Flags: types.FrameApproveBoth, Limits: mdgas.MdGas{Execution: 1_000_000}}},
		Fees:   types.FrameFees{TipCap: *uint256.NewInt(3)},
	}
	frame.Fees.MaxFee.SetUint64(10*frame.GetGasLimit() + 1000)
	header := &types.Header{Number: *uint256.NewInt(1), BaseFee: uint256.NewInt(10), GasLimit: 30000000, GasUsed: 506491}
	bf := &blockFees{
		blockNumber: 1, header: header,
		block:    types.NewBlockFromStorage(common.Hash{}, header, types.Transactions{frame}, nil, nil, nil),
		receipts: types.Receipts{{GasUsed: header.GasUsed}},
	}
	oracle := &Oracle{log: log.New()}
	oracle.processBlock(bf, chain.AllProtocolChanges)
	require.NoError(t, bf.err)
	require.Equal(t, []txGasAndReward{{gasUsed: header.GasUsed, reward: *uint256.NewInt(3)}}, bf.results.tips)
}
