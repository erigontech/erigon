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

package execmodule

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/builder"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/types"
)

func TestBlockValueFrameFeeBudget(t *testing.T) {
	for _, tc := range []struct {
		name          string
		divisor, want uint64
		blobs         []common.Hash
	}{
		{"unused gas funds priority fee", 2, 0, nil},
		{"full gas retains priority remainder", 1, 1000, nil},
		{"blob fees do not pay the builder", 1, 1000, []common.Hash{{1}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			frame := &types.FrameTransaction{
				Frames:              []types.Frame{{Mode: types.FrameVerify, Flags: types.FrameApproveBoth, Limits: mdgas.MdGas{Execution: 1_000_000}}},
				Fees:                types.FrameFees{TipCap: *uint256.NewInt(3)},
				BlobVersionedHashes: tc.blobs,
			}
			gasUsed := frame.GetGasLimit() / tc.divisor
			want := tc.want
			if tc.divisor == 2 {
				want = gasUsed * 3
			}
			frame.Fees.MaxFee.SetUint64(10*frame.GetGasLimit() + 2*frame.GetBlobGas() + 1000)
			header := &types.Header{Number: *uint256.NewInt(1), BaseFee: uint256.NewInt(10)}
			if len(tc.blobs) != 0 {
				// One update fraction of excess blob gas sets the blob base fee to 2.
				header.ExcessBlobGas = common.NewUint64(chain.AllProtocolChanges.GetBlobGasPriceUpdateFraction(header.Time))
			}
			br := &types.BlockWithReceipts{
				Block:    types.NewBlockFromStorage(common.Hash{}, header, types.Transactions{frame}, nil, nil, nil),
				Receipts: types.Receipts{{GasUsed: gasUsed}},
			}
			module := newTestModule(t, func(context.Context, *builder.Parameters, *atomic.Bool) (*types.BlockWithReceipts, error) {
				return br, nil
			})
			module.config = chain.AllProtocolChanges
			build, err := module.AssembleBlock(t.Context(), &builder.Parameters{Timestamp: newTestTimestamp(), Withdrawals: []*types.Withdrawal{}})
			require.NoError(t, err)
			result, err := module.GetAssembledBlock(t.Context(), build.PayloadID)
			require.NoError(t, err)
			require.Equal(t, uint256.NewInt(want), result.BlockValue)
		})
	}
}
