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

package protocol

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tracing"
)

func TestGasFromBlock(t *testing.T) {
	const msgGas = 100_000
	for _, tc := range []struct {
		name      string
		fromBlock bool
		blockLeft uint64
		used      uint64
		err       error
	}{
		{name: "block has more", fromBlock: true, blockLeft: 200_000, used: msgGas},
		{name: "block has less", fromBlock: true, blockLeft: 50_000, used: 50_000},
		{name: "block below intrinsic gas", fromBlock: true, blockLeft: 20_000, err: ErrIntrinsicGas},
		{name: "reserved limit", blockLeft: 50_000, err: ErrGasLimitReached},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ibs := state.New(state.NewNoopReader())
			defer ibs.Close()
			// JUMPDEST PUSH1 0 JUMP: burns all the gas it gets.
			require.NoError(t, ibs.SetCode(recipient, []byte{0x5b, 0x60, 0x00, 0x56}, tracing.CodeChangeUnspecified))
			msg := mintingMsg(new(uint256.Int), 0)
			msg.ChangeGas(0, msgGas)
			msg.SetGasFromBlock(tc.fromBlock)
			gp := NewGasPool(tc.blockLeft, 0)

			res, err := ApplyMessage(newTestEVM(ibs, chain.TestChainOsakaConfig, 30_000_000), msg, gp, true, false, nil)
			if tc.err != nil {
				require.ErrorIs(t, err, tc.err)
				require.Equal(t, tc.blockLeft, gp.Gas())
				return
			}
			require.NoError(t, err)
			require.True(t, res.Failed())
			require.Equal(t, tc.used, res.ReceiptGasUsed)
			require.Equal(t, tc.blockLeft-tc.used, gp.Gas())
		})
	}
}
