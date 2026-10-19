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

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
)

const mintingTxType = 0x7d

func init() {
	types.RegisterTxType(mintingTxType, types.TxTypeSpec{New: func() types.Transaction { return nil }, MintsValue: true})
}

var (
	minter    = accounts.InternAddress(common.HexToAddress("0x1111111111111111111111111111111111111111"))
	recipient = accounts.InternAddress(common.HexToAddress("0x2222222222222222222222222222222222222222"))
)

func mintingMsg(value *uint256.Int, txType byte) *types.Message {
	msg := types.NewMessage(minter, recipient, 0, value, 100_000,
		new(uint256.Int), new(uint256.Int), new(uint256.Int), nil, nil,
		false, false, false, true, nil)
	msg.SetTxType(txType)
	return msg
}

func applyMinting(t *testing.T, ibs *state.IntraBlockState, msg *types.Message) (*evmtypes.ExecutionResult, error) {
	t.Helper()
	return ApplyMessage(newTestEVM(ibs, chain.TestChainOsakaConfig, 30_000_000), msg, new(GasPool).AddGas(30_000_000), true, false, nil)
}

func balance(t *testing.T, ibs *state.IntraBlockState, addr accounts.Address) uint256.Int {
	t.Helper()
	b, err := ibs.GetBalance(addr)
	require.NoError(t, err)
	return b
}

func TestMintedValue(t *testing.T) {
	for _, tc := range []struct {
		name          string
		code          []byte
		senderBalance uint64
		failed        bool
	}{
		{name: "without sender funds"},
		{name: "sender funds untouched", senderBalance: 10},
		// PUSH1 0 PUSH1 0 REVERT
		{name: "revert", code: []byte{0x60, 0x00, 0x60, 0x00, 0xfd}, senderBalance: 10, failed: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ibs := state.New(state.NewNoopReader())
			defer ibs.Close()
			require.NoError(t, ibs.SetCode(recipient, tc.code, tracing.CodeChangeUnspecified))
			require.NoError(t, ibs.AddBalance(minter, *uint256.NewInt(tc.senderBalance), tracing.BalanceChangeUnspecified))

			res, err := applyMinting(t, ibs, mintingMsg(uint256.NewInt(5), mintingTxType))
			require.NoError(t, err)
			require.Equal(t, tc.failed, res.Failed())
			minted := uint64(5)
			if tc.failed {
				minted = 0
			}
			require.Equal(t, *uint256.NewInt(minted), balance(t, ibs, recipient))
			require.Equal(t, *uint256.NewInt(tc.senderBalance), balance(t, ibs, minter))
		})
	}
}

func TestMintedValueRejects(t *testing.T) {
	ibs := state.New(state.NewNoopReader())
	defer ibs.Close()
	require.NoError(t, ibs.AddBalance(minter, *uint256.NewInt(1), tracing.BalanceChangeUnspecified))

	_, err := applyMinting(t, ibs, mintingMsg(new(uint256.Int).SetAllOne(), mintingTxType))
	require.ErrorIs(t, err, ErrMintOverflow)
	require.Equal(t, *uint256.NewInt(1), balance(t, ibs, minter))
	require.Equal(t, uint256.Int{}, balance(t, ibs, recipient))

	// Other types still pay their value.
	_, err = applyMinting(t, ibs, mintingMsg(uint256.NewInt(5), 0))
	require.ErrorIs(t, err, ErrInsufficientFunds)
}
