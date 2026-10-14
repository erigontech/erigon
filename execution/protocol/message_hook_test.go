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
	"errors"
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

const hookedTxType = 0x7d

func init() {
	types.RegisterTxType(hookedTxType, types.TxTypeSpec{New: func() types.Transaction { return nil }})
}

// mintHook credits the message value before execution and takes it back when
// the call fails.
type mintHook struct {
	before, after int
	failed        bool
	err           error
}

func (h *mintHook) BeforeExecute(ibs evmtypes.IntraBlockState, msg Message) error {
	h.before++
	if h.err != nil {
		return h.err
	}
	return ibs.AddBalance(msg.From(), *msg.Value(), tracing.BalanceChangeUnspecified)
}

func (h *mintHook) AfterExecute(ibs evmtypes.IntraBlockState, msg Message, failed bool) error {
	h.after++
	h.failed = failed
	if failed {
		return ibs.SubBalance(msg.From(), *msg.Value(), tracing.BalanceChangeUnspecified)
	}
	return nil
}

func registerMintHook(t *testing.T) *mintHook {
	t.Helper()
	h := &mintHook{}
	RegisterMessageHook(hookedTxType, h)
	t.Cleanup(func() { unregisterMessageHook(hookedTxType) })
	return h
}

func hookedMsg(from, to accounts.Address, value uint64, txType byte) *types.Message {
	msg := types.NewMessage(from, to, 0, uint256.NewInt(value), 100_000,
		new(uint256.Int), new(uint256.Int), new(uint256.Int), nil, nil,
		false, false, false, true, nil)
	msg.SetTxType(txType)
	return msg
}

var (
	hookSender    = accounts.InternAddress(common.HexToAddress("0x1111111111111111111111111111111111111111"))
	hookRecipient = accounts.InternAddress(common.HexToAddress("0x2222222222222222222222222222222222222222"))
)

func TestMessageHookMintsBeforeChecks(t *testing.T) {
	h := registerMintHook(t)
	ibs := state.New(state.NewNoopReader())
	defer ibs.Close()

	// The sender holds nothing: the value check passes only on the minted balance.
	res, err := ApplyMessage(newTestEVM(ibs, chain.TestChainOsakaConfig, 30_000_000), hookedMsg(hookSender, hookRecipient, 5, hookedTxType),
		new(GasPool).AddGas(30_000_000), true, false, nil)
	require.NoError(t, err)
	require.False(t, res.Failed())
	require.Equal(t, 1, h.before)
	require.Equal(t, 1, h.after)
	require.False(t, h.failed)
	balance, err := ibs.GetBalance(hookRecipient)
	require.NoError(t, err)
	require.Equal(t, *uint256.NewInt(5), balance)
}

func TestMessageHookSeesFailure(t *testing.T) {
	h := registerMintHook(t)
	ibs := state.New(state.NewNoopReader())
	defer ibs.Close()
	// PUSH1 0 PUSH1 0 REVERT
	require.NoError(t, ibs.SetCode(hookRecipient, []byte{0x60, 0x00, 0x60, 0x00, 0xfd}, tracing.CodeChangeUnspecified))

	res, err := ApplyMessage(newTestEVM(ibs, chain.TestChainOsakaConfig, 30_000_000), hookedMsg(hookSender, hookRecipient, 5, hookedTxType),
		new(GasPool).AddGas(30_000_000), true, false, nil)
	require.NoError(t, err)
	require.True(t, res.Failed())
	require.Equal(t, 1, h.after)
	require.True(t, h.failed)
	for _, addr := range []accounts.Address{hookSender, hookRecipient} {
		balance, err := ibs.GetBalance(addr)
		require.NoError(t, err)
		require.True(t, balance.IsZero())
	}
}

func TestMessageHookError(t *testing.T) {
	h := registerMintHook(t)
	h.err = errors.New("rejected")
	ibs := state.New(state.NewNoopReader())
	defer ibs.Close()

	_, err := ApplyMessage(newTestEVM(ibs, chain.TestChainOsakaConfig, 30_000_000), hookedMsg(hookSender, hookRecipient, 5, hookedTxType),
		new(GasPool).AddGas(30_000_000), true, false, nil)
	require.ErrorIs(t, err, h.err)
	require.Zero(t, h.after)
	nonce, err := ibs.GetNonce(hookSender)
	require.NoError(t, err)
	require.Zero(t, nonce)
}

func TestMessageHookSkipsOtherTypes(t *testing.T) {
	h := registerMintHook(t)
	ibs := state.New(state.NewNoopReader())
	defer ibs.Close()

	for _, txType := range []byte{0, types.DynamicFeeTxType} {
		_, err := ApplyMessage(newTestEVM(ibs, chain.TestChainOsakaConfig, 30_000_000), hookedMsg(hookSender, hookRecipient, 0, txType),
			new(GasPool).AddGas(30_000_000), true, false, nil)
		require.NoError(t, err)
	}
	require.Zero(t, h.before)
	require.Zero(t, h.after)
}

func TestRegisterMessageHookPanics(t *testing.T) {
	registerMintHook(t)
	require.Panics(t, func() { RegisterMessageHook(hookedTxType, &mintHook{}) })
	require.Panics(t, func() { RegisterMessageHook(0x7c, &mintHook{}) })
	require.Panics(t, func() { RegisterMessageHook(types.DynamicFeeTxType, &mintHook{}) })
	unregisterMessageHook(hookedTxType)
	require.Panics(t, func() { RegisterMessageHook(hookedTxType, nil) })
}
