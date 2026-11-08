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
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/program"
)

func TestPostTxRollback(t *testing.T) {
	t.Parallel()
	evm, tx := frameTestEnv(t)
	ibs := evm.IntraBlockState()
	sender := accounts.InternAddress(tx.SenderAddress)
	body := common.HexToAddress("0x2000")
	assertion := common.HexToAddress("0x3000")
	require.NoError(t, ibs.SetCode(sender, []byte{0x60, 3, 0x5f, 0x5f, 0xaa}, tracing.CodeChangeUnspecified))
	require.NoError(t, ibs.SetCode(accounts.InternAddress(body), program.New().Sstore(0, 1).Push0().Push0().Op(vm.LOG0).Bytes(), tracing.CodeChangeUnspecified))
	require.NoError(t, ibs.SetCode(accounts.InternAddress(assertion), []byte{0x5f, 0x5f, 0xfd}, tracing.CodeChangeUnspecified))
	tx.Fees.FeeCap.SetUint64(1)
	tx.Fees.TipCap.SetUint64(1)
	tx.Frames = []types.Frame{
		{Mode: types.FrameVerify, Flags: types.FrameApproveBoth, Limits: mdgas.MdGas{Execution: 1_000}},
		{Mode: types.FrameSender, Target: &body, Limits: mdgas.MdGas{Execution: 20_000, State: params.StateGasPerStorageSet}},
		{Mode: types.FramePostTx, Target: &assertion, Limits: mdgas.MdGas{Execution: 10_000}},
		{Mode: types.FramePostTx, Target: &assertion, Limits: mdgas.MdGas{Execution: 10_000}},
	}
	result, err := applyFrameTest(t, evm, tx)
	require.NoError(t, err)
	require.ErrorIs(t, result.Err, vm.ErrExecutionReverted)
	require.Equal(t, []uint64{1, 1, 0, 2}, []uint64{
		result.FrameReceipts[0].Status, result.FrameReceipts[1].Status,
		result.FrameReceipts[2].Status, result.FrameReceipts[3].Status,
	})
	require.Empty(t, result.FrameReceipts[1].Logs)
	require.Zero(t, result.BlockStateGasUsed)
	require.Zero(t, result.FrameReceipts[3].GasUsed)
	value, err := ibs.GetState(accounts.InternAddress(body), accounts.InternKey(common.Hash{}))
	require.NoError(t, err)
	require.True(t, value.IsZero())
	nonce, err := ibs.GetNonce(sender)
	require.NoError(t, err)
	require.EqualValues(t, 1, nonce)
	balance, err := ibs.GetBalance(sender)
	require.NoError(t, err)
	require.Equal(t, uint256.NewInt(10_000_000-result.ReceiptGasUsed), &balance)
}

func TestPostTxBalanceTrace(t *testing.T) {
	t.Parallel()
	evm, tx := frameTestEnv(t)
	ibs := evm.IntraBlockState()
	target := common.HexToAddress("0x2000")
	assertion := common.HexToAddress("0x3000")
	require.NoError(t, ibs.SetCode(accounts.InternAddress(tx.SenderAddress), []byte{0x60, 3, 0x5f, 0x5f, 0xaa}, tracing.CodeChangeUnspecified))
	code := program.New().Push0().Push0().Op(vm.TXTRACE)
	assertStackWord(code, 2)
	require.NoError(t, ibs.SetCode(accounts.InternAddress(assertion), code.Bytes(), tracing.CodeChangeUnspecified))
	tx.Frames = []types.Frame{
		{Mode: types.FrameVerify, Flags: types.FrameApproveBoth, Limits: mdgas.MdGas{Execution: 1_000}},
		{Mode: types.FrameSender, Target: &target, Value: *uint256.NewInt(5), Limits: mdgas.MdGas{Execution: 20_000, State: params.StateGasNewAccount}},
		{Mode: types.FramePostTx, Target: &assertion, Limits: mdgas.MdGas{Execution: 10_000}},
	}
	result, err := applyFrameTest(t, evm, tx)
	require.NoError(t, err)
	require.NoError(t, result.Err)
	for _, receipt := range result.FrameReceipts {
		require.EqualValues(t, types.ReceiptStatusSuccessful, receipt.Status)
	}
}

func TestTxDiffValidationErrors(t *testing.T) {
	for _, tc := range []struct {
		name  string
		mode  uint8
		index uint64
		want  error
	}{
		{"outside-post-tx", types.FrameSender, 0, vm.ErrFrameContext},
		{"reserved-input", types.FramePostTx, 1, vm.ErrFrameParameter},
	} {
		t.Run(tc.name, func(t *testing.T) {
			evm, tx := frameTestEnv(t)
			ibs := evm.IntraBlockState()
			target := common.HexToAddress("0x2000")
			require.NoError(t, ibs.SetCode(accounts.InternAddress(tx.SenderAddress), []byte{0x60, 3, 0x5f, 0x5f, 0xaa}, tracing.CodeChangeUnspecified))
			code := program.New().Push(tc.index).Push0().Push(2).Op(vm.TXDIFF, vm.STOP).Bytes()
			require.NoError(t, ibs.SetCode(accounts.InternAddress(target), code, tracing.CodeChangeUnspecified))
			tx.Frames = []types.Frame{
				{Mode: types.FrameVerify, Flags: types.FrameApproveBoth, Limits: mdgas.MdGas{Execution: 1_000}},
				{Mode: tc.mode, Target: &target, Limits: mdgas.MdGas{Execution: 10_000}},
			}
			result, err := applyFrameTest(t, evm, tx)
			require.NoError(t, err)
			require.ErrorIs(t, result.Err, tc.want)
			require.NotErrorIs(t, result.Err, vm.ErrOutOfGas)
		})
	}
}

func TestPostTxStateReadErrorAbortsTransaction(t *testing.T) {
	target := common.HexToAddress("0x2000")
	body := common.HexToAddress("0x3000")
	for _, tc := range []struct {
		name  string
		op    vm.OpCode
		args  *program.Program
		trace bool
	}{
		{"TXTRACE", vm.TXTRACE, program.New().Push0().Push0(), true},
		{"TXDIFF trace", vm.TXDIFF, program.New().Push0().Push0().Push(6), true},
		{"EVENTDATACOPY", vm.EVENTDATACOPY, program.New().Push0().Push0().Push0().Push0(), true},
		{"TXDIFF storage before", vm.TXDIFF, program.New().Push0().Push(target).Push0(), false},
		{"TXDIFF storage after", vm.TXDIFF, program.New().Push0().Push(target).Push(1), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			evm, tx := frameTestEnv(t)
			ibs := evm.IntraBlockState()
			require.NoError(t, ibs.SetCode(accounts.InternAddress(tx.SenderAddress), []byte{0x60, 3, 0x5f, 0x5f, 0xaa}, tracing.CodeChangeUnspecified))
			require.NoError(t, ibs.SetCode(accounts.InternAddress(target), tc.args.Op(tc.op, vm.STOP).Bytes(), tracing.CodeChangeUnspecified))
			require.NoError(t, ibs.SetCode(accounts.InternAddress(body), program.New().Sstore(0, 1).Bytes(), tracing.CodeChangeUnspecified))
			tx.Frames = []types.Frame{
				{Mode: types.FrameVerify, Flags: types.FrameApproveBoth, Limits: mdgas.MdGas{Execution: 1_000}},
				{Mode: types.FrameSender, Target: &body, Limits: mdgas.MdGas{Execution: 20_000, State: params.StateGasPerStorageSet}},
				{Mode: types.FramePostTx, Target: &target, Limits: mdgas.MdGas{Execution: 10_000}},
			}

			var readErr, frameErr error
			config := evm.Config()
			config.Tracer = &tracing.Hooks{
				OnOpcode: func(_ uint64, op byte, _, _ uint64, _ tracing.OpContext, _ []byte, _ int, _ error) {
					if vm.OpCode(op) != tc.op {
						return
					}
					address := accounts.InternAddress(target)
					key := accounts.InternKey(common.Hash{})
					if tc.trace {
						address = accounts.InternAddress(body)
					} else {
						_, err := evm.FrameContext.TransactionTrace(ibs)
						require.NoError(t, err)
					}
					// A materialized account in versioned mode triggers an internal read
					// error without setting StateReadError. Inject it only for this opcode.
					ibs.SetVersionMap(state.NewVersionMap(nil))
					_, readErr = ibs.GetState(address, key)
					require.ErrorContains(t, readErr, "noMaterialize read fell back to a materialized stateObject")
					require.NoError(t, ibs.StateReadError())
				},
				OnExitV2: func(_ int, _ []byte, _ mdgas.MdGasUsage, err error, _ bool) {
					if ibs.IsVersioned() {
						frameErr = err
					}
					ibs.SetVersionMap(nil)
				},
			}
			evm.ResetBetweenBlocks(evm.Context, evm.TxContext, ibs, config, evm.ChainRules())

			result, err := applyFrameTest(t, evm, tx)
			require.Error(t, readErr)
			require.ErrorContains(t, frameErr, readErr.Error())
			require.NoError(t, ibs.StateReadError())
			require.ErrorIs(t, err, vm.ErrIntraBlockStateFailed)
			require.ErrorContains(t, err, readErr.Error())
			require.Nil(t, result)
		})
	}
}

func assertStackWord(code *program.Program, expected uint64) {
	code.Push(expected).Op(vm.EQ)
	destination := code.Size() + 7
	code.Op(vm.PUSH2).Append([]byte{byte(destination >> 8), byte(destination)}).Op(vm.JUMPI).
		Push0().Push0().Op(vm.REVERT, vm.JUMPDEST)
}
