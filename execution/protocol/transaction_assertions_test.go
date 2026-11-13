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

func TestAssertRollback(t *testing.T) {
	t.Parallel()
	evm, tx := frameTestEnv(t)
	ibs := evm.IntraBlockState()
	sender := accounts.InternAddress(tx.SenderAddress)
	body := common.HexToAddress("0x2000")
	assertion := common.HexToAddress("0x3000")
	require.NoError(t, ibs.SetCode(sender, program.New().Push(types.FrameApproveBoth).Push0().Push0().Op(vm.APPROVE).Bytes(), tracing.CodeChangeUnspecified))
	require.NoError(t, ibs.SetCode(accounts.InternAddress(body), program.New().Sstore(0, 1).Push0().Push0().Op(vm.LOG0).Bytes(), tracing.CodeChangeUnspecified))
	require.NoError(t, ibs.SetCode(accounts.InternAddress(assertion), program.New().Push0().Push0().Op(vm.REVERT).Bytes(), tracing.CodeChangeUnspecified))
	tx.Fees.MaxFee.SetUint64(1_000_000)
	tx.Fees.TipCap.SetUint64(1)
	tx.Frames = []types.Frame{
		{Mode: types.FrameVerify, Flags: types.FrameApproveBoth, Limits: mdgas.MdGas{Execution: 1_000}},
		{Mode: types.FrameSender, Target: &body, Limits: mdgas.MdGas{Execution: 20_000, State: params.StateGasPerStorageSet}},
		{Mode: types.FrameAssert, Target: &assertion, Limits: mdgas.MdGas{Execution: 10_000}},
		{Mode: types.FrameAssert, Target: &assertion, Limits: mdgas.MdGas{Execution: 10_000}},
	}
	result, err := applyFrameTest(t, evm, tx)
	require.NoError(t, err)
	require.ErrorIs(t, result.Err, vm.ErrExecutionReverted)
	require.Equal(t, []uint64{
		types.ReceiptStatusSuccessful, types.ReceiptStatusSuccessful,
		types.ReceiptStatusFailed, types.ReceiptStatusSkipped,
	}, []uint64{
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

func TestAssertRollbackPreservesLaterSnapshot(t *testing.T) {
	evm, tx := frameTestEnv(t)
	ibs := evm.IntraBlockState()
	sender := accounts.InternAddress(tx.SenderAddress)
	assertion := common.HexToAddress("0x3000")
	require.NoError(t, ibs.SetCode(sender, program.New().Push(types.FrameApproveBoth).Push0().Push0().Op(vm.APPROVE).Bytes(), tracing.CodeChangeUnspecified))
	require.NoError(t, ibs.SetCode(accounts.InternAddress(assertion), program.New().Push0().Push0().Op(vm.REVERT).Bytes(), tracing.CodeChangeUnspecified))
	tx.Fees.MaxFee.SetUint64(1_000_000)
	tx.Fees.TipCap.SetUint64(1)
	tx.Frames = []types.Frame{
		{Mode: types.FrameVerify, Flags: types.FrameApproveBoth, Limits: mdgas.MdGas{Execution: 1_000}},
		{Mode: types.FrameAssert, Target: &assertion, Limits: mdgas.MdGas{Execution: 10_000}},
	}
	snapshot := -1
	ibs.SetHooks(&tracing.Hooks{OnBalanceChange: func(_ accounts.Address, _, _ uint256.Int, reason tracing.BalanceChangeReason) {
		if reason == tracing.BalanceIncreaseGasReturn {
			snapshot = ibs.PushSnapshot()
		}
	}})
	result, err := applyFrameTest(t, evm, tx)
	require.NoError(t, err)
	require.ErrorIs(t, result.Err, vm.ErrExecutionReverted)
	require.NotEqual(t, -1, snapshot)
	require.NotPanics(t, func() { ibs.RevertToSnapshot(snapshot, nil) })
}

func TestAssertBalanceTrace(t *testing.T) {
	t.Parallel()
	evm, tx := frameTestEnv(t)
	ibs := evm.IntraBlockState()
	target := common.HexToAddress("0x2000")
	assertion := common.HexToAddress("0x3000")
	require.NoError(t, ibs.SetCode(accounts.InternAddress(tx.SenderAddress), program.New().Push(types.FrameApproveBoth).Push0().Push0().Op(vm.APPROVE).Bytes(), tracing.CodeChangeUnspecified))
	code := program.New().Push0().Push0().Op(vm.TXTRACE)
	assertStackWord(code, 2)
	require.NoError(t, ibs.SetCode(accounts.InternAddress(assertion), code.Bytes(), tracing.CodeChangeUnspecified))
	tx.Frames = []types.Frame{
		{Mode: types.FrameVerify, Flags: types.FrameApproveBoth, Limits: mdgas.MdGas{Execution: 1_000}},
		{Mode: types.FrameSender, Target: &target, Value: *uint256.NewInt(5), Limits: mdgas.MdGas{Execution: 20_000, State: params.StateGasNewAccount}},
		{Mode: types.FrameAssert, Target: &assertion, Limits: mdgas.MdGas{Execution: 10_000}},
	}
	result, err := applyFrameTest(t, evm, tx)
	require.NoError(t, err)
	require.NoError(t, result.Err)
	for _, receipt := range result.FrameReceipts {
		require.EqualValues(t, types.ReceiptStatusSuccessful, receipt.Status)
	}
}

func TestTxTraceValidationErrors(t *testing.T) {
	for _, tc := range []struct {
		name  string
		mode  uint8
		index uint64
		want  error
	}{
		{"outside-assert", types.FrameSender, 0, vm.ErrFrameContext},
		{"reserved-input", types.FrameAssert, 1, vm.ErrFrameParameter},
	} {
		t.Run(tc.name, func(t *testing.T) {
			evm, tx := frameTestEnv(t)
			ibs := evm.IntraBlockState()
			target := common.HexToAddress("0x2000")
			require.NoError(t, ibs.SetCode(accounts.InternAddress(tx.SenderAddress), program.New().Push(types.FrameApproveBoth).Push0().Push0().Op(vm.APPROVE).Bytes(), tracing.CodeChangeUnspecified))
			code := program.New().Push(tc.index).Push(2).Op(vm.TXTRACE, vm.STOP).Bytes()
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

func TestAssertStateReadErrorAbortsTransaction(t *testing.T) {
	target := common.HexToAddress("0x2000")
	body := common.HexToAddress("0x3000")
	parent := common.HexToAddress("0x4000")
	for _, tc := range []struct {
		name string
		op   vm.OpCode
		args *program.Program
	}{
		{"TXTRACE", vm.TXTRACE, program.New().Push0().Push0()},
		{"EVENTDATACOPY", vm.EVENTDATACOPY, program.New().Push0().Push0().Push0().Push0()},
	} {
		for _, caller := range []struct {
			name string
			code *program.Program
		}{
			{"direct", nil},
			{"CALL", program.New().Call(uint256.NewInt(5_000), target, 0, 0, 0, 0, 0)},
			{"CALLCODE", program.New().CallCode(uint256.NewInt(5_000), target, 0, 0, 0, 0, 0)},
			{"DELEGATECALL", program.New().DelegateCall(uint256.NewInt(5_000), target, 0, 0, 0, 0)},
			{"STATICCALL", program.New().StaticCall(uint256.NewInt(5_000), target, 0, 0, 0, 0)},
		} {
			t.Run(tc.name+"/"+caller.name, func(t *testing.T) {
				evm, tx := frameTestEnv(t)
				ibs := evm.IntraBlockState()
				require.NoError(t, ibs.SetCode(accounts.InternAddress(tx.SenderAddress), program.New().Push(types.FrameApproveBoth).Push0().Push0().Op(vm.APPROVE).Bytes(), tracing.CodeChangeUnspecified))
				require.NoError(t, ibs.SetCode(accounts.InternAddress(target), program.New().Append(tc.args.Bytes()).Op(tc.op, vm.STOP).Bytes(), tracing.CodeChangeUnspecified))
				require.NoError(t, ibs.SetCode(accounts.InternAddress(body), program.New().Sstore(0, 1).Bytes(), tracing.CodeChangeUnspecified))
				frameTarget := target
				if caller.code != nil {
					// The parent catches the failed child call and completes successfully.
					require.NoError(t, ibs.SetCode(accounts.InternAddress(parent), caller.code.Op(vm.POP, vm.STOP).Bytes(), tracing.CodeChangeUnspecified))
					frameTarget = parent
				}
				tx.Frames = []types.Frame{
					{Mode: types.FrameVerify, Flags: types.FrameApproveBoth, Limits: mdgas.MdGas{Execution: 1_000}},
					{Mode: types.FrameSender, Target: &body, Limits: mdgas.MdGas{Execution: 20_000, State: params.StateGasPerStorageSet}},
					{Mode: types.FrameAssert, Target: &frameTarget, Limits: mdgas.MdGas{Execution: 20_000}},
				}

				var readErr, frameErr error
				config := evm.Config()
				config.Tracer = &tracing.Hooks{
					OnOpcode: func(_ uint64, op byte, _, _ uint64, _ tracing.OpContext, _ []byte, _ int, _ error) {
						if vm.OpCode(op) != tc.op {
							return
						}
						address := accounts.InternAddress(body)
						key := accounts.InternKey(common.Hash{})
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
				require.ErrorIs(t, err, vm.ErrIntraBlockStateFailed)
				require.ErrorContains(t, err, readErr.Error())
				require.Nil(t, result)
				require.ErrorIs(t, ibs.StateReadError(), vm.ErrIntraBlockStateFailed)
				value, err := ibs.GetState(accounts.InternAddress(body), accounts.InternKey(common.Hash{}))
				require.NoError(t, err)
				require.True(t, value.IsZero())
			})
		}
	}
}

func assertStackWord(code *program.Program, expected uint64) {
	code.Push(expected).Op(vm.EQ)
	destination := code.Size() + 7
	code.Op(vm.PUSH2).Append([]byte{byte(destination >> 8), byte(destination)}).Op(vm.JUMPI).
		Push0().Push0().Op(vm.REVERT, vm.JUMPDEST)
}

func TestTxTraceOmitsRestoredBalance(t *testing.T) {
	t.Parallel()
	evm, tx := frameTestEnv(t)
	ibs := evm.IntraBlockState()
	relay := common.HexToAddress("0x2000")
	assertion := common.HexToAddress("0x3000")
	sink := common.HexToAddress("0x4000")
	require.NoError(t, ibs.SetCode(accounts.InternAddress(tx.SenderAddress), program.New().Push(types.FrameApproveBoth).Push0().Push0().Op(vm.APPROVE).Bytes(), tracing.CodeChangeUnspecified))
	require.NoError(t, ibs.SetBalance(accounts.InternAddress(sink), *uint256.NewInt(1), tracing.BalanceChangeUnspecified))
	// The relay forwards the value it receives, so its own balance ends unchanged.
	require.NoError(t, ibs.SetCode(accounts.InternAddress(relay), program.New().Call(uint256.NewInt(0), sink, 5, 0, 0, 0, 0).Op(vm.POP, vm.STOP).Bytes(), tracing.CodeChangeUnspecified))
	code := program.New().Push0().Push0().Op(vm.TXTRACE)
	assertStackWord(code, 2) // payer and sink
	require.NoError(t, ibs.SetCode(accounts.InternAddress(assertion), code.Op(vm.STOP).Bytes(), tracing.CodeChangeUnspecified))
	tx.Frames = []types.Frame{
		{Mode: types.FrameVerify, Flags: types.FrameApproveBoth, Limits: mdgas.MdGas{Execution: 1_000}},
		{Mode: types.FrameSender, Target: &relay, Value: *uint256.NewInt(5), Limits: mdgas.MdGas{Execution: 50_000}},
		{Mode: types.FrameAssert, Target: &assertion, Limits: mdgas.MdGas{Execution: 20_000}},
	}
	result, err := applyFrameTest(t, evm, tx)
	require.NoError(t, err)
	require.NoError(t, result.Err)
	balance, err := ibs.GetBalance(accounts.InternAddress(sink))
	require.NoError(t, err)
	require.Equal(t, uint256.NewInt(6), &balance)
}
