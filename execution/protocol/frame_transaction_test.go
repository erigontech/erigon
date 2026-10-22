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
	"fmt"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
	"github.com/erigontech/erigon/execution/vm/program"
)

func TestFrameTransactionDefaultTransfer(t *testing.T) {
	t.Parallel()
	cfg := chain.AllProtocolChanges.Copy()
	fork := uint64(0)
	cfg.BogotaTime = &fork
	ibs := state.New(state.NewNoopReader())
	defer ibs.Close()
	evm := newTestEVM(ibs, cfg, 30_000_000)
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	sender := crypto.PubkeyToAddress(key.PublicKey)
	recipient := common.HexToAddress("0x1234")
	from := accounts.InternAddress(sender)
	to := accounts.InternAddress(recipient)
	require.NoError(t, ibs.SetBalance(from, *uint256.NewInt(1_000_000), tracing.BalanceChangeUnspecified))
	require.NoError(t, ibs.SetBalance(to, *uint256.NewInt(1), tracing.BalanceChangeUnspecified))
	tx := &types.FrameTransaction{
		ChainID: *cfg.ChainID, SenderAddress: sender,
		Frames: []types.Frame{
			{Mode: types.FrameVerify, Flags: types.FrameApproveBoth, Limits: mdgas.MdGas{Execution: 100}},
			{Mode: types.FrameSender, Target: &recipient, Limits: mdgas.MdGas{Execution: 10_000}, Value: *uint256.NewInt(123)},
		},
		Signatures: []types.FrameSignature{{Scheme: types.FrameSignatureSecp256k1}},
	}
	hash := tx.SigningHash(nil)
	sig, err := crypto.Sign(hash[:], key)
	require.NoError(t, err)
	tx.Signatures[0].Signature = append([]byte{sig[64]}, sig[:64]...)
	msg, err := tx.AsMessage(types.Signer{}, nil, evm.ChainRules())
	require.NoError(t, err)
	gp := NewGasPool(30_000_000, 0)
	result, err := ApplyMessage(evm, msg, gp, true, false, nil)
	require.NoError(t, err)
	require.NoError(t, result.Err)
	balance, err := ibs.GetBalance(to)
	require.NoError(t, err)
	require.Equal(t, uint256.NewInt(124), &balance)
	nonce, err := ibs.GetNonce(from)
	require.NoError(t, err)
	require.Equal(t, uint64(1), nonce)
	require.Equal(t, uint64(0), result.BlockStateGasUsed)
}

func frameTestEnv(t *testing.T) (*vm.EVM, *types.FrameTransaction) {
	t.Helper()
	cfg := chain.AllProtocolChanges.Copy()
	cfg.BogotaTime = common.NewUint64(0)
	ibs := state.New(state.NewNoopReader())
	t.Cleanup(ibs.Close)
	evm := newTestEVM(ibs, cfg, 30_000_000)
	sender := common.HexToAddress("0x1000")
	require.NoError(t, ibs.SetBalance(accounts.InternAddress(sender), *uint256.NewInt(10_000_000), tracing.BalanceChangeUnspecified))
	return evm, &types.FrameTransaction{ChainID: *cfg.ChainID, SenderAddress: sender}
}

func applyFrameTest(t *testing.T, evm *vm.EVM, tx *types.FrameTransaction) (*evmtypes.ExecutionResult, error) {
	t.Helper()
	msg, err := tx.AsMessage(types.Signer{}, &evm.Context.BaseFee, evm.ChainRules())
	require.NoError(t, err)
	return ApplyMessage(evm, msg, NewGasPool(30_000_000, 10_000_000), true, false, nil)
}

func TestFrameTransactionContractApproval(t *testing.T) {
	t.Parallel()
	evm, tx := frameTestEnv(t)
	sender := accounts.InternAddress(tx.SenderAddress)
	require.NoError(t, evm.IntraBlockState().SetCode(sender, []byte{0x60, 3, 0x5f, 0x5f, 0xaa}, tracing.CodeChangeUnspecified))
	tx.Frames = []types.Frame{{Mode: types.FrameVerify, Flags: types.FrameApproveBoth, Limits: mdgas.MdGas{Execution: 1_000}}}
	result, err := applyFrameTest(t, evm, tx)
	require.NoError(t, err)
	require.NoError(t, result.Err)
	require.Equal(t, &tx.SenderAddress, result.Payer)
	require.Equal(t, uint64(107), result.FrameReceipts[0].GasUsed.Execution)
	nonce, err := evm.IntraBlockState().GetNonce(sender)
	require.NoError(t, err)
	require.Equal(t, uint64(1), nonce)
}

func TestFrameTransactionStateBudget(t *testing.T) {
	t.Parallel()
	for _, stateGas := range []uint64{0, params.StateGasPerStorageSet} {
		t.Run(fmt.Sprint(stateGas), func(t *testing.T) {
			evm, tx := frameTestEnv(t)
			ibs := evm.IntraBlockState()
			sender := accounts.InternAddress(tx.SenderAddress)
			target := common.HexToAddress("0x2000")
			addr := accounts.InternAddress(target)
			require.NoError(t, ibs.SetCode(sender, []byte{0x60, 3, 0x5f, 0x5f, 0xaa}, tracing.CodeChangeUnspecified))
			require.NoError(t, ibs.SetCode(addr, []byte{0x60, 1, 0x5f, 0x55, 0x00}, tracing.CodeChangeUnspecified))
			tx.Frames = []types.Frame{
				{Mode: types.FrameVerify, Flags: types.FrameApproveBoth, Limits: mdgas.MdGas{Execution: 1_000}},
				{Mode: types.FrameSender, Target: &target, Limits: mdgas.MdGas{Execution: 200_000, State: stateGas}},
			}
			result, err := applyFrameTest(t, evm, tx)
			require.NoError(t, err)
			stored, err := ibs.GetState(addr, accounts.InternKey(common.Hash{}))
			require.NoError(t, err)
			if stateGas == 0 {
				require.ErrorIs(t, result.Err, vm.ErrOutOfGas)
				require.True(t, stored.IsZero())
				require.Equal(t, uint64(200_000), result.FrameReceipts[1].GasUsed.Execution)
			} else {
				require.NoError(t, result.Err)
				require.Equal(t, uint256.NewInt(1), &stored)
				require.Equal(t, stateGas, result.FrameReceipts[1].GasUsed.State)
				require.Equal(t, stateGas, result.BlockStateGasUsed)
			}
		})
	}
}

func TestFrameTransactionCrossFrameRefill(t *testing.T) {
	t.Parallel()
	for _, revertClear := range []bool{false, true} {
		t.Run(fmt.Sprint(revertClear), func(t *testing.T) {
			evm, tx := frameTestEnv(t)
			ibs := evm.IntraBlockState()
			sender := accounts.InternAddress(tx.SenderAddress)
			target := common.HexToAddress("0x2000")
			addr := accounts.InternAddress(target)
			require.NoError(t, ibs.SetCode(sender, []byte{0x60, 3, 0x5f, 0x5f, 0xaa}, tracing.CodeChangeUnspecified))
			require.NoError(t, ibs.SetCode(addr, []byte{0x5f, 0x35, 0x5f, 0x55, 0x60, 32, 0x35, 0x60, 11, 0x57, 0, 0x5b, 0x5f, 0x5f, 0xfd}, tracing.CodeChangeUnspecified))
			var clearData []byte
			if revertClear {
				clearData = make([]byte, 33)
				clearData[32] = 1
			}
			tx.Frames = []types.Frame{
				{Mode: types.FrameVerify, Flags: types.FrameApproveBoth, Limits: mdgas.MdGas{Execution: 1_000}},
				{Mode: types.FrameSender, Target: &target, Limits: mdgas.MdGas{Execution: 200_000, State: params.StateGasPerStorageSet}, Data: []byte{1}},
				{Mode: types.FrameSender, Target: &target, Limits: mdgas.MdGas{Execution: 200_000}, Data: clearData},
			}
			result, err := applyFrameTest(t, evm, tx)
			require.NoError(t, err)
			if revertClear {
				require.ErrorIs(t, result.Err, vm.ErrExecutionReverted)
				require.EqualValues(t, params.StateGasPerStorageSet, result.FrameReceipts[1].GasUsed.State)
			} else {
				require.NoError(t, result.Err)
				require.Zero(t, result.FrameReceipts[1].GasUsed.State)
			}
			require.Zero(t, result.FrameReceipts[2].GasUsed.State)
		})
	}
}

func TestFrameTransactionAtomicBatch(t *testing.T) {
	t.Parallel()
	evm, tx := frameTestEnv(t)
	ibs := evm.IntraBlockState()
	target := common.HexToAddress("0x2000")
	failure := common.HexToAddress("0x3000")
	end := common.HexToAddress("0x4000")
	require.NoError(t, ibs.SetCode(accounts.InternAddress(tx.SenderAddress), []byte{0x60, 3, 0x5f, 0x5f, 0xaa}, tracing.CodeChangeUnspecified))
	code := program.New().Sstore(0, 1).Push0().Push0().Op(vm.LOG0).Bytes()
	require.NoError(t, ibs.SetCode(accounts.InternAddress(target), code, tracing.CodeChangeUnspecified))
	require.NoError(t, ibs.SetCode(accounts.InternAddress(failure), []byte{0x5f, 0x5f, 0xfd}, tracing.CodeChangeUnspecified))
	require.NoError(t, ibs.SetCode(accounts.InternAddress(end), code, tracing.CodeChangeUnspecified))
	budget := mdgas.MdGas{Execution: 20_000, State: params.StateGasPerStorageSet}
	tx.Frames = []types.Frame{
		{Mode: types.FrameVerify, Flags: types.FrameApproveBoth, Limits: mdgas.MdGas{Execution: 1_000}},
		{Mode: types.FrameSender, Flags: types.FrameAtomicBatch, Target: &target, Limits: budget},
		{Mode: types.FrameSender, Flags: types.FrameAtomicBatch, Target: &failure, Limits: budget},
		{Mode: types.FrameSender, Target: &target, Limits: budget},
		{Mode: types.FrameSender, Target: &end, Limits: budget},
	}
	result, err := applyFrameTest(t, evm, tx)
	require.NoError(t, err)
	require.ErrorIs(t, result.Err, vm.ErrExecutionReverted)
	require.Equal(t, []uint64{1, 1, 0, 2, 1}, []uint64{
		result.FrameReceipts[0].Status, result.FrameReceipts[1].Status, result.FrameReceipts[2].Status,
		result.FrameReceipts[3].Status, result.FrameReceipts[4].Status,
	})
	require.Zero(t, result.FrameReceipts[1].GasUsed.State)
	require.Empty(t, result.FrameReceipts[1].Logs)
	require.Zero(t, result.FrameReceipts[3].GasUsed)
	require.EqualValues(t, params.StateGasPerStorageSet, result.BlockStateGasUsed)
	stored, err := ibs.GetState(accounts.InternAddress(target), accounts.InternKey(common.Hash{}))
	require.NoError(t, err)
	require.True(t, stored.IsZero())
	require.Len(t, ibs.GetRawLogs(ibs.TxIndex()), 1)
}

func TestFrameTransactionTransientStorage(t *testing.T) {
	t.Parallel()
	evm, tx := frameTestEnv(t)
	ibs := evm.IntraBlockState()
	target := common.HexToAddress("0x2000")
	addr := accounts.InternAddress(target)
	require.NoError(t, ibs.SetCode(accounts.InternAddress(tx.SenderAddress), []byte{0x60, 3, 0x5f, 0x5f, 0xaa}, tracing.CodeChangeUnspecified))
	require.NoError(t, ibs.SetCode(addr, []byte{0x5f, 0x5c, 0x5f, 0x55, 0x5f, 0x35, 0x5f, 0x5d, 0}, tracing.CodeChangeUnspecified))
	budget := mdgas.MdGas{Execution: 20_000, State: params.StateGasPerStorageSet}
	tx.Frames = []types.Frame{
		{Mode: types.FrameVerify, Flags: types.FrameApproveBoth, Limits: mdgas.MdGas{Execution: 1_000}},
		{Mode: types.FrameSender, Target: &target, Limits: budget, Data: []byte{1}},
		{Mode: types.FrameSender, Target: &target, Limits: budget},
	}
	result, err := applyFrameTest(t, evm, tx)
	require.NoError(t, err)
	require.NoError(t, result.Err)
	require.Zero(t, result.BlockStateGasUsed)
	stored, err := ibs.GetState(addr, accounts.InternKey(common.Hash{}))
	require.NoError(t, err)
	require.True(t, stored.IsZero())
}

func TestFrameTransactionCodeDepositBudget(t *testing.T) {
	t.Parallel()
	evm, tx := frameTestEnv(t)
	ibs := evm.IntraBlockState()
	sender := accounts.InternAddress(tx.SenderAddress)
	initCode := program.New().Push(1).Push0().Op(vm.RETURN).Bytes()
	code := program.New().Create2(initCode, 0).Push0().Op(vm.MSTORE).Push(32).Push0().Op(vm.RETURN).Bytes()
	require.NoError(t, ibs.SetCode(sender, code, tracing.CodeChangeUnspecified))
	tx.Frames = []types.Frame{{Limits: mdgas.MdGas{Execution: 1_000_000, State: params.StateGasNewAccount}}}
	ctx := ibs.NewFrameContext(tx, uint256.Int{})
	evm.FrameContext = ctx
	ret, _, err := evm.ExecuteTransactionFrame()
	require.NoError(t, err)
	require.Equal(t, make([]byte, 32), ret, "code deposit must not spend execution gas when the state budget is empty")
	require.Zero(t, ctx.Receipts[0].GasUsed.State, "failed creation must roll back its account charge")
}

func TestFrameTransactionMakeReceipt(t *testing.T) {
	t.Parallel()
	evm, tx := frameTestEnv(t)
	require.NoError(t, evm.IntraBlockState().SetCode(accounts.InternAddress(tx.SenderAddress), []byte{0x60, 3, 0x5f, 0x5f, 0xaa}, tracing.CodeChangeUnspecified))
	tx.Frames = []types.Frame{{Mode: types.FrameVerify, Flags: types.FrameApproveBoth, Limits: mdgas.MdGas{Execution: 1_000}}}
	result, err := applyFrameTest(t, evm, tx)
	require.NoError(t, err)
	msg, err := tx.AsMessage(types.Signer{}, nil, evm.ChainRules())
	require.NoError(t, err)
	receipt := MakeReceipt(uint256.NewInt(1), common.Hash{}, msg, tx, result.ReceiptGasUsed, result, evm.IntraBlockState(), evm)
	require.Equal(t, result.Payer, receipt.Payer)
	require.Equal(t, result.FrameReceipts, receipt.FrameReceipts)
	require.Zero(t, receipt.ContractAddress)
}

func TestFrameBlockGasLimits(t *testing.T) {
	tx := &types.FrameTransaction{Frames: []types.Frame{{Limits: mdgas.MdGas{Execution: 10_000, State: 50_000}}}}
	limits, err := TransactionGasLimits(tx, true)
	require.NoError(t, err)
	require.Equal(t, mdgas.MdGas{Execution: 22_475, State: 50_000}, limits)
	require.NoError(t, CheckBlockGasInclusion(NewBlockGasPool(22_475, 50_000, 0), limits.Execution, limits.State, 0))
}

func TestFrameTransactionStateFailureIsFatal(t *testing.T) {
	evm, tx := frameTestEnv(t)
	sender := accounts.InternAddress(tx.SenderAddress)
	require.NoError(t, evm.IntraBlockState().SetCode(sender, []byte{0x60, 3, 0x5f, 0x5f, 0xaa}, tracing.CodeChangeUnspecified))
	tx.Frames = []types.Frame{{Mode: types.FrameVerify, Flags: types.FrameApproveBoth, Limits: mdgas.MdGas{Execution: 1_000}}, {Mode: types.FrameSender, Limits: mdgas.MdGas{Execution: 10_000}, Value: *uint256.NewInt(1)}}
	readErr := errors.New("state read failed")
	evm.Context.CanTransfer = func(evmtypes.IntraBlockState, accounts.Address, uint256.Int) (bool, error) { return false, readErr }
	_, err := applyFrameTest(t, evm, tx)
	require.ErrorIs(t, err, readErr)
	nonce, err := evm.IntraBlockState().GetNonce(sender)
	require.NoError(t, err)
	require.Zero(t, nonce)
}

func TestFrameTransactionCancellation(t *testing.T) {
	evm, tx := frameTestEnv(t)
	sender := accounts.InternAddress(tx.SenderAddress)
	require.NoError(t, evm.IntraBlockState().SetCode(sender, []byte{0x60, 3, 0x5f, 0x5f, 0xaa}, tracing.CodeChangeUnspecified))
	tx.Frames = []types.Frame{{Mode: types.FrameVerify, Flags: types.FrameApproveBoth, Limits: mdgas.MdGas{Execution: 1_000}}}
	evm.Cancel()
	_, err := applyFrameTest(t, evm, tx)
	require.Error(t, err)
}

func TestFrameTransactionFeeSettlementHook(t *testing.T) {
	evm, tx := frameTestEnv(t)
	sender := accounts.InternAddress(tx.SenderAddress)
	burn := common.Address{9}
	evm.ChainConfig().BurntContract = map[string]common.Address{"0": burn}
	evm.Context.BaseFee.SetUint64(1)
	tx.Fees = types.FrameFees{FeeCap: *uint256.NewInt(3), TipCap: *uint256.NewInt(1)}
	require.NoError(t, evm.IntraBlockState().SetCode(sender, []byte{0x60, 3, 0x5f, 0x5f, 0xaa}, tracing.CodeChangeUnspecified))
	tx.Frames = []types.Frame{{Mode: types.FrameVerify, Flags: types.FrameApproveBoth, Limits: mdgas.MdGas{Execution: 1_000}}}
	called := false
	evm.Context.PostApplyMessage = func(_ evmtypes.IntraBlockState, from, _ accounts.Address, _ *evmtypes.ExecutionResult, _ *chain.Rules) {
		called = true
		require.Equal(t, sender, from)
	}
	result, err := applyFrameTest(t, evm, tx)
	require.NoError(t, err)
	require.True(t, called)
	balance, err := evm.IntraBlockState().GetBalance(accounts.InternAddress(burn))
	require.NoError(t, err)
	require.Equal(t, result.ReceiptGasUsed, balance.Uint64())
	balance, err = evm.IntraBlockState().GetBalance(sender)
	require.NoError(t, err)
	require.Equal(t, 10_000_000-result.ReceiptGasUsed*2, balance.Uint64())
}

type frameStorageFailure struct {
	state.StateReader
	err error
}

func (r frameStorageFailure) ReadAccountStorage(accounts.Address, accounts.StorageKey) (uint256.Int, bool, error) {
	return uint256.Int{}, false, r.err
}

func TestFrameTransactionOpcodeStateFailureIsFatal(t *testing.T) {
	evm, tx := frameTestEnv(t)
	readErr := errors.New("storage read failed")
	ibs := state.New(frameStorageFailure{state.NewNoopReader(), readErr})
	defer ibs.Close()
	evm.ResetBetweenBlocks(evm.Context, evm.TxContext, ibs, evm.Config(), evm.ChainRules())
	sender := accounts.InternAddress(tx.SenderAddress)
	target := common.HexToAddress("0x2000")
	require.NoError(t, ibs.SetBalance(sender, *uint256.NewInt(10_000_000), tracing.BalanceChangeUnspecified))
	require.NoError(t, ibs.SetCode(sender, []byte{0x60, 3, 0x5f, 0x5f, 0xaa}, tracing.CodeChangeUnspecified))
	require.NoError(t, ibs.SetCode(accounts.InternAddress(target), []byte{0x5f, 0x54}, tracing.CodeChangeUnspecified))
	tx.Frames = []types.Frame{{Mode: types.FrameVerify, Flags: types.FrameApproveBoth, Limits: mdgas.MdGas{Execution: 1_000}}, {Mode: types.FrameSender, Target: &target, Limits: mdgas.MdGas{Execution: 10_000}}}
	_, err := applyFrameTest(t, evm, tx)
	require.ErrorIs(t, err, readErr)
}
