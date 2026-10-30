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
		NonceKeys: []uint256.Int{{}},
		ChainID:   *cfg.ChainID, SenderAddress: sender,
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

func frameTestEnv(t testing.TB) (*vm.EVM, *types.FrameTransaction) {
	t.Helper()
	cfg := chain.AllProtocolChanges.Copy()
	cfg.BogotaTime = common.NewUint64(0)
	ibs := state.New(state.NewNoopReader())
	t.Cleanup(ibs.Close)
	evm := newTestEVM(ibs, cfg, 30_000_000)
	sender := common.HexToAddress("0x1000")
	require.NoError(t, ibs.SetBalance(accounts.InternAddress(sender), *uint256.NewInt(10_000_000), tracing.BalanceChangeUnspecified))
	return evm, &types.FrameTransaction{NonceKeys: []uint256.Int{{}}, ChainID: *cfg.ChainID, SenderAddress: sender}
}

func applyFrameTest(t testing.TB, evm *vm.EVM, tx *types.FrameTransaction) (*evmtypes.ExecutionResult, error) {
	t.Helper()
	msg, err := tx.AsMessage(types.Signer{}, &evm.Context.BaseFee, evm.ChainRules())
	require.NoError(t, err)
	return ApplyMessage(evm, msg, NewGasPool(30_000_000, 10_000_000), true, false, nil)
}

func TestFrameTransactionGasCostOverflow(t *testing.T) {
	evm, tx := frameTestEnv(t)
	tx.Frames = []types.Frame{{Mode: types.FrameVerify, Flags: types.FrameApproveBoth, Limits: mdgas.MdGas{Execution: 1_000}}}
	tx.Fees.FeeCap.SetAllOne()
	_, err := applyFrameTest(t, evm, tx)
	require.EqualError(t, err, "frame transaction gas cost overflow")
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

func TestFrameTransactionStaticCallSelfBalance(t *testing.T) {
	for _, tc := range []struct {
		name   string
		depth  int
		revert bool
	}{
		{name: "direct", depth: 1},
		{name: "nested", depth: 3},
		{name: "reverted approval", depth: 2, revert: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			evm, tx := frameTestEnv(t)
			tx.Fees.FeeCap.SetUint64(1)
			tx.Frames = []types.Frame{{Mode: types.FrameVerify, Flags: types.FrameApproveBoth, Limits: mdgas.MdGas{Execution: 50_000}}}
			// Calldata length selects the call depth and the initial approval.
			code := program.New().Op(vm.CALLDATASIZE).Push(tc.depth).Op(vm.EQ)
			approveJump := code.Size()
			code.Push(0).Op(vm.JUMPI, vm.SELFBALANCE, vm.POP).
				Push0().Push0().Op(vm.CALLDATASIZE).Push(1).Op(vm.ADD).Push0().Op(vm.ADDRESS, vm.GAS, vm.STATICCALL, vm.POP).
				Op(vm.SELFBALANCE).Push0().Op(vm.MSTORE).
				Op(vm.ADDRESS, vm.BALANCE).Push(32).Op(vm.MSTORE)
			if tc.revert {
				// Revert the inner approval, then approve again in the outermost call.
				code.Op(vm.CALLDATASIZE)
				revertJump := code.Size()
				code.Push(0).Op(vm.JUMPI).Push(types.FrameApproveBoth).Push(64).Push0().Op(vm.APPROVE)
				_, revertPC := code.Jumpdest()
				require.Less(t, revertPC, uint64(256))
				code.Bytes()[revertJump+1] = byte(revertPC)
				code.Push(64).Push0().Op(vm.REVERT)
			} else {
				code.Return(0, 64)
			}
			_, approvePC := code.Jumpdest()
			require.Less(t, approvePC, uint64(256))
			code.Bytes()[approveJump+1] = byte(approvePC)
			code.Push(types.FrameApproveBoth).Push0().Push0().Op(vm.APPROVE)
			sender := accounts.InternAddress(tx.SenderAddress)
			require.NoError(t, evm.IntraBlockState().SetCode(sender, code.Bytes(), tracing.CodeChangeUnspecified))
			observations := 0
			config := evm.Config()
			config.Tracer = &tracing.Hooks{OnExitV2: func(depth int, output []byte, _ mdgas.MdGasUsage, err error, reverted bool) {
				if len(output) != 64 {
					return
				}
				observations++
				want := uint64(10_000_000) - tx.GetGasLimit()
				if tc.revert && depth == 1 {
					want = 10_000_000
				}
				if tc.revert && depth == 2 {
					require.ErrorIs(t, err, vm.ErrExecutionReverted)
					require.True(t, reverted)
				} else {
					require.NoError(t, err)
				}
				require.Equal(t, uint256.NewInt(want), new(uint256.Int).SetBytes(output[32:]), "BALANCE at depth %d", depth)
				require.Equal(t, uint256.NewInt(want), new(uint256.Int).SetBytes(output[:32]), "SELFBALANCE at depth %d", depth)
			}}
			evm.ResetBetweenBlocks(evm.Context, evm.TxContext, evm.IntraBlockState(), config, evm.ChainRules())
			result, err := applyFrameTest(t, evm, tx)
			require.NoError(t, err)
			require.NoError(t, result.Err)
			require.Equal(t, tc.depth, observations)
			nonce, err := evm.IntraBlockState().GetNonce(sender)
			require.NoError(t, err)
			require.Equal(t, uint64(1), nonce)
		})
	}
}

func TestFrameTransactionKeyedNonce(t *testing.T) {
	evm, tx := frameTestEnv(t)
	ibs := evm.IntraBlockState()
	sender := accounts.InternAddress(tx.SenderAddress)
	manager := accounts.InternAddress(common.HexToAddress("0x8250968C12e01A19d6F667b9B2F3b3A4d0e51cB7"))
	slot := accounts.InternKey(crypto.Keccak256Hash(append(common.LeftPadBytes(tx.SenderAddress[:], 32), common.LeftPadBytes([]byte{1}, 32)...)))
	require.NoError(t, ibs.SetNonce(sender, 9, tracing.NonceChangeUnspecified))
	require.NoError(t, ibs.SetCode(sender, program.New().Push(3).Push0().Push0().Op(vm.APPROVE).Bytes(), tracing.CodeChangeUnspecified))
	require.NoError(t, ibs.SetNonce(manager, 1, tracing.NonceChangeUnspecified))
	require.NoError(t, ibs.SetCode(manager, common.FromHex("60006000fd"), tracing.CodeChangeUnspecified))
	tx.NonceKeys = []uint256.Int{{1}}
	tx.Frames = []types.Frame{{Mode: types.FrameVerify, Flags: types.FrameApproveBoth, Limits: mdgas.MdGas{Execution: 1_000, State: params.StateGasPerStorageSet}}}
	result, err := applyFrameTest(t, evm, tx)
	require.NoError(t, err)
	require.NoError(t, result.Err)
	nonce, err := ibs.GetNonce(sender)
	require.NoError(t, err)
	require.EqualValues(t, 9, nonce)
	sequence, err := ibs.GetState(manager, slot)
	require.NoError(t, err)
	require.Equal(t, uint256.Int{1}, sequence)
	require.EqualValues(t, params.StateGasPerStorageSet, result.BlockStateGasUsed)
	require.EqualValues(t, params.StateGasPerStorageSet, result.FrameReceipts[0].GasUsed.State)
	require.False(t, ibs.AddressInAccessList(manager))
	_, warm := ibs.SlotInAccessList(manager, slot)
	require.False(t, warm)
}

func TestFrameTransactionKeyedNonceRefill(t *testing.T) {
	for _, revert := range []bool{false, true} {
		t.Run(fmt.Sprintf("revert=%t", revert), func(t *testing.T) {
			evm, tx := frameTestEnv(t)
			ibs := evm.IntraBlockState()
			tx.NonceKeys = []uint256.Int{{1}}
			manager := accounts.InternAddress(types.FrameNonceManager)
			slot := types.FrameNonceSlot(tx.SenderAddress, tx.NonceKeys[0])
			require.NoError(t, ibs.SetCode(accounts.InternAddress(tx.SenderAddress), program.New().Push(types.FrameApproveBoth).Push0().Push0().Op(vm.APPROVE).Bytes(), tracing.CodeChangeUnspecified))
			// Non-canonical manager code can clear a slot created by protocol bookkeeping.
			code := program.New().Sstore(slot, 0)
			if revert {
				code.Push0().Push0().Op(vm.REVERT)
			}
			require.NoError(t, ibs.SetCode(manager, code.Bytes(), tracing.CodeChangeUnspecified))
			tx.Frames = []types.Frame{
				{Mode: types.FrameVerify, Flags: types.FrameApproveBoth, Limits: mdgas.MdGas{Execution: 1_000, State: params.StateGasPerStorageSet}},
				{Mode: types.FrameSender, Target: &types.FrameNonceManager, Limits: mdgas.MdGas{Execution: 20_000}},
			}
			result, err := applyFrameTest(t, evm, tx)
			require.NoError(t, err)
			var wantStateGas uint64
			var wantSequence uint256.Int
			if revert {
				require.ErrorIs(t, result.Err, vm.ErrExecutionReverted)
				wantStateGas = params.StateGasPerStorageSet
				wantSequence.SetUint64(1)
			} else {
				require.NoError(t, result.Err)
			}
			require.Equal(t, wantStateGas, result.FrameReceipts[0].GasUsed.State)
			require.Zero(t, result.FrameReceipts[1].GasUsed.State)
			require.Equal(t, wantStateGas, result.BlockStateGasUsed)
			sequence, err := ibs.GetState(manager, accounts.InternKey(slot))
			require.NoError(t, err)
			require.Equal(t, wantSequence, sequence)
		})
	}
}

func TestFrameTransactionRPCNonceCheck(t *testing.T) {
	for _, keyed := range []bool{false, true} {
		for _, checkNonce := range []bool{false, true} {
			t.Run(fmt.Sprintf("keyed=%t/checkNonce=%t", keyed, checkNonce), func(t *testing.T) {
				evm, tx := frameTestEnv(t)
				ibs := evm.IntraBlockState()
				sender := accounts.InternAddress(tx.SenderAddress)
				require.NoError(t, ibs.SetCode(sender, program.New().Push(3).Push0().Push0().Op(vm.APPROVE).Bytes(), tracing.CodeChangeUnspecified))
				require.NoError(t, ibs.SetNonce(sender, 5, tracing.NonceChangeUnspecified))
				if keyed {
					tx.NonceKeys = []uint256.Int{{1}}
					manager := accounts.InternAddress(types.FrameNonceManager)
					slot := accounts.InternKey(types.FrameNonceSlot(tx.SenderAddress, tx.NonceKeys[0]))
					require.NoError(t, ibs.SetState(manager, slot, uint256.Int{5}))
				}
				tx.Nonce = 3
				tx.Frames = []types.Frame{{Mode: types.FrameVerify, Flags: types.FrameApproveBoth, Limits: mdgas.MdGas{Execution: 1_000}}}
				msg, err := tx.AsMessage(types.Signer{}, &evm.Context.BaseFee, evm.ChainRules())
				require.NoError(t, err)
				msg.SetCheckNonce(checkNonce)
				result, err := ApplyMessage(evm, msg, NewGasPool(30_000_000, 0), true, false, nil)
				if checkNonce {
					require.ErrorIs(t, err, ErrNonceTooLow)
					return
				}
				require.NoError(t, err)
				require.NoError(t, result.Err)
			})
		}
	}
}

func TestFrameTransactionRPCFeeChecks(t *testing.T) {
	for _, test := range []struct {
		name      string
		noBaseFee bool
		free      bool
		feeCap    uint64
		wantError bool
	}{
		{name: "normal validation", wantError: true},
		{name: "unpriced call", noBaseFee: true},
		{name: "priced call", noBaseFee: true, feeCap: 1, wantError: true},
		{name: "service transaction", free: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			evm, tx := frameTestEnv(t)
			ibs := evm.IntraBlockState()
			sender := accounts.InternAddress(tx.SenderAddress)
			require.NoError(t, ibs.SetCode(sender, program.New().Push(3).Push0().Push0().Op(vm.APPROVE).Bytes(), tracing.CodeChangeUnspecified))
			tx.Frames = []types.Frame{{Mode: types.FrameVerify, Flags: types.FrameApproveBoth, Limits: mdgas.MdGas{Execution: 1_000}}}
			tx.Fees.FeeCap.SetUint64(test.feeCap)
			blockCtx, config := evm.Context, evm.Config()
			blockCtx.BaseFee.SetUint64(100)
			config.NoBaseFee = test.noBaseFee
			evm.ResetBetweenBlocks(blockCtx, evm.TxContext, ibs, config, evm.ChainRules())
			burn := common.HexToAddress("0xb000")
			if test.free {
				evm.ChainConfig().BurntContract = map[string]common.Address{"0": burn}
			}
			msg, err := tx.AsMessage(types.Signer{}, &evm.Context.BaseFee, evm.ChainRules())
			require.NoError(t, err)
			msg.SetIsFree(test.free)
			result, err := ApplyMessage(evm, msg, NewGasPool(30_000_000, 0), true, false, nil)
			if test.wantError {
				require.ErrorIs(t, err, ErrFeeCapTooLow)
				return
			}
			require.NoError(t, err)
			require.NoError(t, result.Err)
			require.True(t, msg.GasPrice().IsZero())
			if test.free {
				require.True(t, result.BurntContractAddress.IsNil())
			}
			balance, err := ibs.GetBalance(sender)
			require.NoError(t, err)
			require.Equal(t, uint256.Int{10_000_000}, balance)
			balance, err = ibs.GetBalance(accounts.InternAddress(burn))
			require.NoError(t, err)
			require.True(t, balance.IsZero())
		})
	}
}

func TestFrameTransactionRPCGasBailout(t *testing.T) {
	for _, bailout := range []bool{false, true} {
		t.Run(fmt.Sprint(bailout), func(t *testing.T) {
			evm, tx := frameTestEnv(t)
			ibs := evm.IntraBlockState()
			sender := accounts.InternAddress(tx.SenderAddress)
			payer := common.HexToAddress("0x2000")
			payerAddress := accounts.InternAddress(payer)
			require.NoError(t, ibs.SetCode(sender, program.New().Push(types.FrameApproveExecution).Push0().Push0().Op(vm.APPROVE).Bytes(), tracing.CodeChangeUnspecified))
			require.NoError(t, ibs.SetCode(payerAddress, program.New().Push(types.FrameApprovePayment).Push0().Push0().Op(vm.APPROVE).Bytes(), tracing.CodeChangeUnspecified))
			tx.Fees.FeeCap.SetUint64(1)
			tx.Frames = []types.Frame{
				{Mode: types.FrameVerify, Flags: types.FrameApproveExecution, Limits: mdgas.MdGas{Execution: 1_000}},
				{Mode: types.FrameVerify, Flags: types.FrameApprovePayment, Target: &payer, Limits: mdgas.MdGas{Execution: 5_000}},
			}
			msg, err := tx.AsMessage(types.Signer{}, &evm.Context.BaseFee, evm.ChainRules())
			require.NoError(t, err)
			result, err := ApplyMessage(evm, msg, NewGasPool(30_000_000, 0), true, bailout, nil)
			if !bailout {
				require.ErrorIs(t, err, vm.ErrExecutionReverted)
				return
			}
			require.NoError(t, err)
			require.NoError(t, result.Err)
			require.Equal(t, &payer, result.Payer)
			balance, err := ibs.GetBalance(payerAddress)
			require.NoError(t, err)
			require.True(t, balance.IsZero(), "gas bailout must neither debit nor refund the payer")
		})
	}
}

func TestFrameTransactionRPCValueBailout(t *testing.T) {
	for _, balance := range []uint64{0, 20} {
		for _, bailout := range []bool{false, true} {
			t.Run(fmt.Sprintf("balance=%d/bailout=%t", balance, bailout), func(t *testing.T) {
				evm, tx := frameTestEnv(t)
				ibs := evm.IntraBlockState()
				sender := accounts.InternAddress(tx.SenderAddress)
				target := common.HexToAddress("0x2000")
				require.NoError(t, ibs.SetBalance(sender, uint256.Int{balance}, tracing.BalanceChangeUnspecified))
				require.NoError(t, ibs.SetBalance(accounts.InternAddress(target), uint256.Int{1}, tracing.BalanceChangeUnspecified))
				require.NoError(t, ibs.SetCode(sender, program.New().Push(types.FrameApproveBoth).Push0().Push0().Op(vm.APPROVE).Bytes(), tracing.CodeChangeUnspecified))
				tx.Frames = []types.Frame{
					{Mode: types.FrameVerify, Flags: types.FrameApproveBoth, Limits: mdgas.MdGas{Execution: 1_000}},
					{Mode: types.FrameSender, Target: &target, Value: uint256.Int{10}, Limits: mdgas.MdGas{Execution: 10_000}},
				}
				msg, err := tx.AsMessage(types.Signer{}, nil, evm.ChainRules())
				require.NoError(t, err)
				result, err := ApplyMessage(evm, msg, NewGasPool(30_000_000, 0), true, bailout, nil)
				require.NoError(t, err)
				if balance < 10 && !bailout {
					require.ErrorIs(t, result.Err, vm.ErrExecutionReverted)
					return
				}
				require.NoError(t, result.Err)
				got, err := ibs.GetBalance(accounts.InternAddress(target))
				require.NoError(t, err)
				require.Equal(t, uint256.Int{11}, got)
				got, err = ibs.GetBalance(sender)
				require.NoError(t, err)
				want := uint64(0)
				if balance >= 10 {
					want = balance - 10
				}
				require.Equal(t, uint256.Int{want}, got)
			})
		}
	}
}

func TestFrameTransactionRPCBailoutRefund(t *testing.T) {
	for _, bailout := range []bool{false, true} {
		t.Run(fmt.Sprint(bailout), func(t *testing.T) {
			evm, tx := frameTestEnv(t)
			ibs := evm.IntraBlockState()
			target := common.HexToAddress("0x2000")
			require.NoError(t, ibs.SetCode(accounts.InternAddress(tx.SenderAddress), program.New().Push(types.FrameApproveBoth).Push0().Push0().Op(vm.APPROVE).Bytes(), tracing.CodeChangeUnspecified))
			require.NoError(t, ibs.SetCode(accounts.InternAddress(target), program.New().Sstore(0, 1).Sstore(0, 0).Op(vm.STOP).Bytes(), tracing.CodeChangeUnspecified))
			tx.Frames = []types.Frame{
				{Mode: types.FrameVerify, Flags: types.FrameApproveBoth, Limits: mdgas.MdGas{Execution: 1_000}},
				{Mode: types.FrameSender, Target: &target, Limits: mdgas.MdGas{Execution: 100_000, State: params.StateGasPerStorageSet}},
			}
			msg, err := tx.AsMessage(types.Signer{}, nil, evm.ChainRules())
			require.NoError(t, err)
			result, err := ApplyMessage(evm, msg, NewGasPool(30_000_000, 0), true, bailout, nil)
			require.NoError(t, err)
			require.NoError(t, result.Err)
			require.NotZero(t, ibs.GetRefund())
			if bailout {
				require.Zero(t, result.GasRefund)
			} else {
				require.NotZero(t, result.GasRefund)
			}
		})
	}
}

func TestFrameTransactionRPCBlobFeeCheck(t *testing.T) {
	for _, test := range []struct {
		name      string
		noBaseFee bool
		blobCap   uint64
		wantError bool
	}{
		{name: "normal validation", wantError: true},
		{name: "unpriced blobs", noBaseFee: true},
		{name: "priced blobs", noBaseFee: true, blobCap: 1, wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			evm, tx := frameTestEnv(t)
			ibs := evm.IntraBlockState()
			require.NoError(t, ibs.SetCode(accounts.InternAddress(tx.SenderAddress), program.New().Push(types.FrameApproveBoth).Push0().Push0().Op(vm.APPROVE).Bytes(), tracing.CodeChangeUnspecified))
			tx.Frames = []types.Frame{{Mode: types.FrameVerify, Flags: types.FrameApproveBoth, Limits: mdgas.MdGas{Execution: 1_000}}}
			tx.BlobVersionedHashes = []common.Hash{{1}}
			tx.Fees.BlobFeeCap.SetUint64(test.blobCap)
			blockCtx, config := evm.Context, evm.Config()
			blockCtx.BlobBaseFee.SetUint64(2)
			config.NoBaseFee = test.noBaseFee
			evm.ResetBetweenBlocks(blockCtx, evm.TxContext, ibs, config, evm.ChainRules())
			result, err := applyFrameTest(t, evm, tx)
			if test.wantError {
				require.ErrorIs(t, err, ErrMaxFeePerBlobGas)
				return
			}
			require.NoError(t, err)
			require.NoError(t, result.Err)
		})
	}
}

func TestFrameTransactionTraceGas(t *testing.T) {
	evm, tx := frameTestEnv(t)
	ibs := evm.IntraBlockState()
	target := common.HexToAddress("0x2000")
	require.NoError(t, ibs.SetCode(accounts.InternAddress(tx.SenderAddress), program.New().Push(types.FrameApproveBoth).Push0().Push0().Op(vm.APPROVE).Bytes(), tracing.CodeChangeUnspecified))
	require.NoError(t, ibs.SetCode(accounts.InternAddress(target), program.New().Sstore(0, 1).Op(vm.STOP).Bytes(), tracing.CodeChangeUnspecified))
	tx.Frames = []types.Frame{
		{Mode: types.FrameVerify, Flags: types.FrameApproveBoth, Limits: mdgas.MdGas{Execution: 1_000}},
		{Mode: types.FrameSender, Target: &target, Limits: mdgas.MdGas{Execution: 100_000, State: params.StateGasPerStorageSet}},
	}
	var rootGas mdgas.MdGasUsage
	config := evm.Config()
	config.Tracer = &tracing.Hooks{OnExitV2: func(depth int, _ []byte, gas mdgas.MdGasUsage, _ error, _ bool) {
		if depth == 0 {
			rootGas = gas
		}
	}}
	evm.ResetBetweenBlocks(evm.Context, evm.TxContext, ibs, config, evm.ChainRules())
	result, err := applyFrameTest(t, evm, tx)
	require.NoError(t, err)
	require.NoError(t, result.Err)
	require.NotZero(t, result.BlockStateGasUsed)
	require.Equal(t, result.ReceiptGasUsed, rootGas.Total())
	require.EqualValues(t, result.BlockStateGasUsed, rootGas.State)
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

func TestFrameTransactionSameFrameRefill(t *testing.T) {
	t.Parallel()
	for _, traced := range []bool{false, true} {
		t.Run(fmt.Sprint(traced), func(t *testing.T) {
			evm, tx := frameTestEnv(t)
			ibs := evm.IntraBlockState()
			target := common.HexToAddress("0x2000")
			addr := accounts.InternAddress(target)
			require.NoError(t, ibs.SetCode(accounts.InternAddress(tx.SenderAddress), program.New().Push(types.FrameApproveBoth).Push0().Push0().Op(vm.APPROVE).Bytes(), tracing.CodeChangeUnspecified))
			require.NoError(t, ibs.SetCode(addr, program.New().Sstore(0, 1).Sstore(0, 0).Sstore(1, 1).Op(vm.STOP).Bytes(), tracing.CodeChangeUnspecified))
			tx.Frames = []types.Frame{
				{Mode: types.FrameVerify, Flags: types.FrameApproveBoth, Limits: mdgas.MdGas{Execution: 1_000}},
				{Mode: types.FrameSender, Target: &target, Limits: mdgas.MdGas{Execution: 200_000, State: params.StateGasPerStorageSet}},
			}
			var stateCosts []int64
			if traced {
				config := evm.Config()
				config.Tracer = &tracing.Hooks{OnOpcodeV2: func(_ uint64, op byte, _ mdgas.MdGas, cost mdgas.MdGasCost, _ tracing.OpContext, _ []byte, _ int, _ error) {
					if op == byte(vm.SSTORE) {
						stateCosts = append(stateCosts, cost.State)
					}
				}}
				evm.ResetBetweenBlocks(evm.Context, evm.TxContext, ibs, config, evm.ChainRules())
			}
			result, err := applyFrameTest(t, evm, tx)
			require.NoError(t, err)
			require.NoError(t, result.Err)
			require.EqualValues(t, params.StateGasPerStorageSet, result.FrameReceipts[1].GasUsed.State)
			require.EqualValues(t, params.StateGasPerStorageSet, result.BlockStateGasUsed)
			for key, want := range []uint256.Int{{}, {1}} {
				stored, err := ibs.GetState(addr, accounts.InternKey(common.Hash{31: byte(key)}))
				require.NoError(t, err)
				require.Equal(t, want, stored)
			}
			if traced {
				require.Equal(t, []int64{params.StateGasPerStorageSet, -params.StateGasPerStorageSet, params.StateGasPerStorageSet}, stateCosts)
			}
		})
	}
}

func TestFrameTransactionCrossFrameRefill(t *testing.T) {
	t.Parallel()
	for _, failure := range []string{"none", "revert", "out of gas"} {
		t.Run(failure, func(t *testing.T) {
			evm, tx := frameTestEnv(t)
			ibs := evm.IntraBlockState()
			sender := accounts.InternAddress(tx.SenderAddress)
			target := common.HexToAddress("0x2000")
			addr := accounts.InternAddress(target)
			require.NoError(t, ibs.SetCode(sender, []byte{0x60, 3, 0x5f, 0x5f, 0xaa}, tracing.CodeChangeUnspecified))
			code := []byte{0x5f, 0x35, 0x5f, 0x55, 0x60, 32, 0x35, 0x60, 11, 0x57, 0, 0x5b}
			wantErr := vm.ErrExecutionReverted
			if failure == "out of gas" {
				code = append(code, program.New().Sstore(1, 1).Bytes()...)
				wantErr = vm.ErrOutOfGas
			} else {
				code = append(code, byte(vm.PUSH0), byte(vm.PUSH0), byte(vm.REVERT))
			}
			require.NoError(t, ibs.SetCode(addr, code, tracing.CodeChangeUnspecified))
			var clearData []byte
			if failure != "none" {
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
			if failure != "none" {
				require.ErrorIs(t, result.Err, wantErr)
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
	receipt := MakeReceipt(uint256.NewInt(1), common.Hash{}, tx, result.ReceiptGasUsed, result, evm.IntraBlockState(), evm)
	require.Equal(t, result.Payer, receipt.Payer)
	require.Equal(t, result.FrameReceipts, receipt.FrameReceipts)
	require.Zero(t, receipt.ContractAddress)
}

func TestFrameBlockGasLimits(t *testing.T) {
	tx := &types.FrameTransaction{NonceKeys: []uint256.Int{{}}, Frames: []types.Frame{{Limits: mdgas.MdGas{Execution: 10_000, State: 50_000}}}}
	limits, err := TransactionGasLimits(tx, true)
	require.NoError(t, err)
	require.Equal(t, mdgas.MdGas{Execution: 22_523, State: 50_000}, limits)
	require.NoError(t, CheckBlockGasInclusion(NewBlockGasPool(22_523, 50_000, 0), limits.Execution, limits.State, 0))
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

func TestFrameTransactionFeeSettlement(t *testing.T) {
	t.Parallel()
	// Three frames and the encoded nonce cost 12,000 + 3*475 + 3*16 = 13,473
	// intrinsic gas. The approvals use 107 and 3,007 gas; each Sstore emits two PUSH1s.
	for _, test := range []struct {
		name                       string
		code                       []byte
		dataLen                    int
		blob                       bool
		wantBlockGas               mdgas.MdGas
		wantReceiptGas, wantRefund uint64
		wantPayerBalance           uint64
		wantCoinbaseBalance        uint64
		wantBurnBalance            uint64
		wantBlobGas                uint64
	}{
		{
			name: "unused budgets", code: program.New().Op(vm.STOP).Bytes(),
			wantBlockGas: mdgas.MdGas{Execution: 19_587}, wantReceiptGas: 19_587,
			wantPayerBalance: 9_902_065, wantCoinbaseBalance: 58_761, wantBurnBalance: 39_174,
		},
		{
			name: "capped storage refund", code: program.New().Sstore(0, 1).Sstore(0, 0).Op(vm.STOP).Bytes(),
			wantBlockGas: mdgas.MdGas{Execution: 31_799}, wantReceiptGas: 25_440, wantRefund: 6_359,
			wantPayerBalance: 9_872_800, wantCoinbaseBalance: 76_320, wantBurnBalance: 50_880,
		},
		{
			name: "calldata floor plus state gas", dataLen: 512,
			code:         program.New().Sstore(0, 1).Sstore(0, 0).Sstore(1, 1).Op(vm.STOP).Bytes(),
			wantBlockGas: mdgas.MdGas{Execution: 46_385, State: 97_920}, wantReceiptGas: 144_305, wantRefund: 10_000,
			wantPayerBalance: 9_278_475, wantCoinbaseBalance: 432_915, wantBurnBalance: 288_610,
		},
		{
			name: "blob fee survives storage refund", blob: true,
			code:         program.New().Sstore(0, 1).Sstore(0, 0).Op(vm.STOP).Bytes(),
			wantBlockGas: mdgas.MdGas{Execution: 31_799}, wantReceiptGas: 25_440, wantRefund: 6_359,
			wantPayerBalance: 8_955_296, wantCoinbaseBalance: 76_320, wantBurnBalance: 50_880, wantBlobGas: 131_072,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			evm, tx := frameTestEnv(t)
			ibs := evm.IntraBlockState()
			sender := accounts.InternAddress(tx.SenderAddress)
			payer, target := common.HexToAddress("0x2000"), common.HexToAddress("0x3000")
			coinbase, burn := common.HexToAddress("0x4000"), common.HexToAddress("0x5000")
			require.NoError(t, ibs.SetBalance(accounts.InternAddress(payer), uint256.Int{10_000_000}, tracing.BalanceChangeUnspecified))
			require.NoError(t, ibs.SetCode(sender, program.New().Push(types.FrameApproveExecution).Push0().Push0().Op(vm.APPROVE).Bytes(), tracing.CodeChangeUnspecified))
			require.NoError(t, ibs.SetCode(accounts.InternAddress(payer), program.New().Push(types.FrameApprovePayment).Push0().Push0().Op(vm.APPROVE).Bytes(), tracing.CodeChangeUnspecified))
			require.NoError(t, ibs.SetCode(accounts.InternAddress(target), test.code, tracing.CodeChangeUnspecified))
			evm.ChainConfig().BurntContract = map[string]common.Address{"0": burn}
			blockCtx, config := evm.Context, evm.Config()
			blockCtx.BaseFee.SetUint64(2)
			blockCtx.BlobBaseFee.SetUint64(7)
			blockCtx.Coinbase = accounts.InternAddress(coinbase)
			config.NoBaseFee = false
			evm.ResetBetweenBlocks(blockCtx, evm.TxContext, ibs, config, evm.ChainRules())
			tx.Fees = types.FrameFees{FeeCap: uint256.Int{11}, TipCap: uint256.Int{3}}
			tx.Frames = []types.Frame{
				{Mode: types.FrameVerify, Flags: types.FrameApproveExecution, Limits: mdgas.MdGas{Execution: 1_000}},
				{Mode: types.FrameVerify, Flags: types.FrameApprovePayment, Target: &payer, Limits: mdgas.MdGas{Execution: 5_000}},
				{Mode: types.FrameSender, Target: &target, Data: make([]byte, test.dataLen), Limits: mdgas.MdGas{Execution: 100_000, State: 200_000}},
			}
			if test.blob {
				tx.BlobVersionedHashes = []common.Hash{{1}}
				tx.Fees.BlobFeeCap.SetUint64(13)
			}
			msg, err := tx.AsMessage(types.Signer{}, &evm.Context.BaseFee, evm.ChainRules())
			require.NoError(t, err)
			const blockGas, blockBlobGas = 30_000_000, 262_144
			gp := NewGasPool(blockGas, blockBlobGas)
			result, err := ApplyMessage(evm, msg, gp, true, false, nil)
			require.NoError(t, err)
			require.NoError(t, result.Err)
			require.Equal(t, &payer, result.Payer)
			require.Equal(t, test.wantReceiptGas, result.ReceiptGasUsed)
			require.Equal(t, test.wantRefund, result.GasRefund)
			require.Equal(t, test.wantBlockGas.Execution, result.BlockExecutionGasUsed)
			require.Equal(t, test.wantBlockGas.State, result.BlockStateGasUsed)
			require.Equal(t, test.wantBlockGas.Total(), result.MaxGasUsed)
			require.Equal(t, blockGas-test.wantBlockGas.Execution, gp.ExecutionGasAvailable())
			require.Equal(t, blockGas-test.wantBlockGas.State, gp.StateGasAvailable())
			require.Equal(t, blockBlobGas-test.wantBlobGas, gp.BlobGas())
			for _, account := range []struct {
				address common.Address
				balance uint64
			}{
				{tx.SenderAddress, 10_000_000},
				{payer, test.wantPayerBalance},
				{coinbase, test.wantCoinbaseBalance},
				{burn, test.wantBurnBalance},
			} {
				balance, err := ibs.GetBalance(accounts.InternAddress(account.address))
				require.NoError(t, err)
				require.Equal(t, uint256.Int{account.balance}, balance, "balance of %s", account.address)
			}
		})
	}
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
