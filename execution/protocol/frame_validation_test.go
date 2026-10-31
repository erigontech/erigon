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
	"math"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/program"
)

func TestFrameValidationStopsAfterPayment(t *testing.T) {
	t.Parallel()
	evm, tx := frameTestEnv(t)
	ibs := evm.IntraBlockState()
	sender := accounts.InternAddress(tx.SenderAddress)
	require.NoError(t, ibs.SetCode(sender, []byte{0x60, 3, 0x5f, 0x5f, 0xaa}, tracing.CodeChangeUnspecified))
	tx.Fees.FeeCap.SetUint64(2)
	tx.Frames = []types.Frame{
		{Mode: types.FrameVerify, Flags: types.FrameApproveBoth, Limits: mdgas.MdGas{Execution: 1_000}},
		{Mode: types.FrameSender, Limits: mdgas.MdGas{Execution: 10_000}},
	}
	result, err := ValidateFrameTransaction(evm, tx)
	require.NoError(t, err)
	require.Equal(t, tx.SenderAddress, result.Payer)
	require.Equal(t, uint256.NewInt(tx.GetGasLimit()*2), &result.MaxCost)
	require.Equal(t, uint256.NewInt(10_000_000), &result.PayerBalance)
	nonce, err := ibs.GetNonce(sender)
	require.NoError(t, err)
	require.Zero(t, nonce, "validation must not commit approval")
}

func TestFrameValidationKeyedNonceDependency(t *testing.T) {
	evm, tx := frameTestEnv(t)
	ibs := evm.IntraBlockState()
	require.NoError(t, ibs.SetCode(accounts.InternAddress(tx.SenderAddress), program.New().Push(3).Push0().Push0().Op(vm.APPROVE).Bytes(), tracing.CodeChangeUnspecified))
	tx.NonceKeys = []uint256.Int{{1}}
	tx.Frames = []types.Frame{{Mode: types.FrameVerify, Flags: types.FrameApproveBoth, Limits: mdgas.MdGas{Execution: 1_000, State: 97_920}}}
	result, err := ValidateFrameTransaction(evm, tx)
	require.NoError(t, err)
	require.Contains(t, result.Dependencies, types.FrameNonceManager)
	sequence, err := ibs.GetState(accounts.InternAddress(types.FrameNonceManager), accounts.InternKey(types.FrameNonceSlot(tx.SenderAddress, tx.NonceKeys[0])))
	require.NoError(t, err)
	require.True(t, sequence.IsZero(), "validation must not consume the key")
}

func TestFrameValidationPolicy(t *testing.T) {
	t.Parallel()
	for name, code := range map[string][]byte{
		"timestamp":       program.New().Op(vm.TIMESTAMP, vm.POP).Bytes(),
		"gas observation": program.New().Op(vm.GAS, vm.POP).Bytes(),
		"empty helper":    program.New().DelegateCall(uint256.NewInt(1_000), common.HexToAddress("0x2000"), 0, 0, 0, 0).Op(vm.POP).Bytes(),
	} {
		t.Run(name, func(t *testing.T) {
			evm, tx := frameTestEnv(t)
			ibs := evm.IntraBlockState()
			sender := accounts.InternAddress(tx.SenderAddress)
			require.NoError(t, ibs.SetCode(sender, append(code, 0x60, 3, 0x5f, 0x5f, 0xaa), tracing.CodeChangeUnspecified))
			tx.Frames = []types.Frame{{Mode: types.FrameVerify, Flags: types.FrameApproveBoth, Limits: mdgas.MdGas{Execution: 10_000}}}
			_, err := ValidateFrameTransaction(evm, tx)
			require.Error(t, err)
		})
	}
}

func TestFrameValidationPrecompileCodeHash(t *testing.T) {
	for _, tc := range []struct {
		name    string
		balance uint64
	}{
		{"empty", 0},
		{"funded", 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			evm, tx := frameTestEnv(t)
			precompile := common.BytesToAddress([]byte{1})
			ibs := evm.IntraBlockState()
			require.NoError(t, ibs.SetBalance(accounts.InternAddress(precompile), *uint256.NewInt(tc.balance), tracing.BalanceChangeUnspecified))
			code := program.New().Push(precompile).Op(vm.EXTCODEHASH, vm.POP).
				Push(3).Push0().Push0().Op(vm.APPROVE).Bytes()
			require.NoError(t, ibs.SetCode(accounts.InternAddress(tx.SenderAddress), code, tracing.CodeChangeUnspecified))
			tx.Frames = []types.Frame{{Mode: types.FrameVerify, Flags: types.FrameApproveBoth, Limits: mdgas.MdGas{Execution: 10_000}}}
			_, err := ValidateFrameTransaction(evm, tx)
			require.ErrorContains(t, err, "opcode EXTCODEHASH is not allowed in frame validation")
			result, err := applyFrameTest(t, evm, tx)
			require.NoError(t, err)
			require.NoError(t, result.Err)
		})
	}
}

func TestFrameValidationCodeAccess(t *testing.T) {
	precompile := common.BytesToAddress([]byte{1})
	helper := common.HexToAddress("0x2000")
	for name, code := range map[string]*program.Program{
		"precompile call": program.New().StaticCall(uint256.NewInt(3_000), precompile, 0, 0, 0, 0).Op(vm.POP),
		"precompile size": program.New().Push(precompile).Op(vm.EXTCODESIZE, vm.POP),
		"precompile copy": program.New().ExtcodeCopy(precompile, 0, 0, 1),
		"contract hash":   program.New().Push(helper).Op(vm.EXTCODEHASH, vm.POP),
	} {
		t.Run(name, func(t *testing.T) {
			evm, tx := frameTestEnv(t)
			ibs := evm.IntraBlockState()
			require.NoError(t, ibs.SetCode(accounts.InternAddress(helper), []byte{byte(vm.STOP)}, tracing.CodeChangeUnspecified))
			code.Push(3).Push0().Push0().Op(vm.APPROVE)
			require.NoError(t, ibs.SetCode(accounts.InternAddress(tx.SenderAddress), code.Bytes(), tracing.CodeChangeUnspecified))
			tx.Frames = []types.Frame{{Mode: types.FrameVerify, Flags: types.FrameApproveBoth, Limits: mdgas.MdGas{Execution: 10_000}}}
			_, err := ValidateFrameTransaction(evm, tx)
			require.NoError(t, err)
		})
	}
}

func TestFrameValidationZeroDeadline(t *testing.T) {
	evm, tx := frameTestEnv(t)
	evm.Context.Time = 1
	ibs := evm.IntraBlockState()
	sender := accounts.InternAddress(tx.SenderAddress)
	require.NoError(t, ibs.SetCode(sender, []byte{0x60, 3, 0x5f, 0x5f, 0xaa}, tracing.CodeChangeUnspecified))
	require.NoError(t, ibs.SetCode(accounts.InternAddress(types.FrameExpiryVerifier), []byte{byte(vm.STOP)}, tracing.CodeChangeUnspecified))
	tx.Frames = []types.Frame{{Mode: types.FrameVerify, Target: &types.FrameExpiryVerifier, Data: make([]byte, 8), Limits: mdgas.MdGas{Execution: 5_000}}, {Mode: types.FrameVerify, Flags: types.FrameApproveBoth, Limits: mdgas.MdGas{Execution: 1_000}}}
	_, err := ValidateFrameTransaction(evm, tx)
	require.ErrorContains(t, err, "expired")
}

func TestFrameValidationExpiryHelper(t *testing.T) {
	evm, tx := frameTestEnv(t)
	ibs := evm.IntraBlockState()
	require.NoError(t, ibs.SetCode(accounts.InternAddress(types.FrameExpiryVerifier), frameExpiryCode, tracing.CodeChangeUnspecified))
	code := program.New().StaticCall(uint256.NewInt(2_000), types.FrameExpiryVerifier, 0, 8, 0, 0).Op(vm.POP).Push(3).Push(0).Push(0).Op(vm.APPROVE).Bytes()
	require.NoError(t, ibs.SetCode(accounts.InternAddress(tx.SenderAddress), code, tracing.CodeChangeUnspecified))
	tx.Frames = []types.Frame{{Mode: types.FrameVerify, Flags: types.FrameApproveBoth, Limits: mdgas.MdGas{Execution: 10_000}}}
	_, err := ValidateFrameTransaction(evm, tx)
	require.ErrorContains(t, err, "TIMESTAMP")
}

func TestFrameValidationDeploymentTransientWrite(t *testing.T) {
	evm, tx := frameTestEnv(t)
	code := []byte{0x60, 0x0a, 0xb0, 0x60, 12, 0x57, 0x60, 1, 0x60, 0, 0x5d, 0, 0x5b, 0x60, 3, 0x5f, 0x5f, 0xaa}
	require.NoError(t, evm.IntraBlockState().SetCode(accounts.InternAddress(tx.SenderAddress), code, tracing.CodeChangeUnspecified))
	tx.Frames = []types.Frame{
		{Mode: types.FrameDefault, Limits: mdgas.MdGas{Execution: 1_000}},
		{Mode: types.FrameVerify, Flags: types.FrameApproveBoth, Limits: mdgas.MdGas{Execution: 1_000}},
	}
	_, err := ValidateFrameTransaction(evm, tx)
	require.NoError(t, err)
}

func TestFrameValidationVerifyTransientWrite(t *testing.T) {
	evm, tx := frameTestEnv(t)
	code := program.New().Tstore(0, 1).Push(3).Push0().Push0().Op(vm.APPROVE).Bytes()
	require.NoError(t, evm.IntraBlockState().SetCode(accounts.InternAddress(tx.SenderAddress), code, tracing.CodeChangeUnspecified))
	tx.Frames = []types.Frame{{Mode: types.FrameVerify, Flags: types.FrameApproveBoth, Limits: mdgas.MdGas{Execution: 1_000}}}
	_, err := ValidateFrameTransaction(evm, tx)
	require.ErrorIs(t, err, vm.ErrWriteProtection)
}

func TestFrameValidationHelperStorage(t *testing.T) {
	for _, op := range []vm.OpCode{vm.SLOAD, vm.TLOAD} {
		t.Run(op.String(), func(t *testing.T) {
			evm, tx := frameTestEnv(t)
			helper := common.HexToAddress("0x2000")
			require.NoError(t, evm.IntraBlockState().SetCode(accounts.InternAddress(helper), program.New().Push0().Op(op, vm.POP, vm.STOP).Bytes(), tracing.CodeChangeUnspecified))
			code := program.New().StaticCall(uint256.NewInt(3_000), helper, 0, 0, 0, 0).Op(vm.POP).Push(3).Push0().Push0().Op(vm.APPROVE).Bytes()
			require.NoError(t, evm.IntraBlockState().SetCode(accounts.InternAddress(tx.SenderAddress), code, tracing.CodeChangeUnspecified))
			tx.Frames = []types.Frame{{Mode: types.FrameVerify, Flags: types.FrameApproveBoth, Limits: mdgas.MdGas{Execution: 10_000}}}
			_, err := ValidateFrameTransaction(evm, tx)
			if op == vm.SLOAD {
				require.ErrorContains(t, err, "SLOAD")
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestFrameValidationExpiryNeedsVerifyMode(t *testing.T) {
	evm, tx := frameTestEnv(t)
	evm.Context.Time = 1
	ibs := evm.IntraBlockState()
	require.NoError(t, ibs.SetCode(accounts.InternAddress(types.FrameExpiryVerifier), frameExpiryCode, tracing.CodeChangeUnspecified))
	require.NoError(t, ibs.SetCode(accounts.InternAddress(tx.SenderAddress), []byte{0x60, 3, 0x5f, 0x5f, 0xaa}, tracing.CodeChangeUnspecified))
	tx.Frames = []types.Frame{
		{Mode: types.FrameDefault, Target: &types.FrameExpiryVerifier, Data: make([]byte, 8), Limits: mdgas.MdGas{Execution: 5_000}},
		{Mode: types.FrameVerify, Flags: types.FrameApproveBoth, Limits: mdgas.MdGas{Execution: 1_000}},
	}
	_, err := ValidateFrameTransaction(evm, tx)
	require.ErrorContains(t, err, "TIMESTAMP")
}

func frameFactoryTestEnv(t *testing.T, creation vm.OpCode, endowment uint64) (*vm.EVM, *types.FrameTransaction, common.Address) {
	t.Helper()
	evm, tx := frameTestEnv(t)
	factory := common.HexToAddress("0x2000")
	runtime := program.New().Push(3).Push0().Push0().Op(vm.APPROVE).Bytes()
	initcode := program.New().ReturnData(runtime).Bytes()
	if creation == vm.CREATE {
		tx.SenderAddress = types.CreateAddress(factory, 1)
	} else {
		tx.SenderAddress = types.CreateAddress2(factory, [32]byte{}, accounts.InternCodeHash(crypto.Keccak256Hash(initcode)))
	}
	ibs := evm.IntraBlockState()
	from := accounts.InternAddress(factory)
	require.NoError(t, ibs.SetBalance(accounts.InternAddress(tx.SenderAddress), *uint256.NewInt(10_000_000), tracing.BalanceChangeUnspecified))
	require.NoError(t, ibs.SetNonce(from, 1, tracing.NonceChangeUnspecified))
	require.NoError(t, ibs.SetBalance(from, *uint256.NewInt(endowment), tracing.BalanceChangeUnspecified))
	code := program.New().Mstore(initcode, 0)
	if creation == vm.CREATE2 {
		code.Push0()
	}
	code.Push(len(initcode)).Push0().Push(endowment).Op(creation, vm.POP, vm.STOP)
	require.NoError(t, ibs.SetCode(from, code.Bytes(), tracing.CodeChangeUnspecified))
	tx.Frames = []types.Frame{
		{Mode: types.FrameDefault, Target: &factory, Limits: mdgas.MdGas{Execution: 80_000, State: 400_000}},
		{Mode: types.FrameVerify, Flags: types.FrameApproveBoth, Limits: mdgas.MdGas{Execution: 1_000}},
	}
	return evm, tx, factory
}

func TestFrameValidationDeploymentEndowment(t *testing.T) {
	for _, creation := range []vm.OpCode{vm.CREATE, vm.CREATE2} {
		t.Run(creation.String(), func(t *testing.T) {
			evm, tx, _ := frameFactoryTestEnv(t, creation, 1)
			_, err := ValidateFrameTransaction(evm, tx)
			require.ErrorContains(t, err, "value transfer")
			result, err := applyFrameTest(t, evm, tx)
			require.NoError(t, err)
			require.NoError(t, result.Err)
		})
	}
}

func TestFrameValidationCreationNonces(t *testing.T) {
	for _, creation := range []vm.OpCode{vm.CREATE, vm.CREATE2} {
		t.Run(creation.String(), func(t *testing.T) {
			evm, tx, factory := frameFactoryTestEnv(t, creation, 0)
			result, err := ValidateFrameTransaction(evm, tx)
			require.NoError(t, err)
			want := FrameCreationNonce{Address: factory, MinNonce: 1, MaxNonce: 1}
			if creation == vm.CREATE2 {
				want.MinNonce, want.MaxNonce = 0, math.MaxUint64-1
			}
			require.Equal(t, []FrameCreationNonce{want}, result.CreationNonces)
		})
	}
}

func TestFrameValidationCreationNonceOverflow(t *testing.T) {
	evm, tx, factory := frameFactoryTestEnv(t, vm.CREATE2, 0)
	ibs := evm.IntraBlockState()
	require.NoError(t, ibs.SetNonce(accounts.InternAddress(factory), math.MaxUint64, tracing.NonceChangeUnspecified))
	// A failed CREATE2 can be ignored when the sender code is already installed.
	require.NoError(t, ibs.SetCode(accounts.InternAddress(tx.SenderAddress), program.New().Push(3).Push0().Push0().Op(vm.APPROVE).Bytes(), tracing.CodeChangeUnspecified))
	result, err := ValidateFrameTransaction(evm, tx)
	require.NoError(t, err)
	require.Equal(t, []FrameCreationNonce{{Address: factory, MinNonce: math.MaxUint64, MaxNonce: math.MaxUint64}}, result.CreationNonces)
}
