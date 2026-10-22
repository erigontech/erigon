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
	require.ErrorContains(t, err, "TSTORE")
}

func TestFrameValidationExpiryNeedsVerifyMode(t *testing.T) {
	evm, tx := frameTestEnv(t)
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
