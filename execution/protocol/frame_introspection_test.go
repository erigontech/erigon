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
	"bytes"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/program"
)

func TestFrameIntrospection(t *testing.T) {
	t.Parallel()
	txparam := vm.OpCode(0xb0)
	frameparam := vm.OpCode(0xb3)
	sigparam := vm.OpCode(0xb4)
	word := func(p *program.Program) []byte { return p.Push0().Op(vm.MSTORE).Return(0, 32).Bytes() }
	for name, test := range map[string]struct {
		code  []byte
		value uint256.Int
	}{
		"tx type":           {word(program.New().Push(0).Op(txparam)), *uint256.NewInt(6)},
		"nonce":             {word(program.New().Push(1).Op(txparam)), *uint256.NewInt(7)},
		"sender":            {word(program.New().Push(2).Op(txparam)), *uint256.NewInt(0x1000)},
		"priority fee":      {word(program.New().Push(3).Op(txparam)), *uint256.NewInt(1)},
		"fee cap":           {word(program.New().Push(4).Op(txparam)), *uint256.NewInt(2)},
		"blob fee":          {word(program.New().Push(5).Op(txparam)), *uint256.NewInt(3)},
		"max cost":          {word(program.New().Push(6).Op(txparam)), *uint256.NewInt(777)},
		"blobs":             {word(program.New().Push(7).Op(txparam)), *uint256.NewInt(1)},
		"frames":            {word(program.New().Push(9).Op(txparam)), *uint256.NewInt(2)},
		"current frame":     {word(program.New().Push(10).Op(txparam)), *uint256.NewInt(1)},
		"signatures":        {word(program.New().Push(11).Op(txparam)), *uint256.NewInt(2)},
		"state pool":        {word(program.New().Push(12).Op(txparam)), *uint256.NewInt(888)},
		"legacy nonce":      {word(program.New().Push(13).Op(txparam)), uint256.Int{}},
		"nonce key count":   {word(program.New().Push(14).Op(txparam)), *uint256.NewInt(2)},
		"nonce keys hash":   {word(program.New().Push(15).Op(txparam)), *new(uint256.Int).SetBytes(crypto.Keccak256(common.FromHex("000000000000000000000000000000000000000000000000000000000000000200000000000000000000000000000000000000000000000000000000000000010000000000000000000000000000000000000000000000000000000000000002")))},
		"first nonce key":   {word(program.New().Push(16).Op(txparam)), *uint256.NewInt(1)},
		"target":            {word(program.New().Push(0).Push(0).Op(frameparam)), *uint256.NewInt(0x1000)},
		"execution limit":   {word(program.New().Push(1).Push(0).Op(frameparam)), *uint256.NewInt(55)},
		"mode":              {word(program.New().Push(2).Push(0).Op(frameparam)), *uint256.NewInt(2)},
		"flags":             {word(program.New().Push(3).Push(0).Op(frameparam)), *uint256.NewInt(1)},
		"data length":       {word(program.New().Push(4).Push(0).Op(frameparam)), *uint256.NewInt(2)},
		"status":            {word(program.New().Push(5).Push(0).Op(frameparam)), *uint256.NewInt(1)},
		"scope":             {word(program.New().Push(6).Push(0).Op(frameparam)), *uint256.NewInt(1)},
		"atomic":            {word(program.New().Push(7).Push(0).Op(frameparam)), uint256.Int{}},
		"value":             {word(program.New().Push(8).Push(0).Op(frameparam)), *uint256.NewInt(9)},
		"state limit":       {word(program.New().Push(9).Push(0).Op(frameparam)), *uint256.NewInt(66)},
		"execution used":    {word(program.New().Push(10).Push(0).Op(frameparam)), *uint256.NewInt(11)},
		"state used":        {word(program.New().Push(11).Push(0).Op(frameparam)), *uint256.NewInt(12)},
		"signer":            {word(program.New().Push(0).Push(1).Op(sigparam)), *uint256.NewInt(0x2000)},
		"signature scheme":  {word(program.New().Push(1).Push(1).Op(sigparam)), *uint256.NewInt(1)},
		"canonical message": {word(program.New().Push(2).Push(0).Op(sigparam)), uint256.Int{}},
		"signature length":  {word(program.New().Push(3).Push(0).Op(sigparam)), *uint256.NewInt(2)},
		"data load":         {word(program.New().Push(0).Push(0).Op(vm.OpCode(0xb1))), *new(uint256.Int).SetBytes(append([]byte{0xab, 0xcd}, make([]byte, 30)...))},
		"data copy":         {program.New().Push(0).Push(32).Push(1).Push0().Op(vm.OpCode(0xb2)).Return(0, 32).Bytes(), *new(uint256.Int).SetBytes(append([]byte{0xcd}, make([]byte, 31)...))},
		"signature copy":    {program.New().Push(0).Push(32).Push(1).Push0().Op(vm.OpCode(0xb5)).Return(0, 32).Bytes(), *new(uint256.Int).SetBytes(append([]byte{2}, make([]byte, 31)...))},
	} {
		t.Run(name, func(t *testing.T) {
			evm, _ := introspectionEnv(t, test.code)
			got, _, err := evm.ExecuteTransactionFrame()
			require.NoError(t, err)
			want := test.value.Bytes32()
			require.Equal(t, want[:], got)
		})
	}
	t.Run("signing hash", func(t *testing.T) {
		evm, tx := introspectionEnv(t, word(program.New().Push(8).Op(txparam)))
		got, _, err := evm.ExecuteTransactionFrame()
		require.NoError(t, err)
		want := tx.SigningHash(nil)
		require.Equal(t, want[:], got)
	})
}

func introspectionEnv(t *testing.T, code []byte) (*vm.EVM, *types.FrameTransaction) {
	t.Helper()
	evm, tx := frameTestEnv(t)
	tx.Nonce = 7
	tx.NonceKeys = []uint256.Int{{1}, {2}}
	tx.Fees = types.FrameFees{TipCap: *uint256.NewInt(1), FeeCap: *uint256.NewInt(2), BlobFeeCap: *uint256.NewInt(3)}
	tx.BlobVersionedHashes = []common.Hash{{1}}
	tx.Frames = []types.Frame{
		{Mode: types.FrameSender, Flags: 1, Value: *uint256.NewInt(9), Limits: mdgas.MdGas{Execution: 55, State: 66}, Data: []byte{0xab, 0xcd}},
		{Mode: types.FrameSender, Limits: mdgas.MdGas{Execution: 20_000, State: 888}},
	}
	signer := common.HexToAddress("0x2000")
	tx.Signatures = []types.FrameSignature{{Signature: []byte{1, 2}}, {Scheme: 1, Signer: signer[:], Message: bytes.Repeat([]byte{1}, 32)}}
	ibs := evm.IntraBlockState()
	require.NoError(t, ibs.SetCode(accounts.InternAddress(tx.SenderAddress), code, tracing.CodeChangeUnspecified))
	evm.FrameContext = ibs.NewFrameContext(tx, *uint256.NewInt(777))
	evm.FrameContext.Index = 1
	evm.FrameContext.Receipts[0] = types.FrameReceipt{Status: 1, GasUsed: mdgas.MdGas{Execution: 11, State: 12}}
	return evm, tx
}

func TestFrameTransactionLegacyNonceSnapshot(t *testing.T) {
	evm, tx := frameTestEnv(t)
	ibs := evm.IntraBlockState()
	sender := accounts.InternAddress(tx.SenderAddress)
	target := common.HexToAddress("0x2000")
	tx.Nonce = 7
	require.NoError(t, ibs.SetNonce(sender, tx.Nonce, tracing.NonceChangeUnspecified))
	require.NoError(t, ibs.SetCode(sender, program.New().Push(3).Push0().Push0().Op(vm.APPROVE).Bytes(), tracing.CodeChangeUnspecified))
	require.NoError(t, ibs.SetCode(accounts.InternAddress(target), program.New().Push(13).Op(vm.TXPARAM).Push0().Op(vm.SSTORE).Bytes(), tracing.CodeChangeUnspecified))
	tx.Frames = []types.Frame{
		{Mode: types.FrameVerify, Flags: types.FrameApproveBoth, Limits: mdgas.MdGas{Execution: 1_000}},
		{Mode: types.FrameSender, Target: &target, Limits: mdgas.MdGas{Execution: 20_000, State: params.StateGasPerStorageSet}},
	}
	result, err := applyFrameTest(t, evm, tx)
	require.NoError(t, err)
	require.NoError(t, result.Err)
	stored, err := ibs.GetState(accounts.InternAddress(target), accounts.InternKey(common.Hash{}))
	require.NoError(t, err)
	require.Equal(t, uint256.Int{7}, stored)
	nonce, err := ibs.GetNonce(sender)
	require.NoError(t, err)
	require.EqualValues(t, 8, nonce)
}
