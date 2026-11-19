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

package vm_test

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/protocol/misc"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
	"github.com/erigontech/erigon/execution/vm/program"
)

func TestCreateInitCodeOverrideCodeHash(t *testing.T) {
	original := program.New().Return(0, 1).Bytes()
	replacement := program.New().Return(0, 2).Bytes()
	for _, fork := range []struct {
		name   string
		config *chain.Config
	}{
		{"Berlin", chain.TestChainBerlinConfig},
		{"Osaka", chain.TestChainOsakaConfig},
		{"Amsterdam", chain.AllProtocolChanges},
	} {
		for _, tc := range []struct {
			name  string
			op    vm.OpCode
			match bool
		}{
			{"CREATE", vm.CREATE, true},
			{"CREATE2", vm.CREATE2, true},
			{"sibling CREATE2", vm.CREATE2, false},
		} {
			t.Run(fork.name+"/"+tc.name, func(t *testing.T) {
				hooks := &tracing.Hooks{}
				evm, factory, target := createOverrideEVM(t, fork.config, tc.op, original, vm.Config{
					Tracer:           hooks,
					OverrideInitCode: func(accounts.Address) ([]byte, bool) { return replacement, tc.match },
				})
				var executedHash common.Hash
				var frameHash accounts.CodeHash
				hooks.OnOpcode = func(pc uint64, _ byte, _, _ uint64, scope tracing.OpContext, _ []byte, _ int, _ error) {
					if pc == 0 && scope.Address() == target {
						executedHash = crypto.Keccak256Hash(scope.Code())
						frameHash = scope.CodeHash()
					}
				}
				_, _, _, err := evm.Call(accounts.ZeroAddress, factory, nil, mdgas.MdGas{Execution: 1_000_000, State: 1_000_000}, uint256.Int{}, false)
				require.NoError(t, err)
				wantInitCode, wantRuntime := original, []byte{0}
				if tc.match {
					wantInitCode, wantRuntime = replacement, []byte{0, 0}
				}
				require.Equal(t, crypto.Keccak256Hash(wantInitCode), executedHash, "executed initcode")
				if !tc.match || !frameHash.IsZero() {
					require.Equal(t, executedHash, frameHash.Value(), "constructor code hash")
				}
				code, err := evm.IntraBlockState().GetCode(target)
				require.NoError(t, err)
				require.Equal(t, wantRuntime, code)
			})
		}
	}
}

func TestCreateInitCodeOverrideSize(t *testing.T) {
	for _, fork := range []struct {
		name   string
		config *chain.Config
		limit  int
	}{
		{"Osaka", chain.TestChainOsakaConfig, params.MaxInitCodeSize},
		{"Amsterdam", chain.AllProtocolChanges, params.MaxInitCodeSizeAmsterdam},
	} {
		for _, op := range []vm.OpCode{vm.CREATE, vm.CREATE2} {
			for _, excess := range []bool{false, true} {
				name := "at limit"
				size := fork.limit
				if excess {
					name = "above limit"
					size++
				}
				t.Run(fork.name+"/"+op.String()+"/"+name, func(t *testing.T) {
					initCode := program.New().ReturnData([]byte{byte(vm.STOP)}).Bytes()
					replacement := make([]byte, size)
					copy(replacement, initCode)
					evm, factory, target := createOverrideEVM(t, fork.config, op, initCode, vm.Config{
						OverrideInitCode: func(accounts.Address) ([]byte, bool) { return replacement, true },
					})
					result, _, _, err := evm.Call(accounts.ZeroAddress, factory, nil, mdgas.MdGas{Execution: 1_000_000, State: 1_000_000}, uint256.Int{}, false)
					if excess {
						require.ErrorIs(t, err, vm.ErrMaxInitCodeSizeExceeded)
						return
					}
					require.NoError(t, err)
					require.Equal(t, target.Value(), common.BytesToAddress(result))
					code, err := evm.IntraBlockState().GetCode(target)
					require.NoError(t, err)
					require.Equal(t, []byte{byte(vm.STOP)}, code)
				})
			}
		}
	}
}

func TestCreateInitCodeOverrideGas(t *testing.T) {
	for _, fork := range []struct {
		name            string
		config          *chain.Config
		initCodeWordGas int64
	}{
		{"Berlin", chain.TestChainBerlinConfig, 0},
		{"Osaka", chain.TestChainOsakaConfig, 2},
		{"Amsterdam", chain.AllProtocolChanges, 2},
	} {
		for _, op := range []vm.OpCode{vm.CREATE, vm.CREATE2} {
			for _, tc := range []struct {
				name                          string
				originalSize, replacementSize int
				extraGas                      int64
			}{
				{"longer", 32, 33, fork.initCodeWordGas},
				{"shorter", 33, 32, -fork.initCodeWordGas},
				{"same word count", 33, 64, 0},
			} {
				t.Run(fork.name+"/"+op.String()+"/"+tc.name, func(t *testing.T) {
					initCode := make([]byte, tc.originalSize)
					replacement := make([]byte, tc.replacementSize)
					constructor := program.New().ReturnData([]byte{byte(vm.STOP)}).Bytes()
					copy(initCode, constructor)
					copy(replacement, constructor)
					run := func(override bool) (remaining, available uint64) {
						var forwarded uint64
						hooks := &tracing.Hooks{
							OnOpcode: func(_ uint64, typ byte, gas, cost uint64, _ tracing.OpContext, _ []byte, _ int, _ error) {
								if vm.OpCode(typ) == op {
									available = gas - cost
								}
							},
							OnEnter: func(_ int, typ byte, _, _ accounts.Address, _ bool, _ []byte, gas uint64, _ uint256.Int, _ []byte) {
								if vm.OpCode(typ) == op {
									forwarded = gas
								}
							},
						}
						vmConfig := vm.Config{Tracer: hooks}
						if override {
							vmConfig.OverrideInitCode = func(accounts.Address) ([]byte, bool) { return replacement, true }
						}
						evm, factory, target := createOverrideEVM(t, fork.config, op, initCode, vmConfig)
						result, gas, _, err := evm.Call(accounts.ZeroAddress, factory, nil, mdgas.MdGas{Execution: 1_000_000, State: 1_000_000}, uint256.Int{}, false)
						require.NoError(t, err)
						require.Equal(t, target.Value(), common.BytesToAddress(result))
						require.Equal(t, available-available/64, forwarded, "gas forwarded to the constructor")
						return gas.Execution, available
					}
					baseRemaining, baseAvailable := run(false)
					remaining, available := run(true)
					require.Equal(t, int64(baseRemaining)-tc.extraGas, int64(remaining), "only the initcode-analysis charge changes")
					expectedAvailable := uint64(int64(baseAvailable) - tc.extraGas)
					require.Equal(t, expectedAvailable, available, "initcode analysis is charged before gas is forwarded")
				})
			}
		}
	}
}

func TestCreate2OverrideErrorBeforeMemoryExpansion(t *testing.T) {
	const size = 256
	prefix := common.RightPadBytes(program.New().Return(0, 1).Bytes(), 32)
	initCode := common.RightPadBytes(prefix, size)
	for _, tc := range []struct {
		name       string
		op         vm.OpCode
		size       uint256.Int
		offset     uint256.Int
		gas        uint64
		wantTarget bool
	}{
		{"unpaid hash and memory expansion", vm.CREATE2, uint256.Int{size}, uint256.Int{}, params.Create2Gas + 40, true},
		{"different input length", vm.CREATE2, uint256.Int{size, 1}, uint256.Int{}, 1_000, false},
		{"different initcode", vm.CREATE2, uint256.Int{size}, uint256.Int{1}, 1_000, false},
		{"offset exceeds 64 bits", vm.CREATE2, uint256.Int{size}, uint256.Int{0, 1}, 1_000, false},
		{"unrelated opcode", vm.LOG0, uint256.Int{size}, uint256.Int{}, 100, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var reported accounts.Address
			evm, factory, target := createOverrideEVM(t, chain.TestChainBerlinConfig, vm.CREATE2, initCode, vm.Config{
				OriginalCreate2InitCode: vm.NewCreate2InitCode(initCode),
				OnInitCodeOverrideError: func(address accounts.Address, err error) {
					require.ErrorIs(t, err, vm.ErrOutOfGas)
					reported = address
				},
			})
			code := program.New().Mstore(prefix, 0).Push(0).Push(tc.size).Push(tc.offset).Push(0).Op(tc.op)
			require.NoError(t, evm.IntraBlockState().SetCode(factory, code.Bytes(), tracing.CodeChangeUnspecified))
			_, _, _, err := evm.Call(accounts.ZeroAddress, factory, nil, mdgas.MdGas{Execution: tc.gas}, uint256.Int{}, false)
			require.ErrorIs(t, err, vm.ErrOutOfGas)
			if tc.wantTarget {
				require.Equal(t, target, reported)
			} else {
				require.NotEqual(t, target, reported)
			}
		})
	}
}

func createOverrideEVM(t *testing.T, config *chain.Config, op vm.OpCode, initCode []byte, vmConfig vm.Config) (*vm.EVM, accounts.Address, accounts.Address) {
	t.Helper()
	factory := accounts.InternAddress(common.HexToAddress("0xfac0"))
	target := accounts.InternAddress(types.CreateAddress(factory.Value(), 0))
	code := program.New().Mstore(initCode, 0).Push(len(initCode)).Push(0).Push(0).Op(vm.CREATE)
	if op == vm.CREATE2 {
		code = program.New().Create2(initCode, 0)
		target = accounts.InternAddress(types.CreateAddress2(factory.Value(), [32]byte{}, accounts.InternCodeHash(crypto.Keccak256Hash(initCode))))
	}
	code.Push(0).Op(vm.MSTORE).Return(0, 32)
	statedb := state.New(state.NewNoopReader())
	t.Cleanup(statedb.Close)
	require.NoError(t, statedb.SetCode(factory, code.Bytes(), tracing.CodeChangeUnspecified))
	evm := vm.NewEVM(evmtypes.BlockContext{CanTransfer: protocol.CanTransfer, Transfer: misc.Transfer}, evmtypes.TxContext{}, statedb, config, vmConfig)
	return evm, factory, target
}
