// Copyright 2016 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
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

package logger

import (
	"bytes"
	"encoding/json"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
)

type dummyContractRef struct {
	calledForEach bool
}

func (dummyContractRef) ReturnGas(*big.Int)          {}
func (dummyContractRef) Address() common.Address     { return accounts.ZeroAddress.Value() }
func (dummyContractRef) Value() *big.Int             { return new(big.Int) }
func (dummyContractRef) SetCode(common.Hash, []byte) {}
func (d *dummyContractRef) ForEachStorage(callback func(key, value common.Hash) bool) {
	d.calledForEach = true
}
func (d *dummyContractRef) SubBalance(amount *big.Int) {}
func (d *dummyContractRef) AddBalance(amount *big.Int) {}
func (d *dummyContractRef) SetBalance(*big.Int)        {}
func (d *dummyContractRef) SetNonce(uint64)            {}
func (d *dummyContractRef) Balance() *big.Int          { return new(big.Int) }

func TestStoreCapture(t *testing.T) {
	//c := vm.NewJumpDestCache(128)
	ibs := state.New(state.NewNoopReader())
	defer ibs.Close()
	ibs.AddRefund(1337)

	var (
		logger   = NewStructLogger(nil)
		evm      = vm.NewEVM(evmtypes.BlockContext{}, evmtypes.TxContext{}, ibs, chain.AllProtocolChanges, vm.Config{Tracer: logger.Hooks()})
		contract = *vm.NewContract(accounts.ZeroAddress, accounts.ZeroAddress, accounts.ZeroAddress, uint256.Int{})
	)
	contract.Code = []byte{byte(vm.PUSH1), 0x1, byte(vm.PUSH1), 0x0, byte(vm.SSTORE)}
	var index common.Hash
	logger.OnTxStart(evm.GetVMContext(), nil, accounts.ZeroAddress)
	_, _, _, err := evm.Run(contract, mdgas.MdGas{Execution: 200_000, State: params.StateGasPerStorageSet}, []byte{}, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(logger.storage[contract.Address()]) == 0 {
		t.Fatalf("expected exactly 1 changed value on address %x, got %d", contract.Address(),
			len(logger.storage[contract.Address()]))
	}
	exp := common.BigToHash(big.NewInt(1))
	if logger.storage[contract.Address()][index] != exp {
		t.Errorf("expected %x, got %x", exp, logger.storage[contract.Address()][index])
	}
	encoded, err := json.Marshal(FormatLogs(logger.StructLogs()))
	require.NoError(t, err)
	var logs []map[string]any
	require.NoError(t, json.Unmarshal(encoded, &logs))
	require.EqualValues(t, params.StateGasPerStorageSet, logs[2]["stateGasReservoir"])
	require.EqualValues(t, params.StateGasPerStorageSet, logs[2]["stateGasCost"])
	require.NotContains(t, logs[0], "stateGasCost")
}

func TestStructLoggerTxEndError(t *testing.T) {
	logger := NewStructLogger(nil)
	logger.Hooks().EmitTxEnd(nil, mdgas.TxnGasUsage{}, vm.ErrInsufficientBalance)
	require.ErrorIs(t, logger.Error(), vm.ErrInsufficientBalance)
}

func TestJSONLoggerOnSystemCallStartSetsEnv(t *testing.T) {
	var buf bytes.Buffer
	logger := NewJSONLogger(nil, &buf)
	logger.OnSystemCallStartV2(&tracing.VMContext{Rules: &chain.Rules{IsAmsterdam: true}, IntraBlockState: &mockIBS{}})

	scope := &mockOpContext{}
	logger.OnOpcodeV2(0, byte(vm.SSTORE), mdgas.MdGas{Execution: 100, State: 200}, mdgas.MdGas{Execution: 10, State: 30}, scope, nil, 0, nil)

	var entry map[string]json.RawMessage
	if err := json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &entry); err != nil {
		t.Fatalf("failed to decode json logger output: %v", err)
	}
	if _, ok := entry["refund"]; !ok {
		t.Fatal("expected json logger to emit opcode output after system call start")
	}
	var stateGas uint64
	require.NoError(t, json.Unmarshal(entry["stateGasReservoir"], &stateGas))
	require.EqualValues(t, 200, stateGas)
	require.Contains(t, entry, "stateGasCost")
	require.JSONEq(t, `30`, string(entry["stateGasCost"]))
	buf.Reset()
	logger.OnExitV2(0, nil, mdgas.MdGasUsage{Execution: 10}, nil, false)
	require.JSONEq(t, `{"output":"","gasUsed":"0xa","stateGasUsed":0}`, buf.String())
}

func TestMarkdownLoggerStateGas(t *testing.T) {
	var output bytes.Buffer
	logger := NewMarkdownLogger(nil, &output)
	logger.OnTxStart(&tracing.VMContext{Rules: &chain.Rules{IsAmsterdam: true}, IntraBlockState: &mockIBS{}}, nil, accounts.ZeroAddress)
	logger.Hooks().EmitEnter(0, byte(vm.CALL), accounts.ZeroAddress, accounts.ZeroAddress, false, nil,
		mdgas.MdGas{Execution: 100, State: 200}, uint256.Int{}, nil)
	logger.OnOpcodeV2(0, byte(vm.SSTORE), mdgas.MdGas{Execution: 100, State: 200},
		mdgas.MdGas{Execution: 10, State: 30}, &mockOpContext{}, nil, 0, nil)
	require.NotContains(t, output.String(), "reservoir")
	require.Contains(t, output.String(), "| Cost | State cost |   Stack   |")
	require.Contains(t, output.String(), "|   10 |  30 |        [] |")
	logger.Hooks().EmitExit(0, nil, mdgas.MdGasUsage{Execution: 50, State: -30, StateSpill: 10}, nil, false)
	require.Contains(t, output.String(), "Consumed state gas: `-30`\n")
	require.NotContains(t, output.String(), "spill")
	output.Reset()
	logger.Hooks().EmitExit(0, nil, mdgas.MdGasUsage{Execution: 50}, nil, false)
	require.Contains(t, output.String(), "Consumed state gas: `0`\n")
}

//func TestStoreCapture(t *testing.T) {
//	c := vm.NewJumpDestCache()
//	var (
//		logger   = NewStructLogger(nil)
//		env      = vm.NewEVM(evmtypes.BlockContext{}, evmtypes.TxContext{}, &dummyStatedb{}, chain.AllProtocolChanges, vm.Config{Tracer: logger.Hooks()})
//		mem      = vm.NewMemory()
//		stack    = vm.New()
//		contract = vm.NewContract(&dummyContractRef{}, accounts.ZeroAddress, new(uint256.Int), 0, c)
//	)
//	stack.push(uint256.NewInt(1))
//	stack.push(uint256.NewInt(0))
//	var index common.Hash
//	logger.OnTxStart(env.GetVMContext(), nil, accounts.ZeroAddress)
//	logger.OnOpcode(0, byte(vm.SSTORE), 0, 0, &vm.ScopeContext{
//		Memory:   mem,
//		Stack:    stack,
//		Contract: contract,
//	}, nil, 0, nil)
//
//	if len(logger.storage[contract.Address()]) == 0 {
//		t.Fatalf("expected exactly 1 changed value on address %x, got %d", contract.Address(), len(logger.storage[contract.Address()]))
//	}
//	exp := common.BigToHash(big.NewInt(1))
//	if logger.storage[contract.Address()][index] != exp {
//		t.Errorf("expected %x, got %x", exp, logger.storage[contract.Address()][index])
//	}
//}
