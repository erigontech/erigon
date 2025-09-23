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
	"math/big"
	"testing"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/vm"
	"github.com/erigontech/erigon/core/vm/evmtypes"
	"github.com/erigontech/erigon/execution/chain"
)

type dummyContractRef struct {
	calledForEach bool
}

func (dummyContractRef) ReturnGas(*big.Int)          {}
func (dummyContractRef) Address() common.Address     { return common.Address{} }
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
	c := vm.NewJumpDestCache(128)
	ibs := state.New(state.NewNoopReader())
	ibs.AddRefund(1337)

	var (
		logger   = NewStructLogger(nil)
		evm      = vm.NewEVM(evmtypes.BlockContext{}, evmtypes.TxContext{}, ibs, chain.TestChainConfig, vm.Config{Tracer: logger.Hooks()})
		contract = vm.NewContract(&dummyContractRef{}, common.Address{}, new(uint256.Int), 100000, false /* skipAnalysis */, c)
	)
	contract.Code = []byte{byte(vm.PUSH1), 0x1, byte(vm.PUSH1), 0x0, byte(vm.SSTORE)}
	var index common.Hash
	logger.OnTxStart(evm.GetVMContext(), nil, common.Address{})
	_, err := evm.Interpreter().Run(contract, []byte{}, false)
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
}

//func TestStoreCapture(t *testing.T) {
//	c := vm.NewJumpDestCache()
//	var (
//		logger   = NewStructLogger(nil)
//		env      = vm.NewEVM(evmtypes.BlockContext{}, evmtypes.TxContext{}, &dummyStatedb{}, chain.TestChainConfig, vm.Config{Tracer: logger.Hooks()})
//		mem      = vm.NewMemory()
//		stack    = vm.New()
//		contract = vm.NewContract(&dummyContractRef{}, common.Address{}, new(uint256.Int), 0, false /* skipAnalysis */, c)
//	)
//	stack.push(uint256.NewInt(1))
//	stack.push(uint256.NewInt(0))
//	var index common.Hash
//	logger.OnTxStart(env.GetVMContext(), nil, common.Address{})
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
