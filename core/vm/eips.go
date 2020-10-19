// Copyright 2019 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package vm

import (
	"fmt"
	"sort"

	"github.com/holiman/uint256"

	"github.com/ledgerwatch/turbo-geth/params"
)

var activators = map[int]func(*JumpTable) func(){
	2200: enable2200,
	1884: enable1884,
	1344: enable1344,
	2315: enable2315,
}

// EnableEIP enables the given EIP on the config.
// This operation writes in-place, and callers need to ensure that the globally
// defined jump tables are not polluted.
func EnableEIP(eipNum int, jt *JumpTable) (func(), error) {
	enablerFn, ok := activators[eipNum]
	if !ok {
		return nil, fmt.Errorf("undefined eip %d", eipNum)
	}
	return enablerFn(jt), nil
}

func ValidEip(eipNum int) bool {
	_, ok := activators[eipNum]
	return ok
}
func ActivateableEips() []string {
	var nums []string //nolint:prealloc
	for k := range activators {
		nums = append(nums, fmt.Sprintf("%d", k))
	}
	sort.Strings(nums)
	return nums
}

// enable1884 applies EIP-1884 to the given jump table:
// - Increase cost of BALANCE to 700
// - Increase cost of EXTCODEHASH to 700
// - Increase cost of SLOAD to 800
// - Define SELFBALANCE, with cost GasFastStep (5)
func enable1884(jt *JumpTable) func() {
	// Gas cost changes
	oldConstantGasSload := jt[SLOAD].constantGas
	jt[SLOAD].constantGas = params.SloadGasEIP1884

	oldConstantGasBalance := jt[BALANCE].constantGas
	jt[BALANCE].constantGas = params.BalanceGasEIP1884

	oldConstantGasExtCodehash := jt[EXTCODEHASH].constantGas
	jt[EXTCODEHASH].constantGas = params.ExtcodeHashGasEIP1884

	// New opcode
	oldSelfBalance := jt[SELFBALANCE]
	jt[SELFBALANCE] = &operation{
		execute:     opSelfBalance,
		constantGas: GasFastStep,
		minStack:    minStack(0, 1),
		maxStack:    maxStack(0, 1),
	}
	return func() {
		jt[SELFBALANCE] = oldSelfBalance
		jt[EXTCODEHASH].constantGas = oldConstantGasExtCodehash
		jt[BALANCE].constantGas = oldConstantGasBalance
		jt[SLOAD].constantGas = oldConstantGasSload
	}
}

func opSelfBalance(_ *uint64, interpreter *EVMInterpreter, callContext *callCtx) ([]byte, error) {
	balance := interpreter.evm.IntraBlockState.GetBalance(callContext.contract.Address())
	callContext.stack.Push(balance)
	return nil, nil
}

// enable1344 applies EIP-1344 (ChainID Opcode)
// - Adds an opcode that returns the current chain’s EIP-155 unique identifier
func enable1344(jt *JumpTable) func() {
	// New opcode
	oldOp := jt[CHAINID]
	jt[CHAINID] = &operation{
		execute:     opChainID,
		constantGas: GasQuickStep,
		minStack:    minStack(0, 1),
		maxStack:    maxStack(0, 1),
	}
	return func() {
		jt[CHAINID] = oldOp
	}
}

// opChainID implements CHAINID opcode
func opChainID(pc *uint64, interpreter *EVMInterpreter, callContext *callCtx) ([]byte, error) {
	chainId, _ := uint256.FromBig(interpreter.evm.chainConfig.ChainID)
	callContext.stack.Push(chainId)
	return nil, nil
}

// enable2200 applies EIP-2200 (Rebalance net-metered SSTORE)
func enable2200(jt *JumpTable) func() {
	oldConstantGasSLoad := jt[SLOAD].constantGas
	jt[SLOAD].constantGas = params.SloadGasEIP2200

	oldDynamicGasSStore := jt[SSTORE].dynamicGas
	jt[SSTORE].dynamicGas = gasSStoreEIP2200
	return func() {
		jt[SSTORE].dynamicGas = oldDynamicGasSStore
		jt[SLOAD].constantGas = oldConstantGasSLoad
	}
}

// enable2315 applies EIP-2315 (Simple Subroutines)
// - Adds opcodes that jump to and return from subroutines
func enable2315(jt *JumpTable) func() {
	// New opcode
	oldBeginSubOp := jt[BEGINSUB]
	jt[BEGINSUB] = &operation{
		execute:     opBeginSub,
		constantGas: GasQuickStep,
		minStack:    minStack(0, 0),
		maxStack:    maxStack(0, 0),
	}

	// New opcode
	oldJumpSub := jt[JUMPSUB]
	jt[JUMPSUB] = &operation{
		execute:     opJumpSub,
		constantGas: GasSlowStep,
		minStack:    minStack(1, 0),
		maxStack:    maxStack(1, 0),
		jumps:       true,
	}

	// New opcode
	oldReturnSub := jt[RETURNSUB]
	jt[RETURNSUB] = &operation{
		execute:     opReturnSub,
		constantGas: GasFastStep,
		minStack:    minStack(0, 0),
		maxStack:    maxStack(0, 0),
		jumps:       true,
	}
	return func() {
		jt[RETURNSUB] = oldReturnSub
		jt[JUMPSUB] = oldJumpSub
		jt[BEGINSUB] = oldBeginSubOp
	}
}
