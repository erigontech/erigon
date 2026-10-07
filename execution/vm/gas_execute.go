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

package vm

import (
	"errors"
	"fmt"

	"github.com/erigontech/erigon/common/math"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// wordMemorySize rounds a memory size up to whole words, in which memory grows and is charged.
func wordMemorySize(size uint64, overflow bool) (uint64, error) {
	if overflow {
		return 0, ErrGasUintOverflow
	}
	size, overflow = math.SafeMul(ToWordSize(size), 32)
	if overflow {
		return 0, ErrGasUintOverflow
	}
	return size, nil
}

// chargeDynamic charges an op's dynamic cost, or fails with the error of its gas func,
// and grows the memory to memorySize.
func (evm *EVM) chargeDynamic(pc uint64, scope *CallContext, t *opTrace, cost mdgas.MdGasCost, err error, memorySize uint64) error {
	if err != nil || t != nil || cost.State != 0 {
		return evm.chargeDynamicSlow(pc, scope, t, cost, err, memorySize)
	}
	if scope.gas < cost.Execution {
		return ErrOutOfGas
	}
	scope.gas -= cost.Execution
	scope.Memory.Resize(memorySize)
	return nil
}

// chargeDynamicSlow is chargeDynamic for a gas func error, a trace or state gas.
func (evm *EVM) chargeDynamicSlow(pc uint64, scope *CallContext, t *opTrace, cost mdgas.MdGasCost, err error, memorySize uint64) error {
	if err != nil {
		if !errors.Is(err, ErrOutOfGas) {
			err = fmt.Errorf("%w: %w", ErrOutOfGas, err)
		}
		return err
	}
	if t != nil {
		evm.traceCost(t.op, t, cost)
	}
	if scope.gas < cost.Execution {
		return ErrOutOfGas
	}
	scope.gas -= cost.Execution
	if cost.State > 0 {
		if !scope.useMdGas(uint64(cost.State), mdgas.StateGas, nil, tracing.GasChangeIgnored) {
			return ErrOutOfGas
		}
	} else if cost.State < 0 {
		scope.refillStateGas(uint64(-cost.State), nil, tracing.GasChangeIgnored)
	}
	if t != nil && t.debug {
		evm.traceOp(scope, t.op, t)
	}
	if memorySize > 0 {
		scope.Memory.Resize(memorySize)
	}
	if t != nil && t.trace {
		evm.tracePrint(scope, t.op, pc, t)
	}
	return nil
}

func opKeccak256WithGas(pc uint64, evm *EVM, scope *CallContext, t *opTrace) (uint64, []byte, error) {
	size, err := wordMemorySize(memoryKeccak256(scope))
	if err != nil {
		return pc, nil, err
	}
	cost, err := gasKeccak256(evm, scope, scope.Gas(), size)
	if err = evm.chargeDynamic(pc, scope, t, cost, err, size); err != nil {
		return pc, nil, err
	}
	return opKeccak256(pc, evm, scope)
}

func opCallDataCopyWithGas(pc uint64, evm *EVM, scope *CallContext, t *opTrace) (uint64, []byte, error) {
	size, err := wordMemorySize(memoryCallDataCopy(scope))
	if err != nil {
		return pc, nil, err
	}
	cost, err := copyGas(scope, size, 2)
	if err = evm.chargeDynamic(pc, scope, t, cost, err, size); err != nil {
		return pc, nil, err
	}
	return opCallDataCopy(pc, evm, scope)
}

func opCodeCopyWithGas(pc uint64, evm *EVM, scope *CallContext, t *opTrace) (uint64, []byte, error) {
	size, err := wordMemorySize(memoryCodeCopy(scope))
	if err != nil {
		return pc, nil, err
	}
	cost, err := copyGas(scope, size, 2)
	if err = evm.chargeDynamic(pc, scope, t, cost, err, size); err != nil {
		return pc, nil, err
	}
	return opCodeCopy(pc, evm, scope)
}

func opReturnDataCopyWithGas(pc uint64, evm *EVM, scope *CallContext, t *opTrace) (uint64, []byte, error) {
	size, err := wordMemorySize(memoryReturnDataCopy(scope))
	if err != nil {
		return pc, nil, err
	}
	cost, err := copyGas(scope, size, 2)
	if err = evm.chargeDynamic(pc, scope, t, cost, err, size); err != nil {
		return pc, nil, err
	}
	return opReturnDataCopy(pc, evm, scope)
}

func opMcopyWithGas(pc uint64, evm *EVM, scope *CallContext, t *opTrace) (uint64, []byte, error) {
	size, err := wordMemorySize(memoryMcopy(scope))
	if err != nil {
		return pc, nil, err
	}
	cost, err := copyGas(scope, size, 2)
	if err = evm.chargeDynamic(pc, scope, t, cost, err, size); err != nil {
		return pc, nil, err
	}
	return opMcopy(pc, evm, scope)
}

func opMloadWithGas(pc uint64, evm *EVM, scope *CallContext, t *opTrace) (uint64, []byte, error) {
	size, err := wordMemorySize(memoryMLoad(scope))
	if err != nil {
		return pc, nil, err
	}
	cost, err := pureMemoryGascost(evm, scope, scope.Gas(), size)
	if err = evm.chargeDynamic(pc, scope, t, cost, err, size); err != nil {
		return pc, nil, err
	}
	return opMload(pc, evm, scope)
}

func opMstoreWithGas(pc uint64, evm *EVM, scope *CallContext, t *opTrace) (uint64, []byte, error) {
	size, err := wordMemorySize(memoryMStore(scope))
	if err != nil {
		return pc, nil, err
	}
	cost, err := pureMemoryGascost(evm, scope, scope.Gas(), size)
	if err = evm.chargeDynamic(pc, scope, t, cost, err, size); err != nil {
		return pc, nil, err
	}
	return opMstore(pc, evm, scope)
}

func opMstore8WithGas(pc uint64, evm *EVM, scope *CallContext, t *opTrace) (uint64, []byte, error) {
	size, err := wordMemorySize(memoryMStore8(scope))
	if err != nil {
		return pc, nil, err
	}
	cost, err := pureMemoryGascost(evm, scope, scope.Gas(), size)
	if err = evm.chargeDynamic(pc, scope, t, cost, err, size); err != nil {
		return pc, nil, err
	}
	return opMstore8(pc, evm, scope)
}

func opReturnWithGas(pc uint64, evm *EVM, scope *CallContext, t *opTrace) (uint64, []byte, error) {
	size, err := wordMemorySize(memoryReturn(scope))
	if err != nil {
		return pc, nil, err
	}
	cost, err := pureMemoryGascost(evm, scope, scope.Gas(), size)
	if err = evm.chargeDynamic(pc, scope, t, cost, err, size); err != nil {
		return pc, nil, err
	}
	return opReturn(pc, evm, scope)
}

func opRevertWithGas(pc uint64, evm *EVM, scope *CallContext, t *opTrace) (uint64, []byte, error) {
	size, err := wordMemorySize(memoryRevert(scope))
	if err != nil {
		return pc, nil, err
	}
	cost, err := pureMemoryGascost(evm, scope, scope.Gas(), size)
	if err = evm.chargeDynamic(pc, scope, t, cost, err, size); err != nil {
		return pc, nil, err
	}
	return opRevert(pc, evm, scope)
}

func makeLogWithGas(topics int) gasExecuteFunc {
	return func(pc uint64, evm *EVM, scope *CallContext, t *opTrace) (uint64, []byte, error) {
		size, err := wordMemorySize(memoryLog(scope))
		if err != nil {
			return pc, nil, err
		}
		cost, err := logGas(scope, size, uint64(topics))
		if err = evm.chargeDynamic(pc, scope, t, cost, err, size); err != nil {
			return pc, nil, err
		}
		return opLog(pc, evm, scope, topics)
	}
}

func opExpFrontierWithGas(pc uint64, evm *EVM, scope *CallContext, t *opTrace) (uint64, []byte, error) {
	cost, err := gasExpFrontier(evm, scope, scope.Gas(), 0)
	if err = evm.chargeDynamic(pc, scope, t, cost, err, 0); err != nil {
		return pc, nil, err
	}
	return opExp(pc, evm, scope)
}

func opExpEIP160WithGas(pc uint64, evm *EVM, scope *CallContext, t *opTrace) (uint64, []byte, error) {
	cost, err := gasExpEIP160(evm, scope, scope.Gas(), 0)
	if err = evm.chargeDynamic(pc, scope, t, cost, err, 0); err != nil {
		return pc, nil, err
	}
	return opExp(pc, evm, scope)
}

func opExtCodeCopyWithGas(pc uint64, evm *EVM, scope *CallContext, t *opTrace) (uint64, []byte, error) {
	size, err := wordMemorySize(memoryExtCodeCopy(scope))
	if err != nil {
		return pc, nil, err
	}
	cost, err := gasExtCodeCopy(evm, scope, scope.Gas(), size)
	if err = evm.chargeDynamic(pc, scope, t, cost, err, size); err != nil {
		return pc, nil, err
	}
	return opExtCodeCopy(pc, evm, scope)
}

func opExtCodeCopyEIP2929(pc uint64, evm *EVM, scope *CallContext, t *opTrace) (uint64, []byte, error) {
	size, err := wordMemorySize(memoryExtCodeCopy(scope))
	if err != nil {
		return pc, nil, err
	}
	cost, err := gasExtCodeCopyEIP2929(evm, scope, scope.Gas(), size)
	if err = evm.chargeDynamic(pc, scope, t, cost, err, size); err != nil {
		return pc, nil, err
	}
	return opExtCodeCopy(pc, evm, scope)
}

func opSstoreWithGas(pc uint64, evm *EVM, scope *CallContext, t *opTrace) (uint64, []byte, error) {
	cost, err := gasSStore(evm, scope, scope.Gas(), 0)
	if err = evm.chargeDynamic(pc, scope, t, cost, err, 0); err != nil {
		return pc, nil, err
	}
	return opSstore(pc, evm, scope)
}

func opSstoreEIP2200(pc uint64, evm *EVM, scope *CallContext, t *opTrace) (uint64, []byte, error) {
	cost, err := gasSStoreEIP2200(evm, scope, scope.Gas(), 0)
	if err = evm.chargeDynamic(pc, scope, t, cost, err, 0); err != nil {
		return pc, nil, err
	}
	return opSstore(pc, evm, scope)
}

func makeSstoreEIP2929(clearingRefund uint64) gasExecuteFunc {
	return func(pc uint64, evm *EVM, scope *CallContext, t *opTrace) (uint64, []byte, error) {
		cost, err := sstoreGasEIP2929(evm, scope, scope.Gas(), clearingRefund)
		if err = evm.chargeDynamic(pc, scope, t, cost, err, 0); err != nil {
			return pc, nil, err
		}
		return opSstore(pc, evm, scope)
	}
}

// opSelfdestructWithGas runs the table's op, which EIP-6780 changes apart from the gas.
func opSelfdestructWithGas(pc uint64, evm *EVM, scope *CallContext, t *opTrace) (uint64, []byte, error) {
	cost, err := gasSelfdestruct(evm, scope, scope.Gas(), 0)
	if err = evm.chargeDynamic(pc, scope, t, cost, err, 0); err != nil {
		return pc, nil, err
	}
	return evm.jt[SELFDESTRUCT].execute(pc, evm, scope)
}

// makeSelfdestructEIP2929 runs the table's op, like opSelfdestructWithGas.
func makeSelfdestructEIP2929(refundsEnabled bool) gasExecuteFunc {
	return func(pc uint64, evm *EVM, scope *CallContext, t *opTrace) (uint64, []byte, error) {
		cost, err := selfdestructGasEIP2929(evm, scope, scope.Gas(), refundsEnabled)
		if err = evm.chargeDynamic(pc, scope, t, cost, err, 0); err != nil {
			return pc, nil, err
		}
		return evm.jt[SELFDESTRUCT].execute(pc, evm, scope)
	}
}

// makeCreateWithGas runs a CREATE op with what its gas func prepared for it.
func makeCreateWithGas(memorySize memorySizeFunc, gas createGasFunc, create createFunc) gasExecuteFunc {
	return func(pc uint64, evm *EVM, scope *CallContext, t *opTrace) (uint64, []byte, error) {
		size, err := wordMemorySize(memorySize(scope))
		if err != nil {
			return pc, nil, err
		}
		cost, prepared, err := gas(evm, scope, scope.Gas(), size)
		if err = evm.chargeDynamic(pc, scope, t, cost, err, size); err != nil {
			return pc, nil, err
		}
		return create(pc, evm, scope, &prepared)
	}
}

// makeCallWithGas runs a call op with the gas its gas func sets aside for the callee.
func makeCallWithGas(memorySize memorySizeFunc, gas callGasFunc, call callFunc) gasExecuteFunc {
	return func(pc uint64, evm *EVM, scope *CallContext, t *opTrace) (uint64, []byte, error) {
		size, err := wordMemorySize(memorySize(scope))
		if err != nil {
			return pc, nil, err
		}
		cost, forwarded, err := gas(evm, scope, scope.Gas(), size)
		if t != nil {
			t.forwarded = forwarded
		}
		if err = evm.chargeDynamic(pc, scope, t, cost, err, size); err != nil {
			return pc, nil, err
		}
		return call(pc, evm, scope, forwarded)
	}
}

// accountSurcharge warms addr and returns what EIP-2929 charges for it on top of
// the warm cost, which the op's constant gas covers.
func accountSurcharge(evm *EVM, addr accounts.Address) uint64 {
	if evm.IntraBlockState().AddAddressToAccessList(addr) {
		return coldAccountAccessCost(evm.chainRules) - params.WarmStorageReadCostEIP2929
	}
	return 0
}

func opBalanceEIP2929(pc uint64, evm *EVM, scope *CallContext, t *opTrace) (uint64, []byte, error) {
	cost := mdgas.MdGasCost{Execution: accountSurcharge(evm, scope.peekAddress(evm))}
	if err := evm.chargeDynamic(pc, scope, t, cost, nil, 0); err != nil {
		return pc, nil, err
	}
	return opBalance(pc, evm, scope)
}

func opExtCodeSizeEIP2929(pc uint64, evm *EVM, scope *CallContext, t *opTrace) (uint64, []byte, error) {
	cost := mdgas.MdGasCost{Execution: accountSurcharge(evm, scope.peekAddress(evm))}
	if err := evm.chargeDynamic(pc, scope, t, cost, nil, 0); err != nil {
		return pc, nil, err
	}
	return opExtCodeSize(pc, evm, scope)
}

func opExtCodeHashEIP2929(pc uint64, evm *EVM, scope *CallContext, t *opTrace) (uint64, []byte, error) {
	cost := mdgas.MdGasCost{Execution: accountSurcharge(evm, scope.peekAddress(evm))}
	if err := evm.chargeDynamic(pc, scope, t, cost, nil, 0); err != nil {
		return pc, nil, err
	}
	return opExtCodeHash(pc, evm, scope)
}
