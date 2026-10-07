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
	"github.com/erigontech/erigon/execution/tracing"
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
func (evm *EVM) chargeDynamic(op OpCode, pc uint64, scope *CallContext, t *opTrace, cost mdgas.MdGasCost, err error, memorySize uint64) error {
	if err != nil || t != nil || cost.State != 0 {
		return evm.chargeDynamicSlow(op, pc, scope, t, cost, err, memorySize)
	}
	if scope.gas < cost.Execution {
		return ErrOutOfGas
	}
	scope.gas -= cost.Execution
	scope.Memory.Resize(memorySize)
	return nil
}

// chargeDynamicSlow is chargeDynamic for a gas func error, a trace or state gas.
func (evm *EVM) chargeDynamicSlow(op OpCode, pc uint64, scope *CallContext, t *opTrace, cost mdgas.MdGasCost, err error, memorySize uint64) error {
	if err != nil {
		if !errors.Is(err, ErrOutOfGas) {
			err = fmt.Errorf("%w: %w", ErrOutOfGas, err)
		}
		return err
	}
	if t != nil {
		evm.traceCost(op, t, cost)
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
		evm.traceOp(scope, op, t)
	}
	if memorySize > 0 {
		scope.Memory.Resize(memorySize)
	}
	if t != nil && t.trace {
		evm.tracePrint(scope, op, pc, t)
	}
	return nil
}

func opKeccak256WithGas(pc uint64, evm *EVM, scope *CallContext, t *opTrace) (uint64, []byte, error) {
	size, err := wordMemorySize(memoryKeccak256(scope))
	if err != nil {
		return pc, nil, err
	}
	cost, err := gasKeccak256(evm, scope, scope.Gas(), size)
	if err = evm.chargeDynamic(KECCAK256, pc, scope, t, cost, err, size); err != nil {
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
	if err = evm.chargeDynamic(CALLDATACOPY, pc, scope, t, cost, err, size); err != nil {
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
	if err = evm.chargeDynamic(CODECOPY, pc, scope, t, cost, err, size); err != nil {
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
	if err = evm.chargeDynamic(RETURNDATACOPY, pc, scope, t, cost, err, size); err != nil {
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
	if err = evm.chargeDynamic(MCOPY, pc, scope, t, cost, err, size); err != nil {
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
	if err = evm.chargeDynamic(MLOAD, pc, scope, t, cost, err, size); err != nil {
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
	if err = evm.chargeDynamic(MSTORE, pc, scope, t, cost, err, size); err != nil {
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
	if err = evm.chargeDynamic(MSTORE8, pc, scope, t, cost, err, size); err != nil {
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
	if err = evm.chargeDynamic(RETURN, pc, scope, t, cost, err, size); err != nil {
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
	if err = evm.chargeDynamic(REVERT, pc, scope, t, cost, err, size); err != nil {
		return pc, nil, err
	}
	return opRevert(pc, evm, scope)
}

func makeLogWithGas(topics int) gasExecuteFunc {
	op := LOG0 + OpCode(topics)
	return func(pc uint64, evm *EVM, scope *CallContext, t *opTrace) (uint64, []byte, error) {
		size, err := wordMemorySize(memoryLog(scope))
		if err != nil {
			return pc, nil, err
		}
		cost, err := logGas(scope, size, uint64(topics))
		if err = evm.chargeDynamic(op, pc, scope, t, cost, err, size); err != nil {
			return pc, nil, err
		}
		return opLog(pc, evm, scope, topics)
	}
}
