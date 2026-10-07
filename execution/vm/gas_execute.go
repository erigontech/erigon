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

// gasExecuteFor builds the gasExecute of an op that charges a dynamic cost and
// then runs: mem is nil for an op that does not grow memory, and gas takes the
// grown size. The ops vmgen inlines into run keep their own named funcs, since
// it splices their bodies.
func gasExecuteFor(mem memorySizeFunc, gas gasFunc, op executionFunc) gasExecuteFunc {
	return func(pc uint64, evm *EVM, scope *CallContext, t *opTrace) (uint64, []byte, error) {
		var size uint64
		var err error
		if mem != nil {
			if size, err = wordMemorySize(mem(scope)); err != nil {
				return pc, nil, err
			}
		}
		cost, err := gas(evm, scope, scope.Gas(), size)
		if err = evm.chargeDynamic(pc, scope, t, cost, err, size); err != nil {
			return pc, nil, err
		}
		return op(pc, evm, scope)
	}
}
