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
	"errors"
	"fmt"
	"slices"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
)

type runtimeGasAccounting struct {
	auth     mdgas.MdGasUsage
	topLevel mdgas.MdGasUsage
	frame    mdgas.MdGasUsage
}

func (g runtimeGasAccounting) total() mdgas.MdGasUsage {
	return mdgas.MdGasUsage{
		Execution:  g.auth.Execution + g.topLevel.Execution + g.frame.Execution,
		State:      g.auth.State + g.topLevel.State + g.frame.State,
		StateSpill: g.auth.StateSpill + g.topLevel.StateSpill + g.frame.StateSpill,
	}
}

func (g *runtimeGasAccounting) consumeAllExecutionGas(execution uint64) {
	*g = runtimeGasAccounting{frame: mdgas.MdGasUsage{Execution: execution}}
}

func (g *runtimeGasAccounting) refillTopLevelState(gasRemaining *mdgas.MdGas, restoreState bool, vmerr error, tracer *tracing.Hooks) {
	RefillTopLevelGas(gasRemaining, &g.topLevel, restoreState, vmerr, tracer)
}

func (g *runtimeGasAccounting) finishFrame(gas, gasRemaining mdgas.MdGas, vmerr error) {
	if vmerr == nil {
		return
	}
	g.frame.State = 0
	g.frame.Execution = gas.Total() - gasRemaining.Total()
}

// HandleRuntimeCall applies destination setup that precedes a transaction's
// first call frame.
func HandleRuntimeCall(evm *vm.EVM, destination accounts.Address, value uint256.Int, gasRemaining mdgas.MdGas) (mdgas.MdGas, mdgas.MdGasUsage, error) {
	var gasUsed mdgas.MdGasUsage
	rules := evm.ChainRules()
	ibs := evm.IntraBlockState()

	if rules.IsAmsterdam {
		ibs.MarkAddressAccess(destination, false)
		if !value.IsZero() {
			empty, err := ibs.Empty(destination)
			if err != nil {
				return gasRemaining, gasUsed, fmt.Errorf("%w: %w", vm.ErrIntraBlockStateFailed, err)
			}
			if empty && !consumeGas(&gasRemaining, &gasUsed, params.StateGasNewAccount, mdgas.StateGas, evm.Config().Tracer, tracing.GasChangeCallNewAccount) {
				return gasRemaining, gasUsed, vm.ErrRuntimeOutOfGas
			}
		}
		if slices.Contains(vm.ActivePrecompiles(rules), destination) {
			return gasRemaining, gasUsed, nil
		}
	}
	if !rules.IsPrague {
		return gasRemaining, gasUsed, nil
	}

	delegatedTo, delegated, err := ibs.GetDelegatedDesignation(destination)
	if err != nil {
		return gasRemaining, gasUsed, fmt.Errorf("%w: %w", vm.ErrIntraBlockStateFailed, err)
	}
	if !delegated {
		return gasRemaining, gasUsed, nil
	}
	if rules.IsAmsterdam {
		accessCost := params.ColdAccountAccessCostEIP8038
		if ibs.AddressInAccessList(delegatedTo) {
			accessCost = params.WarmStorageReadCostEIP2929
		}
		if !consumeGas(&gasRemaining, &gasUsed, accessCost, mdgas.ExecutionGas, evm.Config().Tracer, tracing.GasChangeDelegatedDesignation) {
			return gasRemaining, gasUsed, vm.ErrRuntimeOutOfGas
		}
	}
	ibs.AddAddressToAccessList(delegatedTo)
	return gasRemaining, gasUsed, nil
}

func HandleRuntimeCreate(evm *vm.EVM, destination accounts.Address, gasRemaining mdgas.MdGas) (mdgas.MdGas, mdgas.MdGasUsage, error) {
	var gasUsed mdgas.MdGasUsage
	if !evm.ChainRules().IsAmsterdam {
		return gasRemaining, gasUsed, nil
	}
	ibs := evm.IntraBlockState()
	ibs.MarkAddressAccess(destination, false)
	empty, err := ibs.Empty(destination)
	if err != nil {
		return gasRemaining, gasUsed, fmt.Errorf("%w: %w", vm.ErrIntraBlockStateFailed, err)
	}
	if empty && !consumeGas(&gasRemaining, &gasUsed, params.StateGasNewAccount, mdgas.StateGas, evm.Config().Tracer, tracing.GasChangeCallNewAccount) {
		return gasRemaining, gasUsed, vm.ErrRuntimeOutOfGas
	}
	return gasRemaining, gasUsed, nil
}

func RefillTopLevelGas(gasRemaining *mdgas.MdGas, gasUsed *mdgas.MdGasUsage, restoreState bool, vmerr error, tracer *tracing.Hooks) {
	if gasUsed.State <= 0 || vmerr == nil && !restoreState {
		return
	}
	spill := gasUsed.StateSpill
	refillGas(gasRemaining, gasUsed, uint64(gasUsed.State), mdgas.StateGas, tracer, tracing.GasChangeRefundAccountCreation)
	if errors.Is(vmerr, vm.ErrExecutionReverted) || spill == 0 {
		return
	}
	if !consumeGas(gasRemaining, gasUsed, spill, mdgas.ExecutionGas, tracer, tracing.GasChangeCallFailedExecution) {
		panic("refilled state-gas spill exceeds execution gas")
	}
}

func consumeGas(remaining *mdgas.MdGas, used *mdgas.MdGasUsage, amount uint64, typ mdgas.MdGasType, tracer *tracing.Hooks, reason tracing.GasChangeReason) bool {
	if !tracer.HasGasChangeHook() {
		return mdgas.Consume(remaining, used, amount, typ)
	}
	old := *remaining
	if !mdgas.Consume(remaining, used, amount, typ) {
		return false
	}
	tracer.EmitGasChange(old, *remaining, reason)
	return true
}

func refillGas(remaining *mdgas.MdGas, used *mdgas.MdGasUsage, amount uint64, typ mdgas.MdGasType, tracer *tracing.Hooks, reason tracing.GasChangeReason) {
	if !tracer.HasGasChangeHook() {
		mdgas.Refill(remaining, used, amount, typ)
		return
	}
	old := *remaining
	mdgas.Refill(remaining, used, amount, typ)
	tracer.EmitGasChange(old, *remaining, reason)
}

func HandleRuntimeFailure(evm *vm.EVM, typ vm.OpCode, sender, recipient accounts.Address, input []byte, startGas mdgas.MdGas, gasRemaining *mdgas.MdGas, value uint256.Int, err error) {
	tracer := evm.Config().Tracer
	gasTracing := tracer.HasGasChangeHook()
	if tracer.HasEnterHook() {
		precompile := typ == vm.CALL && slices.Contains(vm.ActivePrecompiles(evm.ChainRules()), recipient)
		tracer.EmitEnter(0, byte(typ), sender, recipient, precompile, input, startGas, value, nil)
	}
	var old mdgas.MdGas
	if gasTracing {
		old = *gasRemaining
		tracer.EmitGasChange(mdgas.MdGas{}, old, tracing.GasChangeCallInitialBalance)
	}
	*gasRemaining = mdgas.MdGas{State: startGas.State}
	if gasTracing {
		tracer.EmitGasChange(old, *gasRemaining, tracing.GasChangeCallFailedExecution)
		if *gasRemaining != (mdgas.MdGas{}) {
			tracer.EmitGasChange(*gasRemaining, mdgas.MdGas{}, tracing.GasChangeCallLeftOverReturned)
		}
	}
	if tracer.HasExitHook() {
		tracer.EmitExit(0, nil, startGas, *gasRemaining, vm.VMErrorFromErr(err), true)
	}
}
