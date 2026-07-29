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
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
)

// PrepareTopLevelCall applies destination setup that precedes a transaction's
// first call frame.
func PrepareTopLevelCall(evm *vm.EVM, destination accounts.Address, value uint256.Int, gasRemaining mdgas.MdGas) (mdgas.MdGas, mdgas.MdGasUsage, error) {
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
			if empty && !mdgas.Consume(&gasRemaining, &gasUsed, params.StateGasNewAccount, mdgas.StateGas) {
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
		if !mdgas.Consume(&gasRemaining, &gasUsed, accessCost, mdgas.RegularGas) {
			return gasRemaining, gasUsed, vm.ErrRuntimeOutOfGas
		}
	}
	ibs.AddAddressToAccessList(delegatedTo)
	return gasRemaining, gasUsed, nil
}

// RefillTopLevelCallGas reverses a new-account state-gas charge when the call
// does not persist the account.
func RefillTopLevelCallGas(gasRemaining *mdgas.MdGas, gasUsed *mdgas.MdGasUsage, restoreState bool, vmerr error) {
	if gasUsed.State <= 0 || vmerr == nil && !restoreState {
		return
	}
	spill := gasUsed.StateSpill
	mdgas.Refill(gasRemaining, gasUsed, uint64(gasUsed.State), mdgas.StateGas)
	if !errors.Is(vmerr, vm.ErrExecutionReverted) && !mdgas.Consume(gasRemaining, gasUsed, spill, mdgas.RegularGas) {
		panic("refilled state-gas spill exceeds regular gas")
	}
}

// TraceTopLevelCallFailure emits the root call frame when setup fails before
// EVM.Call can emit it.
func TraceTopLevelCallFailure(evm *vm.EVM, sender, recipient accounts.Address, input []byte, gas mdgas.MdGas, value uint256.Int, err error) {
	tracer := evm.Config().Tracer
	if tracer == nil {
		return
	}
	precompile := slices.Contains(vm.ActivePrecompiles(evm.ChainRules()), recipient)
	if tracer.OnEnter != nil {
		tracer.OnEnter(0, byte(vm.CALL), sender, recipient, precompile, input, gas.Regular, value, nil)
	}
	if tracer.OnExit != nil {
		tracer.OnExit(0, nil, gas.Regular, vm.VMErrorFromErr(err), true)
	}
}
