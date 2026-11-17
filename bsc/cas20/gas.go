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

package cas20

import (
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/protocol/params"
)

// Every charge mirrors an existing EVM cost function, so nothing a CAS20 call does
// is cheaper than the same work through bytecode (BEP-702 3.14). Opcode, memory
// and call overhead are not synthesized: the table there is exhaustive.
//
// No CAS20 address is warm by default (a transaction access list still applies).
// That holds because they are absent from the static precompile map, and nothing
// here would notice a refactor that added them.

const cas20CalldataWordGas = params.CopyGas + params.MemoryGas

// chargeInternalDispatch is what the reference contract pays to route one entry of
// an announce bundle or an initCalls array: a warm CALL plus the copy of that
// entry into the callee's input. Without it a shared tail could be dispatched N×M
// times for the price of one.
func (ctx *PrecompileContext) chargeInternalDispatch(call []byte) bool {
	words := (uint64(len(call)) + 31) / 32
	return ctx.chargeGas(params.WarmStorageReadCostEIP2929 + words*cas20CalldataWordGas)
}

func (ctx *PrecompileContext) chargeCalldata(input []byte) bool {
	words := (uint64(len(input)) + 31) / 32
	if words == 0 {
		return true
	}
	return ctx.chargeGas(words * cas20CalldataWordGas)
}

func (ctx *PrecompileContext) chargeKeccak(size int) bool {
	words := (uint64(size) + 31) / 32
	return ctx.chargeGas(params.Keccak256Gas + params.Keccak256WordGas*words)
}

func (ctx *PrecompileContext) chargeLog(topics int, dataLen int) bool {
	return ctx.chargeGas(params.LogGas +
		params.LogTopicGas*uint64(topics) +
		params.LogDataGas*uint64(dataLen))
}

func (ctx *PrecompileContext) chargeAccountAccess(addr common.Address) bool {
	if ctx.StateDB.AddressInAccessList(addr) {
		return ctx.chargeGas(params.WarmStorageReadCostEIP2929)
	}
	// Warmed before the charge, in the interpreter's order; the addition is journalled.
	ctx.StateDB.AddAddressToAccessList(addr)
	return ctx.chargeGas(params.ColdAccountAccessCostEIP2929)
}

// The creation cost is owed even at a prefunded address: balance alone does not
// make an account a contract.
func (ctx *PrecompileContext) chargeCodeWrite(addr common.Address, code []byte) bool {
	if ctx.ReadOnly {
		ctx.markWriteProtected()
		return false
	}
	cost := params.CreateDataGas * uint64(len(code))
	if hadNoCode(ctx.StateDB, addr) {
		cost += params.CreateGas
	}
	return ctx.chargeGas(cost) && ctx.chargeKeccak(len(code))
}

// EIP-2200's reentrancy guard, which is what makes transfer()/send() safe: a CAS20
// token writes state without SSTORE, so it applies the check itself.
func (ctx *PrecompileContext) sstoreSentry() bool {
	return ctx.gas.Execution > params.SstoreSentryGasEIP2200
}

// Mirrors makeGasSStoreFunc, arms in the same order with the same clause numbers.
func (s cas20Storage) chargeStorageWrite(slot, value common.Hash) bool {
	if s.ctx == nil {
		return true
	}
	if s.ctx.ReadOnly {
		s.ctx.markWriteProtected()
		return false
	}
	if !s.ctx.sstoreSentry() {
		s.ctx.markOutOfGas()
		return false
	}
	var (
		current, original = s.state.GetStateAndCommittedState(s.token, slot)
		clearingRefund    = params.SstoreClearsScheduleRefundEIP3529
		cost              = uint64(0)
	)
	if _, slotPresent := s.state.SlotInAccessList(s.token, slot); !slotPresent {
		cost = params.ColdSloadCostEIP2929
		s.state.AddSlotToAccessList(s.token, slot)
	}
	switch {
	case current == value: // noop (1)
		return s.ctx.chargeGas(cost + params.WarmStorageReadCostEIP2929)

	case original == current:
		if original == (common.Hash{}) { // create slot (2.1.1)
			return s.ctx.chargeGas(cost + params.SstoreSetGasEIP2200)
		}
		if value == (common.Hash{}) { // delete slot (2.1.2b)
			s.state.AddRefund(clearingRefund)
		}
		// write existing slot (2.1.2)
		return s.ctx.chargeGas(cost + (params.SstoreResetGasEIP2200 - params.ColdSloadCostEIP2929))
	}

	// dirty slot (2.2)
	if original != (common.Hash{}) {
		if current == (common.Hash{}) { // recreate slot (2.2.1.1)
			s.state.SubRefund(clearingRefund)
		} else if value == (common.Hash{}) { // delete slot (2.2.1.2)
			s.state.AddRefund(clearingRefund)
		}
	}
	if original == value {
		if original == (common.Hash{}) { // reset to original inexistent slot (2.2.2.1)
			s.state.AddRefund(params.SstoreSetGasEIP2200 - params.WarmStorageReadCostEIP2929)
		} else { // reset to original existing slot (2.2.2.2)
			s.state.AddRefund((params.SstoreResetGasEIP2200 - params.ColdSloadCostEIP2929) - params.WarmStorageReadCostEIP2929)
		}
	}
	// dirty update (2.2)
	return s.ctx.chargeGas(cost + params.WarmStorageReadCostEIP2929)
}
