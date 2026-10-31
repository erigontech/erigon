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
	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func inPostTx(evm *EVM) bool {
	return evm.FrameContext != nil && evm.FrameContext.Transaction.Frames[evm.FrameContext.Index].Mode == types.FramePostTx
}

func (evm *EVM) prestateStorage(address accounts.Address, key accounts.StorageKey) (uint256.Int, error) {
	if !inPostTx(evm) {
		return uint256.Int{}, ErrFrameContext
	}
	trace, err := evm.FrameContext.TransactionTrace(evm.IntraBlockState())
	if err != nil {
		return uint256.Int{}, err
	}
	return trace.Storage(evm.IntraBlockState(), address, key, true)
}

func opTxTrace(pc uint64, evm *EVM, call *CallContext) (uint64, []byte, error) {
	if !inPostTx(evm) {
		return pc, nil, ErrFrameContext
	}
	param, index := call.Stack.pop1Peek1()
	if !param.IsUint64() || param.Uint64() > 0x15 || !index.IsUint64() {
		return pc, nil, ErrFrameParameter
	}
	p, i := param.Uint64(), index.Uint64()
	if (p <= 2 || p == 0x0c || p >= 0x14) && i != 0 {
		return pc, nil, ErrFrameParameter
	}
	ctx := evm.FrameContext
	if p == 0x14 {
		index.Set(&ctx.MaxCost)
		return pc, nil, nil
	}
	if p == 0x15 {
		address := ctx.Payer.Value()
		index.SetBytes(address[:])
		return pc, nil, nil
	}
	trace, err := ctx.TransactionTrace(evm.IntraBlockState())
	if err != nil {
		return pc, nil, err
	}
	switch {
	case p == 0:
		index.SetUint64(uint64(len(trace.Balances)))
	case p == 1:
		index.SetUint64(uint64(len(trace.Slots)))
	case p == 2:
		index.SetUint64(uint64(len(trace.Deployed)))
	case p <= 5:
		if i >= uint64(len(trace.Balances)) {
			return pc, nil, ErrFrameParameter
		}
		entry := &trace.Balances[i]
		switch p {
		case 3:
			address := entry.Address.Value()
			index.SetBytes(address[:])
		case 4:
			index.Set(&entry.Before)
		case 5:
			index.Set(&entry.After)
		}
	case p <= 9:
		if i >= uint64(len(trace.Slots)) {
			return pc, nil, ErrFrameParameter
		}
		entry := &trace.Slots[i]
		switch p {
		case 6:
			address := entry.Address.Value()
			index.SetBytes(address[:])
		case 7:
			key := entry.Key.Value()
			index.SetBytes(key[:])
		case 8:
			index.Set(&entry.Before)
		case 9:
			index.Set(&entry.After)
		}
	case p <= 0x0b:
		if i >= uint64(len(trace.Deployed)) {
			return pc, nil, ErrFrameParameter
		}
		entry := &trace.Deployed[i]
		if p == 0x0a {
			address := entry.Address.Value()
			index.SetBytes(address[:])
		} else {
			hash := entry.After.Value()
			index.SetBytes(hash[:])
		}
	case p == 0x0c:
		index.SetUint64(uint64(len(trace.Events)))
	default:
		if i >= uint64(len(trace.Events)) {
			return pc, nil, ErrFrameParameter
		}
		event := trace.Events[i]
		switch p {
		case 0x0d:
			index.SetBytes(event.Address[:])
		case 0x0e:
			index.SetUint64(uint64(len(event.Topics)))
		case 0x13:
			index.SetUint64(uint64(len(event.Data)))
		default:
			topic := p - 0x0f
			if topic >= uint64(len(event.Topics)) {
				return pc, nil, ErrFrameParameter
			}
			index.SetBytes(event.Topics[topic][:])
		}
	}
	return pc, nil, nil
}

func gasTxDiff(evm *EVM, call *CallContext, _ mdgas.MdGas, _ uint64) (mdgas.MdGasCost, error) {
	if !inPostTx(evm) {
		return mdgas.MdGasCost{}, ErrFrameContext
	}
	param, key, index := call.Stack.back3(0, 1, 2)
	if !param.IsUint64() || param.Uint64() > 0x0c {
		return mdgas.MdGasCost{}, ErrFrameParameter
	}
	p := param.Uint64()
	if p >= 2 && p != 7 && p != 9 && p != 0x0c && !index.IsZero() {
		return mdgas.MdGasCost{}, ErrFrameParameter
	}
	ibs := evm.IntraBlockState()
	cost := uint64(params.WarmStorageReadCostEIP2929)
	if p <= 1 {
		address, slot := ibs.InternAddress(key), ibs.InternStorageKey(index)
		if _, warm := ibs.SlotInAccessList(address, slot); !warm {
			cost = coldStorageAccessCost(evm.chainRules)
		}
	} else if p <= 5 && !ibs.AddressInAccessList(ibs.InternAddress(key)) {
		cost = coldAccountAccessCost(evm.chainRules)
	}
	return mdgas.MdGasCost{Execution: cost}, nil
}

func opTxDiff(pc uint64, evm *EVM, call *CallContext) (uint64, []byte, error) {
	// The gas calculator checks the mode and reserved inputs before any state
	// access. Execution starts only after the full access cost was charged.
	param, key, index := call.Stack.pop2Peek1()
	p := param.Uint64()
	ibs := evm.IntraBlockState()
	trace, err := evm.FrameContext.TransactionTrace(ibs)
	if err != nil {
		return pc, nil, err
	}
	if p <= 5 {
		address := ibs.InternAddress(key)
		ibs.MarkAddressAccess(address, false)
		if p <= 1 {
			slot := ibs.InternStorageKey(index)
			ibs.AddSlotOnlyToAccessList(address, slot)
			// Read even when the before-value is in the diff: TXDIFF records a
			// storage access in the BAL, unlike TXTRACE's enumeration.
			value, err := ibs.GetState(address, slot)
			if err == nil && p == 0 {
				value, err = trace.Storage(ibs, address, slot, true)
			}
			index.Set(&value)
			return pc, nil, err
		}
		ibs.AddAddressToAccessList(address)
		if p <= 3 {
			value, err := trace.Balance(ibs, address, p == 2)
			index.Set(&value)
			return pc, nil, err
		}
		hash, err := trace.CodeHash(ibs, address, p == 4)
		value := hash.Value()
		index.SetBytes(value[:])
		return pc, nil, err
	}
	var indices []int
	switch p {
	case 6, 7:
		indices = trace.AddressSlots[ibs.InternAddress(key)]
	case 8, 9:
		indices = trace.AddressEvents[ibs.InternAddress(key)]
	case 0x0a:
		index.SetUint64(trace.AccountFlags[ibs.InternAddress(key)])
		return pc, nil, nil
	case 0x0b, 0x0c:
		indices = trace.TopicEvents[key.Bytes32()]
	}
	if p == 6 || p == 8 || p == 0x0b {
		index.SetUint64(uint64(len(indices)))
	} else {
		if !index.IsUint64() || index.Uint64() >= uint64(len(indices)) {
			return pc, nil, ErrFrameParameter
		}
		index.SetUint64(uint64(indices[index.Uint64()]))
	}
	return pc, nil, nil
}

func opEventDataCopy(pc uint64, evm *EVM, call *CallContext) (uint64, []byte, error) {
	if !inPostTx(evm) {
		return pc, nil, ErrFrameContext
	}
	eventIndex, memoryOffset := call.Stack.pop2()
	dataOffset, length := call.Stack.pop2()
	trace, err := evm.FrameContext.TransactionTrace(evm.IntraBlockState())
	if err != nil {
		return pc, nil, err
	}
	if !eventIndex.IsUint64() || eventIndex.Uint64() >= uint64(len(trace.Events)) || !dataOffset.IsUint64() || !length.IsUint64() {
		return pc, nil, ErrFrameParameter
	}
	data := trace.Events[eventIndex.Uint64()].Data
	offset, size := dataOffset.Uint64(), length.Uint64()
	if offset > uint64(len(data)) || size > uint64(len(data))-offset {
		return pc, nil, ErrFrameParameter
	}
	call.Memory.Set(memoryOffset.Uint64(), size, data[offset:offset+size])
	return pc, nil, nil
}
