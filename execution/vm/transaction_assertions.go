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
	"fmt"

	"github.com/erigontech/erigon/execution/types"
)

func inAssert(evm *EVM) bool {
	return evm.FrameContext != nil && evm.FrameContext.Transaction.Frames[evm.FrameContext.Index].Mode == types.FrameAssert
}

// Assertion read errors must survive call rollback and reach the transaction
// executor even when a parent catches the failed EVM call.
func (evm *EVM) assertionReadError(err error) error {
	err = fmt.Errorf("%w: %w", ErrIntraBlockStateFailed, err)
	evm.IntraBlockState().RecordStateReadError(err)
	return err
}

func opTxTrace(pc uint64, evm *EVM, call *CallContext) (uint64, []byte, error) {
	if !inAssert(evm) {
		return pc, nil, ErrFrameContext
	}
	param, index := call.Stack.pop1Peek1()
	if !param.IsUint64() || param.Uint64() > 0x1a || !index.IsUint64() {
		return pc, nil, ErrFrameParameter
	}
	selector, entryIndex := param.Uint64(), index.Uint64()
	if (selector <= 3 || selector == 0x11 || selector >= 0x19) && entryIndex != 0 {
		return pc, nil, ErrFrameParameter
	}
	ctx := evm.FrameContext
	if selector == 0x19 {
		index.Set(&ctx.MaxCost)
		return pc, nil, nil
	}
	if selector == 0x1a {
		address := ctx.Payer.Value()
		index.SetBytes(address[:])
		return pc, nil, nil
	}
	trace, err := ctx.TransactionTrace(evm.IntraBlockState())
	if err != nil {
		return pc, nil, evm.assertionReadError(err)
	}
	switch selector {
	case 0:
		index.SetUint64(uint64(len(trace.Balances)))
	case 1:
		index.SetUint64(uint64(len(trace.Slots)))
	case 2:
		index.SetUint64(uint64(len(trace.Codes)))
	case 3:
		index.SetUint64(uint64(len(trace.Nonces)))
	case 4, 5, 6:
		if entryIndex >= uint64(len(trace.Balances)) {
			return pc, nil, ErrFrameParameter
		}
		entry := &trace.Balances[entryIndex]
		switch selector {
		case 4:
			address := entry.Address.Value()
			index.SetBytes(address[:])
		case 5:
			index.Set(&entry.Before)
		case 6:
			index.Set(&entry.After)
		}
	case 7, 8, 9, 10:
		if entryIndex >= uint64(len(trace.Slots)) {
			return pc, nil, ErrFrameParameter
		}
		entry := &trace.Slots[entryIndex]
		switch selector {
		case 7:
			address := entry.Address.Value()
			index.SetBytes(address[:])
		case 8:
			index.Set(&entry.Before)
		case 9:
			index.Set(&entry.After)
		case 10:
			key := entry.Key.Value()
			index.SetBytes(key[:])
		}
	case 11, 12, 13:
		if entryIndex >= uint64(len(trace.Codes)) {
			return pc, nil, ErrFrameParameter
		}
		entry := &trace.Codes[entryIndex]
		switch selector {
		case 11:
			address := entry.Address.Value()
			index.SetBytes(address[:])
		case 12:
			hash := entry.Before.Value()
			index.SetBytes(hash[:])
		case 13:
			hash := entry.After.Value()
			index.SetBytes(hash[:])
		}
	case 14, 15, 16:
		if entryIndex >= uint64(len(trace.Nonces)) {
			return pc, nil, ErrFrameParameter
		}
		entry := &trace.Nonces[entryIndex]
		switch selector {
		case 14:
			address := entry.Address.Value()
			index.SetBytes(address[:])
		case 15:
			index.SetUint64(entry.Before)
		case 16:
			index.SetUint64(entry.After)
		}
	case 17:
		index.SetUint64(uint64(len(trace.Events)))
	default:
		if entryIndex >= uint64(len(trace.Events)) {
			return pc, nil, ErrFrameParameter
		}
		event := trace.Events[entryIndex]
		switch selector {
		case 18:
			index.SetBytes(event.Address[:])
		case 19:
			index.SetUint64(uint64(len(event.Topics)))
		case 24:
			index.SetUint64(uint64(len(event.Data)))
		default:
			topic := selector - 20
			if topic >= uint64(len(event.Topics)) {
				return pc, nil, ErrFrameParameter
			}
			index.SetBytes(event.Topics[topic][:])
		}
	}
	return pc, nil, nil
}

func opEventDataCopy(pc uint64, evm *EVM, call *CallContext) (uint64, []byte, error) {
	if !inAssert(evm) {
		return pc, nil, ErrFrameContext
	}
	eventIndex, memoryOffset := call.Stack.pop2()
	dataOffset, length := call.Stack.pop2()
	trace, err := evm.FrameContext.TransactionTrace(evm.IntraBlockState())
	if err != nil {
		return pc, nil, evm.assertionReadError(err)
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
