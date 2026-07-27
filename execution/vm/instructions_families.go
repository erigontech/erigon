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
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/types"
)

// Named per-index variants of the DUP/PUSH/LOG families. Naming each table
// slot (instead of filling it from a factory closure) lets the dispatch
// generator verify the slots across fork tables and call or inline them
// directly, and gives profilers distinct symbols per opcode.

func opDup1(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	scope.Stack.dup(0)
	return pc, nil, nil
}

func opDup2(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	scope.Stack.dup(1)
	return pc, nil, nil
}

func opDup3(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	scope.Stack.dup(2)
	return pc, nil, nil
}

func opDup4(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	scope.Stack.dup(3)
	return pc, nil, nil
}

func opDup5(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	scope.Stack.dup(4)
	return pc, nil, nil
}

func opDup6(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	scope.Stack.dup(5)
	return pc, nil, nil
}

func opDup7(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	scope.Stack.dup(6)
	return pc, nil, nil
}

func opDup8(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	scope.Stack.dup(7)
	return pc, nil, nil
}

func opDup9(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	scope.Stack.dup(8)
	return pc, nil, nil
}

func opDup10(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	scope.Stack.dup(9)
	return pc, nil, nil
}

func opDup11(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	scope.Stack.dup(10)
	return pc, nil, nil
}

func opDup12(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	scope.Stack.dup(11)
	return pc, nil, nil
}

func opDup13(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	scope.Stack.dup(12)
	return pc, nil, nil
}

func opDup14(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	scope.Stack.dup(13)
	return pc, nil, nil
}

func opDup15(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	scope.Stack.dup(14)
	return pc, nil, nil
}

func opDup16(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	scope.Stack.dup(15)
	return pc, nil, nil
}

func pushN(pc uint64, scope *CallContext, size uint64, pushByteSize int) (uint64, []byte, error) {
	codeLen := len(scope.Contract.Code)

	startMin := min(int(pc+1), codeLen)
	endMin := min(startMin+pushByteSize, codeLen)

	integer := scope.Stack.pushRef()
	integer.SetBytes(scope.Contract.Code[startMin:endMin])
	// Missing bytes: pushByteSize - len(pushData)
	if missing := pushByteSize - (endMin - startMin); missing > 0 {
		integer.ILsh(uint(8 * missing))
	}

	pc += size
	return pc, nil, nil
}

func opPush3(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	return pushN(pc, scope, 3, 3)
}

func opPush4(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	return pushN(pc, scope, 4, 4)
}

func opPush5(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	return pushN(pc, scope, 5, 5)
}

func opPush6(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	return pushN(pc, scope, 6, 6)
}

func opPush7(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	return pushN(pc, scope, 7, 7)
}

func opPush8(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	return pushN(pc, scope, 8, 8)
}

func opPush9(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	return pushN(pc, scope, 9, 9)
}

func opPush10(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	return pushN(pc, scope, 10, 10)
}

func opPush11(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	return pushN(pc, scope, 11, 11)
}

func opPush12(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	return pushN(pc, scope, 12, 12)
}

func opPush13(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	return pushN(pc, scope, 13, 13)
}

func opPush14(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	return pushN(pc, scope, 14, 14)
}

func opPush15(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	return pushN(pc, scope, 15, 15)
}

func opPush16(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	return pushN(pc, scope, 16, 16)
}

func opPush17(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	return pushN(pc, scope, 17, 17)
}

func opPush18(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	return pushN(pc, scope, 18, 18)
}

func opPush19(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	return pushN(pc, scope, 19, 19)
}

func opPush20(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	return pushN(pc, scope, 20, 20)
}

func opPush21(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	return pushN(pc, scope, 21, 21)
}

func opPush22(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	return pushN(pc, scope, 22, 22)
}

func opPush23(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	return pushN(pc, scope, 23, 23)
}

func opPush24(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	return pushN(pc, scope, 24, 24)
}

func opPush25(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	return pushN(pc, scope, 25, 25)
}

func opPush26(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	return pushN(pc, scope, 26, 26)
}

func opPush27(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	return pushN(pc, scope, 27, 27)
}

func opPush28(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	return pushN(pc, scope, 28, 28)
}

func opPush29(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	return pushN(pc, scope, 29, 29)
}

func opPush30(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	return pushN(pc, scope, 30, 30)
}

func opPush31(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	return pushN(pc, scope, 31, 31)
}

func opPush32(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	return pushN(pc, scope, 32, 32)
}

func logN(pc uint64, evm *EVM, scope *CallContext, size int) (uint64, []byte, error) {
	if evm.readOnly {
		return pc, nil, ErrWriteProtection
	}
	topics := make([]common.Hash, size)
	stack := &scope.Stack
	mStart, mSize := stack.pop2Uint64()
	for i := range size {
		topics[i] = stack.pop().Bytes32()
	}

	d := scope.Memory.GetCopy(mStart, mSize)
	evm.IntraBlockState().AddLog(&types.Log{
		Address: scope.Contract.Address().Value(),
		Topics:  topics,
		Data:    d,
		// This is a non-consensus field, but assigned here because
		// execution/state doesn't know the current block number.
		BlockNumber: hexutil.Uint64(evm.Context.BlockNumber),
	})

	return pc, nil, nil
}

func opLog0(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	return logN(pc, evm, scope, 0)
}

func opLog1(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	return logN(pc, evm, scope, 1)
}

func opLog2(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	return logN(pc, evm, scope, 2)
}

func opLog3(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	return logN(pc, evm, scope, 3)
}

func opLog4(pc uint64, evm *EVM, scope *CallContext) (uint64, []byte, error) {
	return logN(pc, evm, scope, 4)
}
