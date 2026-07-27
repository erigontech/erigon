// Copyright 2017 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
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

func memoryKeccak256(callContext *CallContext) (uint64, bool) {
	return calcMemSize64(callContext.Stack.back2(0, 1))
}

func memoryCallDataCopy(callContext *CallContext) (uint64, bool) {
	return calcMemSize64(callContext.Stack.back2(0, 2))
}

func memoryReturnDataCopy(callContext *CallContext) (uint64, bool) {
	return calcMemSize64(callContext.Stack.back2(0, 2))
}

func memoryCodeCopy(callContext *CallContext) (uint64, bool) {
	return calcMemSize64(callContext.Stack.back2(0, 2))
}

func memoryExtCodeCopy(callContext *CallContext) (uint64, bool) {
	return calcMemSize64(callContext.Stack.back2(1, 3))
}

func memoryMLoad(callContext *CallContext) (uint64, bool) {
	return calcMemSize64WithUint(callContext.Stack.back(0), 32)
}

func memoryMStore8(callContext *CallContext) (uint64, bool) {
	return calcMemSize64WithUint(callContext.Stack.back(0), 1)
}

func memoryMStore(callContext *CallContext) (uint64, bool) {
	return calcMemSize64WithUint(callContext.Stack.back(0), 32)
}

func memoryMcopy(callContext *CallContext) (uint64, bool) {
	dst, src, length := callContext.Stack.back3(0, 1, 2)
	mStart := dst
	if src.Gt(mStart) {
		mStart = src
	}
	return calcMemSize64(mStart, length)
}

func memoryCreate(callContext *CallContext) (uint64, bool) {
	return calcMemSize64(callContext.Stack.back2(1, 2))
}

func memoryCreate2(callContext *CallContext) (uint64, bool) {
	return calcMemSize64(callContext.Stack.back2(1, 2))
}

func memoryCall(callContext *CallContext) (uint64, bool) {
	x, overflow := calcMemSize64(callContext.Stack.back2(5, 6))
	if overflow {
		return 0, true
	}
	y, overflow := calcMemSize64(callContext.Stack.back2(3, 4))
	if overflow {
		return 0, true
	}
	return max(x, y), false
}
func memoryDelegateCall(callContext *CallContext) (uint64, bool) {
	x, overflow := calcMemSize64(callContext.Stack.back2(4, 5))
	if overflow {
		return 0, true
	}
	y, overflow := calcMemSize64(callContext.Stack.back2(2, 3))
	if overflow {
		return 0, true
	}
	return max(x, y), false
}

var memoryStaticCall = memoryDelegateCall

func memoryReturn(callContext *CallContext) (uint64, bool) {
	return calcMemSize64(callContext.Stack.back2(0, 1))
}

func memoryRevert(callContext *CallContext) (uint64, bool) {
	return calcMemSize64(callContext.Stack.back2(0, 1))
}

func memoryLog(callContext *CallContext) (uint64, bool) {
	return calcMemSize64(callContext.Stack.back2(0, 1))
}
