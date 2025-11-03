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
	return calcMemSize64(callContext.Stack.Back(0), callContext.Stack.Back(1))
}

func memoryCallDataCopy(callContext *CallContext) (uint64, bool) {
	return calcMemSize64(callContext.Stack.Back(0), callContext.Stack.Back(2))
}

func memoryReturnDataCopy(callContext *CallContext) (uint64, bool) {
	return calcMemSize64(callContext.Stack.Back(0), callContext.Stack.Back(2))
}

func memoryCodeCopy(callContext *CallContext) (uint64, bool) {
	return calcMemSize64(callContext.Stack.Back(0), callContext.Stack.Back(2))
}

func memoryExtCodeCopy(callContext *CallContext) (uint64, bool) {
	return calcMemSize64(callContext.Stack.Back(1), callContext.Stack.Back(3))
}

func memoryMLoad(callContext *CallContext) (uint64, bool) {
	return calcMemSize64WithUint(callContext.Stack.Back(0), 32)
}

func memoryMStore8(callContext *CallContext) (uint64, bool) {
	return calcMemSize64WithUint(callContext.Stack.Back(0), 1)
}

func memoryMStore(callContext *CallContext) (uint64, bool) {
	return calcMemSize64WithUint(callContext.Stack.Back(0), 32)
}

func memoryMcopy(callContext *CallContext) (uint64, bool) {
	mStart := callContext.Stack.Back(0) // stack[0]: dest
	if callContext.Stack.Back(1).Gt(mStart) {
		mStart = callContext.Stack.Back(1) // stack[1]: source
	}
	return calcMemSize64(mStart, callContext.Stack.Back(2)) // stack[2]: length
}

func memoryCreate(callContext *CallContext) (uint64, bool) {
	return calcMemSize64(callContext.Stack.Back(1), callContext.Stack.Back(2))
}

func memoryCreate2(callContext *CallContext) (uint64, bool) {
	return calcMemSize64(callContext.Stack.Back(1), callContext.Stack.Back(2))
}

func memoryCall(callContext *CallContext) (uint64, bool) {
	x, overflow := calcMemSize64(callContext.Stack.Back(5), callContext.Stack.Back(6))
	if overflow {
		return 0, true
	}
	y, overflow := calcMemSize64(callContext.Stack.Back(3), callContext.Stack.Back(4))
	if overflow {
		return 0, true
	}
	return max(x, y), false
}
func memoryDelegateCall(callContext *CallContext) (uint64, bool) {
	x, overflow := calcMemSize64(callContext.Stack.Back(4), callContext.Stack.Back(5))
	if overflow {
		return 0, true
	}
	y, overflow := calcMemSize64(callContext.Stack.Back(2), callContext.Stack.Back(3))
	if overflow {
		return 0, true
	}
	return max(x, y), false
}

func memoryStaticCall(callContext *CallContext) (uint64, bool) {
	x, overflow := calcMemSize64(callContext.Stack.Back(4), callContext.Stack.Back(5))
	if overflow {
		return 0, true
	}
	y, overflow := calcMemSize64(callContext.Stack.Back(2), callContext.Stack.Back(3))
	if overflow {
		return 0, true
	}
	return max(x, y), false
}

func memoryReturn(callContext *CallContext) (uint64, bool) {
	return calcMemSize64(callContext.Stack.Back(0), callContext.Stack.Back(1))
}

func memoryRevert(callContext *CallContext) (uint64, bool) {
	return calcMemSize64(callContext.Stack.Back(0), callContext.Stack.Back(1))
}

func memoryLog(callContext *CallContext) (uint64, bool) {
	return calcMemSize64(callContext.Stack.Back(0), callContext.Stack.Back(1))
}
