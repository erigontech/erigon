// Copyright 2017 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package vm

import "github.com/ledgerwatch/turbo-geth/core/vm/stack"

func memorySha3(stack *stack.Stack) (uint64, bool) {
	return calcMemSize64(stack.Back(0), stack.Back(1))
}

func memoryCallDataCopy(stack *stack.Stack) (uint64, bool) {
	return calcMemSize64(stack.Back(0), stack.Back(2))
}

func memoryReturnDataCopy(stack *stack.Stack) (uint64, bool) {
	return calcMemSize64(stack.Back(0), stack.Back(2))
}

func memoryCodeCopy(stack *stack.Stack) (uint64, bool) {
	return calcMemSize64(stack.Back(0), stack.Back(2))
}

func memoryExtCodeCopy(stack *stack.Stack) (uint64, bool) {
	return calcMemSize64(stack.Back(1), stack.Back(3))
}

func memoryMLoad(stack *stack.Stack) (uint64, bool) {
	return calcMemSize64WithUint(stack.Back(0), 32)
}

func memoryMStore8(stack *stack.Stack) (uint64, bool) {
	return calcMemSize64WithUint(stack.Back(0), 1)
}

func memoryMStore(stack *stack.Stack) (uint64, bool) {
	return calcMemSize64WithUint(stack.Back(0), 32)
}

func memoryCreate(stack *stack.Stack) (uint64, bool) {
	return calcMemSize64(stack.Back(1), stack.Back(2))
}

func memoryCreate2(stack *stack.Stack) (uint64, bool) {
	return calcMemSize64(stack.Back(1), stack.Back(2))
}

func memoryCall(stack *stack.Stack) (uint64, bool) {
	x, overflow := calcMemSize64(stack.Back(5), stack.Back(6))
	if overflow {
		return 0, true
	}
	y, overflow := calcMemSize64(stack.Back(3), stack.Back(4))
	if overflow {
		return 0, true
	}
	if x > y {
		return x, false
	}
	return y, false
}
func memoryDelegateCall(stack *stack.Stack) (uint64, bool) {
	x, overflow := calcMemSize64(stack.Back(4), stack.Back(5))
	if overflow {
		return 0, true
	}
	y, overflow := calcMemSize64(stack.Back(2), stack.Back(3))
	if overflow {
		return 0, true
	}
	if x > y {
		return x, false
	}
	return y, false
}

func memoryStaticCall(stack *stack.Stack) (uint64, bool) {
	x, overflow := calcMemSize64(stack.Back(4), stack.Back(5))
	if overflow {
		return 0, true
	}
	y, overflow := calcMemSize64(stack.Back(2), stack.Back(3))
	if overflow {
		return 0, true
	}
	if x > y {
		return x, false
	}
	return y, false
}

func memoryReturn(stack *stack.Stack) (uint64, bool) {
	return calcMemSize64(stack.Back(0), stack.Back(1))
}

func memoryRevert(stack *stack.Stack) (uint64, bool) {
	return calcMemSize64(stack.Back(0), stack.Back(1))
}

func memoryLog(stack *stack.Stack) (uint64, bool) {
	return calcMemSize64(stack.Back(0), stack.Back(1))
}
