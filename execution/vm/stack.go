// Copyright 2014 The go-ethereum Authors
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

import (
	"sync"

	"github.com/holiman/uint256"
)

const stackLimit = 1024

var stackPool = sync.Pool{
	New: func() any {
		return &Stack{}
	},
}

// Stack is an object for basic stack operations. Items popped to the stack are
// expected to be changed and modified. stack does not take care of adding newly
// initialised objects.
type Stack struct {
	data [stackLimit]uint256.Int
	top  int
}

func New() *Stack {
	return stackPool.Get().(*Stack)
}

func (st *Stack) push(d uint256.Int) {
	// NOTE push limit (1024) is checked in baseCheck
	st.data[st.top] = d
	st.top++
}

// pushRef grows the stack by one and returns a pointer to the new top slot so
// the caller can build the value in place instead of constructing a temporary
// and copying it in. The slot holds stale data from a prior use, so the caller
// must fully overwrite it
func (st *Stack) pushRef() *uint256.Int {
	ref := &st.data[st.top]
	st.top++
	return ref
}

func (st *Stack) pop() uint256.Int {
	st.top--
	return st.data[st.top]
}

// pop2uint64 pops the top two items as their low 64 bits (x topmost) — for
// opcodes that need only the low word (memory offsets/sizes).
func (st *Stack) pop2uint64() (x, y uint64) {
	st.top -= 2
	if st.top < 0 || st.top+1 >= stackLimit {
		panic("stack overflow")
	}
	return st.data[st.top+1].Uint64(), st.data[st.top].Uint64()
}

// popRef pops the top item and returns a pointer to its slot. The value stays
// valid until the stack next push/dup/swap
func (st *Stack) popRef() *uint256.Int {
	st.top--
	return &st.data[st.top]
}

// popRef1Peek1 pops the top item and returns it together with a pointer to the
// new top: the operand and write target of a binary op. One range check covers
// both slots. Same validity rule as popRef.
func (st *Stack) popRef1Peek1() (x, y *uint256.Int) {
	st.top--
	if st.top-1 < 0 || st.top >= stackLimit {
		panic("stack overflow")
	}
	return &st.data[st.top], &st.data[st.top-1]
}

// drop discards the top item without reading it.
func (st *Stack) drop() {
	st.top--
}

// popRef2 pops the top two items and returns pointers to their slots, x
// topmost. One range check covers both. Same validity rule as popRef.
func (st *Stack) popRef2() (x, y *uint256.Int) {
	st.top -= 2
	if st.top < 0 || st.top+1 >= stackLimit {
		panic("stack overflow")
	}
	return &st.data[st.top+1], &st.data[st.top]
}

func (st *Stack) popRef3() (x, y, z *uint256.Int) {
	st.top -= 3
	if st.top < 0 || st.top+2 >= stackLimit {
		panic("stack overflow")
	}
	return &st.data[st.top+2], &st.data[st.top+1], &st.data[st.top]
}

func (st *Stack) Cap() int {
	return stackLimit
}

// exchange swaps the (n+1)'th and (m+1)'th items counting from the top.
func (st *Stack) exchange(n, m int) {
	i, j := st.top-n-1, st.top-m-1
	// Explicit range check: reducing amount of bounds check
	if i < 0 || i >= stackLimit || j < 0 || j >= stackLimit {
		panic("stack overflow")
	}
	st.data[i], st.data[j] = st.data[j], st.data[i]
}

func (st *Stack) swap(n int) { st.exchange(n, 0) }

func (st *Stack) dup(n int) {
	i, j := st.top-n, st.top
	if i < 0 || i >= stackLimit || j < 0 || j >= stackLimit {
		panic("stack overflow")
	}
	st.data[j] = st.data[i]
	st.top++
}

func (st *Stack) peek() *uint256.Int {
	return &st.data[st.top-1]
}

// back2 returns the n'th and m'th items from the top under one range check.
func (st *Stack) back2(n, m int) (x, y *uint256.Int) {
	i, j := st.top-n-1, st.top-m-1
	if i < 0 || i >= stackLimit || j < 0 || j >= stackLimit {
		panic("stack overflow")
	}
	return &st.data[i], &st.data[j]
}

// Back returns the n'th item in stack
func (st *Stack) Back(n int) *uint256.Int {
	return &st.data[st.top-n-1]
}

func (st *Stack) Reset() {
	st.top = 0
}

func (st *Stack) len() int {
	return st.top
}

func ReturnNormalStack(s *Stack) {
	s.top = 0
	stackPool.Put(s)
}
