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
// must fully overwrite it — a Set*/Clear that writes all four words, never a
// partial update.
func (st *Stack) pushRef() *uint256.Int {
	ref := &st.data[st.top]
	st.top++
	return ref
}

func (st *Stack) pop() (ret uint256.Int) {
	st.top--
	ret = st.data[st.top]
	return
}

// pop2uint64 pops the top two items as their low 64 bits (x topmost) — for
// opcodes that need only the low word (memory offsets/sizes).
func (st *Stack) pop2uint64() (x, y uint64) {
	st.top -= 2
	return st.data[st.top+1].Uint64(), st.data[st.top].Uint64()
}

// popRef pops the top item and returns a pointer to its slot. The value stays
// valid until the stack next grows (push or dup reuse that slot), so it can be
// read as an operand while a following peek() writes the result into the slot
// below it.
func (st *Stack) popRef() *uint256.Int {
	st.top--
	return &st.data[st.top]
}

func (st *Stack) popHash() [32]byte {
	st.top--
	return st.data[st.top].Bytes32()
}

func (st *Stack) Cap() int {
	return stackLimit
}

func (st *Stack) swap(n int) {
	st.data[st.top-n-1], st.data[st.top-1] = st.data[st.top-1], st.data[st.top-n-1]
}

func (st *Stack) dup(n int) {
	st.data[st.top] = st.data[st.top-n]
	st.top++
}

func (st *Stack) peek() *uint256.Int {
	return &st.data[st.top-1]
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
