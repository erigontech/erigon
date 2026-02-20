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

	"github.com/erigontech/erigon/common/log/v3"
)

var stackPool = sync.Pool{
	New: func() any {
		return &Stack{data: make([]uint256.Int, 0, 16)}
	},
}

// Stack is an object for basic stack operations. Items popped to the stack are
// expected to be changed and modified. stack does not take care of adding newly
// initialised objects.
type Stack struct {
	data []uint256.Int
}

func New() *Stack {
	stack, ok := stackPool.Get().(*Stack)
	if !ok {
		log.Error("Type assertion failure", "err", "cannot get Stack pointer from stackPool")
	}
	return stack
}
func (st *Stack) push(d uint256.Int) {
	// NOTE push limit (1024) is checked in baseCheck
	st.data = append(st.data, d)
}

func (st *Stack) pop() (ret uint256.Int) {
	ret = st.data[len(st.data)-1]
	st.data = st.data[:len(st.data)-1]
	return
}
func (st *Stack) Push(d *uint256.Int) { st.push(*d) }
func (st *Stack) Pop() uint256.Int    { return st.pop() }
func (st *Stack) Cap() int {
	return cap(st.data)
}

func (st *Stack) swap(n int) {
	st.data[st.len()-n-1], st.data[st.len()-1] = st.data[st.len()-1], st.data[st.len()-n-1]
}

func (st *Stack) dup(n int) {
	st.data = append(st.data, st.data[len(st.data)-n])
}

func (st *Stack) peek() *uint256.Int {
	return &st.data[len(st.data)-1]
}

// Back returns the n'th item in stack
func (st *Stack) Back(n int) *uint256.Int {
	return &st.data[len(st.data)-n-1]
}

func (st *Stack) Reset() {
	st.data = st.data[:0]
}

func (st *Stack) len() int {
	return len(st.data)
}

func ReturnNormalStack(s *Stack) {
	s.data = s.data[:0]
	stackPool.Put(s)
}

func (st *Stack) String() string {
	var s string
	for _, di := range st.data {
		s += di.Hex() + ", "
	}
	return s
}
