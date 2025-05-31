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

	"github.com/erigontech/erigon-lib/log/v3"
)

var stackPool = sync.Pool{
	New: func() interface{} {
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

//go:inline
func (st *Stack) push(d *uint256.Int) {
	// NOTE push limit (1024) is checked in baseCheck
	st.data = append(st.data, *d)
}

//go:inline
func (st *Stack) pop() (ret uint256.Int) {
	n := len(st.data)
	ret = st.data[n-1]
	st.data = st.data[:n-1]
	return
}

func (st *Stack) Cap() int {
	return cap(st.data)
}

//go:inline
func (st *Stack) swap1() {
	n := len(st.data)
	// Eliminate bounds checks
	_ = st.data[n-2] // BCE hint
	st.data[n-2], st.data[n-1] = st.data[n-1], st.data[n-2]
}

//go:inline
func (st *Stack) swap2() {
	n := len(st.data)
	_ = st.data[n-3] // BCE hint
	st.data[n-3], st.data[n-1] = st.data[n-1], st.data[n-3]
}

//go:inline
func (st *Stack) swap3() {
	n := len(st.data)
	_ = st.data[n-4] // BCE hint
	st.data[n-4], st.data[n-1] = st.data[n-1], st.data[n-4]
}

//go:inline
func (st *Stack) swap4() {
	n := len(st.data)
	_ = st.data[n-5] // BCE hint
	st.data[n-5], st.data[n-1] = st.data[n-1], st.data[n-5]
}

//go:inline
func (st *Stack) swap5() {
	n := len(st.data)
	_ = st.data[n-6] // BCE hint
	st.data[n-6], st.data[n-1] = st.data[n-1], st.data[n-6]
}

//go:inline
func (st *Stack) swap6() {
	n := len(st.data)
	_ = st.data[n-7] // BCE hint
	st.data[n-7], st.data[n-1] = st.data[n-1], st.data[n-7]
}

//go:inline
func (st *Stack) swap7() {
	n := len(st.data)
	_ = st.data[n-8] // BCE hint
	st.data[n-8], st.data[n-1] = st.data[n-1], st.data[n-8]
}

//go:inline
func (st *Stack) swap8() {
	n := len(st.data)
	_ = st.data[n-9] // BCE hint
	st.data[n-9], st.data[n-1] = st.data[n-1], st.data[n-9]
}

//go:inline
func (st *Stack) swap9() {
	n := len(st.data)
	_ = st.data[n-10] // BCE hint
	st.data[n-10], st.data[n-1] = st.data[n-1], st.data[n-10]
}

//go:inline
func (st *Stack) swap10() {
	n := len(st.data)
	_ = st.data[n-11] // BCE hint
	st.data[n-11], st.data[n-1] = st.data[n-1], st.data[n-11]
}

//go:inline
func (st *Stack) swap11() {
	n := len(st.data)
	_ = st.data[n-12] // BCE hint
	st.data[n-12], st.data[n-1] = st.data[n-1], st.data[n-12]
}

//go:inline
func (st *Stack) swap12() {
	n := len(st.data)
	_ = st.data[n-13] // BCE hint
	st.data[n-13], st.data[n-1] = st.data[n-1], st.data[n-13]
}

//go:inline
func (st *Stack) swap13() {
	n := len(st.data)
	_ = st.data[n-14] // BCE hint
	st.data[n-14], st.data[n-1] = st.data[n-1], st.data[n-14]
}

//go:inline
func (st *Stack) swap14() {
	n := len(st.data)
	_ = st.data[n-15] // BCE hint
	st.data[n-15], st.data[n-1] = st.data[n-1], st.data[n-15]
}

//go:inline
func (st *Stack) swap15() {
	n := len(st.data)
	_ = st.data[n-16] // BCE hint
	st.data[n-16], st.data[n-1] = st.data[n-1], st.data[n-16]
}

//go:inline
func (st *Stack) swap16() {
	n := len(st.data)
	_ = st.data[n-17] // BCE hint
	st.data[n-17], st.data[n-1] = st.data[n-1], st.data[n-17]
}

//go:inline
func (st *Stack) dup(n int) {
	length := len(st.data)
	// Fast path for common cases with bounds check elimination
	switch n {
	case 1:
		_ = st.data[length-1] // BCE hint
		st.data = append(st.data, st.data[length-1])
	case 2:
		_ = st.data[length-2] // BCE hint
		st.data = append(st.data, st.data[length-2])
	default:
		st.data = append(st.data, st.data[length-n])
	}
}

//go:inline
func (st *Stack) peek() *uint256.Int {
	n := len(st.data)
	_ = st.data[n-1] // BCE hint
	return &st.data[n-1]
}

// Back returns the n'th item in stack
//
//go:inline
func (st *Stack) Back(n int) *uint256.Int {
	length := len(st.data)
	idx := length - n - 1
	_ = st.data[idx] // BCE hint
	return &st.data[idx]
}

func (st *Stack) Reset() {
	st.data = st.data[:0]
}

//go:inline
func (st *Stack) len() int {
	return len(st.data)
}

func ReturnNormalStack(s *Stack) {
	s.data = s.data[:0]
	stackPool.Put(s)
}
