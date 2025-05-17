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

package stack

import (
	"fmt"
	"sync"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon-lib/log/v3"
)

var stackPool = sync.Pool{
	New: func() interface{} {
		return &Stack{Data: make([]uint256.Int, 0, 16)}
	},
}

// Stack is an object for basic stack operations. Items popped to the stack are
// expected to be changed and modified. stack does not take care of adding newly
// initialised objects.
type Stack struct {
	Data []uint256.Int
}

func New() *Stack {
	stack, ok := stackPool.Get().(*Stack)
	if !ok {
		log.Error("Type assertion failure", "err", "cannot get Stack pointer from stackPool")
	}
	return stack
}

func (st *Stack) Push(d *uint256.Int) {
	// NOTE push limit (1024) is checked in baseCheck
	st.Data = append(st.Data, *d)
}

func (st *Stack) PushN(ds ...uint256.Int) {
	// FIXME: Is there a way to pass args by pointers.
	st.Data = append(st.Data, ds...)
}

func (st *Stack) Pop() (ret uint256.Int) {
	ret = st.Data[len(st.Data)-1]
	st.Data = st.Data[:len(st.Data)-1]
	return
}

func (st *Stack) Cap() int {
	return cap(st.Data)
}

func (st *Stack) Swap1() {
	st.Data[st.Len()-2], st.Data[st.Len()-1] = st.Data[st.Len()-1], st.Data[st.Len()-2]
}
func (st *Stack) Swap2() {
	st.Data[st.Len()-3], st.Data[st.Len()-1] = st.Data[st.Len()-1], st.Data[st.Len()-3]
}
func (st *Stack) Swap3() {
	st.Data[st.Len()-4], st.Data[st.Len()-1] = st.Data[st.Len()-1], st.Data[st.Len()-4]
}
func (st *Stack) Swap4() {
	st.Data[st.Len()-5], st.Data[st.Len()-1] = st.Data[st.Len()-1], st.Data[st.Len()-5]
}
func (st *Stack) Swap5() {
	st.Data[st.Len()-6], st.Data[st.Len()-1] = st.Data[st.Len()-1], st.Data[st.Len()-6]
}
func (st *Stack) Swap6() {
	st.Data[st.Len()-7], st.Data[st.Len()-1] = st.Data[st.Len()-1], st.Data[st.Len()-7]
}
func (st *Stack) Swap7() {
	st.Data[st.Len()-8], st.Data[st.Len()-1] = st.Data[st.Len()-1], st.Data[st.Len()-8]
}
func (st *Stack) Swap8() {
	st.Data[st.Len()-9], st.Data[st.Len()-1] = st.Data[st.Len()-1], st.Data[st.Len()-9]
}
func (st *Stack) Swap9() {
	st.Data[st.Len()-10], st.Data[st.Len()-1] = st.Data[st.Len()-1], st.Data[st.Len()-10]
}
func (st *Stack) Swap10() {
	st.Data[st.Len()-11], st.Data[st.Len()-1] = st.Data[st.Len()-1], st.Data[st.Len()-11]
}
func (st *Stack) Swap11() {
	st.Data[st.Len()-12], st.Data[st.Len()-1] = st.Data[st.Len()-1], st.Data[st.Len()-12]
}
func (st *Stack) Swap12() {
	st.Data[st.Len()-13], st.Data[st.Len()-1] = st.Data[st.Len()-1], st.Data[st.Len()-13]
}
func (st *Stack) Swap13() {
	st.Data[st.Len()-14], st.Data[st.Len()-1] = st.Data[st.Len()-1], st.Data[st.Len()-14]
}
func (st *Stack) Swap14() {
	st.Data[st.Len()-15], st.Data[st.Len()-1] = st.Data[st.Len()-1], st.Data[st.Len()-15]
}
func (st *Stack) Swap15() {
	st.Data[st.Len()-16], st.Data[st.Len()-1] = st.Data[st.Len()-1], st.Data[st.Len()-16]
}
func (st *Stack) Swap16() {
	st.Data[st.Len()-17], st.Data[st.Len()-1] = st.Data[st.Len()-1], st.Data[st.Len()-17]
}

func (st *Stack) Dup(n int) {
	st.Data = append(st.Data, st.Data[len(st.Data)-n])
}

func (st *Stack) Peek() *uint256.Int {
	return &st.Data[len(st.Data)-1]
}

// Back returns the n'th item in stack
func (st *Stack) Back(n int) *uint256.Int {
	return &st.Data[len(st.Data)-n-1]
}

func (st *Stack) Reset() {
	st.Data = st.Data[:0]
}

func (st *Stack) Len() int {
	return len(st.Data)
}

// Print dumps the content of the stack
func (st *Stack) Print() {
	fmt.Println("### stack ###")
	if len(st.Data) > 0 {
		for i, val := range st.Data {
			fmt.Printf("%-3d  %v\n", i, val)
		}
	} else {
		fmt.Println("-- empty --")
	}
	fmt.Println("#############")
}

func ReturnNormalStack(s *Stack) {
	s.Data = s.Data[:0]
	stackPool.Put(s)
}
