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
	"github.com/holiman/uint256"
)

const stackLimit = 1024

// Multi-slot helpers guard with a single unsigned compare:
//
//	if uint(st.top) > stackLimit-2 // ⇔ st.top < 0 || st.top > stackLimit-2
//
// One branch covers both bounds (negative ints wrap to huge uints), and the
// prove pass then drops the bounds checks on st.data[st.top+k]. Index off the
// st.top field directly — a hoisted local (t := st.top - 2; st.data[t+1]) gets
// reassociated away from the guarded value and the checks come back.
// Verify: go build -gcflags='-d=ssa/check_bce/debug=1' ./execution/vm/

// Stack is the EVM operand stack: a fixed 1024-slot array indexed by top.
// Helpers return pointers into data so ops mutate slots in place; position
// arguments are depths below the top, with depth 0 the top item.
type Stack struct {
	data [stackLimit]uint256.Int
	top  int
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

// drop discards the top item without reading it.
func (st *Stack) drop() {
	st.top--
}

func (st *Stack) popCopy() uint256.Int {
	st.top--
	return st.data[st.top]
}

// pop pops the top item and returns a pointer to its slot. The value stays
// valid until the stack next push/dup/swap
func (st *Stack) pop() *uint256.Int {
	st.top--
	return &st.data[st.top]
}

func (st *Stack) pop2() (x, y *uint256.Int) {
	st.top -= 2
	if uint(st.top) > stackLimit-2 { // Bounds Check Elimination: 1 manual check, or compiler will add 2 checks
		panic("stack index out of range")
	}
	return &st.data[st.top+1], &st.data[st.top]
}

// pop1Peek1 pops one slot and peeks the next, shaped as popCopy-two-push-one so
// both indices sit on the guarded st.top.
func (st *Stack) pop1Peek1() (x, y *uint256.Int) {
	st.top -= 2
	if uint(st.top) > stackLimit-2 {
		panic("stack index out of range")
	}
	x, y = &st.data[st.top+1], &st.data[st.top]
	st.top++
	return
}

func (st *Stack) popHash() [32]byte {
	st.top--
	return st.data[st.top].Bytes32()
}

// pop2Peek1 pops two slots and peeks the third, shaped as popCopy-three-push-one
// so all indices sit on the guarded st.top.
func (st *Stack) pop2Peek1() (x, y, z *uint256.Int) {
	st.top -= 3
	if uint(st.top) > stackLimit-3 {
		panic("stack index out of range")
	}
	x, y, z = &st.data[st.top+2], &st.data[st.top+1], &st.data[st.top]
	st.top++
	return
}

func (st *Stack) pop3() (x, y, z *uint256.Int) {
	st.top -= 3
	if uint(st.top) > stackLimit-3 {
		panic("stack index out of range")
	}
	return &st.data[st.top+2], &st.data[st.top+1], &st.data[st.top]
}

func (st *Stack) pop2Uint64() (x, y uint64) {
	st.top -= 2
	if uint(st.top) > stackLimit-2 {
		panic("stack index out of range")
	}
	return st.data[st.top+1].Uint64(), st.data[st.top].Uint64()
}

func (st *Stack) Cap() int {
	return stackLimit
}

// exchange swaps the items at depths n and m.
func (st *Stack) exchange(n, m int) {
	i, j := st.top-n-1, st.top-m-1
	if uint(i) >= stackLimit || uint(j) >= stackLimit {
		panic("stack index out of range")
	}
	st.data[i], st.data[j] = st.data[j], st.data[i]
}

func (st *Stack) swap(n int) { st.exchange(n, 0) }

// dup copies the item at depth n onto the top.
func (st *Stack) dup(n int) {
	i, j := st.top-n-1, st.top
	if uint(i) >= stackLimit || uint(j) >= stackLimit {
		panic("stack index out of range")
	}
	st.data[j] = st.data[i]
	st.top++
}

func (st *Stack) peek() *uint256.Int {
	return &st.data[st.top-1]
}

// back2 returns the items at depths n and m under one range check.
func (st *Stack) back2(n, m int) (x, y *uint256.Int) {
	i, j := st.top-n-1, st.top-m-1
	if uint(i) >= stackLimit || uint(j) >= stackLimit {
		panic("stack index out of range")
	}
	return &st.data[i], &st.data[j]
}

// back3 returns the items at depths n, m and k under one range check.
func (st *Stack) back3(n, m, k int) (x, y, z *uint256.Int) {
	i, j, l := st.top-n-1, st.top-m-1, st.top-k-1
	if uint(i) >= stackLimit || uint(j) >= stackLimit || uint(l) >= stackLimit {
		panic("stack index out of range")
	}
	return &st.data[i], &st.data[j], &st.data[l]
}

// back returns the item at depth n.
func (st *Stack) back(n int) *uint256.Int {
	return &st.data[st.top-n-1]
}

func (st *Stack) Reset() {
	st.top = 0
}

func (st *Stack) len() int {
	return st.top
}
