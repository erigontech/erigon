// Copyright 2024 The Erigon Authors
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

package ring

type Buffer[T any] struct {
	buf []T
	// real head is head-1, like this so nil ring is valid
	head   int
	tail   int
	length int
}

func MakeBuffer[T any](length, capacity int) Buffer[T] {
	if length > capacity {
		panic("length must be less than capacity")
	}
	return Buffer[T]{
		buf:    make([]T, capacity),
		tail:   length,
		length: length,
	}
}

func NewBuffer[T any](length, capacity int) *Buffer[T] {
	r := MakeBuffer[T](length, capacity)
	return &r
}

func (r *Buffer[T]) grow() {
	size := len(r.buf) * 2
	if size == 0 {
		size = 2
	}

	buf := make([]T, size)
	copy(buf, r.buf[r.head:])
	copy(buf[len(r.buf[r.head:]):], r.buf[:r.head])
	r.head = 0
	r.tail = r.length
	r.buf = buf
}

func (r *Buffer[T]) incHead() {
	// resize
	if r.length == 0 {
		panic("smashing detected")
	}
	r.length--

	r.head++
	if r.head == len(r.buf) {
		r.head = 0
	}
}

func (r *Buffer[T]) decHead() {
	// resize
	if r.length == len(r.buf) {
		r.grow()
	}
	r.length++

	r.head--
	if r.head == -1 {
		r.head = len(r.buf) - 1
	}
}

func (r *Buffer[T]) incTail() {
	// resize
	if r.length == len(r.buf) {
		r.grow()
	}
	r.length++

	r.tail++
	if r.tail == len(r.buf) {
		r.tail = 0
	}
}

func (r *Buffer[T]) decTail() {
	// resize
	if r.length == 0 {
		panic("smashing detected")
	}
	r.length--

	r.tail--
	if r.tail == -1 {
		r.tail = len(r.buf) - 1
	}
}

func (r *Buffer[T]) tailSub1() int {
	tail := r.tail - 1
	if tail == -1 {
		tail = len(r.buf) - 1
	}
	return tail
}

func (r *Buffer[T]) PopFront() (T, bool) {
	if r.length == 0 {
		return *new(T), false
	}

	front := r.buf[r.head]
	r.buf[r.head] = *new(T)
	r.incHead()
	return front, true
}

func (r *Buffer[T]) PopBack() (T, bool) {
	if r.length == 0 {
		return *new(T), false
	}

	r.decTail()
	back := r.buf[r.tail]
	r.buf[r.tail] = *new(T)
	return back, true
}

func (r *Buffer[T]) Clear() {
	r.head = 0
	r.tail = 0
	r.length = 0
}

func (r *Buffer[T]) PushFront(value T) {
	r.decHead()
	r.buf[r.head] = value
}

func (r *Buffer[T]) PushBack(value T) {
	r.incTail()
	r.buf[r.tailSub1()] = value
}

func (r *Buffer[T]) Length() int {
	return r.length
}

func (r *Buffer[T]) Capacity() int {
	return len(r.buf)
}

func (r *Buffer[T]) Get(n int) T {
	if n >= r.length {
		panic("index out of range")
	}
	ptr := r.head + n
	if ptr >= len(r.buf) {
		ptr -= len(r.buf)
	}
	return r.buf[ptr]
}
