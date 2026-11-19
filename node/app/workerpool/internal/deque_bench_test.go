// Copyright 2026 The Erigon Authors
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

package internal

import (
	"testing"
)

func BenchmarkPushFront(b *testing.B) {
	var q Deque[int]
	for i := 0; i < b.N; i++ {
		q.PushFront(i)
	}
}

func BenchmarkPushBack(b *testing.B) {
	var q Deque[int]
	for i := 0; i < b.N; i++ {
		q.PushBack(i)
	}
}

func BenchmarkSerial(b *testing.B) {
	var q Deque[int]
	for i := 0; i < b.N; i++ {
		q.PushBack(i)
	}
	for i := 0; i < b.N; i++ {
		q.PopFront()
	}
}

func BenchmarkSerialReverse(b *testing.B) {
	var q Deque[int]
	for i := 0; i < b.N; i++ {
		q.PushFront(i)
	}
	for i := 0; i < b.N; i++ {
		q.PopBack()
	}
}

func BenchmarkRotate(b *testing.B) {
	q := new(Deque[int])
	for i := 0; i < b.N; i++ {
		q.PushBack(i)
	}
	b.ResetTimer()
	// N complete rotations on length N - 1.
	for i := 0; i < b.N; i++ {
		q.Rotate(b.N - 1)
	}
}

func BenchmarkInsert(b *testing.B) {
	q := new(Deque[int])
	for i := 0; i < b.N; i++ {
		q.PushBack(i)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.Insert(q.Len()/2, -i)
	}
}

func BenchmarkRemove(b *testing.B) {
	q := new(Deque[int])
	for i := 0; i < b.N; i++ {
		q.PushBack(i)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.Remove(q.Len() / 2)
	}
}

func BenchmarkYoyo(b *testing.B) {
	var q Deque[int]
	for i := 0; i < b.N; i++ {
		for j := range 65536 {
			q.PushBack(j)
		}
		for range 65536 {
			q.PopFront()
		}
	}
}

func BenchmarkYoyoFixed(b *testing.B) {
	var q Deque[int]
	q.SetMinCapacity(16)
	for i := 0; i < b.N; i++ {
		for j := range 65536 {
			q.PushBack(j)
		}
		for range 65536 {
			q.PopFront()
		}
	}
}
