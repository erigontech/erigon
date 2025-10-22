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

package stream

import (
	"bytes"
)

// often used shortcuts
type (
	U64 Uno[uint64]
	KV  Duo[[]byte, []byte] // key,  value
)

var (
	EmptyU64 = &Empty[uint64]{}
	EmptyKV  = &EmptyDuo[[]byte, []byte]{}
)

var (
	TracedU64 = &Traced[uint64]{}
	TracedKV  = &TracedDuo[[]byte, []byte]{}
)

func FilterU64(it U64, filter func(k uint64) bool) *Filtered[uint64] {
	return Filter[uint64](it, filter)
}
func FilterKV(it KV, filter func(k, v []byte) bool) *FilteredDuo[[]byte, []byte] {
	return FilterDuo[[]byte, []byte](it, filter)
}

func ToArrayU64(s U64) ([]uint64, error)         { return ToArray[uint64](s) }
func ToArrayKV(s KV) ([][]byte, [][]byte, error) { return ToArrayDuo[[]byte, []byte](s) }

func ToArrU64Must(s U64) []uint64 {
	arr, err := ToArray[uint64](s)
	if err != nil {
		panic(err)
	}
	return arr
}
func ToArrKVMust(s KV) ([][]byte, [][]byte) {
	keys, values, err := ToArrayDuo[[]byte, []byte](s)
	if err != nil {
		panic(err)
	}
	return keys, values
}

func CountU64(s U64) (int, error) { return Count[uint64](s) }
func CountKV(s KV) (int, error)   { return CountDuo[[]byte, []byte](s) }

func TransformKV(it KV, transform func(k, v []byte) ([]byte, []byte, error)) *TransformedDuo[[]byte, []byte] {
	return TransformDuo[[]byte, []byte](it, transform)
}

// internal types
type (
	NextPageUno[T any]    func(pageToken string) (arr []T, nextPageToken string, err error)
	NextPageDuo[K, V any] func(pageToken string) (keys []K, values []V, nextPageToken string, err error)
)

func PaginateKV(f NextPageDuo[[]byte, []byte]) *PaginatedDuo[[]byte, []byte] {
	return PaginateDuo[[]byte, []byte](f)
}
func PaginateU64(f NextPageUno[uint64]) *Paginated[uint64] {
	return Paginate[uint64](f)
}

type TransformKV2U64Iter[K, V []byte] struct {
	it        KV
	transform func(K, V) (uint64, error)
}

func TransformKV2U64[K, V []byte](it KV, transform func(K, V) (uint64, error)) *TransformKV2U64Iter[K, V] {
	return &TransformKV2U64Iter[K, V]{it: it, transform: transform}
}
func (m *TransformKV2U64Iter[K, V]) HasNext() bool { return m.it.HasNext() }
func (m *TransformKV2U64Iter[K, V]) Next() (uint64, error) {
	k, v, err := m.it.Next()
	if err != nil {
		return 0, err
	}
	return m.transform(k, v)
}
func (m *TransformKV2U64Iter[K, v]) Close() {
	if x, ok := m.it.(Closer); ok {
		x.Close()
	}
}

// UnionKVIter - for duplicate keys, the first stream (x) takes precedence.
// In set theory: A ∪ B contains all elements that are in A, or in B, or in both
type UnionKVIter struct {
	x, y               KV
	xHasNext, yHasNext bool
	xNextK, xNextV     []byte
	yNextK, yNextV     []byte
	limit              int
	err                error
}

// UnionKV - for duplicate keys, the first stream (x) takes precedence.
// In set theory: A ∪ B contains all elements that are in A, or in B, or in both
func UnionKV(x, y KV, limit int) KV {
	if x == nil && y == nil {
		return EmptyKV
	}
	if x == nil {
		return y
	}
	if y == nil {
		return x
	}
	m := &UnionKVIter{x: x, y: y, limit: limit}
	m.advanceX()
	m.advanceY()
	return m
}
func (m *UnionKVIter) HasNext() bool {
	if m.err != nil {
		return true
	}
	return (m.limit != 0 && m.xHasNext) || (m.limit != 0 && m.yHasNext)
}
func (m *UnionKVIter) advanceX() {
	if m.err != nil {
		return
	}
	m.xHasNext = m.x.HasNext()
	if m.xHasNext {
		m.xNextK, m.xNextV, m.err = m.x.Next()
	}
}
func (m *UnionKVIter) advanceY() {
	if m.err != nil {
		return
	}
	m.yHasNext = m.y.HasNext()
	if m.yHasNext {
		m.yNextK, m.yNextV, m.err = m.y.Next()
	}
}
func (m *UnionKVIter) Next() ([]byte, []byte, error) {
	if m.err != nil {
		return nil, nil, m.err
	}
	m.limit--
	if m.xHasNext && m.yHasNext {
		cmp := bytes.Compare(m.xNextK, m.yNextK)
		if cmp < 0 {
			k, v, err := m.xNextK, m.xNextV, m.err
			m.advanceX()
			return k, v, err
		} else if cmp == 0 {
			k, v, err := m.xNextK, m.xNextV, m.err
			m.advanceX()
			m.advanceY()
			return k, v, err
		}
		k, v, err := m.yNextK, m.yNextV, m.err
		m.advanceY()
		return k, v, err
	}
	if m.xHasNext {
		k, v, err := m.xNextK, m.xNextV, m.err
		m.advanceX()
		return k, v, err
	}
	k, v, err := m.yNextK, m.yNextV, m.err
	m.advanceY()
	return k, v, err
}

// func (m *UnionKVIter) ToArray() (keys, values [][]byte, err error) { return ToArrayKV(m) }
func (m *UnionKVIter) Close() {
	if x, ok := m.x.(Closer); ok {
		x.Close()
	}
	if y, ok := m.y.(Closer); ok {
		y.Close()
	}
}

type Closer interface {
	Close()
}
