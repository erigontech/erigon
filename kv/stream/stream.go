package stream

import (
	"bytes"
	"fmt"

	"github.com/ledgerwatch/erigon-lib/kv"
)

type ArrStream[V any] struct {
	arr []V
	i   int
}

func Array[V any](arr []V) kv.UnaryStream[V] { return &ArrStream[V]{arr: arr} }
func (it *ArrStream[V]) HasNext() bool       { return it.i < len(it.arr) }
func (it *ArrStream[V]) Close()              {}
func (it *ArrStream[V]) Next() (V, error) {
	v := it.arr[it.i]
	it.i++
	return v, nil
}
func (it *ArrStream[V]) NextBatch() ([]V, error) {
	v := it.arr[it.i:]
	it.i = len(it.arr)
	return v, nil
}

// MergePairsStream - merge 2 kv.Pairs streams to 1 in lexicographically order
// 1-st stream has higher priority - when 2 streams return same key
type MergePairsStream struct {
	x, y               kv.Pairs
	xHasNext, yHasNext bool
	xNextK, xNextV     []byte
	yNextK, yNextV     []byte
	nextErr            error
}

func MergePairs(x, y kv.Pairs) *MergePairsStream {
	m := &MergePairsStream{x: x, y: y}
	m.advanceX()
	m.advanceY()
	return m
}
func (m *MergePairsStream) HasNext() bool { return m.xHasNext || m.yHasNext }
func (m *MergePairsStream) advanceX() {
	if m.nextErr != nil {
		m.xNextK, m.xNextV = nil, nil
		return
	}
	m.xHasNext = m.x.HasNext()
	if m.xHasNext {
		m.xNextK, m.xNextV, m.nextErr = m.x.Next()
	}
}
func (m *MergePairsStream) advanceY() {
	if m.nextErr != nil {
		m.yNextK, m.yNextV = nil, nil
		return
	}
	m.yHasNext = m.y.HasNext()
	if m.yHasNext {
		m.yNextK, m.yNextV, m.nextErr = m.y.Next()
	}
}
func (m *MergePairsStream) Next() ([]byte, []byte, error) {
	if m.nextErr != nil {
		return nil, nil, m.nextErr
	}
	if !m.xHasNext && !m.yHasNext {
		panic(1)
	}
	if m.xHasNext && m.yHasNext {
		cmp := bytes.Compare(m.xNextK, m.yNextK)
		if cmp < 0 {
			k, v, err := m.xNextK, m.xNextV, m.nextErr
			m.advanceX()
			return k, v, err
		} else if cmp == 0 {
			k, v, err := m.xNextK, m.xNextV, m.nextErr
			m.advanceX()
			m.advanceY()
			return k, v, err
		}
		k, v, err := m.yNextK, m.yNextV, m.nextErr
		m.advanceY()
		return k, v, err
	}
	if m.xHasNext {
		k, v, err := m.xNextK, m.xNextV, m.nextErr
		m.advanceX()
		return k, v, err
	}
	k, v, err := m.yNextK, m.yNextV, m.nextErr
	m.advanceY()
	return k, v, err
}
func (m *MergePairsStream) ToArray() (keys, values [][]byte, err error) { return NaivePairs2Arr(m) }

func NaivePairs2Arr(it kv.Pairs) (keys, values [][]byte, err error) {
	for it.HasNext() {
		k, v, err := it.Next()
		if err != nil {
			return keys, values, err
		}
		keys = append(keys, k)
		values = append(values, v)
	}
	return keys, values, nil
}

// PairsWithErrorStream - return N, keys and then error
type PairsWithErrorStream struct {
	errorAt, i int
}

func PairsWithError(errorAt int) *PairsWithErrorStream {
	return &PairsWithErrorStream{errorAt: errorAt}
}
func (m *PairsWithErrorStream) HasNext() bool { return true }
func (m *PairsWithErrorStream) Next() ([]byte, []byte, error) {
	if m.i >= m.errorAt {
		return nil, nil, fmt.Errorf("expected error at iteration: %d", m.errorAt)
	}
	m.i++
	return []byte(fmt.Sprintf("%x", m.i)), []byte(fmt.Sprintf("%x", m.i)), nil
}

// TransformStream - analog `map` (in terms of map-filter-reduce pattern)
type TransformStream[K, V any] struct {
	it        kv.Stream[K, V]
	transform func(K, V) (K, V)
}

func TransformPairs(it kv.Pairs, transform func(k, v []byte) ([]byte, []byte)) *TransformStream[[]byte, []byte] {
	return &TransformStream[[]byte, []byte]{it: it, transform: transform}
}
func Transform[K, V any](it kv.Stream[K, V], transform func(K, V) (K, V)) *TransformStream[K, V] {
	return &TransformStream[K, V]{it: it, transform: transform}
}
func (m *TransformStream[K, V]) HasNext() bool { return m.it.HasNext() }
func (m *TransformStream[K, V]) Next() (K, V, error) {
	k, v, err := m.it.Next()
	if err != nil {
		return k, v, err
	}
	newK, newV := m.transform(k, v)
	return newK, newV, nil
}

// FilterStream - analog `map` (in terms of map-filter-reduce pattern)
type FilterStream[K, V any] struct {
	it     kv.Stream[K, V]
	filter func(K, V) bool
}

func FilterPairs(it kv.Pairs, filter func(k, v []byte) bool) *FilterStream[[]byte, []byte] {
	return &FilterStream[[]byte, []byte]{it: it, filter: filter}
}
func Filter[K, V any](it kv.Stream[K, V], filter func(K, V) bool) *FilterStream[K, V] {
	return &FilterStream[K, V]{it: it, filter: filter}
}
func (m *FilterStream[K, V]) HasNext() bool { return m.it.HasNext() }
func (m *FilterStream[K, V]) Next() (k K, v V, err error) {
	for m.it.HasNext() {
		k, v, err = m.it.Next()
		if err != nil {
			return k, v, err
		}
		if !m.filter(k, v) {
			continue
		}
		return k, v, nil
	}
	return k, v, nil
}
