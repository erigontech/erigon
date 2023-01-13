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
func (m *MergePairsStream) Keys() ([][]byte, error)   { return naiveKeys(m) }
func (m *MergePairsStream) Values() ([][]byte, error) { return naiveValues(m) }

func naiveKeys(it kv.Pairs) (keys [][]byte, err error) {
	for it.HasNext() {
		k, _, err := it.Next()
		if err != nil {
			return keys, err
		}
		keys = append(keys, k)
	}
	return keys, nil
}
func naiveValues(it kv.Pairs) (values [][]byte, err error) {
	for it.HasNext() {
		_, v, err := it.Next()
		if err != nil {
			return values, err
		}
		values = append(values, v)
	}
	return values, nil
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
