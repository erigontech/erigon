package iter

import (
	"bytes"
	"fmt"

	"golang.org/x/exp/constraints"
	"golang.org/x/exp/slices"
)

type ArrStream[V any] struct {
	arr []V
	i   int
}

func ReverseArray[V any](arr []V) *ArrStream[V] {
	arr = slices.Clone(arr)
	for i, j := 0, len(arr)-1; i < j; i, j = i+1, j-1 {
		arr[i], arr[j] = arr[j], arr[i]
	}
	return Array(arr)
}
func Array[V any](arr []V) *ArrStream[V] { return &ArrStream[V]{arr: arr} }
func (it *ArrStream[V]) HasNext() bool   { return it.i < len(it.arr) }
func (it *ArrStream[V]) Close()          {}
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

func Range[T constraints.Integer](from, to T) *RangeIter[T] {
	if from == to {
		to++
	}
	return &RangeIter[T]{i: from, to: to}
}

type RangeIter[T constraints.Integer] struct {
	i, to T
}

func (it *RangeIter[T]) HasNext() bool { return it.i < it.to }
func (it *RangeIter[T]) Close()        {}
func (it *RangeIter[T]) Next() (T, error) {
	v := it.i
	it.i++
	return v, nil
}

// UnionPairsStream - merge 2 kv.Pairs streams to 1 in lexicographically order
// 1-st stream has higher priority - when 2 streams return same key
type UnionPairsStream struct {
	x, y               KV
	xHasNext, yHasNext bool
	xNextK, xNextV     []byte
	yNextK, yNextV     []byte
	err                error
}

func UnionPairs(x, y KV) *UnionPairsStream {
	m := &UnionPairsStream{x: x, y: y}
	m.advanceX()
	m.advanceY()
	return m
}
func (m *UnionPairsStream) HasNext() bool { return m.xHasNext || m.yHasNext }
func (m *UnionPairsStream) advanceX() {
	if m.err != nil {
		return
	}
	m.xHasNext = m.x.HasNext()
	if m.xHasNext {
		m.xNextK, m.xNextV, m.err = m.x.Next()
	}
}
func (m *UnionPairsStream) advanceY() {
	if m.err != nil {
		return
	}
	m.yHasNext = m.y.HasNext()
	if m.yHasNext {
		m.yNextK, m.yNextV, m.err = m.y.Next()
	}
}
func (m *UnionPairsStream) Next() ([]byte, []byte, error) {
	if m.err != nil {
		return nil, nil, m.err
	}
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
func (m *UnionPairsStream) ToArray() (keys, values [][]byte, err error) { return ToKVArray(m) }

// UnionStream
type UnionStream[T constraints.Ordered] struct {
	x, y           Unary[T]
	xHas, yHas     bool
	xNextK, yNextK T
	err            error
}

func Union[T constraints.Ordered](x, y Unary[T]) *UnionStream[T] {
	m := &UnionStream[T]{x: x, y: y}
	m.advanceX()
	m.advanceY()
	return m
}

func (m *UnionStream[T]) HasNext() bool {
	return m.err != nil || m.xHas || m.yHas
}
func (m *UnionStream[T]) advanceX() {
	if m.err != nil {
		return
	}
	m.xHas = m.x.HasNext()
	if m.xHas {
		m.xNextK, m.err = m.x.Next()
	}
}
func (m *UnionStream[T]) advanceY() {
	if m.err != nil {
		return
	}
	m.yHas = m.y.HasNext()
	if m.yHas {
		m.yNextK, m.err = m.y.Next()
	}
}
func (m *UnionStream[T]) Next() (res T, err error) {
	if m.err != nil {
		return res, m.err
	}
	if m.xHas && m.yHas {
		if m.xNextK < m.yNextK {
			k, err := m.xNextK, m.err
			m.advanceX()
			return k, err
		} else if m.xNextK == m.yNextK {
			k, err := m.xNextK, m.err
			m.advanceX()
			m.advanceY()
			return k, err
		}
		k, err := m.yNextK, m.err
		m.advanceY()
		return k, err
	}
	if m.xHas {
		k, err := m.xNextK, m.err
		m.advanceX()
		return k, err
	}
	k, err := m.yNextK, m.err
	m.advanceY()
	return k, err
}

// IntersectStream
type IntersectStream[T constraints.Ordered] struct {
	x, y               Unary[T]
	xHasNext, yHasNext bool
	xNextK, yNextK     T
	err                error
}

func Intersect[T constraints.Ordered](x, y Unary[T]) *IntersectStream[T] {
	m := &IntersectStream[T]{x: x, y: y}
	m.advance()
	return m
}
func (m *IntersectStream[T]) HasNext() bool { return m.xHasNext && m.yHasNext }
func (m *IntersectStream[T]) advance() {
	m.advanceX()
	m.advanceY()
	for m.xHasNext && m.yHasNext {
		if m.err != nil {
			break
		}
		if m.xNextK < m.yNextK {
			m.advanceX()
			continue
		} else if m.xNextK == m.yNextK {
			return
		} else {
			m.advanceY()
			continue
		}
	}
	m.xHasNext = false
}

func (m *IntersectStream[T]) advanceX() {
	if m.err != nil {
		return
	}
	m.xHasNext = m.x.HasNext()
	if m.xHasNext {
		m.xNextK, m.err = m.x.Next()
	}
}
func (m *IntersectStream[T]) advanceY() {
	if m.err != nil {
		return
	}
	m.yHasNext = m.y.HasNext()
	if m.yHasNext {
		m.yNextK, m.err = m.y.Next()
	}
}
func (m *IntersectStream[T]) Next() (T, error) {
	k, err := m.xNextK, m.err
	m.advance()
	return k, err
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

// TransformDualIter - analog `map` (in terms of map-filter-reduce pattern)
type TransformDualIter[K, V any] struct {
	it        Dual[K, V]
	transform func(K, V) (K, V)
}

func TransformDual[K, V any](it Dual[K, V], transform func(K, V) (K, V)) *TransformDualIter[K, V] {
	return &TransformDualIter[K, V]{it: it, transform: transform}
}
func (m *TransformDualIter[K, V]) HasNext() bool { return m.it.HasNext() }
func (m *TransformDualIter[K, V]) Next() (K, V, error) {
	k, v, err := m.it.Next()
	if err != nil {
		return k, v, err
	}
	newK, newV := m.transform(k, v)
	return newK, newV, nil
}

// FilterStream - analog `map` (in terms of map-filter-reduce pattern)
type FilterStream[K, V any] struct {
	it     Dual[K, V]
	filter func(K, V) bool
}

func FilterPairs(it KV, filter func(k, v []byte) bool) *FilterStream[[]byte, []byte] {
	return &FilterStream[[]byte, []byte]{it: it, filter: filter}
}
func Filter[K, V any](it Dual[K, V], filter func(K, V) bool) *FilterStream[K, V] {
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

// PaginatedIter - for remote-list pagination
//
//	Rationale: If an API does not support pagination from the start, supporting it later is troublesome because adding pagination breaks the API's behavior. Clients that are unaware that the API now uses pagination could incorrectly assume that they received a complete result, when in fact they only received the first page.
//
// To support pagination (returning list results in pages) in a List method, the API shall:
//   - The client uses this field to request a specific page of the list results.
//   - define an int32 field page_size in the List method's request message. Clients use this field to specify the maximum number of results to be returned by the server. The server may further constrain the maximum number of results returned in a single page. If the page_size is 0, the server will decide the number of results to be returned.
//   - define a string field next_page_token in the List method's response message. This field represents the pagination token to retrieve the next page of results. If the value is "", it means no further results for the request.
//
// see: https://cloud.google.com/apis/design/design_patterns
type Paginated[T any] struct {
	arr           []T
	i             int
	err           error
	nextPage      NextPageUnary[T]
	nextPageToken string
	initialized   bool
}

func Paginate[T any](f NextPageUnary[T]) *Paginated[T] { return &Paginated[T]{nextPage: f} }
func (it *Paginated[T]) HasNext() bool {
	if it.err != nil || it.i < len(it.arr) {
		return true
	}
	if it.initialized && it.nextPageToken == "" {
		return false
	}
	it.initialized = true
	it.i = 0
	it.arr, it.nextPageToken, it.err = it.nextPage(it.nextPageToken)
	return it.err != nil || it.i < len(it.arr)
}
func (it *Paginated[T]) Close() {}
func (it *Paginated[T]) Next() (v T, err error) {
	if it.err != nil {
		return v, it.err
	}
	v = it.arr[it.i]
	it.i++
	return v, nil
}

type PaginatedDual[K, V any] struct {
	keys          []K
	values        []V
	i             int
	err           error
	nextPage      NextPageDual[K, V]
	nextPageToken string
	initialized   bool
}

func PaginateDual[K, V any](f NextPageDual[K, V]) *PaginatedDual[K, V] {
	return &PaginatedDual[K, V]{nextPage: f}
}
func (it *PaginatedDual[K, V]) HasNext() bool {
	if it.err != nil || it.i < len(it.keys) {
		return true
	}
	if it.initialized && it.nextPageToken == "" {
		return false
	}
	it.initialized = true
	it.i = 0
	it.keys, it.values, it.nextPageToken, it.err = it.nextPage(it.nextPageToken)
	return it.err != nil || it.i < len(it.keys)
}
func (it *PaginatedDual[K, V]) Close() {}
func (it *PaginatedDual[K, V]) Next() (k K, v V, err error) {
	if it.err != nil {
		return k, v, it.err
	}
	k, v = it.keys[it.i], it.values[it.i]
	it.i++
	return k, v, nil
}
