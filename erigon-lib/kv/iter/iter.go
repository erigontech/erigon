/*
   Copyright 2021 Erigon contributors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package iter

import (
	"bytes"
	"slices"

	"github.com/ledgerwatch/erigon-lib/kv/order"
	"golang.org/x/exp/constraints"
)

type Closer interface {
	Close()
}

var (
	EmptyU64 = &EmptyUno[uint64]{}
	EmptyKV  = &EmptyDuo[[]byte, []byte]{}
	EmptyKVS = &EmptyTrio[[]byte, []byte, uint64]{}
)

type (
	EmptyUno[T any]          struct{}
	EmptyDuo[K, V any]       struct{}
	EmptyTrio[K, V1, V2 any] struct{}
)

func (EmptyUno[T]) HasNext() bool                                 { return false }
func (EmptyUno[T]) Next() (v T, err error)                        { return v, err }
func (EmptyDuo[K, V]) HasNext() bool                              { return false }
func (EmptyDuo[K, V]) Next() (k K, v V, err error)                { return k, v, err }
func (EmptyTrio[K, V1, v2]) HasNext() bool                        { return false }
func (EmptyTrio[K, V1, V2]) Next() (k K, v1 V1, v2 V2, err error) { return k, v1, v2, err }

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

// UnionKVIter - merge 2 kv.Pairs streams to 1 in lexicographically order
// 1-st stream has higher priority - when 2 streams return same key
type UnionKVIter struct {
	x, y               KV
	xHasNext, yHasNext bool
	xNextK, xNextV     []byte
	yNextK, yNextV     []byte
	limit              int
	err                error
}

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
	return m.err != nil || (m.limit != 0 && m.xHasNext) || (m.limit != 0 && m.yHasNext)
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

// func (m *UnionKVIter) ToArray() (keys, values [][]byte, err error) { return ToKVArray(m) }
func (m *UnionKVIter) Close() {
	if x, ok := m.x.(Closer); ok {
		x.Close()
	}
	if y, ok := m.y.(Closer); ok {
		y.Close()
	}
}

type WrapKVSIter struct {
	y              KV
	yHasNext       bool
	yNextK, yNextV []byte
	err            error
}

func WrapKVS(y KV) KVS {
	if y == nil {
		return EmptyKVS
	}
	m := &WrapKVSIter{y: y}
	m.advance()
	return m
}

func (m *WrapKVSIter) HasNext() bool {
	return m.err != nil || m.yHasNext
}
func (m *WrapKVSIter) advance() {
	if m.err != nil {
		return
	}
	m.yHasNext = m.y.HasNext()
	if m.yHasNext {
		m.yNextK, m.yNextV, m.err = m.y.Next()
	}
}
func (m *WrapKVSIter) Next() ([]byte, []byte, uint64, error) {
	if m.err != nil {
		return nil, nil, 0, m.err
	}
	k, v, err := m.yNextK, m.yNextV, m.err
	m.advance()
	return k, v, 0, err
}

// func (m *WrapKVSIter) ToArray() (keys, values [][]byte, err error) { return ToKVArray(m) }
func (m *WrapKVSIter) Close() {
	if y, ok := m.y.(Closer); ok {
		y.Close()
	}
}

type WrapKVIter struct {
	x              KVS
	xHasNext       bool
	xNextK, xNextV []byte
	err            error
}

func WrapKV(x KVS) KV {
	if x == nil {
		return EmptyKV
	}
	m := &WrapKVIter{x: x}
	m.advance()
	return m
}

func (m *WrapKVIter) HasNext() bool {
	return m.err != nil || m.xHasNext
}
func (m *WrapKVIter) advance() {
	if m.err != nil {
		return
	}
	m.xHasNext = m.x.HasNext()
	if m.xHasNext {
		m.xNextK, m.xNextV, _, m.err = m.x.Next()
	}
}
func (m *WrapKVIter) Next() ([]byte, []byte, error) {
	if m.err != nil {
		return nil, nil, m.err
	}
	k, v, err := m.xNextK, m.xNextV, m.err
	m.advance()
	return k, v, err
}

// func (m *WrapKVIter) ToArray() (keys, values [][]byte, err error) { return ToKVArray(m) }
func (m *WrapKVIter) Close() {
	if x, ok := m.x.(Closer); ok {
		x.Close()
	}
}

// MergeKVIter - merge 2 kv.Pairs streams (without replacements, or "shadowing",
// meaning that all input pairs will appear in the output stream - this is
// difference to UnionKVIter), to 1 in lexicographically order
// 1-st stream has higher priority - when 2 streams return same key
type MergeKVIter struct {
	x                  KVS
	y                  KV
	xHasNext, yHasNext bool
	xNextK, xNextV     []byte
	yNextK, yNextV     []byte
	xStep              uint64
	limit              int
	err                error
}

func MergeKVS(x KVS, y KV, limit int) KVS {
	if x == nil && y == nil {
		return EmptyKVS
	}
	if x == nil {
		return WrapKVS(y)
	}
	if y == nil {
		return x
	}
	m := &MergeKVIter{x: x, y: y, limit: limit}
	m.advanceX()
	m.advanceY()
	return m
}
func (m *MergeKVIter) HasNext() bool {
	return m.err != nil || (m.limit != 0 && m.xHasNext) || (m.limit != 0 && m.yHasNext)
}
func (m *MergeKVIter) advanceX() {
	if m.err != nil {
		return
	}
	m.xHasNext = m.x.HasNext()
	if m.xHasNext {
		m.xNextK, m.xNextV, m.xStep, m.err = m.x.Next()
	}
}
func (m *MergeKVIter) advanceY() {
	if m.err != nil {
		return
	}
	m.yHasNext = m.y.HasNext()
	if m.yHasNext {
		m.yNextK, m.yNextV, m.err = m.y.Next()
	}
}
func (m *MergeKVIter) Next() ([]byte, []byte, uint64, error) {
	if m.err != nil {
		return nil, nil, 0, m.err
	}
	m.limit--
	if m.xHasNext && m.yHasNext {
		cmp := bytes.Compare(m.xNextK, m.yNextK)
		if cmp <= 0 {
			k, v, step, err := m.xNextK, m.xNextV, m.xStep, m.err
			m.advanceX()
			return k, v, step, err
		}
		k, v, err := m.yNextK, m.yNextV, m.err
		m.advanceY()
		return k, v, 0, err
	}
	if m.xHasNext {
		k, v, step, err := m.xNextK, m.xNextV, m.xStep, m.err
		m.advanceX()
		return k, v, step, err
	}
	k, v, err := m.yNextK, m.yNextV, m.err
	m.advanceY()
	return k, v, 0, err
}

// func (m *MergeKVIter) ToArray() (keys, values [][]byte, err error) { return ToKVArray(m) }
func (m *MergeKVIter) Close() {
	if x, ok := m.x.(Closer); ok {
		x.Close()
	}
	if y, ok := m.y.(Closer); ok {
		y.Close()
	}
}

// UnionUno
type UnionUno[T constraints.Ordered] struct {
	x, y           Uno[T]
	asc            bool
	xHas, yHas     bool
	xNextK, yNextK T
	err            error
	limit          int
}

func Union[T constraints.Ordered](x, y Uno[T], asc order.By, limit int) Uno[T] {
	if x == nil && y == nil {
		return &EmptyUno[T]{}
	}
	if x == nil {
		return y
	}
	if y == nil {
		return x
	}
	if !x.HasNext() {
		return y
	}
	if !y.HasNext() {
		return x
	}
	m := &UnionUno[T]{x: x, y: y, asc: bool(asc), limit: limit}
	m.advanceX()
	m.advanceY()
	return m
}

func (m *UnionUno[T]) HasNext() bool {
	return m.err != nil || (m.limit != 0 && m.xHas) || (m.limit != 0 && m.yHas)
}
func (m *UnionUno[T]) advanceX() {
	if m.err != nil {
		return
	}
	m.xHas = m.x.HasNext()
	if m.xHas {
		m.xNextK, m.err = m.x.Next()
	}
}
func (m *UnionUno[T]) advanceY() {
	if m.err != nil {
		return
	}
	m.yHas = m.y.HasNext()
	if m.yHas {
		m.yNextK, m.err = m.y.Next()
	}
}

func (m *UnionUno[T]) less() bool {
	return (m.asc && m.xNextK < m.yNextK) || (!m.asc && m.xNextK > m.yNextK)
}

func (m *UnionUno[T]) Next() (res T, err error) {
	if m.err != nil {
		return res, m.err
	}
	m.limit--
	if m.xHas && m.yHas {
		if m.less() {
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
func (m *UnionUno[T]) Close() {
	if x, ok := m.x.(Closer); ok {
		x.Close()
	}
	if y, ok := m.y.(Closer); ok {
		y.Close()
	}
}

// IntersectIter
type IntersectIter[T constraints.Ordered] struct {
	x, y               Uno[T]
	xHasNext, yHasNext bool
	xNextK, yNextK     T
	limit              int
	err                error
}

func Intersect[T constraints.Ordered](x, y Uno[T], limit int) Uno[T] {
	if x == nil || y == nil || !x.HasNext() || !y.HasNext() {
		return &EmptyUno[T]{}
	}
	m := &IntersectIter[T]{x: x, y: y, limit: limit}
	m.advance()
	return m
}
func (m *IntersectIter[T]) HasNext() bool {
	return m.err != nil || (m.limit != 0 && m.xHasNext && m.yHasNext)
}
func (m *IntersectIter[T]) advance() {
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

func (m *IntersectIter[T]) advanceX() {
	if m.err != nil {
		return
	}
	m.xHasNext = m.x.HasNext()
	if m.xHasNext {
		m.xNextK, m.err = m.x.Next()
	}
}
func (m *IntersectIter[T]) advanceY() {
	if m.err != nil {
		return
	}
	m.yHasNext = m.y.HasNext()
	if m.yHasNext {
		m.yNextK, m.err = m.y.Next()
	}
}
func (m *IntersectIter[T]) Next() (T, error) {
	if m.err != nil {
		return m.xNextK, m.err
	}
	m.limit--
	k, err := m.xNextK, m.err
	m.advance()
	return k, err
}
func (m *IntersectIter[T]) Close() {
	if x, ok := m.x.(Closer); ok {
		x.Close()
	}
	if y, ok := m.y.(Closer); ok {
		y.Close()
	}
}

// TransformDuoIter - analog `map` (in terms of map-filter-reduce pattern)
type TransformDuoIter[K, V any] struct {
	it        Duo[K, V]
	transform func(K, V) (K, V, error)
}

func TransformDuo[K, V any](it Duo[K, V], transform func(K, V) (K, V, error)) *TransformDuoIter[K, V] {
	return &TransformDuoIter[K, V]{it: it, transform: transform}
}
func (m *TransformDuoIter[K, V]) HasNext() bool { return m.it.HasNext() }
func (m *TransformDuoIter[K, V]) Next() (K, V, error) {
	k, v, err := m.it.Next()
	if err != nil {
		return k, v, err
	}
	return m.transform(k, v)
}
func (m *TransformDuoIter[K, v]) Close() {
	if x, ok := m.it.(Closer); ok {
		x.Close()
	}
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

// FilterDuoIter - analog `map` (in terms of map-filter-reduce pattern)
// please avoid reading from Disk/DB more elements and then filter them. Better
// push-down filter conditions to lower-level iterator to reduce disk reads amount.
type FilterDuoIter[K, V any] struct {
	it      Duo[K, V]
	filter  func(K, V) bool
	hasNext bool
	err     error
	nextK   K
	nextV   V
}

func FilterKV(it KV, filter func(k, v []byte) bool) *FilterDuoIter[[]byte, []byte] {
	return FilterDuo[[]byte, []byte](it, filter)
}
func FilterDuo[K, V any](it Duo[K, V], filter func(K, V) bool) *FilterDuoIter[K, V] {
	i := &FilterDuoIter[K, V]{it: it, filter: filter}
	i.advance()
	return i
}
func (m *FilterDuoIter[K, V]) advance() {
	if m.err != nil {
		return
	}
	m.hasNext = false
	for m.it.HasNext() {
		// create new variables, to avoid leaking outside of loop
		key, val, err := m.it.Next()
		if err != nil {
			m.err = err
			return
		}
		if m.filter(key, val) {
			m.hasNext = true
			m.nextK, m.nextV = key, val
			break
		}
	}
}
func (m *FilterDuoIter[K, V]) HasNext() bool { return m.err != nil || m.hasNext }
func (m *FilterDuoIter[K, V]) Next() (k K, v V, err error) {
	k, v, err = m.nextK, m.nextV, m.err
	m.advance()
	return k, v, err
}
func (m *FilterDuoIter[K, v]) Close() {
	if x, ok := m.it.(Closer); ok {
		x.Close()
	}
}

// FilterUnoIter - analog `map` (in terms of map-filter-reduce pattern)
// please avoid reading from Disk/DB more elements and then filter them. Better
// push-down filter conditions to lower-level iterator to reduce disk reads amount.
type FilterUnoIter[T any] struct {
	it      Uno[T]
	filter  func(T) bool
	hasNext bool
	err     error
	nextK   T
}

func FilterU64(it U64, filter func(k uint64) bool) *FilterUnoIter[uint64] {
	return FilterUno[uint64](it, filter)
}
func FilterUno[T any](it Uno[T], filter func(T) bool) *FilterUnoIter[T] {
	i := &FilterUnoIter[T]{it: it, filter: filter}
	i.advance()
	return i
}
func (m *FilterUnoIter[T]) advance() {
	if m.err != nil {
		return
	}
	m.hasNext = false
	for m.it.HasNext() {
		// create new variables, to avoid leaking outside of loop
		key, err := m.it.Next()
		if err != nil {
			m.err = err
			return
		}
		if m.filter(key) {
			m.hasNext, m.nextK = true, key
			break
		}
	}
}
func (m *FilterUnoIter[T]) HasNext() bool { return m.err != nil || m.hasNext }
func (m *FilterUnoIter[T]) Next() (k T, err error) {
	k, err = m.nextK, m.err
	m.advance()
	return k, err
}
func (m *FilterUnoIter[T]) Close() {
	if x, ok := m.it.(Closer); ok {
		x.Close()
	}
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
	nextPage      NextPageUno[T]
	nextPageToken string
	initialized   bool
}

func Paginate[T any](f NextPageUno[T]) *Paginated[T] { return &Paginated[T]{nextPage: f} }
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

type PaginatedDuo[K, V any] struct {
	keys          []K
	values        []V
	i             int
	err           error
	nextPage      NextPageDuo[K, V]
	nextPageToken string
	initialized   bool
}

func PaginateDuo[K, V any](f NextPageDuo[K, V]) *PaginatedDuo[K, V] {
	return &PaginatedDuo[K, V]{nextPage: f}
}
func (it *PaginatedDuo[K, V]) HasNext() bool {
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
func (it *PaginatedDuo[K, V]) Close() {}
func (it *PaginatedDuo[K, V]) Next() (k K, v V, err error) {
	if it.err != nil {
		return k, v, it.err
	}
	k, v = it.keys[it.i], it.values[it.i]
	it.i++
	return k, v, nil
}
