// Copyright 2021 The Erigon Authors
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
	"cmp"
	"fmt"
	"slices"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv/order"
)

type integer interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 | ~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~uintptr
}

type (
	Empty[T any]             struct{}
	EmptyDuo[K, V any]       struct{}
	EmptyTrio[K, V1, V2 any] struct{}
	SingleDuo[K, V any]      struct {
		k       K
		v       V
		hasNext bool
	}
)

func (Empty[T]) HasNext() bool          { return false }
func (Empty[T]) Next() (v T, err error) { return v, ErrIteratorExhausted }
func (Empty[T]) Close()                 {}

func (EmptyDuo[K, V]) HasNext() bool               { return false }
func (EmptyDuo[K, V]) Next() (k K, v V, err error) { return k, v, ErrIteratorExhausted }
func (EmptyDuo[K, V]) Close()                      {}

func (EmptyTrio[K, V1, V2]) HasNext() bool { return false }
func (EmptyTrio[K, V1, V2]) Next() (k K, v1 V1, v2 V2, err error) {
	return k, v1, v2, ErrIteratorExhausted
}
func (EmptyTrio[K, V1, V2]) Close() {}

func NewSingleDuo[K, V any](k K, v V) *SingleDuo[K, V] {
	return &SingleDuo[K, V]{k: k, v: v, hasNext: true}
}
func (s *SingleDuo[K, V]) HasNext() bool { return s.hasNext }
func (s *SingleDuo[K, V]) Next() (k K, v V, err error) {
	if !s.hasNext {
		return k, v, ErrIteratorExhausted
	}
	s.hasNext = false
	return s.k, s.v, nil
}
func (s *SingleDuo[K, V]) Close() {}

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
func (it *ArrStream[V]) Next() (v V, err error) {
	if !it.HasNext() {
		return v, ErrIteratorExhausted
	}
	v = it.arr[it.i]
	it.i++
	return v, nil
}
func (it *ArrStream[V]) NextBatch() ([]V, error) {
	v := it.arr[it.i:]
	it.i = len(it.arr)
	return v, nil
}

// Range - ascending [from, to)
func Range[T integer](from, to T) *RangeIter[T] {
	return &RangeIter[T]{i: from, to: to}
}

type RangeIter[T integer] struct {
	i, to T
}

func (it *RangeIter[T]) HasNext() bool { return it.i < it.to }
func (it *RangeIter[T]) Close()        {}
func (it *RangeIter[T]) Next() (v T, err error) {
	if !it.HasNext() {
		return v, ErrIteratorExhausted
	}
	v = it.i
	it.i++
	return v, nil
}

// ReverseRange - descending [from, to), matching kv.TemporalTx.IndexRange's Desc semantic.
// For unsigned T the last element is `to+1`, so 0 is not reachable as an element.
func ReverseRange[T integer](from, to T) *ReverseRangeIter[T] {
	return &ReverseRangeIter[T]{i: from, to: to}
}

type ReverseRangeIter[T integer] struct {
	i, to T
}

func (it *ReverseRangeIter[T]) HasNext() bool { return it.i > it.to }
func (it *ReverseRangeIter[T]) Close()        {}
func (it *ReverseRangeIter[T]) Next() (v T, err error) {
	if !it.HasNext() {
		return v, ErrIteratorExhausted
	}
	v = it.i
	it.i--
	return v, nil
}

// Limit - caps a stream at `limit` elements. limit<0 (kv.Unlim) means unlimited and returns `it` as-is.
func Limit[T any](it Uno[T], limit int) Uno[T] {
	if limit < 0 {
		return it
	}
	return &Limited[T]{it: it, limit: limit}
}

type Limited[T any] struct {
	it    Uno[T]
	limit int
}

func (m *Limited[T]) HasNext() bool { return m.limit > 0 && m.it.HasNext() }
func (m *Limited[T]) Close()        { m.it.Close() }
func (m *Limited[T]) Next() (v T, err error) {
	if m.limit <= 0 {
		return v, ErrIteratorExhausted
	}
	v, err = m.it.Next()
	if err == nil {
		m.limit--
	}
	return v, err
}

// LimitDuo - caps a stream at `limit` elements. limit<0 (kv.Unlim) means unlimited and returns `it` as-is.
func LimitDuo[K, V any](it Duo[K, V], limit int) Duo[K, V] {
	if limit < 0 {
		return it
	}
	return &LimitedDuo[K, V]{it: it, limit: limit}
}

type LimitedDuo[K, V any] struct {
	it    Duo[K, V]
	limit int
}

func (m *LimitedDuo[K, V]) HasNext() bool { return m.limit > 0 && m.it.HasNext() }
func (m *LimitedDuo[K, V]) Close()        { m.it.Close() }
func (m *LimitedDuo[K, V]) Next() (k K, v V, err error) {
	if m.limit <= 0 {
		return k, v, ErrIteratorExhausted
	}
	k, v, err = m.it.Next()
	if err == nil {
		m.limit--
	}
	return k, v, err
}

type UnionUno[T cmp.Ordered] struct {
	x, y           Uno[T]
	asc            bool
	xHas, yHas     bool
	xNextK, yNextK T
	err            error
	limit          int
}

// Union - returns all elements that are in A, or in B, or in both. When duplicate elements - first stream (x) takes precedence.
// in Set Theory: A ∪ B = {x | x ∈ A ∨ x ∈ B}
func Union[T cmp.Ordered](x, y Uno[T], asc order.By, limit int) Uno[T] {
	if x == nil && y == nil {
		return &Empty[T]{}
	}
	if x == nil {
		return Limit(y, limit)
	}
	if y == nil {
		return Limit(x, limit)
	}
	if !x.HasNext() {
		x.Close()
		return Limit(y, limit)
	}
	if !y.HasNext() {
		y.Close()
		return Limit(x, limit)
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
	if !m.HasNext() {
		return res, ErrIteratorExhausted
	}
	m.limit--
	if m.xHas && m.yHas {
		if m.less() {
			k := m.xNextK
			m.advanceX()
			return k, nil
		} else if m.xNextK == m.yNextK {
			k := m.xNextK
			m.advanceX()
			m.advanceY()
			return k, nil
		}
		k := m.yNextK
		m.advanceY()
		return k, nil
	}
	if m.xHas {
		k := m.xNextK
		m.advanceX()
		return k, nil
	}
	k := m.yNextK
	m.advanceY()
	return k, nil
}
func (m *UnionUno[T]) Close() {
	m.x.Close()
	m.y.Close()
}

// Intersected
type Intersected[T cmp.Ordered] struct {
	x, y               Uno[T]
	xHasNext, yHasNext bool
	xNextK, yNextK     T
	asc                order.By
	limit              int
	err                error
}

// Intersect - returns only elements that exist in BOTH A AND B
// Set Theory Definition: A ∩ B = {x | x ∈ A ∧ x ∈ B}
func Intersect[T cmp.Ordered](x, y Uno[T], asc order.By, limit int) Uno[T] {
	if x == nil || y == nil || !x.HasNext() || !y.HasNext() {
		if x != nil {
			x.Close()
		}
		if y != nil {
			y.Close()
		}
		return &Empty[T]{}
	}
	m := &Intersected[T]{x: x, y: y, asc: asc, limit: limit}
	m.advance()
	return m
}
func (m *Intersected[T]) HasNext() bool {
	return m.err != nil || (m.limit != 0 && m.xHasNext && m.yHasNext)
}
func (m *Intersected[T]) advance() {
	m.advanceX()
	m.advanceY()
	for m.xHasNext && m.yHasNext {
		if m.err != nil {
			break
		}
		if m.xNextK == m.yNextK {
			return
		}
		if m.asc {
			if m.xNextK < m.yNextK {
				m.advanceX()
				continue
			} else {
				m.advanceY()
				continue
			}
		} else {
			if m.xNextK < m.yNextK {
				m.advanceY()
				continue
			} else {
				m.advanceX()
				continue
			}

		}
	}
	m.xHasNext = false
}

func (m *Intersected[T]) advanceX() {
	if m.err != nil {
		return
	}
	m.xHasNext = m.x.HasNext()
	if m.xHasNext {
		m.xNextK, m.err = m.x.Next()
	}
}
func (m *Intersected[T]) advanceY() {
	if m.err != nil {
		return
	}
	m.yHasNext = m.y.HasNext()
	if m.yHasNext {
		m.yNextK, m.err = m.y.Next()
	}
}
func (m *Intersected[T]) Next() (res T, err error) {
	if m.err != nil {
		return res, m.err
	}
	if !m.HasNext() {
		return res, ErrIteratorExhausted
	}
	m.limit--
	k := m.xNextK
	m.advance()
	return k, nil
}
func (m *Intersected[T]) Close() {
	m.x.Close()
	m.y.Close()
}

// TransformedDuo - analog `map` (in terms of map-filter-reduce pattern)
type TransformedDuo[K, V any] struct {
	it        Duo[K, V]
	transform func(K, V) (K, V, error)
	err       error
}

func TransformDuo[K, V any](it Duo[K, V], transform func(K, V) (K, V, error)) *TransformedDuo[K, V] {
	return &TransformedDuo[K, V]{it: it, transform: transform}
}
func (m *TransformedDuo[K, V]) HasNext() bool { return m.err != nil || m.it.HasNext() }
func (m *TransformedDuo[K, V]) Next() (k K, v V, err error) {
	if m.err != nil {
		return k, v, m.err
	}
	k, v, err = m.it.Next()
	if err != nil {
		return k, v, err
	}
	k, v, m.err = m.transform(k, v)
	return k, v, m.err
}
func (m *TransformedDuo[K, v]) Close() {
	m.it.Close()
}

// TransformedDuoV - analog `map` (in terms of map-filter-reduce pattern) but with different value type
type TransformedDuoV[K, V, VR any] struct {
	it        Duo[K, V]
	transform func(K, V) (K, VR, error)
	err       error
}

func TransformDuoV[K, V, VR any](it Duo[K, V], transform func(K, V) (K, VR, error)) *TransformedDuoV[K, V, VR] {
	return &TransformedDuoV[K, V, VR]{it: it, transform: transform}
}
func (m *TransformedDuoV[K, V, VR]) HasNext() bool { return m.err != nil || m.it.HasNext() }
func (m *TransformedDuoV[K, V, VR]) Next() (k K, vr VR, err error) {
	if m.err != nil {
		return k, vr, m.err
	}
	k, v, err := m.it.Next()
	if err != nil {
		return k, vr, err
	}
	k, vr, m.err = m.transform(k, v)
	return k, vr, m.err
}
func (m *TransformedDuoV[K, V, VR]) Close() {
	m.it.Close()
}

// FilteredDuo - analog `map` (in terms of map-filter-reduce pattern)
// please avoid reading from Disk/DB more elements and then filter them. Better
// push-down filter conditions to lower-level iterator to reduce disk reads amount.
type FilteredDuo[K, V any] struct {
	it      Duo[K, V]
	filter  func(K, V) bool
	hasNext bool
	err     error
	nextK   K
	nextV   V
}

func FilterDuo[K, V any](it Duo[K, V], filter func(K, V) bool) *FilteredDuo[K, V] {
	i := &FilteredDuo[K, V]{it: it, filter: filter}
	i.advance()
	return i
}
func (m *FilteredDuo[K, V]) advance() {
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
func (m *FilteredDuo[K, V]) HasNext() bool { return m.err != nil || m.hasNext }
func (m *FilteredDuo[K, V]) Next() (k K, v V, err error) {
	if m.err != nil {
		return k, v, m.err
	}
	if !m.hasNext {
		return k, v, ErrIteratorExhausted
	}
	k, v = m.nextK, m.nextV
	m.advance()
	return k, v, nil
}
func (m *FilteredDuo[K, v]) Close() {
	m.it.Close()
}

// Filtered - analog `map` (in terms of map-filter-reduce pattern)
// please avoid reading from Disk/DB more elements and then filter them. Better
// push-down filter conditions to lower-level iterator to reduce disk reads amount.
type Filtered[T any] struct {
	it      Uno[T]
	filter  func(T) bool
	hasNext bool
	err     error
	nextK   T
}

func Filter[T any](it Uno[T], filter func(T) bool) *Filtered[T] {
	i := &Filtered[T]{it: it, filter: filter}
	i.advance()
	return i
}
func (m *Filtered[T]) advance() {
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
func (m *Filtered[T]) HasNext() bool { return m.err != nil || m.hasNext }
func (m *Filtered[T]) Next() (k T, err error) {
	if m.err != nil {
		return k, m.err
	}
	if !m.hasNext {
		return k, ErrIteratorExhausted
	}
	k = m.nextK
	m.advance()
	return k, nil
}
func (m *Filtered[T]) Close() {
	m.it.Close()
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
	emptyPages    int
}

func Paginate[T any](f NextPageUno[T]) *Paginated[T] { return &Paginated[T]{nextPage: f} }

// maxEmptyPages - only "" terminates a listing, so a server that keeps handing back fresh tokens
// with no rows would be polled forever. Cycles of any length end up here, not just an echoed token.
const maxEmptyPages = 1024

func countEmptyPages(seen, pageLen int) int {
	if pageLen > 0 {
		return 0
	}
	return seen + 1
}

// errNoPageProgress - fails a run of empty pages rather than letting HasNext spin on it. An echoed
// token is caught on the spot; a longer cycle of fresh tokens falls to the empty-page cap.
func errNoPageProgress(err error, pageLen, emptyPages int, sent, got string) error {
	if err != nil || pageLen > 0 {
		return err
	}
	if got != "" && got == sent {
		return fmt.Errorf("stream: pagination made no progress, token %q returned an empty page", sent)
	}
	if emptyPages > maxEmptyPages {
		return fmt.Errorf("stream: pagination made no progress, %d empty pages, last token %q", emptyPages, sent)
	}
	return nil
}
func (it *Paginated[T]) HasNext() bool {
	for it.err == nil && it.i >= len(it.arr) {
		if it.initialized && it.nextPageToken == "" {
			return false
		}
		sent := it.nextPageToken
		it.initialized = true
		it.i = 0
		it.arr, it.nextPageToken, it.err = it.nextPage(sent)
		it.emptyPages = countEmptyPages(it.emptyPages, len(it.arr))
		it.err = errNoPageProgress(it.err, len(it.arr), it.emptyPages, sent, it.nextPageToken)
	}
	return true
}
func (it *Paginated[T]) Close() {}
func (it *Paginated[T]) Next() (v T, err error) {
	if it.err != nil {
		return v, it.err
	}
	if it.i >= len(it.arr) { // not !HasNext(): that one fetches a page
		return v, ErrIteratorExhausted
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
	emptyPages    int
}

func PaginateDuo[K, V any](f NextPageDuo[K, V]) *PaginatedDuo[K, V] {
	return &PaginatedDuo[K, V]{nextPage: f}
}
func (it *PaginatedDuo[K, V]) HasNext() bool {
	for it.err == nil && it.i >= len(it.keys) {
		if it.initialized && it.nextPageToken == "" {
			return false
		}
		sent := it.nextPageToken
		it.initialized = true
		it.i = 0
		it.keys, it.values, it.nextPageToken, it.err = it.nextPage(sent)
		it.emptyPages = countEmptyPages(it.emptyPages, len(it.keys))
		it.err = errNoPageProgress(it.err, len(it.keys), it.emptyPages, sent, it.nextPageToken)
	}
	return true
}
func (it *PaginatedDuo[K, V]) Close() {}
func (it *PaginatedDuo[K, V]) Next() (k K, v V, err error) {
	if it.err != nil {
		return k, v, it.err
	}
	if it.i >= len(it.keys) { // not !HasNext(): that one fetches a page
		return k, v, ErrIteratorExhausted
	}
	k, v = it.keys[it.i], it.values[it.i]
	it.i++
	return k, v, nil
}

// ---- tracing ----

// Traced - does `log.Warn` every .Next() call
type Traced[T any] struct {
	it     Uno[T]
	logger log.Logger
	prefix string
}

func Trace[T any](it Uno[T], logger log.Logger, prefix string) *Traced[T] {
	if logger == nil {
		logger = log.Root()
	}
	return &Traced[T]{it: it, logger: logger, prefix: prefix}
}
func (m *Traced[T]) HasNext() bool {
	res := m.it.HasNext()
	m.logger.Warn(m.prefix, "hasNext", res)
	return res
}
func (m *Traced[T]) Next() (k T, err error) {
	k, err = m.it.Next()
	m.logger.Warn(m.prefix, "next", k)
	return k, err
}
func (m *Traced[T]) Close() {
	m.it.Close()
}

// TracedDuo - does `log.Warn` every .Next() call
type TracedDuo[K, V any] struct {
	it     Duo[K, V]
	logger log.Logger
	prefix string
}

func TraceDuo[K, V any](it Duo[K, V], logger log.Logger, prefix string) *TracedDuo[K, V] {
	if logger == nil {
		logger = log.Root()
	}
	return &TracedDuo[K, V]{it: it, logger: logger, prefix: prefix}
}
func (m *TracedDuo[K, V]) HasNext() bool {
	res := m.it.HasNext()
	m.logger.Warn(m.prefix, "hasNext", res)
	return res
}
func (m *TracedDuo[K, V]) Next() (k K, v V, err error) {
	k, v, err = m.it.Next()
	switch typedK := any(k).(type) {
	case []byte:
		m.logger.Warn(m.prefix, "next", fmt.Sprintf("%x", typedK))
	default:
		m.logger.Warn(m.prefix, "next", typedK)
	}
	return k, v, err
}
func (m *TracedDuo[K, V]) Close() {
	m.it.Close()
}

// Union Duo
type UnionDuo[K cmp.Ordered, V any] struct {
	x, y           Duo[K, V]
	asc            bool
	xHas, yHas     bool
	xNextK, yNextK K
	xNextV, yNextV V
	err            error
	limit          int
}

// Union - returns all elements that are in A, or in B, or in both. When duplicate elements - first stream (x) takes precedence.
// in Set Theory: A ∪ B = {x | x ∈ A ∨ x ∈ B}
func Union2[K cmp.Ordered, V any](x, y Duo[K, V], asc order.By, limit int) Duo[K, V] {
	if x == nil && y == nil {
		return &EmptyDuo[K, V]{}
	}
	if x == nil {
		return LimitDuo(y, limit)
	}
	if y == nil {
		return LimitDuo(x, limit)
	}
	if !x.HasNext() {
		x.Close()
		return LimitDuo(y, limit)
	}
	if !y.HasNext() {
		y.Close()
		return LimitDuo(x, limit)
	}
	m := &UnionDuo[K, V]{x: x, y: y, asc: bool(asc), limit: limit}
	m.advanceX()
	m.advanceY()
	return m
}

func (m *UnionDuo[K, V]) HasNext() bool {
	return m.err != nil || (m.limit != 0 && m.xHas) || (m.limit != 0 && m.yHas)
}
func (m *UnionDuo[K, V]) advanceX() {
	if m.err != nil {
		return
	}
	m.xHas = m.x.HasNext()
	if m.xHas {
		m.xNextK, m.xNextV, m.err = m.x.Next()
	}
}
func (m *UnionDuo[K, V]) advanceY() {
	if m.err != nil {
		return
	}
	m.yHas = m.y.HasNext()
	if m.yHas {
		m.yNextK, m.yNextV, m.err = m.y.Next()
	}
}

func (m *UnionDuo[K, V]) less() bool {
	return (m.asc && m.xNextK < m.yNextK) || (!m.asc && m.xNextK > m.yNextK)
}

func (m *UnionDuo[K, V]) Next() (res K, resV V, err error) {
	if m.err != nil {
		return res, resV, m.err
	}
	if !m.HasNext() {
		return res, resV, ErrIteratorExhausted
	}
	m.limit--
	if m.xHas && m.yHas {
		if m.less() {
			k, v := m.xNextK, m.xNextV
			m.advanceX()
			return k, v, nil
		} else if m.xNextK == m.yNextK {
			k, v := m.xNextK, m.xNextV
			m.advanceX()
			m.advanceY()
			return k, v, nil
		}
		k, v := m.yNextK, m.yNextV
		m.advanceY()
		return k, v, nil
	}
	if m.xHas {
		k, v := m.xNextK, m.xNextV
		m.advanceX()
		return k, v, nil
	}
	k, v := m.yNextK, m.yNextV
	m.advanceY()
	return k, v, nil
}
func (m *UnionDuo[K, V]) Close() {
	m.x.Close()
	m.y.Close()
}

// AssertValid wraps a []byte-keyed stream to enforce invariant 2 in debug builds: a key must
// stay readable across the following Next(), because combinators pre-fetch and burn one call of
// validity before the caller ever sees the value. Panics on a producer that recycles sooner.
// Returns `it` unchanged unless ERIGON_ASSERT is set.
func AssertValid[V any](it Duo[[]byte, V]) Duo[[]byte, V] {
	if !dbg.AssertEnabled {
		return it
	}
	return NewValidated(it)
}

// NewValidated - AssertValid without the ERIGON_ASSERT gate, for callers that want the check
// unconditionally.
func NewValidated[V any](it Duo[[]byte, V]) *Validated[V] { return &Validated[V]{it: it} }

type Validated[V any] struct {
	it   Duo[[]byte, V]
	prev [2]handout // key and value handed out last time: producer memory plus its contents then
}

// handout - a buffer the producer gave us, alongside a snapshot of what it held at the time.
type handout struct {
	buf  []byte
	snap []byte
	set  bool
}

func (h *handout) check(what string) {
	if h.set && !bytes.Equal(h.buf, h.snap) {
		panic(fmt.Sprintf("stream invariant 2: %s %x was recycled after a single Next(), now %x", what, h.snap, h.buf))
	}
}

func (h *handout) remember(b []byte) { *h = handout{buf: b, snap: bytes.Clone(b), set: true} }

func (m *Validated[V]) HasNext() bool { return m.it.HasNext() }
func (m *Validated[V]) Close()        { m.it.Close() }

func (m *Validated[V]) Next() ([]byte, V, error) {
	k, v, err := m.it.Next()
	m.prev[0].check("key")
	m.prev[1].check("value")
	if err != nil {
		return k, v, err
	}
	m.prev[0].remember(k)
	if vb, ok := any(v).([]byte); ok {
		m.prev[1].remember(vb)
	}
	return k, v, nil
}
