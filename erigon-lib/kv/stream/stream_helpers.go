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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func ToArray[T any](s Uno[T]) (res []T, err error) {
	for s.HasNext() {
		k, err := s.Next()
		if err != nil {
			return res, err
		}
		res = append(res, k)
	}
	return res, nil
}

func ToArrayDuo[K, V any](s Duo[K, V]) (keys []K, values []V, err error) {
	for s.HasNext() {
		k, v, err := s.Next()
		if err != nil {
			return keys, values, err
		}
		keys = append(keys, k)
		values = append(values, v)
	}
	return keys, values, nil
}

func ExpectEqualU64(tb testing.TB, s1, s2 Uno[uint64]) {
	tb.Helper()
	ExpectEqual[uint64](tb, s1, s2)
}
func ExpectEqual[V comparable](tb testing.TB, s1, s2 Uno[V]) {
	tb.Helper()
	for s1.HasNext() && s2.HasNext() {
		k1, e1 := s1.Next()
		k2, e2 := s2.Next()
		require.Equal(tb, e1 == nil, e2 == nil)
		require.Equal(tb, k1, k2)
	}

	has1 := s1.HasNext()
	has2 := s2.HasNext()
	var label string
	if has1 {
		v1, _ := s1.Next()
		label = fmt.Sprintf("v1: %v", v1)
	}
	if has2 {
		v2, _ := s2.Next()
		label += fmt.Sprintf(" v2: %v", v2)
	}
	require.False(tb, has1, label)
	require.False(tb, has2, label)
}

// PairsWithErrorIter - return N, keys and then error
type PairsWithErrorIter struct {
	errorAt, i int
}

func PairsWithError(errorAt int) *PairsWithErrorIter {
	return &PairsWithErrorIter{errorAt: errorAt}
}
func (m *PairsWithErrorIter) Close()        {}
func (m *PairsWithErrorIter) HasNext() bool { return true }
func (m *PairsWithErrorIter) Next() ([]byte, []byte, error) {
	if m.i >= m.errorAt {
		return nil, nil, fmt.Errorf("expected error at iteration: %d", m.errorAt)
	}
	m.i++
	return []byte(fmt.Sprintf("%x", m.i)), []byte(fmt.Sprintf("%x", m.i)), nil
}

func Count[T any](s Uno[T]) (cnt int, err error) {
	for s.HasNext() {
		_, err := s.Next()
		if err != nil {
			return cnt, err
		}
		cnt++
	}
	return cnt, err
}

func CountDuo[K, V any](s Duo[K, V]) (cnt int, err error) {
	for s.HasNext() {
		_, _, err := s.Next()
		if err != nil {
			return cnt, err
		}
		cnt++
	}
	return cnt, err
}
