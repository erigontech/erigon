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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func ToArr[T any](s Unary[T]) (res []T, err error) {
	for s.HasNext() {
		k, err := s.Next()
		if err != nil {
			return res, err
		}
		res = append(res, k)
	}
	return res, nil
}

func ToDualArray[K, V any](s Dual[K, V]) (keys []K, values []V, err error) {
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

func ExpectEqualU64(tb testing.TB, s1, s2 Unary[uint64]) {
	tb.Helper()
	ExpectEqual[uint64](tb, s1, s2)
}
func ExpectEqual[V comparable](tb testing.TB, s1, s2 Unary[V]) {
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
func (m *PairsWithErrorIter) HasNext() bool { return true }
func (m *PairsWithErrorIter) Next() ([]byte, []byte, error) {
	if m.i >= m.errorAt {
		return nil, nil, fmt.Errorf("expected error at iteration: %d", m.errorAt)
	}
	m.i++
	return []byte(fmt.Sprintf("%x", m.i)), []byte(fmt.Sprintf("%x", m.i)), nil
}

func Count[T any](s Unary[T]) (cnt int, err error) {
	for s.HasNext() {
		_, err := s.Next()
		if err != nil {
			return cnt, err
		}
		cnt++
	}
	return cnt, err
}

func CountDual[K, V any](s Dual[K, V]) (cnt int, err error) {
	for s.HasNext() {
		_, _, err := s.Next()
		if err != nil {
			return cnt, err
		}
		cnt++
	}
	return cnt, err
}
