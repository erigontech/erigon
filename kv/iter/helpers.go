package iter

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func ToArr[T any](s Unary[T]) ([]T, error) {
	res := make([]T, 0)
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

func ExpectEqual[V comparable](tb testing.TB, s1, s2 Unary[V]) {
	tb.Helper()
	for s1.HasNext() && s2.HasNext() {
		k1, e1 := s1.Next()
		k2, e2 := s2.Next()
		require.Equal(tb, k1, k2)
		require.Equal(tb, e1 == nil, e2 == nil)
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
