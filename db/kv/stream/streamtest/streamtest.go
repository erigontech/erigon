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

package streamtest

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv/stream"
)

func ExpectEqualU64(tb testing.TB, s1, s2 stream.Uno[uint64]) {
	tb.Helper()
	ExpectEqual[uint64](tb, s1, s2)
}
func ExpectEqual[V any](tb testing.TB, s1, s2 stream.Uno[V]) {
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
