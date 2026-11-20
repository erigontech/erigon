// Copyright 2026 The Erigon Authors
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

package solid_test

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/cltypes/solid"
)

const uint64ListJSONLimit = 16

var uint64JSONLengths = []int{0, 1, 3, 4, 5, 9}

func uint64JSONValues(n int, seed uint64) []uint64 {
	values := make([]uint64, n)
	for i := range values {
		values[i] = seed + uint64(i)*7 + 1
	}
	return values
}

func uint64JSON(t *testing.T, values []uint64) []byte {
	t.Helper()
	strs := make([]string, len(values))
	for i, v := range values {
		strs[i] = strconv.FormatUint(v, 10)
	}
	data, err := json.Marshal(strs)
	require.NoError(t, err)
	return data
}

func uint64SSZ(values []uint64) []byte {
	buf := make([]byte, 0, len(values)*8)
	for _, v := range values {
		buf = binary.LittleEndian.AppendUint64(buf, v)
	}
	return buf
}

type uint64Iterable interface {
	Length() int
	Get(int) uint64
	HashSSZ() ([32]byte, error)
	EncodeSSZ([]byte) ([]byte, error)
}

func requireSameUint64Iterable(t *testing.T, expected, actual uint64Iterable) {
	t.Helper()
	require.Equal(t, expected.Length(), actual.Length())
	for i := 0; i < expected.Length(); i++ {
		require.Equal(t, expected.Get(i), actual.Get(i), "index %d", i)
	}
	expectedEnc, err := expected.EncodeSSZ(nil)
	require.NoError(t, err)
	actualEnc, err := actual.EncodeSSZ(nil)
	require.NoError(t, err)
	require.Equal(t, expectedEnc, actualEnc)
	expectedRoot, err := expected.HashSSZ()
	require.NoError(t, err)
	actualRoot, err := actual.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, expectedRoot, actualRoot)
}

func TestUint64ListSSZUnmarshalJSON(t *testing.T) {
	for _, n := range uint64JSONLengths {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			values := uint64JSONValues(n, 100)

			list := solid.NewUint64ListSSZ(uint64ListJSONLimit)
			require.NoError(t, json.Unmarshal(uint64JSON(t, values), list))

			requireSameUint64Iterable(t, solid.NewUint64ListSSZFromSlice(uint64ListJSONLimit, values), list)
			decoded := solid.NewUint64ListSSZ(uint64ListJSONLimit)
			require.NoError(t, decoded.DecodeSSZ(uint64SSZ(values), 0))
			requireSameUint64Iterable(t, decoded, list)
		})
	}
}

func TestUint64ListSSZUnmarshalJSONReusesList(t *testing.T) {
	for _, n := range uint64JSONLengths {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			list := solid.NewUint64ListSSZFromSlice(uint64ListJSONLimit, uint64JSONValues(12, 1000))
			_, err := list.HashSSZ()
			require.NoError(t, err)

			values := uint64JSONValues(n, 100)
			require.NoError(t, json.Unmarshal(uint64JSON(t, values), list))

			requireSameUint64Iterable(t, solid.NewUint64ListSSZFromSlice(uint64ListJSONLimit, values), list)
		})
	}
}

func TestUint64ListSSZUnmarshalJSONRejectsOverLimit(t *testing.T) {
	before := uint64JSONValues(3, 1000)
	list := solid.NewUint64ListSSZFromSlice(uint64ListJSONLimit, before)
	_, err := list.HashSSZ()
	require.NoError(t, err)

	require.ErrorContains(t, json.Unmarshal(uint64JSON(t, uint64JSONValues(uint64ListJSONLimit+1, 100)), list), "exceeds limit")

	requireSameUint64Iterable(t, solid.NewUint64ListSSZFromSlice(uint64ListJSONLimit, before), list)
}

func TestUint64VectorSSZUnmarshalJSON(t *testing.T) {
	for _, n := range uint64JSONLengths {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			values := uint64JSONValues(n, 100)

			vector := solid.NewUint64VectorSSZ(n)
			for i := range n {
				vector.Set(i, 1000+uint64(i))
			}
			_, err := vector.HashSSZ()
			require.NoError(t, err)
			require.NoError(t, json.Unmarshal(uint64JSON(t, values), vector))

			expected := solid.NewUint64VectorSSZ(n)
			require.NoError(t, expected.DecodeSSZ(uint64SSZ(values), 0))
			requireSameUint64Iterable(t, expected, vector)
		})
	}
}

func TestUint64VectorSSZUnmarshalJSONRejectsLengthMismatch(t *testing.T) {
	const size = 5
	before := uint64JSONValues(size, 1000)
	for _, n := range []int{0, size - 1, size + 1} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			vector := solid.NewUint64VectorSSZ(size)
			require.NoError(t, vector.DecodeSSZ(uint64SSZ(before), 0))
			_, err := vector.HashSSZ()
			require.NoError(t, err)

			require.ErrorContains(t, json.Unmarshal(uint64JSON(t, uint64JSONValues(n, 100)), vector), "length mismatch")

			expected := solid.NewUint64VectorSSZ(size)
			require.NoError(t, expected.DecodeSSZ(uint64SSZ(before), 0))
			requireSameUint64Iterable(t, expected, vector)
		})
	}
}

func TestUint64ListSSZJSONRoundTrip(t *testing.T) {
	for _, n := range uint64JSONLengths {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			values := uint64JSONValues(n, 100)
			if n > 0 {
				values[n-1] = math.MaxInt64
			}
			list := solid.NewUint64ListSSZFromSlice(uint64ListJSONLimit, values)

			data, err := json.Marshal(list)
			require.NoError(t, err)
			decoded := solid.NewUint64ListSSZ(uint64ListJSONLimit)
			require.NoError(t, json.Unmarshal(data, decoded))

			requireSameUint64Iterable(t, list, decoded)
		})
	}
}

func TestUint64VectorSSZJSONRoundTrip(t *testing.T) {
	for _, n := range uint64JSONLengths {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			values := uint64JSONValues(n, 100)
			if n > 0 {
				values[n-1] = math.MaxInt64
			}
			vector := solid.NewUint64VectorSSZ(n)
			require.NoError(t, vector.DecodeSSZ(uint64SSZ(values), 0))

			data, err := json.Marshal(vector)
			require.NoError(t, err)
			decoded := solid.NewUint64VectorSSZ(n)
			require.NoError(t, json.Unmarshal(data, decoded))

			requireSameUint64Iterable(t, vector, decoded)
		})
	}
}

func TestUint64VectorSSZUnmarshalJSONAfterClear(t *testing.T) {
	const size = 5
	values := uint64JSONValues(size, 100)

	vector := solid.NewUint64VectorSSZ(size)
	vector.Clear()
	require.ErrorContains(t, json.Unmarshal([]byte("[]"), vector), "length mismatch")
	require.NoError(t, json.Unmarshal(uint64JSON(t, values), vector))

	expected := solid.NewUint64VectorSSZ(size)
	require.NoError(t, expected.DecodeSSZ(uint64SSZ(values), 0))
	requireSameUint64Iterable(t, expected, vector)
}
