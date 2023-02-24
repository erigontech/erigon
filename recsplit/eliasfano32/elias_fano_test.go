/*
   Copyright 2022 Erigon contributors

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

package eliasfano32

import (
	"bytes"
	"math"
	"testing"

	"github.com/ledgerwatch/erigon-lib/kv/iter"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEliasFanoSeek(t *testing.T) {
	count := uint64(1_000_000)
	maxOffset := (count - 1) * 123
	ef := NewEliasFano(count, maxOffset)
	for offset := uint64(0); offset < count; offset++ {
		ef.AddOffset(offset * 123)
	}
	ef.Build()

	v2, ok2 := ef.Search(ef.Max())
	require.True(t, ok2, v2)
	require.Equal(t, ef.Max(), v2)

	v2, ok2 = ef.Search(ef.Min())
	require.True(t, ok2, v2)
	require.Equal(t, ef.Min(), v2)

	v2, ok2 = ef.Search(0)
	require.True(t, ok2, v2)
	require.Equal(t, ef.Min(), v2)

	v2, ok2 = ef.Search(math.MaxUint32)
	require.False(t, ok2, v2)

	v2, ok2 = ef.Search((count+1)*123 + 1)
	require.False(t, ok2, v2)

	for i := uint64(0); i < count; i++ {
		v := i * 123
		v2, ok2 = ef.Search(v)
		require.True(t, ok2, v)
		require.GreaterOrEqual(t, int(v2), int(v))
	}
}

func TestEliasFano(t *testing.T) {
	offsets := []uint64{1, 4, 6, 8, 10, 14, 16, 19, 22, 34, 37, 39, 41, 43, 48, 51, 54, 58, 62}
	count := uint64(len(offsets))
	maxOffset := offsets[0]
	for _, offset := range offsets {
		if offset > maxOffset {
			maxOffset = offset
		}
	}
	ef := NewEliasFano(count, maxOffset)
	for _, offset := range offsets {
		ef.AddOffset(offset)
	}
	ef.Build()
	for i, offset := range offsets {
		offset1 := ef.Get(uint64(i))
		assert.Equal(t, offset, offset1, "offset")
	}
	v, ok := ef.Search(37)
	assert.True(t, ok, "search1")
	assert.Equal(t, uint64(37), v, "search1")
	v, ok = ef.Search(0)
	assert.True(t, ok, "search2")
	assert.Equal(t, uint64(1), v, "search2")
	_, ok = ef.Search(100)
	assert.False(t, ok, "search3")
	v, ok = ef.Search(11)
	assert.True(t, ok, "search4")
	assert.Equal(t, uint64(14), v, "search4")

	buf := bytes.NewBuffer(nil)
	ef.Write(buf)
	assert.Equal(t, ef.AppendBytes(nil), buf.Bytes())

	ef2, _ := ReadEliasFano(buf.Bytes())
	assert.Equal(t, ef.Min(), ef2.Min())
	assert.Equal(t, ef.Max(), ef2.Max())
	assert.Equal(t, ef2.Max(), Max(buf.Bytes()))
	assert.Equal(t, ef2.Min(), Min(buf.Bytes()))
	assert.Equal(t, ef2.Count(), Count(buf.Bytes()))
}

func TestIterator(t *testing.T) {
	offsets := []uint64{1, 4, 6, 8, 10, 14, 16, 19, 22, 34, 37, 39, 41, 43, 48, 51, 54, 58, 62}
	count := uint64(len(offsets))
	maxOffset := offsets[0]
	for _, offset := range offsets {
		if offset > maxOffset {
			maxOffset = offset
		}
	}
	ef := NewEliasFano(count, maxOffset)
	for _, offset := range offsets {
		ef.AddOffset(offset)
	}
	ef.Build()
	t.Run("scan", func(t *testing.T) {
		efi := ef.Iterator()
		i := 0
		var values []uint64
		for efi.HasNext() {
			v, _ := efi.Next()
			values = append(values, v)
			assert.Equal(t, offsets[i], v, "iter")
			i++
		}
		iter.ExpectEqualU64(t, iter.ReverseArray(values), ef.ReverseIterator())
	})

	t.Run("seek", func(t *testing.T) {
		iter2 := ef.Iterator()
		iter2.Seek(2)
		n, err := iter2.Next()
		require.NoError(t, err)
		require.Equal(t, 4, int(n))

		iter2.Seek(5)
		n, err = iter2.Next()
		require.NoError(t, err)
		require.Equal(t, 6, int(n))

		iter2.Seek(1024)
		require.False(t, iter2.HasNext())
	})
}
