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
	"math/bits"
	"testing"

	"github.com/ledgerwatch/erigon-lib/kv/iter"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEliasFanoSeek(t *testing.T) {
	count := uint64(1_000_000)
	maxOffset := (count - 1) * 123
	ef := NewEliasFano(count, maxOffset)
	vals := make([]uint64, 0, count)
	for offset := uint64(0); offset < count; offset++ {
		val := offset * 123
		vals = append(vals, val)
		ef.AddOffset(val)
	}
	ef.Build()

	t.Run("iter match vals", func(t *testing.T) {
		it := ef.Iterator()
		for i := 0; it.HasNext(); i++ {
			n, err := it.Next()
			require.NoError(t, err)
			require.Equal(t, int(vals[i]), int(n))
		}
	})
	t.Run("iter grow", func(t *testing.T) {
		it := ef.Iterator()
		prev, _ := it.Next()
		for it.HasNext() {
			n, _ := it.Next()
			require.GreaterOrEqual(t, int(n), int(prev))
		}
	})

	{
		v2, ok2 := ef.Search(ef.Max())
		require.True(t, ok2, v2)
		require.Equal(t, ef.Max(), v2)
		it := ef.Iterator()
		//it.SeekDeprecated(ef.Max())
		for i := 0; i < int(ef.Count()-1); i++ {
			it.Next()
		}
		//save all fields values
		//v1, v2, v3, v4, v5 := it.upperIdx, it.upperMask, it.lowerIdx, it.upper, it.idx
		// seek to same item and check new fields
		it.Seek(ef.Max())
		//require.Equal(t, int(v1), int(it.upperIdx))
		//require.Equal(t, int(v3), int(it.lowerIdx))
		//require.Equal(t, int(v5), int(it.idx))
		//require.Equal(t, bits.TrailingZeros64(v2), bits.TrailingZeros64(it.upperMask))
		//require.Equal(t, int(v4), int(it.upper))

		require.True(t, it.HasNext(), v2)
		itV, err := it.Next()
		require.NoError(t, err)
		require.Equal(t, int(ef.Max()), int(itV))
	}

	{
		v2, ok2 := ef.Search(ef.Min())
		require.True(t, ok2, v2)
		require.Equal(t, int(ef.Min()), int(v2))
		it := ef.Iterator()
		it.Seek(ef.Min())
		require.True(t, it.HasNext(), v2)
		itV, err := it.Next()
		require.NoError(t, err)
		require.Equal(t, int(ef.Min()), int(itV))
	}

	{
		v2, ok2 := ef.Search(0)
		require.True(t, ok2, v2)
		require.Equal(t, int(ef.Min()), int(v2))
		it := ef.Iterator()
		it.Seek(0)
		require.True(t, it.HasNext(), v2)
		itV, err := it.Next()
		require.NoError(t, err)
		require.Equal(t, int(ef.Min()), int(itV))
	}

	{
		v2, ok2 := ef.Search(math.MaxUint32)
		require.False(t, ok2, v2)
		it := ef.Iterator()
		it.Seek(math.MaxUint32)
		require.False(t, it.HasNext(), v2)
	}

	{
		v2, ok2 := ef.Search((count+1)*123 + 1)
		require.False(t, ok2, v2)
		it := ef.Iterator()
		it.Seek((count+1)*123 + 1)
		require.False(t, it.HasNext(), v2)
	}

	t.Run("search and seek can't return smaller", func(t *testing.T) {
		for i := uint64(0); i < count; i++ {
			search := i * 123
			v, ok2 := ef.Search(search)
			require.True(t, ok2, search)
			require.GreaterOrEqual(t, int(v), int(search))
			it := ef.Iterator()
			it.Seek(search)
			itV, err := it.Next()
			require.NoError(t, err)
			require.GreaterOrEqual(t, int(itV), int(search), int(v))
		}
	})

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

		iter2.Seek(62)
		n, err = iter2.Next()
		require.NoError(t, err)
		require.Equal(t, 62, int(n))

		iter2.Seek(1024)
		require.False(t, iter2.HasNext())
	})
}

func TestIteratorAndSeekAreBasedOnSameFields(t *testing.T) {
	vals := []uint64{1, 123, 789}
	ef := NewEliasFano(uint64(len(vals)), vals[len(vals)-1])
	for _, v := range vals {
		ef.AddOffset(v)
	}
	ef.Build()

	for i := range vals {
		checkSeek(t, i, ef, vals)
	}
}

func checkSeek(t *testing.T, j int, ef *EliasFano, vals []uint64) {
	t.Helper()
	efi := ef.Iterator()
	// drain iterator to given item
	for i := 0; i < j; i++ {
		efi.Next()
	}
	//save all fields values
	v1, v2, v3, v4, v5 := efi.upperIdx, efi.upperMask, efi.lowerIdx, efi.upper, efi.idx
	// seek to same item and check new fields
	efi.Seek(vals[j])
	require.Equal(t, int(v1), int(efi.upperIdx))
	require.Equal(t, int(v3), int(efi.lowerIdx))
	require.Equal(t, int(v4), int(efi.upper))
	require.Equal(t, int(v5), int(efi.idx))
	require.Equal(t, bits.TrailingZeros64(v2), bits.TrailingZeros64(efi.upperMask))
}

func BenchmarkName(b *testing.B) {
	count := uint64(1_000_000)
	maxOffset := (count - 1) * 123
	ef := NewEliasFano(count, maxOffset)
	for offset := uint64(0); offset < count; offset++ {
		ef.AddOffset(offset * 123)
	}
	ef.Build()
	b.Run("next", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			it := ef.Iterator()
			for it.HasNext() {
				n, _ := it.Next()
				if n > 1_000_000 {
					break
				}
			}
		}
	})
	b.Run("seek", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			it := ef.Iterator()
			it.SeekDeprecated(1_000_000)
		}
	})
	b.Run("seek2", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			it := ef.Iterator()
			it.Seek(1_000_000)
		}
	})
}
