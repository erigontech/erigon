// Copyright 2022 The Erigon Authors
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

package eliasfano32

import (
	"bytes"
	"math"
	"math/bits"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/kv/stream"
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
		var i int
		it := ef.Iterator()
		for i = 0; it.HasNext(); i++ {
			n, err := it.Next()
			require.NoError(t, err)
			require.Equal(t, int(vals[i]), int(n))
		}
		require.Equal(t, len(vals), i)

		var j int
		rit := ef.ReverseIterator()
		for j = len(vals) - 1; rit.HasNext(); j-- {
			n, err := rit.Next()
			require.NoError(t, err)
			require.Equal(t, vals[j], n)
		}
		require.Equal(t, -1, j)
	})
	t.Run("iter grow", func(t *testing.T) {
		it := ef.Iterator()
		prev, err := it.Next()
		require.NoError(t, err)
		for it.HasNext() {
			n, err := it.Next()
			require.NoError(t, err)
			require.GreaterOrEqual(t, int(n), int(prev))
		}
	})
	t.Run("reverse iter decreases", func(t *testing.T) {
		it := ef.ReverseIterator()
		prev, err := it.Next()
		require.NoError(t, err)
		for it.HasNext() {
			n, err := it.Next()
			require.NoError(t, err)
			require.LessOrEqual(t, int(n), int(prev))
		}
	})

	{
		v2, ok2 := ef.Search(ef.Max())
		require.True(t, ok2, v2)
		require.Equal(t, int(ef.Max()), int(v2))
		it := ef.Iterator()
		for i := 0; i < int(ef.Count()-1); i++ {
			_, err := it.Next()
			require.NoError(t, err)
		}
		//save all fields values
		v1, v2, v3, v4, v5 := it.upperIdx, it.upperMask, it.lowerIdx, it.upper, it.itemsIterated
		// seek to same item and check new fields
		it.Seek(ef.Max())
		require.Equal(t, int(v1), int(it.upperIdx))
		require.Equal(t, int(v3), int(it.lowerIdx))
		require.Equal(t, int(v5), int(it.itemsIterated))
		require.Equal(t, bits.TrailingZeros64(v2), bits.TrailingZeros64(it.upperMask))
		require.Equal(t, int(v4), int(it.upper))

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
	err := ef.Write(buf)
	assert.NoError(t, err)
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
		stream.ExpectEqualU64(t, stream.ReverseArray(values), ef.ReverseIterator())
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

	t.Run("seek reverse", func(t *testing.T) {
		it := ef.ReverseIterator()

		it.Seek(90)
		n, err := it.Next()
		require.NoError(t, err)
		require.Equal(t, 62, int(n))

		it.Seek(62)
		n, err = it.Next()
		require.NoError(t, err)
		require.Equal(t, 62, int(n))

		it.Seek(59)
		n, err = it.Next()
		require.NoError(t, err)
		require.Equal(t, 58, int(n))

		it.Seek(57)
		n, err = it.Next()
		require.NoError(t, err)
		require.Equal(t, 54, int(n))

		it.Seek(1)
		n, err = it.Next()
		require.NoError(t, err)
		require.Equal(t, 1, int(n))

		it.Seek(0)
		require.False(t, it.HasNext())
	})

	t.Run("iterator exhausted", func(t *testing.T) {
		it := ef.Iterator()
		for range offsets {
			_, err := it.Next()
			require.NoError(t, err)
		}
		_, err := it.Next()
		require.ErrorIs(t, err, ErrEliasFanoIterExhausted)

		it = ef.ReverseIterator()
		for range offsets {
			_, err := it.Next()
			require.NoError(t, err)
		}
		_, err = it.Next()
		require.ErrorIs(t, err, ErrEliasFanoIterExhausted)
	})

	t.Run("article-example1", func(t *testing.T) {
		// https://www.antoniomallia.it/sorted-integers-compression-with-elias-fano-encoding.html
		offsets := []uint64{2, 3, 5, 7, 11, 13, 24}
		count := uint64(len(offsets))
		maxOffset := offsets[len(offsets)-1]

		ef := NewEliasFano(count, maxOffset)
		for _, offset := range offsets {
			ef.AddOffset(offset)
		}
		ef.Build()

		stream.ExpectEqualU64(t, stream.Array(offsets), ef.Iterator())
		stream.ExpectEqualU64(t, stream.ReverseArray(offsets), ef.ReverseIterator())
	})

	t.Run("article-example2", func(t *testing.T) {
		// https://arxiv.org/pdf/1206.4300
		offsets := []uint64{5, 8, 8, 15, 32}
		count := uint64(len(offsets))
		maxOffset := offsets[len(offsets)-1]

		ef := NewEliasFano(count, maxOffset)
		for _, offset := range offsets {
			ef.AddOffset(offset)
		}
		ef.Build()

		stream.ExpectEqualU64(t, stream.Array(offsets), ef.Iterator())
		stream.ExpectEqualU64(t, stream.ReverseArray(offsets), ef.ReverseIterator())
	})

	t.Run("1 element", func(t *testing.T) {
		ef := NewEliasFano(1, 15)
		ef.AddOffset(7)
		ef.Build()

		stream.ExpectEqualU64(t, stream.Array([]uint64{7}), ef.Iterator())
		stream.ExpectEqualU64(t, stream.ReverseArray([]uint64{7}), ef.ReverseIterator())
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
		checkSeekReverse(t, i, ef, vals)
	}
}

func checkSeek(t *testing.T, j int, ef *EliasFano, vals []uint64) {
	t.Helper()
	efi := ef.Iterator()
	// drain iterator to given item
	for i := 0; i < j; i++ {
		_, err := efi.Next()
		require.NoError(t, err)
	}
	//save all fields values
	v1, v2, v3, v4, v5 := efi.upperIdx, efi.upperMask, efi.lowerIdx, efi.upper, efi.itemsIterated
	// seek to same item and check new fields
	efi.Seek(vals[j])
	require.Equal(t, int(v1), int(efi.upperIdx))
	require.Equal(t, int(v3), int(efi.lowerIdx))
	require.Equal(t, int(v4), int(efi.upper))
	require.Equal(t, int(v5), int(efi.itemsIterated))
	require.Equal(t, bits.TrailingZeros64(v2), bits.TrailingZeros64(efi.upperMask))
}

func checkSeekReverse(t *testing.T, j int, ef *EliasFano, vals []uint64) {
	t.Helper()
	efi := ef.ReverseIterator()
	// drain iterator to given item
	for i := len(vals) - 1; i > j; i-- {
		_, err := efi.Next()
		require.NoError(t, err)
	}
	// save all fields values
	prevUpperIdx := efi.upperIdx
	prevUpperMask := efi.upperMask
	prevLowerIdx := efi.lowerIdx
	prevUpper := efi.upper
	prevItemsIterated := efi.itemsIterated
	// seek to same item and check new fields
	efi.Seek(vals[j])
	require.Equal(t, int(prevUpperIdx), int(efi.upperIdx))
	require.Equal(t, int(prevLowerIdx), int(efi.lowerIdx))
	require.Equal(t, int(prevUpper), int(efi.upper))
	require.Equal(t, int(prevItemsIterated), int(efi.itemsIterated))
	require.Equal(t, bits.TrailingZeros64(prevUpperMask), bits.TrailingZeros64(efi.upperMask))
}

func BenchmarkName(b *testing.B) {
	count := uint64(1_000_000)
	maxOffset := (count - 1) * 123
	ef := NewEliasFano(count, maxOffset)
	for offset := uint64(0); offset < count; offset++ {
		ef.AddOffset(offset * 123)
	}
	ef.Build()
	b.Run("next to value 1_000_000", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			it := ef.Iterator()
			for it.HasNext() {
				n, err := it.Next()
				require.NoError(b, err)
				if n > 1_000_000 {
					break
				}
			}
		}
	})
	b.Run("seek to value 1_000_000", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			it := ef.Iterator()
			it.Seek(1_000_000)
		}
	})
	b.Run("reverse next to value 1_230", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			it := ef.ReverseIterator()
			for it.HasNext() {
				n, err := it.Next()
				require.NoError(b, err)
				if n <= 1_230 {
					break
				}
			}
			require.True(b, it.HasNext())
			n, err := it.Next()
			require.NoError(b, err)
			require.Equal(b, uint64(1_230-123), n)
		}
	})
	b.Run("reverse seek to value 1_230", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			it := ef.ReverseIterator()
			it.Seek(1_230)
			n, err := it.Next()
			require.NoError(b, err)
			require.Equal(b, n, uint64(1_230))
		}
	})
	b.Run("naive reverse iterator", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			it := naiveReverseIterator(ef)
			for it.HasNext() {
				_, err := it.Next()
				require.NoError(b, err)
			}
		}
	})
	b.Run("reverse iterator", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			it := ef.ReverseIterator()
			for it.HasNext() {
				_, err := it.Next()
				require.NoError(b, err)
			}
		}
	})
}

func naiveReverseIterator(ef *EliasFano) *stream.ArrStream[uint64] {
	it := ef.Iterator()
	var values []uint64
	for it.HasNext() {
		v, err := it.Next()
		if err != nil {
			panic(err)
		}
		values = append(values, v)
	}
	return stream.ReverseArray[uint64](values)
}
