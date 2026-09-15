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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func BenchmarkRead(b *testing.B) {
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
	buf := bytes.NewBuffer(nil)
	require.NoError(b, ef.Write(buf))

	b.Run("read", func(b *testing.B) {
		for b.Loop() {
			ReadEliasFano(buf.Bytes())
		}
	})

	b.Run("reset", func(b *testing.B) {
		ef := NewEliasFano(1, 1)
		for b.Loop() {
			ef.Reset(buf.Bytes())
		}
	})
	b.Run("read.search", func(b *testing.B) {
		for b.Loop() {
			Seek(buf.Bytes(), 1)
		}
	})

	b.Run("reset.search", func(b *testing.B) {
		ef := NewEliasFano(1, 1)
		for b.Loop() {
			ef.Reset(buf.Bytes()).Seek(1)
		}
	})

}

func BenchmarkEF(b *testing.B) {
	count := uint64(1_000_000)
	maxOffset := (count - 1) * 123
	ef := NewEliasFano(count, maxOffset)
	for offset := range count {
		ef.AddOffset(offset * 123)
	}
	ef.Build()
	b.Run("next to value 1_000_000", func(b *testing.B) {
		for b.Loop() {
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
		for b.Loop() {
			it := ef.Iterator()
			it.Seek(1_000_000)
		}
	})
	b.Run("reverse next to value 1_230", func(b *testing.B) {
		for b.Loop() {
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
		it := ef.ReverseIterator()
		it.Seek(1_230)

		for b.Loop() {
			it := ef.ReverseIterator()
			it.Seek(1_230)
			n, err := it.Next()
			require.NoError(b, err)
			require.Equal(b, uint64(1_230), n)
		}
	})
	b.Run("naive reverse iterator", func(b *testing.B) {
		for b.Loop() {
			it := naiveReverseIterator(ef)
			for it.HasNext() {
				_, err := it.Next()
				require.NoError(b, err)
			}
		}
	})
	b.Run("reverse iterator", func(b *testing.B) {
		for b.Loop() {
			it := ef.ReverseIterator()
			for it.HasNext() {
				_, err := it.Next()
				require.NoError(b, err)
			}
		}
	})
}

func BenchmarkBuild(b *testing.B) {
	for _, count := range []uint64{100, 1_000_000} {
		b.Run(fmt.Sprintf("count=%d", count), func(b *testing.B) {
			maxOffset := (count - 1) * 123
			ef := NewEliasFano(count, maxOffset)
			for i := range count {
				ef.AddOffset(i * 123)
			}
			b.ResetTimer()
			for b.Loop() {
				ef.Build()
			}
		})
	}
}
