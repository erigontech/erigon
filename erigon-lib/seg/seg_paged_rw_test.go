// Copyright 2024 The Erigon Authors
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

package seg

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/common"
)

func TestPagedReader(t *testing.T) {
	d := prepareLoremDictOnPagedWriter(t, 2, false)
	defer d.Close()
	require := require.New(t)
	g1 := NewPagedReader(d.MakeGetter(), 2, false)
	var buf []byte
	_, _, buf, o1 := g1.Next2(buf[:0])
	require.Zero(o1)
	_, _, buf, o1 = g1.Next2(buf[:0])
	require.Zero(o1)
	_, _, buf, o1 = g1.Next2(buf[:0])
	require.NotZero(o1)

	fmt.Printf("sz: %d\n", d.size)

	g := NewPagedReader(d.MakeGetter(), 2, false)
	i := 0
	for g.HasNext() {
		w := loremStrings[i]
		var word []byte
		_, word, buf, _ = g.Next2(buf[:0])
		expected := fmt.Sprintf("%s %d", w, i)
		require.Equal(expected, string(word))
		i++
	}

	g.Reset(0)
	_, offset := g.Next(buf[:0])
	require.Equal(0, int(offset))
	_, offset = g.Next(buf[:0])
	require.Equal(42, int(offset))
	_, offset = g.Next(buf[:0])
	require.Equal(42, int(offset))
	_, offset = g.Next(buf[:0])
	require.Equal(82, int(offset))
}

// multyBytesWriter is a writer for [][]byte, similar to bytes.Writer.
type multyBytesWriter struct {
	buffer [][]byte
}

func (w *multyBytesWriter) Write(p []byte) (n int, err error) {
	w.buffer = append(w.buffer, common.Copy(p))
	return len(p), nil
}
func (w *multyBytesWriter) Bytes() [][]byte  { return w.buffer }
func (w *multyBytesWriter) FileName() string { return "" }
func (w *multyBytesWriter) Count() int       { return 0 }
func (w *multyBytesWriter) Close()           {}
func (w *multyBytesWriter) Compress() error  { return nil }
func (w *multyBytesWriter) Reset()           { w.buffer = nil }

func TestPage(t *testing.T) {
	buf, require := &multyBytesWriter{}, require.New(t)
	sampling := 2
	w := NewPagedWriter(buf, sampling, false)
	for i := 0; i < sampling+1; i++ {
		k, v := fmt.Sprintf("k %d", i), fmt.Sprintf("v %d", i)
		require.NoError(w.Add([]byte(k), []byte(v)))
	}
	require.NoError(w.Flush())
	pages := buf.Bytes()
	pageNum := 0
	p1 := &Page{}
	p1.Reset(pages[0], false)

	iter := 0
	for i := 0; i < sampling+1; i++ {
		iter++
		expectK, expectV := fmt.Sprintf("k %d", i), fmt.Sprintf("v %d", i)
		v, _ := GetFromPage([]byte(expectK), pages[pageNum], nil, false)
		require.Equal(expectV, string(v), i)
		require.True(p1.HasNext())
		k, v := p1.Next()
		require.Equal(expectK, string(k), i)
		require.Equal(expectV, string(v), i)

		if iter%sampling == 0 {
			pageNum++

			require.False(p1.HasNext())
			p1.Reset(pages[pageNum], false)
		}
	}
}

func BenchmarkName(b *testing.B) {
	buf := &multyBytesWriter{}
	w := NewPagedWriter(buf, 16, false)
	for i := 0; i < 16; i++ {
		w.Add([]byte{byte(i)}, []byte{10 + byte(i)})
	}
	bts := buf.Bytes()[0]

	k := []byte{15}

	b.Run("1", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			GetFromPage(k, bts, nil, false)
		}
	})

}
