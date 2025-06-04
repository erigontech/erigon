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

	g := NewPagedReader(d.MakeGetter(), 2, false)
	i := 0
	for g.HasNext() {
		w := loremStrings[i]
		if i%2 == 0 {
			g.Skip()
		} else {
			var word []byte
			_, word, buf, _ = g.Next2(buf[:0])
			expected := fmt.Sprintf("%s %d", w, i)
			require.Equal(expected, string(word))
		}
		i++
	}

	g.Reset(0)
	_, offset := g.Next(buf[:0])
	require.Equal(0, int(offset))
	_, offset = g.Next(buf[:0])
	require.Equal(0x2a, int(offset))
	_, offset = g.Next(buf[:0])
	require.Equal(0x2a, int(offset))
	_, offset = g.Next(buf[:0])
	require.Equal(0x52, int(offset))
}
