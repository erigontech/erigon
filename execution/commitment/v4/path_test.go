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

package v4

import (
	"bytes"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPackPathRoundTrip(t *testing.T) {
	var packedBuffer [32]byte
	var unpackedBuffer [64]byte
	for count := 0; count <= 64; count++ {
		path := make([]byte, count)
		for i := range path {
			path[i] = byte((i*7 + count) & 0x0f)
		}

		packed := packPath(path, packedBuffer[:])
		require.Len(t, packed, packedLen(count))
		if count&1 == 1 {
			require.Zero(t, packed[len(packed)-1]&0x0f)
		}
		require.Equal(t, path, unpackPath(packed, count, unpackedBuffer[:]))
	}
}

func TestPackPathReusesDestinationAndClearsOddPadding(t *testing.T) {
	dst := make([]byte, 3)
	packed := packPath([]byte{1, 2, 3, 4, 5}, dst)
	require.Equal(t, []byte{0x12, 0x34, 0x50}, packed)

	packed = packPath([]byte{6, 7, 8, 9}, packed)
	require.Equal(t, []byte{0x67, 0x89}, packed)
}

func TestPackedPathSortOrder(t *testing.T) {
	makeKey := func(path []byte) []byte {
		key := []byte{0x40}
		key = append(key, packPath(path, nil)...)
		return append(key, byte(len(path)))
	}
	paths := [][]byte{
		{0x09, 0x0f},
		{0x0a, 0x00, 0x00},
		{0x0a},
		{0x0a, 0x00, 0x02},
		{0x0a, 0x0f, 0x01},
		{0x0b, 0x00},
	}
	keys := make([][]byte, len(paths))
	for i, path := range paths {
		keys[i] = makeKey(path)
	}
	slices.SortFunc(keys, bytes.Compare)

	require.True(t, bytes.Equal(keys[1], []byte{0x40, 0xa0, 0x00, 0x03}))
	require.True(t, bytes.Equal(keys[2], []byte{0x40, 0xa0, 0x01}))
	require.True(t, bytes.Equal(keys[3], []byte{0x40, 0xa0, 0x20, 0x03}))

	first, last := -1, -1
	for i, key := range keys {
		path := unpackPath(key[1:len(key)-1], int(key[len(key)-1]), nil)
		if path[0] == 0x0a {
			if first == -1 {
				first = i
			}
			last = i
		}
	}
	require.NotEqual(t, -1, first)
	for _, key := range keys[first : last+1] {
		path := unpackPath(key[1:len(key)-1], int(key[len(key)-1]), nil)
		require.Equal(t, byte(0x0a), path[0])
	}
	require.Less(t, bytes.Compare(keys[1], keys[2]), 0)
	require.Less(t, bytes.Compare(keys[2], keys[3]), 0)
}
