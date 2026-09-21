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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRootRecordFormsAcrossPlanes(t *testing.T) {
	for _, plane := range []byte{planeAccount, planeStorage} {
		t.Run(fmtPlane(plane), func(t *testing.T) {
			n := fork(nil)
			n.plane = plane
			path := appendPath(nil, 3, bytes.Repeat([]byte{4}, 63))
			n.setLeaf(3, packPath(path[1:], nil), []byte{1})
			require.Equal(t, byte(hdrIsLeafRoot), encodeRecord(n, 0, nil)[0])

			second := appendPath(nil, 9, bytes.Repeat([]byte{5}, 63))
			require.NoError(t, insertRoot(n, second, []byte{2}))
			require.Zero(t, n.path)
			require.Equal(t, uint16(1<<3|1<<9), n.childMask)
			require.Equal(t, byte(recordFormat), encodeRecord(n, 0, nil)[0])

			require.NoError(t, removeRoot(n, second))
			require.Zero(t, n.path)
			require.Equal(t, byte(hdrIsLeafRoot), encodeRecord(n, 0, nil)[0])
		})
	}
}

func TestRootExtensionInsertDivergenceKeepsChildBody(t *testing.T) {
	for _, plane := range []byte{planeAccount, planeStorage} {
		t.Run(fmtPlane(plane), func(t *testing.T) {
			hash := bytes.Repeat([]byte{0xa5}, 32)
			n := fork([]byte{1, 2, 3})
			n.plane = plane
			n.setStoredChild(4, hash, []byte{5, 6})
			incoming := append([]byte{1, 9}, bytes.Repeat([]byte{7}, 62)...)

			require.NoError(t, insertRoot(n, incoming, []byte{0x42}))
			require.Zero(t, n.path)
			require.Equal(t, hash, n.childHash[2])
			require.Equal(t, []byte{3, 4, 5, 6}, n.childExt[2])
			require.Equal(t, []byte{0x42}, n.leafValue[9])
			data := encodeRecord(n, 0, nil)
			require.NoError(t, Validate(data, 0))
			require.Equal(t, byte(hdrHasChildExt), data[0])
		})
	}
}

func TestRootCollapseRewritesOnlyTheRoot(t *testing.T) {
	for _, plane := range []byte{planeAccount, planeStorage} {
		t.Run(fmtPlane(plane), func(t *testing.T) {
			hash := bytes.Repeat([]byte{0x31}, 32)
			n := fork(nil)
			n.plane = plane
			n.setStoredChild(3, hash, []byte{4, 5})
			removed := appendPath(nil, 9, bytes.Repeat([]byte{6}, 63))
			n.setLeaf(9, packPath(removed[1:], nil), []byte{0x44})

			require.NoError(t, removeRoot(n, removed))
			require.Equal(t, []byte{3, 4, 5}, n.path)
			require.Equal(t, hash, n.childHash[3])
			require.Empty(t, n.childExt[3])
			data := encodeRecord(n, 0, nil)
			require.NoError(t, Validate(data, 0))
			require.Equal(t, []byte{3, 4, 5}, unpackPath(NewRecord(data, 0).SelfExt()[1:], 3, nil))
		})
	}
}

func TestRootTransitionsRejectMalformedShape(t *testing.T) {
	n := fork([]byte{1})
	n.setChild(2, fork([]byte{1, 2}))
	require.ErrorIs(t, insertRoot(n, bytes.Repeat([]byte{2}, 64), []byte{1}), ErrRootShape)

	n = fork(nil)
	n.setChild(1, fork([]byte{1}))
	require.ErrorIs(t, collapseRoot(n), ErrRootShape)
}
