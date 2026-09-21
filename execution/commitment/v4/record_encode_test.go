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

func TestEncodeRecordRoundTrip(t *testing.T) {
	suffix := packPath(bytes.Repeat([]byte{1}, 60), nil)
	hash := bytes.Repeat([]byte{0xab}, 32)
	tests := []struct {
		name  string
		depth int
		build func() *node
		check func(*testing.T, Record)
	}{
		{
			name:  "branch with leaf",
			depth: 3,
			build: func() *node {
				n := fork(nil)
				n.setStoredChild(0, hash, nil)
				n.setLeaf(1, suffix, []byte{0x42})
				return n
			},
			check: func(t *testing.T, r Record) {
				require.Equal(t, uint16(3), r.ChildMask())
				require.Equal(t, uint16(2), r.LeafMask())
				require.Equal(t, hash, r.SlotAt(0))
				gotSuffix, gotValue := r.LeafAt(1)
				require.Equal(t, suffix, gotSuffix)
				require.Equal(t, []byte{0x42}, gotValue)
			},
		},
		{
			name:  "child extension",
			depth: 3,
			build: func() *node {
				n := fork(nil)
				n.setStoredChild(4, hash, []byte{2, 3, 4})
				return n
			},
			check: func(t *testing.T, r Record) {
				require.Equal(t, uint16(1<<4), r.ExtMask())
				require.Equal(t, []byte{3, 0x23, 0x40}, r.ExtAt(4))
				require.Equal(t, hash, r.SlotAt(4))
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := tt.build()
			data := encodeRecord(n, tt.depth, bytes.Repeat([]byte{0xff}, 8))
			require.NoError(t, Validate(data, tt.depth))
			tt.check(t, NewRecord(data, tt.depth))
		})
	}
}

func TestEncodeRecordRootForms(t *testing.T) {
	hash := bytes.Repeat([]byte{0x11}, 32)
	tests := []struct {
		name  string
		build func() *node
		check func(*testing.T, []byte)
	}{
		{
			name: "leaf root",
			build: func() *node {
				n := fork(nil)
				n.setLeaf(4, packPath(bytes.Repeat([]byte{2}, 63), nil), []byte{1, 2})
				return n
			},
			check: func(t *testing.T, data []byte) {
				require.Equal(t, byte(hdrIsLeafRoot), data[0])
				require.NoError(t, Validate(data, 0))
				key, value := NewRecord(data, 0).LeafRootBody()
				require.Equal(t, packPath(append([]byte{4}, bytes.Repeat([]byte{2}, 63)...), nil), key)
				require.Equal(t, []byte{1, 2}, value)
			},
		},
		{
			name: "extension root",
			build: func() *node {
				n := fork([]byte{1, 2})
				n.setStoredChild(3, hash, nil)
				return n
			},
			check: func(t *testing.T, data []byte) {
				require.Equal(t, byte(hdrHasSelfExt), data[0])
				require.NoError(t, Validate(data, 0))
				r := NewRecord(data, 0)
				require.Equal(t, []byte{2, 0x12}, r.SelfExt())
				require.Equal(t, hash, r.SlotAt(3))
			},
		},
		{
			name: "branch root",
			build: func() *node {
				n := fork(nil)
				n.setStoredChild(3, hash, nil)
				n.setStoredChild(9, bytes.Repeat([]byte{0x22}, 32), nil)
				return n
			},
			check: func(t *testing.T, data []byte) {
				require.Equal(t, byte(recordFormat), data[0])
				require.NoError(t, Validate(data, 0))
				require.Equal(t, uint16(1<<3|1<<9), NewRecord(data, 0).ChildMask())
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.check(t, encodeRecord(tt.build(), 0, nil))
		})
	}
}

func TestEncodeRecordTombstone(t *testing.T) {
	dst := []byte{1, 2, 3}
	require.Empty(t, encodeRecord(fork(nil), 7, dst))
}

func TestEncodeRecordReusesDestination(t *testing.T) {
	n := fork(nil)
	n.setStoredChild(2, bytes.Repeat([]byte{0x33}, 32), nil)
	dst := make([]byte, 0, 256)
	data := encodeRecord(n, 7, dst)
	require.NoError(t, Validate(data, 7))
	data = encodeRecord(fork(nil), 7, data)
	require.Empty(t, data)
}

func TestEncodeRecordRejectsUnrepresentableRoots(t *testing.T) {
	require.Panics(t, func() {
		n := fork(nil)
		n.setStoredChild(1, bytes.Repeat([]byte{1}, 32), nil)
		encodeRecord(n, 0, nil)
	})
	require.Panics(t, func() {
		n := fork([]byte{1})
		n.setStoredChild(1, bytes.Repeat([]byte{1}, 32), []byte{2})
		encodeRecord(n, 0, nil)
	})
}
