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

package eliasfano32

import (
	"bytes"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// Heap vs off-heap builds on identical input must produce byte-identical
// serialized output and identical iteration results.
func TestEliasFanoOffHeapMatchesHeap(t *testing.T) {
	cases := []struct {
		name   string
		count  uint64
		stride uint64
	}{
		{"tiny", 8, 64},
		{"small", 1024, 1 << 10},
		{"sparse", 512, 1 << 21},
		{"dense", 4096, 7},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			vals := make([]uint64, 0, tc.count)
			for i := uint64(0); i < tc.count; i++ {
				vals = append(vals, i*tc.stride)
			}
			maxOffset := vals[len(vals)-1]

			heap := NewEliasFano(tc.count, maxOffset)
			for _, v := range vals {
				heap.AddOffset(v)
			}
			heap.Build()
			heapBytes := heap.AppendBytes(nil)

			tmpBase := filepath.Join(t.TempDir(), "index.ef")
			off, err := NewEliasFanoOffHeap(tc.count, maxOffset, tmpBase)
			require.NoError(t, err)
			t.Cleanup(off.Close)
			for _, v := range vals {
				off.AddOffset(v)
			}
			off.Build()
			offBytes := off.AppendBytes(nil)

			require.True(t, bytes.Equal(heapBytes, offBytes),
				"off-heap serialization must match heap (lens: heap=%d off=%d)", len(heapBytes), len(offBytes))

			for i, want := range vals {
				require.Equal(t, want, off.Get(uint64(i)))
			}

			// Close is idempotent and releases the tmp file.
			off.Close()
			off.Close()
		})
	}
}
