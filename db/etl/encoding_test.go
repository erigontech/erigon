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

package etl

import (
	"bytes"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEntryCodecRoundTrip(t *testing.T) {
	field := func(n int) []byte {
		b := make([]byte, n)
		for i := range b {
			b[i] = byte(i)
		}
		return b
	}
	// Lengths around the inline/escape boundary, plus nil and empty, which must
	// stay distinguishable.
	lengths := []int{0, 1, lenInlineMax - 1, lenInlineMax, lenInlineMax + 1, 1000, 70000}

	var want [][2][]byte
	for _, kl := range lengths {
		for _, vl := range lengths {
			want = append(want, [2][]byte{field(kl), field(vl)})
		}
	}
	want = append(want,
		[2][]byte{nil, nil},
		[2][]byte{nil, field(3)},
		[2][]byte{field(3), nil},
		[2][]byte{nil, {}},
		[2][]byte{{}, nil},
	)

	entries := make([]sortableBufferEntry, 0, len(want))
	for _, kv := range want {
		entries = append(entries, sortableBufferEntry{key: kv[0], value: kv[1]})
	}
	var buf bytes.Buffer
	require.NoError(t, writeSortedEntries(&buf, entries))

	r := &mmapBytesReader{data: buf.Bytes()}
	for i, kv := range want {
		k, v, err := r.nextEntry()
		require.NoError(t, err, "entry %d", i)
		require.Equal(t, kv[0], k, "entry %d key", i)
		require.Equal(t, kv[1], v, "entry %d value", i)
		// Equal treats nil and empty as equal, so pin the distinction directly.
		require.Equal(t, kv[0] == nil, k == nil, "entry %d key nil-ness", i)
		require.Equal(t, kv[1] == nil, v == nil, "entry %d value nil-ness", i)
	}
	_, _, err := r.nextEntry()
	require.ErrorIs(t, err, io.EOF)
}

func TestEntryCodecTruncated(t *testing.T) {
	var buf bytes.Buffer
	require.NoError(t, writeSortedEntries(&buf, []sortableBufferEntry{
		{key: []byte("key"), value: bytes.Repeat([]byte{7}, 1000)},
	}))
	full := buf.Bytes()

	for cut := 1; cut < len(full); cut += 97 {
		t.Run(fmt.Sprintf("cut_%d", cut), func(t *testing.T) {
			r := &mmapBytesReader{data: full[:cut]}
			_, _, err := r.nextEntry()
			require.ErrorIs(t, err, io.ErrUnexpectedEOF)
		})
	}
}
