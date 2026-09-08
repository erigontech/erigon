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
	"testing"
)

func BenchmarkPagedWriterAdd(b *testing.B) {
	const pageSize = 16
	key := make([]byte, 20)
	val := make([]byte, 100)
	for i := range key {
		key[i] = byte(i)
	}
	for i := range val {
		val[i] = byte(i)
	}

	cases := []struct {
		name       string
		compress   bool
		numWorkers int
	}{
		{"noCompression", false, 1},
		{"compression_sync", true, 1},
		{"compression_workers2", true, 2},
		{"compression_workers4", true, 4},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			buf := &multyBytesWriter{pageSize: pageSize}
			w := NewPagedWriter(b.Context(), buf, tc.compress, tc.numWorkers)
			b.ResetTimer()
			for b.Loop() {
				w.Add(key, val) //nolint:errcheck
			}
			w.Flush() //nolint:errcheck
		})
	}
}

func BenchmarkName(b *testing.B) {
	buf := &multyBytesWriter{pageSize: 16}
	w := NewPagedWriter(b.Context(), buf, false, 1)
	for i := range 16 {
		w.Add([]byte{byte(i)}, []byte{10 + byte(i)}) //nolint:errcheck
	}
	bts := buf.Bytes()[0]

	k := []byte{15}

	b.Run("1", func(b *testing.B) {
		for b.Loop() {
			GetFromPage(k, bts, nil, false)
		}
	})

}
