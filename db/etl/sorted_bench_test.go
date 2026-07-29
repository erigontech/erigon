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
	"encoding/binary"
	"testing"

	"github.com/c2h5oh/datasize"

	"github.com/erigontech/erigon/common/log/v3"
)

// BenchmarkLoadMonotoneDupKeys mimics the ii indexKeys workload: monotone BE8
// keys with several values per key, spilled to multiple runs. bufSize forces
// ~10 runs per iteration.
func BenchmarkLoadMonotoneDupKeys(b *testing.B) {
	logger := log.New()
	key := make([]byte, 8)
	val := make([]byte, 32)

	for _, tc := range []struct {
		name       string
		numKeys    int
		valsPerKey int
		bufSize    datasize.ByteSize
	}{
		{"100k_x4", 100_000, 4, 2 * datasize.MB},
		{"500k_x4", 500_000, 4, 10 * datasize.MB},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			tmpdir := b.TempDir()
			for b.Loop() {
				b.StopTimer()
				c := NewCollector(b.Name(), tmpdir, NewSortableBuffer(tc.bufSize), logger)
				for i := range tc.numKeys {
					binary.BigEndian.PutUint64(key, uint64(i))
					for j := range tc.valsPerKey {
						binary.BigEndian.PutUint64(val, uint64(tc.valsPerKey-j))
						if err := c.Collect(key, val); err != nil {
							b.Fatal(err)
						}
					}
				}
				b.StartTimer()
				if err := c.Load(nil, "", func(k, v []byte, _ CurrentTableReader, _ LoadNextFunc) error {
					return nil
				}, TransformArgs{}); err != nil {
					b.Fatal(err)
				}
				b.StopTimer()
				c.Close()
				b.StartTimer()
			}
		})
	}
}
