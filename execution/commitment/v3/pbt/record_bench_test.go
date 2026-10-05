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

package pbt

import "testing"

func BenchmarkDecodeRow(b *testing.B) {
	key := rowKey(8)
	full, err := EncodeRecord(key, sixteenBranchRecord())
	if err != nil {
		b.Fatal(err)
	}
	tests := []struct {
		name string
		data []byte
	}{
		{name: "sixteen_branch_cells", data: full},
		{name: "golden_row", data: manualRowOracle()},
	}
	for _, test := range tests {
		b.Run(test.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if _, err := DecodeRecord(key, test.data); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func sixteenBranchRecord() *Record {
	record := &Record{Form: RowRoot}
	for slot := range record.Cells {
		record.Cells[slot] = Cell{Kind: BranchCell, Left: hash(byte(slot + 1)), Right: hash(byte(slot + 17))}
	}
	return record
}
