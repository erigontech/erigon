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

package hexutil

import (
	"encoding/json"
	"slices"
	"testing"

	"github.com/holiman/uint256"
)

func BenchmarkUnmarshalBig(b *testing.B) {
	input := []byte(`"0x123456789abcdef123456789abcdef"`)
	for b.Loop() {
		var v Big
		if err := v.UnmarshalJSON(input); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkU256AppendText(b *testing.B) {
	buf := make([]byte, 0, 66)
	for _, tc := range []struct {
		name string
		v    *uint256.Int
	}{
		{"small", uint256.NewInt(42)},
		{"u64", uint256.NewInt(0x1234567890abcdef)},
		{"full", new(uint256.Int).SetAllOne()},
	} {
		v := U256(*tc.v)
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				buf, _ = v.AppendText(buf[:0])
			}
		})
	}
}

func BenchmarkUnmarshalUint64(b *testing.B) {
	input := []byte(`"0x123456789abcdf"`)
	for b.Loop() {
		var v Uint64
		_ = v.UnmarshalJSON(input)
	}
}

// discardJSONWriter reuses one buffer, as a response stream does.
type discardJSONWriter struct{ buf []byte }

func (w *discardJSONWriter) AvailableBuffer(n int) []byte { return slices.Grow(w.buf[:0], n) }
func (w *discardJSONWriter) WriteRawBytes(v []byte)       { w.buf = v[:0] }

// BenchmarkBytesMarshalJSON compares a 64KB eth_getCode result encoded by stdlib reflection vs MarshalFastJSONTo.
func BenchmarkBytesMarshalJSON(b *testing.B) {
	code := make(Bytes, 64*1024)
	for i := range code {
		code[i] = byte(i)
	}
	size := int64(QuotedLen(len(code)))

	b.Run("stdlib_reflect", func(b *testing.B) {
		b.SetBytes(size)
		b.ReportAllocs()
		for b.Loop() {
			if _, err := json.Marshal(code); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("fast", func(b *testing.B) {
		var w discardJSONWriter
		b.SetBytes(size)
		b.ReportAllocs()
		for b.Loop() {
			if err := code.MarshalFastJSONTo(&w); err != nil {
				b.Fatal(err)
			}
		}
	})
}
