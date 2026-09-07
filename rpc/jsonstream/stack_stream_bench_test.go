// Copyright 2025 The Erigon Authors
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

package jsonstream

import (
	"io"
	"strings"
	"testing"
)

func BenchmarkStreamAcquire(b *testing.B) {
	result := strings.Repeat("0xabcdef", 512)
	write := func(s Stream) {
		s.WriteObjectStart()
		s.WriteObjectField("jsonrpc")
		s.WriteString("2.0")
		s.WriteMore()
		s.WriteObjectField("result")
		s.WriteString(result)
		s.WriteObjectEnd()
	}
	b.Run("impl=new", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			s := New(io.Discard)
			write(s)
			_ = s.Flush()
		}
	})
	b.Run("impl=pool", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			s := Get(io.Discard)
			write(s)
			_ = s.Flush()
			Put(s)
		}
	})
}
