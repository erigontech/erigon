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
	"bytes"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/erigontech/erigon/common/length"
)

// benchmarkSimpleObject is used to compare writing a simple JSON object
func benchmarkSimpleObject(b *testing.B, s *Stream) {
	b.Helper()
	for b.Loop() {
		s.WriteObjectStart()
		s.Field("name")
		s.WriteString("John")
		s.Field("age")
		s.Int(30)
		s.WriteObjectEnd()

		err := s.ClosePending(0)
		assert.NoError(b, err)
		err = s.Flush()
		assert.NoError(b, err)
	}
}

func BenchmarkSimpleObject_Stream(b *testing.B) {
	benchmarkSimpleObject(b, newStream(nil, InitialBufferSize))
}

// benchmarkNestedStructure is used to compare writing a nested JSON structure
func benchmarkNestedStructure(b *testing.B, s *Stream) {
	b.Helper()
	for b.Loop() {
		s.WriteObjectStart()
		s.Field("person")
		s.WriteObjectStart()
		s.Field("name")
		s.WriteString("John")
		s.Field("address")
		s.WriteObjectStart()
		s.Field("city")
		s.WriteString("New York")
		s.Field("zip")
		s.WriteString("10001")
		s.WriteObjectEnd()
		s.WriteObjectEnd()
		s.Field("active")
		s.WriteTrue()
		s.WriteObjectEnd()

		err := s.ClosePending(0)
		assert.NoError(b, err)
		err = s.Flush()
		assert.NoError(b, err)
	}
}

func BenchmarkNestedStructure_Stream(b *testing.B) {
	benchmarkNestedStructure(b, newStream(nil, InitialBufferSize))
}

// benchmarkLargeArray is used to compare writing a large array
func benchmarkLargeArray(b *testing.B, s *Stream) {
	b.Helper()
	for b.Loop() {
		s.WriteArrayStart()
		for j := range 1000 {
			s.Int(int64(j))
		}
		s.WriteArrayEnd()

		err := s.ClosePending(0)
		assert.NoError(b, err)
		err = s.Flush()
		assert.NoError(b, err)
	}
}

func BenchmarkLargeArray_Stream(b *testing.B) {
	benchmarkLargeArray(b, newStream(nil, InitialBufferSize))
}

// benchmarkMixedTypes is used to compare writing mixed data types
func benchmarkMixedTypes(b *testing.B, s *Stream) {
	b.Helper()
	for b.Loop() {
		s.WriteObjectStart()
		s.Field("string")
		s.WriteString("value")
		s.Field("int")
		s.Int(42)
		s.Field("float")
		s.WriteFloat64(3.14159)
		s.Field("bool")
		s.WriteBool(true)
		s.Field("null")
		s.WriteNil()
		s.WriteObjectEnd()

		err := s.ClosePending(0)
		assert.NoError(b, err)
		err = s.Flush()
		assert.NoError(b, err)
	}
}

func BenchmarkMixedTypes_Stream(b *testing.B) {
	benchmarkMixedTypes(b, newStream(nil, InitialBufferSize))
}

// benchmarkWriteToBuffer is used to compare writing to a buffer
func benchmarkWriteToBuffer(b *testing.B, s *Stream) {
	b.Helper()
	buf := bytes.NewBuffer(nil)
	for b.Loop() {
		s.Reset(buf)
		s.WriteObjectStart()
		s.Field("name")
		s.WriteString("John")
		s.Field("age")
		s.Int(30)
		s.WriteObjectEnd()

		err := s.ClosePending(0)
		assert.NoError(b, err)
		err = s.Flush()
		assert.NoError(b, err)
	}
}

func BenchmarkWriteToBuffer_Stream(b *testing.B) {
	benchmarkWriteToBuffer(b, newStream(nil, InitialBufferSize))
}

// benchmarkIncompleteStructure is used to compare handling incomplete structures
func benchmarkIncompleteStructure(b *testing.B, s *Stream) {
	b.Helper()
	for b.Loop() {
		// Create an incomplete structure
		s.WriteObjectStart()
		s.Field("name")
		s.WriteString("John")
		s.Field("details")
		s.WriteObjectStart()
		s.Field("age")
		s.Int(30)
		s.Field("address") // Missing value

		err := s.Flush()
		assert.NoError(b, err)
	}
}

func BenchmarkIncompleteStructure_Stream(b *testing.B) {
	benchmarkIncompleteStructure(b, newStream(nil, InitialBufferSize))
}

// BenchmarkHexesField covers the two shapes the field serves on hot paths: a block's
// transaction hashes and a log's topics, with the writer a served response has and without one.
func BenchmarkHexesField(b *testing.B) {
	for _, n := range []int{4, 300} {
		hashes := make([][length.Hash]byte, n)
		for i := range hashes {
			hashes[i][0], hashes[i][1] = byte(i), byte(i>>8)
		}
		for name, out := range map[string]io.Writer{"discard": io.Discard, "nowriter": nil} {
			b.Run(fmt.Sprintf("hashes=%d/%s", n, name), func(b *testing.B) {
				s := New(out)
				b.ReportAllocs()
				for b.Loop() {
					s.Reset(out)
					s.WriteObjectStart()
					HexesField(s, "transactions", hashes)
					s.WriteObjectEnd()
				}
			})
		}
	}
}

// BenchmarkWriteHexBytes covers its two callers: an account proof's trie nodes and a txpool
// account's slots, with the writer a served response has and without one.
func BenchmarkWriteHexBytes(b *testing.B) {
	for _, shape := range []struct {
		name  string
		n, sz int
	}{{"proof", 9, 532}, {"slots", 2000, 32}} {
		items := make([][]byte, shape.n)
		for i := range items {
			items[i] = bytes.Repeat([]byte{byte(i)}, shape.sz)
		}
		for name, out := range map[string]io.Writer{"discard": io.Discard, "nowriter": nil} {
			b.Run(fmt.Sprintf("%s/%s", shape.name, name), func(b *testing.B) {
				s := New(out)
				b.ReportAllocs()
				for b.Loop() {
					s.Reset(out)
					WriteHexBytes(s, items)
				}
			})
		}
	}
}
