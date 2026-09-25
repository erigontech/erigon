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
	"testing"

	"github.com/stretchr/testify/assert"
)

// benchmarkSimpleObject is used to compare writing a simple JSON object
func benchmarkSimpleObject(b *testing.B, s Stream) {
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

func BenchmarkSimpleObject_StackStream(b *testing.B) {
	benchmarkSimpleObject(b, newStackStream(nil, InitialBufferSize))
}

// benchmarkNestedStructure is used to compare writing a nested JSON structure
func benchmarkNestedStructure(b *testing.B, s Stream) {
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

func BenchmarkNestedStructure_StackStream(b *testing.B) {
	benchmarkNestedStructure(b, newStackStream(nil, InitialBufferSize))
}

// benchmarkLargeArray is used to compare writing a large array
func benchmarkLargeArray(b *testing.B, s Stream) {
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

func BenchmarkLargeArray_StackStream(b *testing.B) {
	benchmarkLargeArray(b, newStackStream(nil, InitialBufferSize))
}

// benchmarkMixedTypes is used to compare writing mixed data types
func benchmarkMixedTypes(b *testing.B, s Stream) {
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

func BenchmarkMixedTypes_StackStream(b *testing.B) {
	benchmarkMixedTypes(b, newStackStream(nil, InitialBufferSize))
}

// benchmarkWriteToBuffer is used to compare writing to a buffer
func benchmarkWriteToBuffer(b *testing.B, s Stream) {
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

func BenchmarkWriteToBuffer_StackStream(b *testing.B) {
	benchmarkWriteToBuffer(b, newStackStream(nil, InitialBufferSize))
}

// benchmarkIncompleteStructure is used to compare handling incomplete structures
func benchmarkIncompleteStructure(b *testing.B, s Stream) {
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

func BenchmarkIncompleteStructure_StackStream(b *testing.B) {
	benchmarkIncompleteStructure(b, newStackStream(nil, InitialBufferSize))
}
