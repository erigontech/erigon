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

package json

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"

	jsoniter "github.com/json-iterator/go"
)

// Benchmark tests comparing the performance of JsoniterStream (thin jsoniter.Stream wrapper) and StackStream
// Both implementations use the common Stream interface

func newStream() *jsoniter.Stream {
	return jsoniter.NewStream(jsoniter.ConfigDefault, nil, 4096)
}

// benchmarkSimpleObject is used to compare writing a simple JSON object
func benchmarkSimpleObject(b *testing.B, s Stream) {
	b.Helper()
	for i := 0; i < b.N; i++ {
		s.WriteObjectStart()
		s.WriteObjectField("name")
		s.WriteString("John")
		s.WriteMore()
		s.WriteObjectField("age")
		s.WriteInt(30)
		s.WriteObjectEnd()

		err := s.ClosePending(0)
		assert.NoError(b, err)
		err = s.Flush()
		assert.NoError(b, err)
	}
}

func BenchmarkSimpleObject_JsoniterStream(b *testing.B) {
	benchmarkSimpleObject(b, NewJsoniterStream(newStream()))
}

func BenchmarkSimpleObject_StackStream(b *testing.B) {
	benchmarkSimpleObject(b, NewStackStream(newStream()))
}

// benchmarkNestedStructure is used to compare writing a nested JSON structure
func benchmarkNestedStructure(b *testing.B, s Stream) {
	b.Helper()
	for i := 0; i < b.N; i++ {
		s.WriteObjectStart()
		s.WriteObjectField("person")
		s.WriteObjectStart()
		s.WriteObjectField("name")
		s.WriteString("John")
		s.WriteMore()
		s.WriteObjectField("address")
		s.WriteObjectStart()
		s.WriteObjectField("city")
		s.WriteString("New York")
		s.WriteMore()
		s.WriteObjectField("zip")
		s.WriteString("10001")
		s.WriteObjectEnd()
		s.WriteObjectEnd()
		s.WriteMore()
		s.WriteObjectField("active")
		s.WriteTrue()
		s.WriteObjectEnd()

		err := s.ClosePending(0)
		assert.NoError(b, err)
		err = s.Flush()
		assert.NoError(b, err)
	}
}

func BenchmarkNestedStructure_JsoniterStream(b *testing.B) {
	benchmarkNestedStructure(b, NewJsoniterStream(newStream()))
}

func BenchmarkNestedStructure_StackStream(b *testing.B) {
	benchmarkNestedStructure(b, NewStackStream(newStream()))
}

// benchmarkLargeArray is used to compare writing a large array
func benchmarkLargeArray(b *testing.B, s Stream) {
	b.Helper()
	for i := 0; i < b.N; i++ {
		s.WriteArrayStart()
		for j := 0; j < 1000; j++ {
			if j > 0 {
				s.WriteMore()
			}
			s.WriteInt(j)
		}
		s.WriteArrayEnd()

		err := s.ClosePending(0)
		assert.NoError(b, err)
		err = s.Flush()
		assert.NoError(b, err)
	}
}

func BenchmarkLargeArray_JsoniterStream(b *testing.B) {
	benchmarkLargeArray(b, NewJsoniterStream(newStream()))
}

func BenchmarkLargeArray_StackStream(b *testing.B) {
	benchmarkLargeArray(b, NewStackStream(newStream()))
}

// benchmarkMixedTypes is used to compare writing mixed data types
func benchmarkMixedTypes(b *testing.B, s Stream) {
	b.Helper()
	for i := 0; i < b.N; i++ {
		s.WriteObjectStart()
		s.WriteObjectField("string")
		s.WriteString("value")
		s.WriteMore()
		s.WriteObjectField("int")
		s.WriteInt(42)
		s.WriteMore()
		s.WriteObjectField("float")
		s.WriteFloat64(3.14159)
		s.WriteMore()
		s.WriteObjectField("bool")
		s.WriteBool(true)
		s.WriteMore()
		s.WriteObjectField("null")
		s.WriteNil()
		s.WriteObjectEnd()

		err := s.ClosePending(0)
		assert.NoError(b, err)
		err = s.Flush()
		assert.NoError(b, err)
	}
}

func BenchmarkMixedTypes_JsoniterStream(b *testing.B) {
	benchmarkMixedTypes(b, NewJsoniterStream(newStream()))
}

func BenchmarkMixedTypes_StackStream(b *testing.B) {
	benchmarkMixedTypes(b, NewStackStream(newStream()))
}

// benchmarkWriteToBuffer is used to compare writing to a buffer
func benchmarkWriteToBuffer(b *testing.B, s Stream) {
	b.Helper()
	buf := bytes.NewBuffer(nil)
	for i := 0; i < b.N; i++ {
		s.Reset(buf)
		s.WriteObjectStart()
		s.WriteObjectField("name")
		s.WriteString("John")
		s.WriteMore()
		s.WriteObjectField("age")
		s.WriteInt(30)
		s.WriteObjectEnd()

		err := s.ClosePending(0)
		assert.NoError(b, err)
		err = s.Flush()
		assert.NoError(b, err)
	}
}

func BenchmarkWriteToBuffer_JsoniterStream(b *testing.B) {
	benchmarkWriteToBuffer(b, NewJsoniterStream(newStream()))
}

func BenchmarkWriteToBuffer_StackStream(b *testing.B) {
	benchmarkWriteToBuffer(b, NewStackStream(newStream()))
}

// benchmarkIncompleteStructure is used to compare handling incomplete structures
func benchmarkIncompleteStructure(b *testing.B, s Stream) {
	b.Helper()
	for i := 0; i < b.N; i++ {
		// Create an incomplete structure
		s.WriteObjectStart()
		s.WriteObjectField("name")
		s.WriteString("John")
		s.WriteMore()
		s.WriteObjectField("details")
		s.WriteObjectStart()
		s.WriteObjectField("age")
		s.WriteInt(30)
		s.WriteMore()
		s.WriteObjectField("address") // Missing value

		err := s.Flush()
		assert.NoError(b, err)
	}
}

func BenchmarkIncompleteStructure_JsoniterStream(b *testing.B) {
	benchmarkIncompleteStructure(b, NewJsoniterStream(newStream()))
}

func BenchmarkIncompleteStructure_StackStream(b *testing.B) {
	benchmarkIncompleteStructure(b, NewStackStream(newStream()))
}
