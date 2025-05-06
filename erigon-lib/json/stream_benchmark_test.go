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

	jsoniter "github.com/json-iterator/go"
)

// Benchmark tests comparing the performance of jsoniter.Stream and StackStream
// Both implementations use the common Stream interface

// BenchmarkSimpleObject compares writing a simple JSON object
func BenchmarkSimpleObject(b *testing.B) {
	b.Run("JsoniterStream", func(b *testing.B) {
		json := jsoniter.ConfigCompatibleWithStandardLibrary
		s := json.BorrowStream(nil)
		for i := 0; i < b.N; i++ {
			s.WriteObjectStart()
			s.WriteObjectField("name")
			s.WriteString("John")
			s.WriteMore()
			s.WriteObjectField("age")
			s.WriteInt(30)
			s.WriteObjectEnd()

			_ = s.Flush()
			json.ReturnStream(s)
		}
	})

	b.Run("StackStream", func(b *testing.B) {
		json := jsoniter.ConfigCompatibleWithStandardLibrary
		s := json.BorrowStream(nil)
		ss := NewStackStream(s)
		for i := 0; i < b.N; i++ {
			ss.WriteObjectStart()
			ss.WriteObjectField("name")
			ss.WriteString("John")
			ss.WriteMore()
			ss.WriteObjectField("age")
			ss.WriteInt(30)
			ss.WriteObjectEnd()

			_ = ss.Flush()
			json.ReturnStream(s)
		}
	})
}

// BenchmarkNestedStructure compares writing a nested JSON structure
func BenchmarkNestedStructure(b *testing.B) {
	b.Run("JsoniterStream", func(b *testing.B) {
		json := jsoniter.ConfigCompatibleWithStandardLibrary
		s := json.BorrowStream(nil)
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

			_ = s.Flush()
			json.ReturnStream(s)
		}
	})

	b.Run("StackStream", func(b *testing.B) {
		json := jsoniter.ConfigCompatibleWithStandardLibrary
		s := json.BorrowStream(nil)
		ss := NewStackStream(s)
		for i := 0; i < b.N; i++ {
			ss.WriteObjectStart()
			ss.WriteObjectField("person")
			ss.WriteObjectStart()
			ss.WriteObjectField("name")
			ss.WriteString("John")
			ss.WriteMore()
			ss.WriteObjectField("address")
			ss.WriteObjectStart()
			ss.WriteObjectField("city")
			ss.WriteString("New York")
			ss.WriteMore()
			ss.WriteObjectField("zip")
			ss.WriteString("10001")
			ss.WriteObjectEnd()
			ss.WriteObjectEnd()
			ss.WriteMore()
			ss.WriteObjectField("active")
			ss.WriteTrue()
			ss.WriteObjectEnd()

			_ = ss.Flush()
			json.ReturnStream(s)
		}
	})
}

// BenchmarkLargeArray compares writing a large array
func BenchmarkLargeArray(b *testing.B) {
	b.Run("JsoniterStream", func(b *testing.B) {
		json := jsoniter.ConfigCompatibleWithStandardLibrary
		s := json.BorrowStream(nil)
		for i := 0; i < b.N; i++ {
			s.WriteArrayStart()
			for j := 0; j < 1000; j++ {
				if j > 0 {
					s.WriteMore()
				}
				s.WriteInt(j)
			}
			s.WriteArrayEnd()

			_ = s.Flush()
			json.ReturnStream(s)
		}
	})

	b.Run("StackStream", func(b *testing.B) {
		json := jsoniter.ConfigCompatibleWithStandardLibrary
		stream := json.BorrowStream(nil)
		ss := NewStackStream(stream)
		for i := 0; i < b.N; i++ {
			ss.WriteArrayStart()
			for j := 0; j < 1000; j++ {
				if j > 0 {
					ss.WriteMore()
				}
				ss.WriteInt(j)
			}
			ss.WriteArrayEnd()

			_ = ss.Flush()
			json.ReturnStream(stream)
		}
	})
}

// BenchmarkMixedTypes compares writing mixed data types
func BenchmarkMixedTypes(b *testing.B) {
	b.Run("JsoniterStream", func(b *testing.B) {
		json := jsoniter.ConfigCompatibleWithStandardLibrary
		s := json.BorrowStream(nil)
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

			_ = s.Flush()
			json.ReturnStream(s)
		}
	})

	b.Run("StackStream", func(b *testing.B) {
		json := jsoniter.ConfigCompatibleWithStandardLibrary
		s := json.BorrowStream(nil)
		ss := NewStackStream(s)
		for i := 0; i < b.N; i++ {
			ss.WriteObjectStart()
			ss.WriteObjectField("string")
			ss.WriteString("value")
			ss.WriteMore()
			ss.WriteObjectField("int")
			ss.WriteInt(42)
			ss.WriteMore()
			ss.WriteObjectField("float")
			ss.WriteFloat64(3.14159)
			ss.WriteMore()
			ss.WriteObjectField("bool")
			ss.WriteBool(true)
			ss.WriteMore()
			ss.WriteObjectField("null")
			ss.WriteNil()
			ss.WriteObjectEnd()

			_ = ss.Flush()
			json.ReturnStream(s)
		}
	})
}

// BenchmarkIncompleteStructure compares handling incomplete structures
func BenchmarkIncompleteStructure(b *testing.B) {
	b.Run("JsoniterStream", func(b *testing.B) {
		json := jsoniter.ConfigCompatibleWithStandardLibrary
		s := json.BorrowStream(nil)
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

			// For jsoniter, we need to manually complete the structure
			s.WriteNil()
			s.WriteObjectEnd()
			s.WriteObjectEnd()

			_ = s.Flush()
			json.ReturnStream(s)
		}
	})

	b.Run("StackStream", func(b *testing.B) {
		json := jsoniter.ConfigCompatibleWithStandardLibrary
		s := json.BorrowStream(nil)
		ss := NewStackStream(s)
		for i := 0; i < b.N; i++ {
			// Create an incomplete structure
			ss.WriteObjectStart()
			ss.WriteObjectField("name")
			ss.WriteString("John")
			ss.WriteMore()
			ss.WriteObjectField("details")
			ss.WriteObjectStart()
			ss.WriteObjectField("age")
			ss.WriteInt(30)
			ss.WriteMore()
			ss.WriteObjectField("address") // Missing value

			// StackStream will auto-complete on Flush
			_ = ss.Flush()
			json.ReturnStream(s)
		}
	})
}

// BenchmarkWriteToBuffer compares writing to a buffer
func BenchmarkWriteToBuffer(b *testing.B) {
	b.Run("JsoniterStream", func(b *testing.B) {
		buf := bytes.NewBuffer(nil)
		json := jsoniter.ConfigCompatibleWithStandardLibrary
		s := json.BorrowStream(buf)
		for i := 0; i < b.N; i++ {
			s.WriteObjectStart()
			s.WriteObjectField("name")
			s.WriteString("John")
			s.WriteMore()
			s.WriteObjectField("age")
			s.WriteInt(30)
			s.WriteObjectEnd()

			_ = s.Flush()
			json.ReturnStream(s)
		}
	})

	b.Run("StackStream", func(b *testing.B) {
		buf := bytes.NewBuffer(nil)
		json := jsoniter.ConfigCompatibleWithStandardLibrary
		s := json.BorrowStream(buf)
		ss := NewStackStream(s)
		for i := 0; i < b.N; i++ {
			ss.WriteObjectStart()
			ss.WriteObjectField("name")
			ss.WriteString("John")
			ss.WriteMore()
			ss.WriteObjectField("age")
			ss.WriteInt(30)
			ss.WriteObjectEnd()

			_ = ss.Flush()
			json.ReturnStream(s)
		}
	})
}
