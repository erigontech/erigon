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
	"errors"
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"

	jsoniter "github.com/json-iterator/go"
)

func (s *StackStream) closeAllPendingElements() error {
	return s.ClosePending(0)
}

func TestStackStream_BasicOperations(t *testing.T) {
	json := jsoniter.ConfigCompatibleWithStandardLibrary
	stream := json.BorrowStream(nil)
	defer json.ReturnStream(stream)

	ss := NewStackStream(stream)

	// Write a simple object
	ss.WriteObjectStart()
	ss.WriteObjectField("name")
	ss.WriteString("John")
	ss.WriteMore()
	ss.WriteObjectField("age")
	ss.WriteInt(30)
	ss.WriteObjectEnd()

	assert.Equal(t, `{"name":"John","age":30}`, string(ss.Buffer()))
	assert.True(t, ss.IsComplete())
}

func TestStackStream_NestedStructures(t *testing.T) {
	json := jsoniter.ConfigCompatibleWithStandardLibrary
	stream := json.BorrowStream(nil)
	defer json.ReturnStream(stream)

	ss := NewStackStream(stream)

	// Write a nested structure
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

	assert.Equal(t, `{"person":{"name":"John","address":{"city":"New York","zip":"10001"}},"active":true}`, string(ss.Buffer()))
	assert.True(t, ss.IsComplete())
}

func TestStackStream_ArrayOperations(t *testing.T) {
	json := jsoniter.ConfigCompatibleWithStandardLibrary
	stream := json.BorrowStream(nil)
	defer json.ReturnStream(stream)

	ss := NewStackStream(stream)

	// Write an array
	ss.WriteArrayStart()
	ss.WriteInt(1)
	ss.WriteMore()
	ss.WriteInt(2)
	ss.WriteMore()
	ss.WriteInt(3)
	ss.WriteArrayEnd()

	assert.Equal(t, `[1,2,3]`, string(ss.Buffer()))
	assert.True(t, ss.IsComplete())
}

func TestStackStream_MixedStructures(t *testing.T) {
	json := jsoniter.ConfigCompatibleWithStandardLibrary
	stream := json.BorrowStream(nil)
	defer json.ReturnStream(stream)

	ss := NewStackStream(stream)

	// Write a complex structure
	ss.WriteObjectStart()
	ss.WriteObjectField("name")
	ss.WriteString("John")
	ss.WriteMore()
	ss.WriteObjectField("scores")
	ss.WriteArrayStart()
	ss.WriteInt(85)
	ss.WriteMore()
	ss.WriteInt(90)
	ss.WriteMore()
	ss.WriteInt(95)
	ss.WriteArrayEnd()
	ss.WriteMore()
	ss.WriteObjectField("details")
	ss.WriteObjectStart()
	ss.WriteObjectField("active")
	ss.WriteTrue()
	ss.WriteObjectEnd()
	ss.WriteObjectEnd()

	assert.Equal(t, `{"name":"John","scores":[85,90,95],"details":{"active":true}}`, string(ss.Buffer()))
	assert.True(t, ss.IsComplete())
}

func TestStackStream_ClosePendingObjects_Object(t *testing.T) {
	json := jsoniter.ConfigCompatibleWithStandardLibrary
	stream := json.BorrowStream(nil)
	defer json.ReturnStream(stream)

	ss := NewStackStream(stream)

	// Start an object but don't finish it
	ss.WriteObjectStart()
	ss.WriteObjectField("name")
	ss.WriteString("John")
	ss.WriteMore()
	ss.WriteObjectField("age") // Missing value

	// Incomplete JSON at this point
	assert.Equal(t, `{"name":"John","age":`, string(ss.Buffer()))
	assert.False(t, ss.IsComplete())
	assert.Equal(t, 2, ss.GetCurrentDepth()) // Object, Field

	// Close pending objects if necessary
	err := ss.closeAllPendingElements()
	assert.NoError(t, err)

	// Should have completed the JSON properly
	assert.Equal(t, `{"name":"John","age":null}`, string(ss.Buffer()))
	assert.True(t, ss.IsComplete())
}

func TestStackStream_ClosePendingObjects_Array(t *testing.T) {
	json := jsoniter.ConfigCompatibleWithStandardLibrary
	stream := json.BorrowStream(nil)
	defer json.ReturnStream(stream)

	ss := NewStackStream(stream)

	// Start an array but don't finish it
	ss.WriteArrayStart()
	ss.WriteInt(1)
	ss.WriteMore()
	ss.WriteInt(2)
	ss.WriteMore() // Missing final value

	// Incomplete JSON at this point
	assert.Equal(t, `[1,2,`, string(ss.Buffer()))
	assert.False(t, ss.IsComplete())
	assert.Equal(t, 2, ss.GetCurrentDepth()) // Array, Field

	// Flush closing pending objects if necessary
	err := ss.closeAllPendingElements()
	assert.NoError(t, err)

	// Should have completed the JSON properly
	assert.Equal(t, `[1,2,null]`, string(ss.Buffer()))
	assert.True(t, ss.IsComplete())
}

func TestStackStream_ClosePendingObjects_ComplexNested(t *testing.T) {
	json := jsoniter.ConfigCompatibleWithStandardLibrary
	stream := json.BorrowStream(nil)
	defer json.ReturnStream(stream)

	ss := NewStackStream(stream)

	// Create a deeply nested structure but don't complete it
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
	ss.WriteObjectField("zip") // Missing value
	// Several unclosed objects

	// Incomplete JSON at this point
	assert.Equal(t, `{"person":{"name":"John","address":{"city":"New York","zip":`, string(ss.Buffer()))
	assert.False(t, ss.IsComplete())
	assert.Equal(t, 4, ss.GetCurrentDepth()) // Object, Object, Object, Field

	// Flush closing pending objects if necessary
	err := ss.closeAllPendingElements()
	assert.NoError(t, err)

	// Should have completed the JSON properly
	assert.Equal(t, `{"person":{"name":"John","address":{"city":"New York","zip":null}}}`, string(ss.Buffer()))
	assert.True(t, ss.IsComplete())
}

func TestStackStream_ClosePendingObjects_ComplexNestedWithArray(t *testing.T) {
	json := jsoniter.ConfigCompatibleWithStandardLibrary
	stream := json.BorrowStream(nil)
	defer json.ReturnStream(stream)

	ss := NewStackStream(stream)

	// Create a deeply nested structure but don't complete it
	ss.WriteArrayStart()
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
	ss.WriteObjectField("zip") // Missing value
	// Several unclosed objects

	// Incomplete JSON at this point
	assert.Equal(t, `[{"person":{"name":"John","address":{"city":"New York","zip":`, string(ss.Buffer()))
	assert.False(t, ss.IsComplete())
	assert.Equal(t, 5, ss.GetCurrentDepth()) // Array, Object, Object, Object, Field

	// Flush closing pending objects if necessary
	err := ss.closeAllPendingElements()
	assert.NoError(t, err)

	// Should have completed the JSON properly
	assert.Equal(t, `[{"person":{"name":"John","address":{"city":"New York","zip":null}}}]`, string(ss.Buffer()))
	assert.True(t, ss.IsComplete())
}

func TestStackStream_BufferAsString(t *testing.T) {
	json := jsoniter.ConfigCompatibleWithStandardLibrary
	stream := json.BorrowStream(nil)
	defer json.ReturnStream(stream)

	ss := NewStackStream(stream)

	// Create incomplete JSON
	ss.WriteObjectStart()
	ss.WriteObjectField("status")
	ss.WriteString("pending")
	ss.WriteMore()
	ss.WriteObjectField("data") // Missing value

	// Get buffer as a string (should auto-close)
	result, err := ss.BufferAsString()
	assert.NoError(t, err)
	assert.Equal(t, `{"status":"pending","data":null}`, result)
	assert.True(t, ss.IsComplete())
}

func TestStackStream_Reset(t *testing.T) {
	json := jsoniter.ConfigCompatibleWithStandardLibrary
	stream := json.BorrowStream(nil)
	defer json.ReturnStream(stream)

	ss := NewStackStream(stream)

	// Write some data
	ss.WriteObjectStart()
	ss.WriteObjectField("name")
	ss.WriteString("John")
	ss.WriteObjectEnd()

	// Reset
	ss.Reset(nil)

	// Should be empty
	assert.Equal(t, 0, ss.Size())
	assert.True(t, ss.IsComplete())

	// Write new data
	ss.WriteArrayStart()
	ss.WriteInt(1)
	ss.WriteMore()
	ss.WriteInt(2)
	ss.WriteArrayEnd()

	assert.Equal(t, `[1,2]`, string(ss.Buffer()))
}

func TestStackStream_GetStackSummary(t *testing.T) {
	json := jsoniter.ConfigCompatibleWithStandardLibrary
	stream := json.BorrowStream(nil)
	defer json.ReturnStream(stream)

	ss := NewStackStream(stream)

	// Empty stack
	assert.Equal(t, "Empty", ss.GetStackSummary())

	// Add items to the stack
	ss.WriteObjectStart()
	ss.WriteObjectField("users")
	ss.WriteArrayStart()
	ss.WriteObjectStart()
	ss.WriteObjectField("name")

	// Check summary
	summary := ss.GetStackSummary()
	assert.Contains(t, summary, "Object")
	assert.Contains(t, summary, "Field")
	assert.Contains(t, summary, "Array")
	assert.Contains(t, summary, "Object")
	assert.Contains(t, summary, "Field")
}

// TestStackStream_SequentialOperations tests sequential operations without chaining
func TestStackStream_SequentialOperations(t *testing.T) {
	json := jsoniter.ConfigCompatibleWithStandardLibrary
	stream := json.BorrowStream(nil)
	defer json.ReturnStream(stream)

	ss := NewStackStream(stream)

	// Perform operations without chaining
	ss.WriteObjectStart()
	ss.WriteObjectField("name")
	ss.WriteString("John")
	ss.WriteMore()
	ss.WriteObjectField("age")
	ss.WriteInt(30)
	ss.WriteObjectEnd()

	expected := `{"name":"John","age":30}`
	assert.Equal(t, expected, string(ss.Buffer()))
	assert.True(t, ss.IsComplete())
}

// TestStackStream_RecoveryFromIncompleteState tests recovery from the incomplete state
func TestStackStream_RecoveryFromIncompleteState(t *testing.T) {
	json := jsoniter.ConfigCompatibleWithStandardLibrary
	stream := json.BorrowStream(nil)
	defer json.ReturnStream(stream)

	ss := NewStackStream(stream)

	// Create an incomplete structure
	ss.WriteObjectStart()
	ss.WriteObjectField("incomplete")

	// Check that it's incomplete
	assert.False(t, ss.IsComplete())

	// Get the current state
	beforeState := ss.GetStackSummary()
	assert.Contains(t, beforeState, "Field")

	// Complete the structure manually
	ss.WriteString("value")
	ss.WriteObjectEnd()

	// Verify it's now complete
	assert.True(t, ss.IsComplete())
	assert.Equal(t, `{"incomplete":"value"}`, string(ss.Buffer()))
}

// TestStackStream_NestedIncompleteStructures tests handling of nested incomplete structures
func TestStackStream_NestedIncompleteStructures(t *testing.T) {
	testCases := []struct {
		name           string
		buildStructure func(*StackStream)
		expected       string
	}{
		{
			name: "incomplete nested arrays",
			buildStructure: func(ss *StackStream) {
				ss.WriteArrayStart()
				ss.WriteArrayStart()
				ss.WriteArrayStart()
			},
			expected: `[[[]]]`,
		},
		{
			name: "incomplete nested objects",
			buildStructure: func(ss *StackStream) {
				ss.WriteObjectStart()
				ss.WriteObjectField("a")
				ss.WriteObjectStart()
				ss.WriteObjectField("b")
				ss.WriteObjectStart()
			},
			expected: `{"a":{"b":{}}}`,
		},
		{
			name: "mixed incomplete structures",
			buildStructure: func(ss *StackStream) {
				ss.WriteObjectStart()
				ss.WriteObjectField("array")
				ss.WriteArrayStart()
				ss.WriteObjectStart()
				ss.WriteObjectField("field")
			},
			expected: `{"array":[{"field":null}]}`,
		},
		{
			name: "array with multiple trailing commas",
			buildStructure: func(ss *StackStream) {
				ss.WriteArrayStart()
				ss.WriteInt(1)
				ss.WriteMore()
				ss.WriteInt(2)
				ss.WriteMore()
			},
			expected: `[1,2,null]`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			json := jsoniter.ConfigCompatibleWithStandardLibrary
			stream := json.BorrowStream(nil)
			defer json.ReturnStream(stream)

			ss := NewStackStream(stream)
			tc.buildStructure(ss)

			// Verify structure is incomplete
			assert.False(t, ss.IsComplete())

			// Flush should complete the structure
			err := ss.closeAllPendingElements()
			assert.NoError(t, err)

			// Verify the result
			assert.Equal(t, tc.expected, string(ss.Buffer()))
			assert.True(t, ss.IsComplete())
		})
	}
}

// TestStackStream_ClosePendingObjectsWithEmptyStack tests closePendingObjects with empty stack
func TestStackStream_ClosePendingObjectsWithEmptyStack(t *testing.T) {
	json := jsoniter.ConfigCompatibleWithStandardLibrary
	stream := json.BorrowStream(nil)
	defer json.ReturnStream(stream)

	ss := NewStackStream(stream)

	// Stack is already empty
	assert.True(t, ss.IsComplete())

	// Call closeAllPending should be a no-op
	err := ss.closeAllPendingElements()
	assert.NoError(t, err)
	assert.True(t, ss.IsComplete())
}

// TestStackStream_MultipleFlushCalls tests multiple flush calls
func TestStackStream_MultipleFlushCalls(t *testing.T) {
	json := jsoniter.ConfigCompatibleWithStandardLibrary
	stream := json.BorrowStream(nil)
	defer json.ReturnStream(stream)

	ss := NewStackStream(stream)

	// Create an incomplete structure
	ss.WriteObjectStart()
	ss.WriteObjectField("test")

	// The first flush should complete the structure
	err := ss.closeAllPendingElements()
	assert.NoError(t, err)
	assert.Equal(t, `{"test":null}`, string(ss.Buffer()))
	assert.True(t, ss.IsComplete())

	// The second flush should be a no-op
	err = ss.closeAllPendingElements()
	assert.NoError(t, err)
	assert.Equal(t, `{"test":null}`, string(ss.Buffer()))
	assert.True(t, ss.IsComplete())
}

// TestStackStream_EmptyStructures tests handling of empty objects and arrays
func TestStackStream_EmptyStructures(t *testing.T) {
	json := jsoniter.ConfigCompatibleWithStandardLibrary
	stream := json.BorrowStream(nil)
	defer json.ReturnStream(stream)

	ss := NewStackStream(stream)

	// Test empty object
	ss.WriteObjectStart()
	ss.WriteObjectEnd()
	assert.Equal(t, `{}`, string(ss.Buffer()))
	assert.True(t, ss.IsComplete())

	// Reset and test empty array
	ss.Reset(nil)
	ss.WriteArrayStart()
	ss.WriteArrayEnd()
	assert.Equal(t, `[]`, string(ss.Buffer()))
	assert.True(t, ss.IsComplete())

	// Reset and test nested empty structures
	ss.Reset(nil)
	ss.WriteObjectStart()
	ss.WriteObjectField("emptyObj")
	ss.WriteObjectStart()
	ss.WriteObjectEnd()
	ss.WriteMore()
	ss.WriteObjectField("emptyArr")
	ss.WriteArrayStart()
	ss.WriteArrayEnd()
	ss.WriteObjectEnd()
	assert.Equal(t, `{"emptyObj":{},"emptyArr":[]}`, string(ss.Buffer()))
	assert.True(t, ss.IsComplete())
}

// TestStackStream_AllDataTypes tests all data types supported by StackStream
func TestStackStream_AllDataTypes(t *testing.T) {
	json := jsoniter.ConfigCompatibleWithStandardLibrary
	stream := json.BorrowStream(nil)
	defer json.ReturnStream(stream)

	ss := NewStackStream(stream)

	// Test all primitive data types
	ss.WriteObjectStart()
	ss.WriteObjectField("nil")
	ss.WriteNil()
	ss.WriteMore()
	ss.WriteObjectField("bool_true")
	ss.WriteTrue()
	ss.WriteMore()
	ss.WriteObjectField("bool_false")
	ss.WriteFalse()
	ss.WriteMore()
	ss.WriteObjectField("bool_var")
	ss.WriteBool(true)
	ss.WriteMore()
	ss.WriteObjectField("int")
	ss.WriteInt(-42)
	ss.WriteMore()
	ss.WriteObjectField("int8")
	ss.WriteInt8(127)
	ss.WriteMore()
	ss.WriteObjectField("int16")
	ss.WriteInt16(-32000)
	ss.WriteMore()
	ss.WriteObjectField("int32")
	ss.WriteInt32(2147483647)
	ss.WriteMore()
	ss.WriteObjectField("int64")
	ss.WriteInt64(-9223372036854775807)
	ss.WriteMore()
	ss.WriteObjectField("uint")
	ss.WriteUint(42)
	ss.WriteMore()
	ss.WriteObjectField("uint8")
	ss.WriteUint8(255)
	ss.WriteMore()
	ss.WriteObjectField("uint16")
	ss.WriteUint16(65535)
	ss.WriteMore()
	ss.WriteObjectField("uint32")
	ss.WriteUint32(4294967295)
	ss.WriteMore()
	ss.WriteObjectField("uint64")
	ss.WriteUint64(18446744073709551615)
	ss.WriteMore()
	ss.WriteObjectField("float32")
	ss.WriteFloat32(3.14159)
	ss.WriteMore()
	ss.WriteObjectField("float64")
	ss.WriteFloat64(2.7182818284590452353602874713527)
	ss.WriteMore()
	ss.WriteObjectField("string")
	ss.WriteString("Hello, World!")
	ss.WriteObjectEnd()

	result := string(ss.Buffer())
	assert.Contains(t, result, `"nil":null`)
	assert.Contains(t, result, `"bool_true":true`)
	assert.Contains(t, result, `"bool_false":false`)
	assert.Contains(t, result, `"bool_var":true`)
	assert.Contains(t, result, `"int":-42`)
	assert.Contains(t, result, `"int8":127`)
	assert.Contains(t, result, `"int16":-32000`)
	assert.Contains(t, result, `"int32":2147483647`)
	assert.Contains(t, result, `"int64":-9223372036854775807`)
	assert.Contains(t, result, `"uint":42`)
	assert.Contains(t, result, `"uint8":255`)
	assert.Contains(t, result, `"uint16":65535`)
	assert.Contains(t, result, `"uint32":4294967295`)
	assert.Contains(t, result, `"uint64":18446744073709551615`)
	assert.Contains(t, result, `"float32":3.14159`)
	assert.Contains(t, result, `"float64":2.718281828459045`)
	assert.Contains(t, result, `"string":"Hello, World!"`)
	assert.True(t, ss.IsComplete())
}

// TestStackStream_BoundaryValues tests boundary values for numeric types
func TestStackStream_BoundaryValues(t *testing.T) {
	json := jsoniter.ConfigCompatibleWithStandardLibrary
	stream := json.BorrowStream(nil)
	defer json.ReturnStream(stream)

	ss := NewStackStream(stream)

	// Test boundary values
	ss.WriteObjectStart()
	ss.WriteObjectField("int8_min")
	ss.WriteInt8(math.MinInt8)
	ss.WriteMore()
	ss.WriteObjectField("int8_max")
	ss.WriteInt8(math.MaxInt8)
	ss.WriteMore()
	ss.WriteObjectField("int16_min")
	ss.WriteInt16(math.MinInt16)
	ss.WriteMore()
	ss.WriteObjectField("int16_max")
	ss.WriteInt16(math.MaxInt16)
	ss.WriteMore()
	ss.WriteObjectField("int32_min")
	ss.WriteInt32(math.MinInt32)
	ss.WriteMore()
	ss.WriteObjectField("int32_max")
	ss.WriteInt32(math.MaxInt32)
	ss.WriteMore()
	ss.WriteObjectField("int64_min")
	ss.WriteInt64(math.MinInt64)
	ss.WriteMore()
	ss.WriteObjectField("int64_max")
	ss.WriteInt64(math.MaxInt64)
	ss.WriteMore()
	ss.WriteObjectField("uint8_max")
	ss.WriteUint8(math.MaxUint8)
	ss.WriteMore()
	ss.WriteObjectField("uint16_max")
	ss.WriteUint16(math.MaxUint16)
	ss.WriteMore()
	ss.WriteObjectField("uint32_max")
	ss.WriteUint32(math.MaxUint32)
	ss.WriteObjectEnd()

	// NaN and Infinity for Float64 not supported by jsoniter

	result := string(ss.Buffer())
	assert.Contains(t, result, `"int8_min":-128`)
	assert.Contains(t, result, `"int8_max":127`)
	assert.Contains(t, result, `"int16_min":-32768`)
	assert.Contains(t, result, `"int16_max":32767`)
	assert.Contains(t, result, `"int32_min":-2147483648`)
	assert.Contains(t, result, `"int32_max":2147483647`)
	assert.Contains(t, result, `"int64_min":-9223372036854775808`)
	assert.Contains(t, result, `"int64_max":9223372036854775807`)
	assert.Contains(t, result, `"uint8_max":255`)
	assert.Contains(t, result, `"uint16_max":65535`)
	assert.Contains(t, result, `"uint32_max":4294967295`)
	// NaN and Infinity are represented as null in JSON
	//assert.Contains(t, result, `"float32_special":null`)
	//assert.Contains(t, result, `"float64_inf":null`)
	//assert.Contains(t, result, `"float64_neg_inf":null`)
	assert.True(t, ss.IsComplete())
}

// TestStackStream_ExtremeNesting tests deeply nested structures
func TestStackStream_ExtremeNesting(t *testing.T) {
	json := jsoniter.ConfigCompatibleWithStandardLibrary
	stream := json.BorrowStream(nil)
	defer json.ReturnStream(stream)

	ss := NewStackStream(stream)

	// Create a deeply nested structure (50 levels deep)
	const nestingDepth = 50

	// Open nested objects
	for i := 0; i < nestingDepth; i++ {
		ss.WriteObjectStart()
		ss.WriteObjectField(fmt.Sprintf("level%d", i))
	}

	// Write a value at the deepest level
	ss.WriteString("deep value")

	// Close all objects
	for i := 0; i < nestingDepth; i++ {
		ss.WriteObjectEnd()
	}

	// Verify the structure is complete
	assert.True(t, ss.IsComplete())
	assert.Equal(t, 0, ss.GetCurrentDepth())

	// Verify the JSON is valid by parsing it back
	var result interface{}
	err := jsoniter.Unmarshal(ss.Buffer(), &result)
	assert.NoError(t, err)
}

// TestStackStream_ErrorHandlingWithoutClosing tests error handling and propagation *without* closing pending elements
func TestStackStream_ErrorHandlingWithoutClosing(t *testing.T) {
	json := jsoniter.ConfigCompatibleWithStandardLibrary

	// Test with a writer that will fail
	failWriter := &failingWriter{failAfter: 10}
	stream := json.BorrowStream(failWriter)
	defer json.ReturnStream(stream)

	ss := NewStackStream(stream)

	// Write enough data to trigger the error
	ss.WriteObjectStart()
	ss.WriteObjectField("longString")
	ss.WriteString("This string should cause the writer to fail")

	// Flush should propagate the error
	err := ss.Flush()
	assert.Error(t, err)
	assert.Equal(t, "write failed", err.Error())
}

// TestStackStream_ErrorHandlingWithClosing tests error handling and propagation *with* closing pending elements
func TestStackStream_ErrorHandlingWithClosing(t *testing.T) {
	json := jsoniter.ConfigCompatibleWithStandardLibrary

	// Test with a writer that will fail
	failWriter := &failingWriter{failAfter: 10}
	stream := json.BorrowStream(failWriter)
	defer json.ReturnStream(stream)

	ss := NewStackStream(stream)

	// Write enough data to trigger the error
	ss.WriteObjectStart()
	ss.WriteObjectField("longString")
	ss.WriteString("This string should cause the writer to fail")
	err := ss.closeAllPendingElements()

	// Flush should propagate the error
	err = ss.Flush()
	assert.Error(t, err)
	assert.Equal(t, "write failed", err.Error())
}

// TestStackStream_StackManipulationEdgeCases tests edge cases in stack manipulation
func TestStackStream_StackManipulationEdgeCases(t *testing.T) {
	json := jsoniter.ConfigCompatibleWithStandardLibrary
	stream := json.BorrowStream(nil)
	defer json.ReturnStream(stream)

	ss := NewStackStream(stream)

	// Test 1: Popping from an empty stack should not panic
	ss.pop(ItemObject)
	assert.Equal(t, 0, ss.GetCurrentDepth())

	// Test 2: Popping an item that doesn't match the top of the stack does nothing
	ss.push(ItemArray)
	ss.pop(ItemObject)
	assert.Equal(t, 1, ss.GetCurrentDepth()) // Stack should still have the array

	// Test 3: Multiple pushes and pops
	ss.Reset(nil)
	ss.push(ItemObject)
	ss.push(ItemField)
	ss.push(ItemComma)
	assert.Equal(t, 3, ss.GetCurrentDepth())

	ss.pop(ItemComma)
	ss.pop(ItemField)
	assert.Equal(t, 1, ss.GetCurrentDepth())

	// Test 4: Verify stack state with GetStackSummary
	summary := ss.GetStackSummary()
	assert.Contains(t, summary, "Object")
	assert.NotContains(t, summary, "Field")
	assert.NotContains(t, summary, "Comma")
}

// TestStackStream_MixedWriteOperations tests mixing different write operations
func TestStackStream_MixedWriteOperations(t *testing.T) {
	json := jsoniter.ConfigCompatibleWithStandardLibrary
	stream := json.BorrowStream(nil)
	defer json.ReturnStream(stream)

	ss := NewStackStream(stream)

	// Test mixing WriteRaw with other operations
	ss.WriteObjectStart()
	ss.WriteObjectField("raw")
	ss.WriteRaw("42")
	ss.WriteMore()
	ss.WriteObjectField("normal")
	ss.WriteInt(42)
	ss.WriteObjectEnd()

	assert.Equal(t, `{"raw":42,"normal":42}`, string(ss.Buffer()))
	assert.True(t, ss.IsComplete())

	// Test using Write method
	ss.Reset(nil)
	ss.WriteArrayStart()
	n, err := ss.Write([]byte(`"hello"`))
	assert.NoError(t, err)
	assert.Equal(t, 7, n)
	ss.WriteMore()
	n, err = ss.Write([]byte(`123`))
	assert.NoError(t, err)
	assert.Equal(t, 3, n)
	ss.WriteArrayEnd()

	assert.Equal(t, `["hello",123]`, string(ss.Buffer()))
	assert.True(t, ss.IsComplete())
}

// TestStackStream_IncompleteStructuresWithFlush tests flushing with various incomplete structures
func TestStackStream_IncompleteStructuresWithFlush(t *testing.T) {
	testCases := []struct {
		name           string
		buildStructure func(*StackStream)
		expected       string
	}{
		{
			name: "object with missing field value",
			buildStructure: func(ss *StackStream) {
				ss.WriteObjectStart()
				ss.WriteObjectField("field") // Missing value
			},
			expected: `{"field":null}`,
		},
		{
			name: "array with trailing comma",
			buildStructure: func(ss *StackStream) {
				ss.WriteArrayStart()
				ss.WriteInt(1)
				ss.WriteMore() // Trailing comma
			},
			expected: `[1,null]`,
		},
		{
			name: "nested object with missing field in inner object",
			buildStructure: func(ss *StackStream) {
				ss.WriteObjectStart()
				ss.WriteObjectField("outer")
				ss.WriteObjectStart()
				ss.WriteObjectField("inner") // Missing value for the inner field
			},
			expected: `{"outer":{"inner":null}}`,
		},
		{
			name: "object with field and separator",
			buildStructure: func(ss *StackStream) {
				ss.WriteObjectStart()
				ss.WriteObjectField("first")
				ss.WriteString("value")
				ss.WriteMore()
				ss.WriteObjectField("second")
				ss.WriteInt(42)
				ss.WriteMore()
			},
			expected: `{"first":"value","second":42,"":""}`,
		},
		{
			name: "multiple nested incomplete structures",
			buildStructure: func(ss *StackStream) {
				ss.WriteObjectStart()
				ss.WriteObjectField("a")
				ss.WriteArrayStart()
				ss.WriteObjectStart()
				ss.WriteObjectField("b") // Missing value for b
			},
			expected: `{"a":[{"b":null}]}`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			json := jsoniter.ConfigCompatibleWithStandardLibrary
			stream := json.BorrowStream(nil)
			defer json.ReturnStream(stream)

			ss := NewStackStream(stream)
			tc.buildStructure(ss)

			// Verify structure is incomplete
			assert.False(t, ss.IsComplete())

			// Flush should complete the structure
			err := ss.closeAllPendingElements()
			assert.NoError(t, err)

			// Verify the result
			assert.Equal(t, tc.expected, string(ss.Buffer()))
			assert.True(t, ss.IsComplete())
		})
	}
}

// TestStackStream_BufferAsStringWithErrors tests BufferAsString with error conditions
func TestStackStream_BufferAsStringWithErrors(t *testing.T) {
	// Test with a writer that will fail
	failWriter := &failingWriter{failAfter: 10}
	json := jsoniter.ConfigCompatibleWithStandardLibrary
	stream := json.BorrowStream(failWriter)
	defer json.ReturnStream(stream)

	ss := NewStackStream(stream)

	// Write enough data to trigger the error
	ss.WriteObjectStart()
	ss.WriteObjectField("longString")
	ss.WriteString("This string should cause the writer to fail")

	// Complete the structure and flush
	err := ss.closeAllPendingElements()
	err = ss.Flush()

	// Attempt to get buffer as a string, which should propagate the error
	result, err := ss.BufferAsString()
	assert.Error(t, err)
	assert.Equal(t, "", result)
	assert.Equal(t, "write failed", err.Error())
}

// Helper type for testing error conditions
type failingWriter struct {
	bytesWritten int
	failAfter    int
}

func (w *failingWriter) Write(p []byte) (n int, err error) {
	if w.bytesWritten+len(p) > w.failAfter {
		return 0, errors.New("write failed")
	}
	w.bytesWritten += len(p)
	return len(p), nil
}
