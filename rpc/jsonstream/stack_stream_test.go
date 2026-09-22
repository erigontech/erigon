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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	jsoniter "github.com/json-iterator/go"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/hexutil"
)

func (s *StackStream) closeAllPendingElements() error {
	return s.ClosePending(0)
}

func TestStackStream_BasicOperations(t *testing.T) {
	ss := newStackStream(nil, InitialBufferSize)

	// Write a simple object
	ss.WriteObjectStart()
	ss.Field("name")
	ss.WriteString("John")
	ss.Field("age")
	ss.Int(30)
	ss.WriteObjectEnd()

	assert.Equal(t, `{"name":"John","age":30}`, string(ss.Buffer()))
	assert.True(t, ss.IsComplete())
}

func TestStackStream_NestedStructures(t *testing.T) {
	ss := newStackStream(nil, InitialBufferSize)

	// Write a nested structure
	ss.WriteObjectStart()
	ss.Field("person")
	ss.WriteObjectStart()
	ss.Field("name")
	ss.WriteString("John")
	ss.Field("address")
	ss.WriteObjectStart()
	ss.Field("city")
	ss.WriteString("New York")
	ss.Field("zip")
	ss.WriteString("10001")
	ss.WriteObjectEnd()
	ss.WriteObjectEnd()
	ss.Field("active")
	ss.WriteTrue()
	ss.WriteObjectEnd()

	assert.Equal(t, `{"person":{"name":"John","address":{"city":"New York","zip":"10001"}},"active":true}`, string(ss.Buffer()))
	assert.True(t, ss.IsComplete())
}

func TestStackStream_ArrayOperations(t *testing.T) {
	ss := newStackStream(nil, InitialBufferSize)

	// Write an array
	ss.WriteArrayStart()
	ss.Int(1)
	ss.Int(2)
	ss.Int(3)
	ss.WriteArrayEnd()

	assert.Equal(t, `[1,2,3]`, string(ss.Buffer()))
	assert.True(t, ss.IsComplete())
}

func TestStackStream_MixedStructures(t *testing.T) {
	ss := newStackStream(nil, InitialBufferSize)

	// Write a complex structure
	ss.WriteObjectStart()
	ss.Field("name")
	ss.WriteString("John")
	ss.Field("scores")
	ss.WriteArrayStart()
	ss.Int(85)
	ss.Int(90)
	ss.Int(95)
	ss.WriteArrayEnd()
	ss.Field("details")
	ss.WriteObjectStart()
	ss.Field("active")
	ss.WriteTrue()
	ss.WriteObjectEnd()
	ss.WriteObjectEnd()

	assert.Equal(t, `{"name":"John","scores":[85,90,95],"details":{"active":true}}`, string(ss.Buffer()))
	assert.True(t, ss.IsComplete())
}

func TestStackStream_ClosePendingObjects_Object(t *testing.T) {
	ss := newStackStream(nil, InitialBufferSize)

	// Start an object but don't finish it
	ss.WriteObjectStart()
	ss.Field("name")
	ss.WriteString("John")
	ss.Field("age") // Missing value

	// Incomplete JSON at this point
	assert.Equal(t, `{"name":"John","age":`, string(ss.Buffer()))
	assert.False(t, ss.IsComplete())
	assert.Equal(t, 2, ss.Depth()) // Object, Field

	// Close pending objects if necessary
	err := ss.closeAllPendingElements()
	assert.NoError(t, err)

	// Should have completed the JSON properly
	assert.Equal(t, `{"name":"John","age":null}`, string(ss.Buffer()))
	assert.True(t, ss.IsComplete())
}

func TestStackStream_ClosePendingObjects_Array(t *testing.T) {
	ss := newStackStream(nil, InitialBufferSize)

	// Start an array but don't finish it
	ss.WriteArrayStart()
	ss.Int(1)
	ss.Int(2)

	// Incomplete JSON at this point
	assert.Equal(t, `[1,2`, string(ss.Buffer()))
	assert.False(t, ss.IsComplete())
	assert.Equal(t, 1, ss.Depth()) // Array

	// Flush closing pending objects if necessary
	err := ss.closeAllPendingElements()
	assert.NoError(t, err)

	// Should have completed the JSON properly
	assert.Equal(t, `[1,2]`, string(ss.Buffer()))
	assert.True(t, ss.IsComplete())
}

func TestStackStream_ClosePendingObjects_ComplexNested(t *testing.T) {
	ss := newStackStream(nil, InitialBufferSize)

	// Create a deeply nested structure but don't complete it
	ss.WriteObjectStart()
	ss.Field("person")
	ss.WriteObjectStart()
	ss.Field("name")
	ss.WriteString("John")
	ss.Field("address")
	ss.WriteObjectStart()
	ss.Field("city")
	ss.WriteString("New York")
	ss.Field("zip") // Missing value
	// Several unclosed objects

	// Incomplete JSON at this point
	assert.Equal(t, `{"person":{"name":"John","address":{"city":"New York","zip":`, string(ss.Buffer()))
	assert.False(t, ss.IsComplete())
	assert.Equal(t, 4, ss.Depth()) // Object, Object, Object, Field

	// Flush closing pending objects if necessary
	err := ss.closeAllPendingElements()
	assert.NoError(t, err)

	// Should have completed the JSON properly
	assert.Equal(t, `{"person":{"name":"John","address":{"city":"New York","zip":null}}}`, string(ss.Buffer()))
	assert.True(t, ss.IsComplete())
}

func TestStackStream_ClosePendingObjects_ComplexNestedWithArray(t *testing.T) {
	ss := newStackStream(nil, InitialBufferSize)

	// Create a deeply nested structure but don't complete it
	ss.WriteArrayStart()
	ss.WriteObjectStart()
	ss.Field("person")
	ss.WriteObjectStart()
	ss.Field("name")
	ss.WriteString("John")
	ss.Field("address")
	ss.WriteObjectStart()
	ss.Field("city")
	ss.WriteString("New York")
	ss.Field("zip") // Missing value
	// Several unclosed objects

	// Incomplete JSON at this point
	assert.Equal(t, `[{"person":{"name":"John","address":{"city":"New York","zip":`, string(ss.Buffer()))
	assert.False(t, ss.IsComplete())
	assert.Equal(t, 5, ss.Depth()) // Array, Object, Object, Object, Field

	// Flush closing pending objects if necessary
	err := ss.closeAllPendingElements()
	assert.NoError(t, err)

	// Should have completed the JSON properly
	assert.Equal(t, `[{"person":{"name":"John","address":{"city":"New York","zip":null}}}]`, string(ss.Buffer()))
	assert.True(t, ss.IsComplete())
}

func TestStackStream_BufferAsString(t *testing.T) {
	ss := newStackStream(nil, InitialBufferSize)

	// Create incomplete JSON
	ss.WriteObjectStart()
	ss.Field("status")
	ss.WriteString("pending")
	ss.Field("data") // Missing value

	// Get buffer as a string (should auto-close)
	result, err := ss.BufferAsString()
	assert.NoError(t, err)
	assert.Equal(t, `{"status":"pending","data":null}`, result)
	assert.True(t, ss.IsComplete())
}

func TestStackStream_Reset(t *testing.T) {
	ss := newStackStream(nil, InitialBufferSize)

	// Write some data
	ss.WriteObjectStart()
	ss.Field("name")
	ss.WriteString("John")
	ss.WriteObjectEnd()

	// Reset
	ss.Reset(nil)

	// Should be empty
	assert.Equal(t, 0, len(ss.Buffer()))
	assert.True(t, ss.IsComplete())

	// Write new data
	ss.WriteArrayStart()
	ss.Int(1)
	ss.Int(2)
	ss.WriteArrayEnd()

	assert.Equal(t, `[1,2]`, string(ss.Buffer()))
}

func TestStackStream_GetStackSummary(t *testing.T) {
	ss := newStackStream(nil, InitialBufferSize)

	// Empty stack
	assert.Equal(t, "Empty", ss.StackSummary())

	// Add items to the stack
	ss.WriteObjectStart()
	ss.Field("users")
	ss.WriteArrayStart()
	ss.WriteObjectStart()
	ss.Field("name")

	// Check summary
	summary := ss.StackSummary()
	assert.Contains(t, summary, "Object")
	assert.Contains(t, summary, "Field")
	assert.Contains(t, summary, "Array")
	assert.Contains(t, summary, "Object")
	assert.Contains(t, summary, "Field")
}

// TestStackStream_SequentialOperations tests sequential operations without chaining
func TestStackStream_SequentialOperations(t *testing.T) {
	ss := newStackStream(nil, InitialBufferSize)

	// Perform operations without chaining
	ss.WriteObjectStart()
	ss.Field("name")
	ss.WriteString("John")
	ss.Field("age")
	ss.Int(30)
	ss.WriteObjectEnd()

	expected := `{"name":"John","age":30}`
	assert.Equal(t, expected, string(ss.Buffer()))
	assert.True(t, ss.IsComplete())
}

// TestStackStream_RecoveryFromIncompleteState tests recovery from the incomplete state
func TestStackStream_RecoveryFromIncompleteState(t *testing.T) {
	ss := newStackStream(nil, InitialBufferSize)

	// Create an incomplete structure
	ss.WriteObjectStart()
	ss.Field("incomplete")

	// Check that it's incomplete
	assert.False(t, ss.IsComplete())

	// Get the current state
	beforeState := ss.StackSummary()
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
				ss.Field("a")
				ss.WriteObjectStart()
				ss.Field("b")
				ss.WriteObjectStart()
			},
			expected: `{"a":{"b":{}}}`,
		},
		{
			name: "mixed incomplete structures",
			buildStructure: func(ss *StackStream) {
				ss.WriteObjectStart()
				ss.Field("array")
				ss.WriteArrayStart()
				ss.WriteObjectStart()
				ss.Field("field")
			},
			expected: `{"array":[{"field":null}]}`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ss := newStackStream(nil, InitialBufferSize)
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
	ss := newStackStream(nil, InitialBufferSize)

	// Stack is already empty
	assert.True(t, ss.IsComplete())

	// Call closeAllPending should be a no-op
	err := ss.closeAllPendingElements()
	assert.NoError(t, err)
	assert.True(t, ss.IsComplete())
}

// TestStackStream_MultipleFlushCalls tests multiple flush calls
func TestStackStream_MultipleFlushCalls(t *testing.T) {
	ss := newStackStream(nil, InitialBufferSize)

	// Create an incomplete structure
	ss.WriteObjectStart()
	ss.Field("test")

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
	ss := newStackStream(nil, InitialBufferSize)

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
	ss.Field("emptyObj")
	ss.WriteObjectStart()
	ss.WriteObjectEnd()
	ss.Field("emptyArr")
	ss.WriteArrayStart()
	ss.WriteArrayEnd()
	ss.WriteObjectEnd()
	assert.Equal(t, `{"emptyObj":{},"emptyArr":[]}`, string(ss.Buffer()))
	assert.True(t, ss.IsComplete())
}

// TestStackStream_AllDataTypes tests all data types supported by StackStream
func TestStackStream_AllDataTypes(t *testing.T) {
	ss := newStackStream(nil, InitialBufferSize)

	// Test all primitive data types
	ss.WriteObjectStart()
	ss.Field("nil")
	ss.WriteNil()
	ss.Field("bool_true")
	ss.WriteTrue()
	ss.Field("bool_false")
	ss.WriteFalse()
	ss.Field("bool_var")
	ss.WriteBool(true)
	ss.Field("int")
	ss.Int(int64(-42))
	ss.Field("int8")
	ss.Int(127)
	ss.Field("int16")
	ss.Int(int64(-32000))
	ss.Field("int32")
	ss.Int(2147483647)
	ss.Field("int64")
	ss.Int(-9223372036854775807)
	ss.Field("uint")
	ss.Uint(42)
	ss.Field("uint8")
	ss.Uint(255)
	ss.Field("uint16")
	ss.Uint(65535)
	ss.Field("uint32")
	ss.Uint(4294967295)
	ss.Field("uint64")
	ss.Uint(18446744073709551615)
	ss.Field("float32")
	ss.WriteFloat32(3.14159)
	ss.Field("float64")
	ss.WriteFloat64(2.7182818284590452353602874713527)
	ss.Field("string")
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
	ss := newStackStream(nil, InitialBufferSize)

	// Test boundary values
	ss.WriteObjectStart()
	ss.Field("int8_min")
	ss.Int(int64(math.MinInt8))
	ss.Field("int8_max")
	ss.Int(int64(math.MaxInt8))
	ss.Field("int16_min")
	ss.Int(int64(math.MinInt16))
	ss.Field("int16_max")
	ss.Int(int64(math.MaxInt16))
	ss.Field("int32_min")
	ss.Int(int64(math.MinInt32))
	ss.Field("int32_max")
	ss.Int(int64(math.MaxInt32))
	ss.Field("int64_min")
	ss.Int(math.MinInt64)
	ss.Field("int64_max")
	ss.Int(math.MaxInt64)
	ss.Field("uint8_max")
	ss.Uint(uint64(math.MaxUint8))
	ss.Field("uint16_max")
	ss.Uint(uint64(math.MaxUint16))
	ss.Field("uint32_max")
	ss.Uint(uint64(math.MaxUint32))
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
	ss := newStackStream(nil, InitialBufferSize)

	// Create a deeply nested structure (50 levels deep)
	const nestingDepth = 50

	// Open nested objects
	for i := range nestingDepth {
		ss.WriteObjectStart()
		ss.Field(fmt.Sprintf("level%d", i))
	}

	// Write a value at the deepest level
	ss.WriteString("deep value")

	// Close all objects
	for range nestingDepth {
		ss.WriteObjectEnd()
	}

	// Verify the structure is complete
	assert.True(t, ss.IsComplete())
	assert.Equal(t, 0, ss.Depth())

	// Verify the JSON is valid by parsing it back
	var result any
	err := jsoniter.Unmarshal(ss.Buffer(), &result)
	assert.NoError(t, err)
}

// TestStackStream_ErrorHandlingWithoutClosing tests error handling and propagation *without* closing pending elements
func TestStackStream_ErrorHandlingWithoutClosing(t *testing.T) {

	// Test with a writer that will fail
	failWriter := &failingWriter{failAfter: 10}

	ss := newStackStream(failWriter, InitialBufferSize)

	// Write enough data to trigger the error
	ss.WriteObjectStart()
	ss.Field("longString")
	ss.WriteString("This string should cause the writer to fail")

	// Flush should propagate the error
	err := ss.Flush()
	assert.Error(t, err)
	assert.Equal(t, "write failed", err.Error())
}

// TestStackStream_ErrorHandlingWithClosing tests error handling and propagation *with* closing pending elements
func TestStackStream_ErrorHandlingWithClosing(t *testing.T) {

	// Test with a writer that will fail
	failWriter := &failingWriter{failAfter: 10}

	ss := newStackStream(failWriter, InitialBufferSize)

	// Write enough data to trigger the error
	ss.WriteObjectStart()
	ss.Field("longString")
	ss.WriteString("This string should cause the writer to fail")
	err := ss.closeAllPendingElements()
	assert.NoError(t, err)

	// Flush should propagate the error
	err = ss.Flush()
	assert.Error(t, err)
	assert.Equal(t, "write failed", err.Error())
}

// TestStackStream_StackManipulationEdgeCases tests edge cases in stack manipulation
func TestStackStream_StackManipulationEdgeCases(t *testing.T) {
	ss := newStackStream(nil, InitialBufferSize)

	// Test 1: Popping from an empty stack should not panic
	ss.pop(ItemObject)
	assert.Equal(t, 0, ss.Depth())

	// Test 2: Popping an item that doesn't match the top of the stack does nothing
	ss.push(ItemArray)
	ss.pop(ItemObject)
	assert.Equal(t, 1, ss.Depth()) // Stack should still have the array

	// Test 3: Multiple pushes and pops
	ss.Reset(nil)
	ss.push(ItemObject)
	ss.push(ItemArray)
	ss.push(ItemField)
	assert.Equal(t, 3, ss.Depth())

	ss.pop(ItemField)
	ss.pop(ItemArray)
	assert.Equal(t, 1, ss.Depth())

	// Test 4: Verify stack state with StackSummary
	summary := ss.StackSummary()
	assert.Contains(t, summary, "Object")
	assert.NotContains(t, summary, "Field")
	assert.NotContains(t, summary, "Array")
}

// TestStackStream_MixedWriteOperations tests mixing different write operations
func TestStackStream_MixedWriteOperations(t *testing.T) {
	ss := newStackStream(nil, InitialBufferSize)

	// Test mixing WriteRaw with other operations
	ss.WriteObjectStart()
	ss.Field("raw")
	ss.WriteRaw("42")
	ss.Field("normal")
	ss.Int(42)
	ss.WriteObjectEnd()

	assert.Equal(t, `{"raw":42,"normal":42}`, string(ss.Buffer()))
	assert.True(t, ss.IsComplete())

	// Test writing already-encoded JSON held as bytes
	ss.Reset(nil)
	ss.WriteArrayStart()
	ss.WriteRawBytes([]byte(`"hello"`))
	ss.WriteRawBytes([]byte(`123`))
	ss.WriteArrayEnd()

	assert.Equal(t, `["hello",123]`, string(ss.Buffer()))
	assert.True(t, ss.IsComplete())

	// Test using WriteRawBytes: it must clear the pending field, or ClosePending
	// appends a placeholder null right after the value just written.
	ss.Reset(nil)
	ss.WriteObjectStart()
	ss.Field("result")
	ss.WriteRawBytes([]byte(`{"gas":21000}`))
	assert.Equal(t, 1, ss.Depth())
	assert.NoError(t, ss.ClosePending(0))

	assert.Equal(t, `{"result":{"gas":21000}}`, string(ss.Buffer()))
	assert.True(t, ss.IsComplete())

	// Same for a pending comma inside an array.
	ss.Reset(nil)
	ss.WriteArrayStart()
	ss.WriteRawBytes([]byte(`1`))
	ss.WriteRawBytes([]byte(`2`))
	assert.Equal(t, 1, ss.Depth())
	assert.NoError(t, ss.ClosePending(0))

	assert.Equal(t, `[1,2]`, string(ss.Buffer()))
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
				ss.Field("field") // Missing value
			},
			expected: `{"field":null}`,
		},
		{
			name: "nested object with missing field in inner object",
			buildStructure: func(ss *StackStream) {
				ss.WriteObjectStart()
				ss.Field("outer")
				ss.WriteObjectStart()
				ss.Field("inner") // Missing value for the inner field
			},
			expected: `{"outer":{"inner":null}}`,
		},
		{
			name: "multiple nested incomplete structures",
			buildStructure: func(ss *StackStream) {
				ss.WriteObjectStart()
				ss.Field("a")
				ss.WriteArrayStart()
				ss.WriteObjectStart()
				ss.Field("b") // Missing value for b
			},
			expected: `{"a":[{"b":null}]}`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ss := newStackStream(nil, InitialBufferSize)
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

	ss := newStackStream(failWriter, InitialBufferSize)

	// Write enough data to trigger the error
	ss.WriteObjectStart()
	ss.Field("longString")
	ss.WriteString("This string should cause the writer to fail")

	// Complete the structure and flush
	err := ss.closeAllPendingElements()
	assert.NoError(t, err)
	err = ss.Flush()
	assert.Error(t, err)

	// Attempt to get buffer as a string, which should propagate the error
	result, err := ss.BufferAsString()
	assert.Error(t, err)
	assert.Equal(t, "", result)
	assert.Equal(t, "write failed", err.Error())
}

// TestStackStream_Depth verifies that Depth() tracks nesting level correctly.
func TestStackStream_Depth(t *testing.T) {
	ss := newStackStream(nil, InitialBufferSize)

	assert.Equal(t, 0, ss.Depth())
	ss.WriteArrayStart()
	assert.Equal(t, 1, ss.Depth())
	ss.WriteObjectStart()
	assert.Equal(t, 2, ss.Depth())
	ss.Field("k")
	assert.Equal(t, 3, ss.Depth()) // ItemField on stack
	ss.WriteString("v")
	assert.Equal(t, 2, ss.Depth()) // ItemField consumed
	ss.WriteObjectEnd()
	assert.Equal(t, 1, ss.Depth())
	ss.WriteArrayEnd()
	assert.Equal(t, 0, ss.Depth())
}

// TestStackStream_ClosePendingPreservesStack verifies the fix: ClosePending(N) closes
// elements above depth N and leaves the first N entries on the stack intact so that
// subsequent writes continue inside the preserved nesting level.
func TestStackStream_ClosePendingPreservesStack(t *testing.T) {
	newSS := func() *StackStream {
		return newStackStream(nil, InitialBufferSize)
	}

	t.Run("nothing_to_close_when_at_target_depth", func(t *testing.T) {
		ss := newSS()
		ss.WriteArrayStart()  // depth 1
		ss.WriteObjectStart() // depth 2
		ss.Field("txHash")
		ss.WriteString("0xabc") // depth back to 2

		assert.Equal(t, 2, ss.Depth())
		err := ss.ClosePending(2) // nothing above depth 2
		assert.NoError(t, err)
		assert.Equal(t, 2, ss.Depth()) // stack preserved

		// Can still write inside the tx object
		ss.Field("error")
		ss.WriteString("oops")
		ss.WriteObjectEnd()
		ss.WriteArrayEnd()

		assert.Equal(t, `[{"txHash":"0xabc","error":"oops"}]`, string(ss.Buffer()))
		assert.Equal(t, 0, ss.Depth())
	})

	t.Run("closes_inner_structures_above_target_depth", func(t *testing.T) {
		ss := newSS()
		ss.WriteArrayStart()  // depth 1
		ss.WriteObjectStart() // depth 2  ← tx object
		txDepth := ss.Depth()

		// Tracer starts writing result: opens a nested object
		ss.Field("result")
		ss.WriteObjectStart() // depth 3  ← partial result
		ss.Field("structLogs")
		ss.WriteArrayStart() // depth 4  ← partial array, left open on error

		assert.Equal(t, 4, ss.Depth())
		err := ss.ClosePending(uint(txDepth)) // close everything above tx object
		assert.NoError(t, err)
		assert.Equal(t, txDepth, ss.Depth()) // back to tx object level

		// The tx object is still open; write the error field and close it
		ss.Field("error")
		ss.WriteString("trace failed")
		ss.WriteObjectEnd()
		ss.WriteArrayEnd()

		assert.Equal(t, 0, ss.Depth())
		// Output must be valid: partial result closed, error inside tx object
		assert.Equal(t,
			`[{"result":{"structLogs":[]},"error":"trace failed"}]`,
			string(ss.Buffer()),
		)
	})

	t.Run("targetDepth_beyond_stack_depth_is_safe", func(t *testing.T) {
		ss := newSS()
		ss.WriteObjectStart() // depth 1
		depth := ss.Depth()
		err := ss.ClosePending(uint(depth) + 10) // targetDepth > stack depth: clamped
		assert.NoError(t, err)
		assert.Equal(t, depth, ss.Depth()) // clamped to actual depth, no panic
	})
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

var errWriterGone = errors.New("client gone")

type goneWriter struct{}

func (goneWriter) Write([]byte) (int, error) { return 0, errWriterGone }

// TestFlushErrorDoesNotBuffer pins that a disconnected client cannot make a
// response accumulate. jsoniter's Flush returns early on a latched error without
// truncating, so ignoring it would restore the unbounded growth this bounds.
func TestFlushErrorDoesNotBuffer(t *testing.T) {
	s := New(goneWriter{}).(*StackStream)

	chunk := strings.Repeat("x", 4096)
	for range 128 * FlushThreshold / len(chunk) {
		s.WriteRaw(chunk)
	}

	require.Less(t, len(s.stream.Buffer()), 2*FlushThreshold,
		"buffer grew to %d after the writer failed", len(s.stream.Buffer()))
	require.Error(t, s.Flush(), "the failure must still be reported")
}

// discardCounter accepts everything and records how much a response produced.
type discardCounter struct{ n int64 }

func (w *discardCounter) Write(p []byte) (int, error) { w.n += int64(len(p)); return len(p), nil }

// TestBufferBoundedForEveryWriter pins the bound on the writers other than
// WriteRaw and WriteString. A response made of numbers (trace gas, pc, depth) or
// of already-encoded bytes has to stream just like a string-heavy one.
func TestBufferBoundedForEveryWriter(t *testing.T) {
	rawValue := []byte(`{"pc":1024,"op":"SSTORE","gas":"0x5208"}`)
	for name, writeValue := range map[string]func(s *StackStream, i int){
		"WriteInt":      func(s *StackStream, i int) { s.Int(int64(i)) },
		"WriteUint64":   func(s *StackStream, i int) { s.Uint(uint64(i)) },
		"WriteRawBytes": func(s *StackStream, i int) { s.WriteRawBytes(rawValue) },
	} {
		t.Run(name, func(t *testing.T) {
			var out discardCounter
			s := New(&out).(*StackStream)
			s.WriteArrayStart()

			peak := 0
			for i := range 200_000 {
				writeValue(s, i)
				if n := len(s.Buffer()); n > peak {
					peak = n
				}
			}
			s.WriteArrayEnd()
			require.NoError(t, s.Flush())

			require.Greater(t, out.n, int64(1<<20), "the response must dwarf the buffer to mean anything")
			require.Less(t, peak, 2*FlushThreshold, "buffer peaked at %d for a %dMB response", peak, out.n>>20)
		})
	}
}

// TestStackStreamEndClosesWhatIsOpen pins the point of the stack tracking: a
// container end repairs whatever the caller left open inside it, so a handler
// that stops early still yields a parseable response.
func TestStackStreamEndClosesWhatIsOpen(t *testing.T) {
	for _, tc := range []struct {
		name  string
		write func(s *StackStream)
		want  string
	}{
		{"field with no value", func(s *StackStream) {
			s.WriteObjectStart()
			s.Field("a")
			s.WriteObjectEnd()
		}, `{"a":null}`},
		{"inner array left open", func(s *StackStream) {
			s.WriteObjectStart()
			s.Field("result")
			s.WriteArrayStart()
			s.Int(1)
			s.WriteObjectEnd()
		}, `{"result":[1]}`},
		{"complete output is untouched", func(s *StackStream) {
			s.WriteObjectStart()
			s.Field("a")
			s.WriteArrayStart()
			s.Int(1)
			s.WriteArrayEnd()
			s.WriteObjectEnd()
		}, `{"a":[1]}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newStackStream(nil, 64)
			tc.write(s)

			require.Equal(t, tc.want, string(s.Buffer()))
			require.NoError(t, json.Unmarshal(s.Buffer(), new(any)))
			require.True(t, s.IsComplete(), "stack left as %s", s.StackSummary())
		})
	}
}

// TestStackStreamResetClearsError pins that a reused stream works again. jsoniter
// latches the error on the stream, so leaving it set makes every later Flush
// fail without draining, and the buffer bound then discards the response.
func TestStackStreamResetClearsError(t *testing.T) {
	s := New(goneWriter{}).(*StackStream)
	s.WriteRaw(strings.Repeat("x", 2*FlushThreshold))
	require.Error(t, s.Flush())

	var out bytes.Buffer
	s.Reset(&out)
	s.WriteString("ok")

	require.NoError(t, s.Flush())
	require.Equal(t, `"ok"`, out.String())
}

// TestLazyFieldStreamWritesFieldFirst pins the wrapper's one invariant: whatever
// value a caller writes first, the field name lands before it and the object
// still parses. A method that slips through unensured puts the value's bytes at
// the enclosing object's level.
func TestLazyFieldStreamWritesFieldFirst(t *testing.T) {
	for name, first := range map[string]func(s Stream){
		"WriteInt":         func(s Stream) { s.Int(1) },
		"WriteString":      func(s Stream) { s.WriteString("a") },
		"WriteNil":         func(s Stream) { s.WriteNil() },
		"WriteRaw":         func(s Stream) { s.WriteRaw("1") },
		"WriteRawBytes":    func(s Stream) { s.WriteRawBytes([]byte("1")) },
		"WriteArrayStart":  func(s Stream) { s.WriteArrayStart() },
		"WriteObjectStart": func(s Stream) { s.WriteObjectStart() },
		"WriteEmptyArray":  func(s Stream) { s.WriteEmptyArray() },
	} {
		t.Run(name, func(t *testing.T) {
			inner := newStackStream(nil, 64)
			inner.WriteObjectStart()
			lazy := NewLazyFieldStream(inner, "result", false)

			first(lazy)

			require.True(t, lazy.Written(), "the field was never opened")
			require.True(t, strings.HasPrefix(string(inner.Buffer()), `{"result":`),
				"buffer starts with %q", string(inner.Buffer()))
			require.NoError(t, inner.ClosePending(0))
			require.NoError(t, json.Unmarshal(inner.Buffer(), new(any)), "produced %q", string(inner.Buffer()))
		})
	}
}

// A field name carries no value, so the wrapper leaves it alone: opening the field
// for one emits `"result":` with nothing able to follow it.
func TestLazyFieldStreamPassesValuelessWrites(t *testing.T) {
	defer func(prev bool) { dbg.AssertEnabled = prev }(dbg.AssertEnabled)
	dbg.AssertEnabled = false
	for name, write := range map[string]func(s Stream){
		"Field": func(s Stream) { s.Field("a") },
	} {
		t.Run(name, func(t *testing.T) {
			inner := newStackStream(nil, 64)
			inner.WriteObjectStart()
			lazy := NewLazyFieldStream(inner, "result", false)

			write(lazy)

			require.False(t, lazy.Written(), "the field was opened for a write with no value")
			require.NotContains(t, string(inner.Buffer()), `"result":`)
		})
	}
}

// Nested wrappers must hand the chained value to the stream that took the field name, not to a
// wrapper still holding a pending field of its own.
func TestLazyFieldStreamNestedChainsValueOntoExplicitField(t *testing.T) {
	defer func(prev bool) { dbg.AssertEnabled = prev }(dbg.AssertEnabled)
	dbg.AssertEnabled = false
	inner := newStackStream(nil, 64)
	inner.WriteObjectStart()
	outer := NewLazyFieldStream(inner, "outer", false)
	nested := NewLazyFieldStream(outer, "inner", false)

	nested.Field("error").WriteString("boom")

	require.False(t, nested.Written(), "a chained value must not open the nested pending field")
	require.False(t, outer.Written(), "a chained value must not open the outer pending field")
	require.Equal(t, `{"error":"boom"`, string(inner.Buffer()))
}

// WriteQuotedText writes its text unscanned, so a byte JSON would escape has to be caught
// where it is produced rather than reaching a client as malformed JSON.
func TestWriteQuotedTextRejectsEscapableText(t *testing.T) {
	defer func(prev bool) { dbg.AssertEnabled = prev }(dbg.AssertEnabled)
	dbg.AssertEnabled = true
	s := newStackStream(nil, 64)

	require.PanicsWithValue(t, `jsonstream: quoted text holds '"', which JSON escapes`, func() {
		s.WriteQuotedText(appenderFunc(`say "hi"`))
	})
	require.NotPanics(t, func() { s.WriteQuotedText(appenderFunc("0xdeadbeef")) })
}

type appenderFunc string

func (a appenderFunc) AppendText(dst []byte) ([]byte, error) { return append(dst, a...), nil }

// Open must reach the stream that owns the buffer, however many wrappers sit above it.
func TestLazyFieldStreamNestedOpenReturnsTheOwner(t *testing.T) {
	defer func(prev bool) { dbg.AssertEnabled = prev }(dbg.AssertEnabled)
	dbg.AssertEnabled = false
	inner := newStackStream(nil, 64)
	inner.WriteObjectStart()
	outer := NewLazyFieldStream(inner, "outer", false)
	nested := NewLazyFieldStream(outer, "inner", false)

	require.Same(t, inner, nested.Open())
	nested.Open().WriteString("v")
	require.Equal(t, `{"inner":"v"`, string(inner.Buffer()))
}

// Put clears the writer as well as the bytes. A pooled stream that kept one
// would pin the connection it came from until the next Get.
func TestPutReleasesWriterAndBytes(t *testing.T) {
	var out bytes.Buffer
	s := Get(&out)
	s.WriteString("pending")
	Put(s)

	require.Empty(t, s.Buffer())
	require.NoError(t, s.Flush())
	require.Empty(t, out.String())
}

// A response above the bound is dropped rather than pooled, so one outsized
// value cannot pin its peak per goroutine.
func TestPutDropsOversizedBuffer(t *testing.T) {
	s := Get(nil)
	s.WriteString(strings.Repeat("x", maxPooledBufferSize))
	require.Greater(t, cap(s.Buffer()), maxPooledBufferSize)

	Put(s)
	require.NotEmpty(t, s.Buffer(), "an oversized stream is dropped, not reset and pooled")
	require.NotSame(t, s, Get(nil))
}

// WriteHex matches json.Marshal of hexutil.Bytes inside a container, whether the value
// stays buffered or is written through.
func TestWriteHex(t *testing.T) {
	t.Parallel()
	for name, b := range map[string][]byte{
		"nil":             nil,
		"empty":           {},
		"one":             {0xab},
		"below-threshold": bytes.Repeat([]byte{0x5a}, FlushThreshold/2-3),
		"at-threshold":    bytes.Repeat([]byte{0x5a}, FlushThreshold/2-2),
		"far-above":       bytes.Repeat([]byte{0x5a}, 4*FlushThreshold),
	} {
		want, err := json.Marshal(hexutil.Bytes(b))
		require.NoError(t, err)
		for _, out := range []io.Writer{new(bytes.Buffer), nil} {
			t.Run(fmt.Sprintf("%s/writer=%t", name, out != nil), func(t *testing.T) {
				s := New(out)
				s.WriteObjectStart()
				s.Field("result")
				s.WriteArrayStart()
				s.WriteHex(b)
				s.WriteHex(b)
				s.WriteArrayEnd()
				s.WriteObjectEnd()
				require.NoError(t, s.Flush())

				got := s.Buffer()
				if b, ok := out.(*bytes.Buffer); ok {
					got = b.Bytes()
				}
				require.Equal(t, `{"result":[`+string(want)+`,`+string(want)+`]}`, string(got))
			})
		}
	}
}

// A raw payload at or above FlushThreshold goes to the writer instead of being
// copied into the buffer. Both branches emit the same bytes, so the buffer is the
// only thing that shows which one ran.
func TestWriteRawBytesLargePayloadWritesThrough(t *testing.T) {
	t.Parallel()
	// Sizes are pre-quoting; two quote bytes are added, so these land the quoted
	// length exactly on FlushThreshold and exactly one byte below it.
	for name, tc := range map[string]struct {
		size          int
		writesThrough bool
	}{
		"below-threshold": {FlushThreshold - 3, false},
		"at-threshold":    {FlushThreshold - 2, true},
		"far-above":       {32 * FlushThreshold, true},
	} {
		t.Run(name, func(t *testing.T) {
			payload := append([]byte(`"`), bytes.Repeat([]byte("a"), tc.size)...)
			payload = append(payload, '"')

			var out bytes.Buffer
			s := New(&out)
			s.WriteObjectStart()
			s.Field("result")
			s.WriteRawBytes(payload)
			s.WriteObjectEnd()
			require.NoError(t, s.Flush())

			require.Equal(t, `{"result":`+string(payload)+`}`, out.String())

			if tc.writesThrough {
				require.Less(t, cap(s.(*StackStream).Buffer()), FlushThreshold,
					"payload must reach the writer without being copied into the buffer")
			} else {
				require.GreaterOrEqual(t, cap(s.(*StackStream).Buffer()), FlushThreshold,
					"a payload below the threshold must still be buffered")
			}
		})
	}
}

// With no writer the caller reads the response back out of Buffer, so everything
// must still be buffered no matter how large.
func TestWriteRawBytesNilWriterAlwaysBuffers(t *testing.T) {
	t.Parallel()
	payload := append([]byte(`"`), bytes.Repeat([]byte("a"), 4*FlushThreshold)...)
	payload = append(payload, '"')

	s := New(nil)
	s.WriteObjectStart()
	s.Field("result")
	s.WriteRawBytes(payload)
	s.WriteObjectEnd()

	require.Equal(t, `{"result":`+string(payload)+`}`, string(s.Buffer()))
}

// The mirror of TestPutDropsOversizedBuffer: a large result no longer grows the
// buffer, so the stream survives Put instead of being dropped by the size check.
func TestPutKeepsStreamAfterLargeWriteThrough(t *testing.T) {
	var out bytes.Buffer
	s := Get(&out)
	s.WriteObjectStart()
	s.Field("result")
	s.WriteRawBytes(append(bytes.Repeat([]byte(`"a`), 2<<20), '"'))
	s.WriteObjectEnd()
	require.NoError(t, s.Flush())

	require.LessOrEqual(t, cap(s.Buffer()), maxPooledBufferSize,
		"a written-through result must not grow the buffer past the pool limit")

	Put(s)
	// sync.Pool may drop an admitted stream at any time, so admission is observed
	// through the reset Put does on the way in, not through what Get hands back.
	require.Empty(t, s.Buffer(), "an admitted stream is reset by Put")
	require.Nil(t, s.out, "an admitted stream pins no writer")
}

// The write-through path must surface a writer failure the same way the buffered
// path does, and must not let the failed bytes accumulate.
func TestWriteRawBytesWriteThroughError(t *testing.T) {
	t.Parallel()
	payload := append([]byte(`"`), bytes.Repeat([]byte("a"), 4*FlushThreshold)...)
	payload = append(payload, '"')

	for name, out := range map[string]io.Writer{
		// Fails on the prefix flush, before the payload is handed over.
		"prefix-flush": goneWriter{},
		// Takes the prefix, then fails on the direct write of the payload.
		"direct-write": &failingWriter{failAfter: len(payload) - 1},
	} {
		t.Run(name, func(t *testing.T) {
			s := New(out).(*StackStream)
			s.WriteObjectStart()
			s.Field("result")
			s.WriteRawBytes(payload)
			s.WriteObjectEnd()

			require.Error(t, s.Flush(), "the writer failure must reach the caller")
			require.Less(t, len(s.Buffer()), FlushThreshold,
				"a failed write must not accumulate, buffer holds %d", len(s.Buffer()))
		})
	}
}

// TestStackStream_SeparatorsAreAutomatic pins the contract: the stream writes the comma a
// value needs.
func TestStackStream_SeparatorsAreAutomatic(t *testing.T) {
	for _, tc := range []struct {
		name  string
		write func(*StackStream)
		want  string
	}{
		{"array elements", func(s *StackStream) {
			s.WriteArrayStart()
			s.Int(1)
			s.Int(2)
			s.Int(3)
			s.WriteArrayEnd()
		}, `[1,2,3]`},
		{"object fields", func(s *StackStream) {
			s.WriteObjectStart()
			s.Field("a")
			s.Int(1)
			s.Field("b")
			s.WriteString("x")
			s.WriteObjectEnd()
		}, `{"a":1,"b":"x"}`},
		{"nested containers", func(s *StackStream) {
			s.WriteObjectStart()
			s.Field("list")
			s.WriteArrayStart()
			s.WriteObjectStart()
			s.Field("k")
			s.Int(7)
			s.WriteObjectEnd()
			s.WriteObjectStart()
			s.WriteObjectEnd()
			s.WriteArrayEnd()
			s.Field("after")
			s.WriteBool(true)
			s.WriteObjectEnd()
		}, `{"list":[{"k":7},{}],"after":true}`},
		{"empty containers", func(s *StackStream) {
			s.WriteObjectStart()
			s.Field("o")
			s.WriteObjectStart()
			s.WriteObjectEnd()
			s.Field("a")
			s.WriteArrayStart()
			s.WriteArrayEnd()
			s.WriteObjectEnd()
		}, `{"o":{},"a":[]}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ss := newStackStream(nil, InitialBufferSize)
			tc.write(ss)
			require.Equal(t, tc.want, string(ss.Buffer()))
			require.True(t, json.Valid(ss.Buffer()))
			require.True(t, ss.IsComplete())
		})
	}
}

type failingAppender struct{}

func (failingAppender) AppendText(dst []byte) ([]byte, error) {
	return nil, errors.New("append failed")
}

// A failing appender must not truncate what the stream already holds.
func TestWriteQuotedTextKeepsBufferOnError(t *testing.T) {
	t.Parallel()
	var out bytes.Buffer
	s := New(&out)
	s.WriteObjectStart()
	s.Field("balance")
	s.WriteQuotedText(failingAppender{})
	s.WriteObjectEnd()
	require.Equal(t, `{"balance":""}`, string(s.Buffer()))
	require.Error(t, s.Flush())
}

// A latched write error must reach the caller. Flush cannot report it on a writerless stream,
// so marshalFastJSONTo would otherwise clone a buffer holding the empty-string placeholder.
func TestStackStreamErrSurvivesWriterlessFlush(t *testing.T) {
	s := Get(nil)
	defer Put(s)
	s.WriteQuotedText(failingAppender{})

	require.NoError(t, s.Flush(), "jsoniter reports nil for a stream with no writer")
	require.Error(t, s.Err(), "the latched appender error must stay reachable")
}

// Closing to the root ends the last value's container, so the next top-level value is not a
// member of anything and takes no separator.
func TestClosePendingToRootClearsSeparator(t *testing.T) {
	s := newStackStream(nil, InitialBufferSize)
	s.WriteArrayStart()
	s.Int(1)
	require.NoError(t, s.ClosePending(0))
	s.Int(2)

	require.Equal(t, `[1]2`, string(s.Buffer()))
}

// A field name or separator written before the lazy field opened would put its value in the
// enclosing object, silently dropping the field. Asserts catch a marshaller that starts with
// Stream.Field instead of a value write.
func TestLazyFieldStreamAssertsFieldBeforeValue(t *testing.T) {
	defer func(prev bool) { dbg.AssertEnabled = prev }(dbg.AssertEnabled)
	dbg.AssertEnabled = true
	for name, write := range map[string]func(s Stream){
		"Field": func(s Stream) { s.Field("a") },
	} {
		t.Run(name, func(t *testing.T) {
			inner := newStackStream(nil, 64)
			inner.WriteObjectStart()
			lazy := NewLazyFieldStream(inner, "result", false)

			require.Panics(t, func() { write(lazy) })

			lazy.WriteObjectStart()
			require.NotPanics(t, func() { write(lazy) })
		})
	}
}

// A nil slice is the caller's to write as null: WriteHexBytes always writes an array.
func TestWriteHexBytes(t *testing.T) {
	for name, tc := range map[string]struct {
		items [][]byte
		want  string
	}{
		"nil":           {nil, `[]`},
		"empty":         {[][]byte{}, `[]`},
		"empty element": {[][]byte{{}}, `["0x"]`},
		"multi":         {[][]byte{{0x01}, {0xab, 0xcd}, nil}, `["0x01","0xabcd","0x"]`},
	} {
		t.Run(name, func(t *testing.T) {
			s := Get(nil)
			defer Put(s)
			WriteHexBytes(s, tc.items)
			require.NoError(t, s.Err())
			require.Equal(t, tc.want, string(s.Buffer()))
		})
	}
}
