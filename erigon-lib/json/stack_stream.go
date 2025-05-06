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
	"fmt"
	"io"

	jsoniter "github.com/json-iterator/go"
)

// InitialStackSize is the initial capacity of the stack
// Most JSON documents don't have deep nesting, so 16 is a reasonable default
const InitialStackSize = 16

// stackItem represents the type of item on the stack
type stackItem int

const (
	ItemObject stackItem = iota
	ItemArray
	ItemField
	ItemComma
)

// StackStream wraps jsoniter.Stream with a stack to track unclosed JSON elements
// It implements the Stream interface
type StackStream struct {
	stream *jsoniter.Stream
	stack  []stackItem
}

// NewStackStream creates a new StackStream with the given jsoniter.Stream
// The stack is pre-allocated with a capacity of InitialStackSize
func NewStackStream(stream *jsoniter.Stream) *StackStream {
	return &StackStream{
		stream: stream,
		stack:  make([]stackItem, 0, InitialStackSize),
	}
}

// Buffer returns the underlying jsoniter.Stream's buffer
func (s *StackStream) Buffer() []byte {
	return s.stream.Buffer()
}

// Reset resets the underlying jsoniter.Stream and clears the stack
func (s *StackStream) Reset(out io.Writer) {
	s.stream.Reset(out)
	s.stream.Error = nil
	s.stack = s.stack[:0]
}

// Write raw bytes to the stream
func (s *StackStream) Write(content []byte) (int, error) {
	s.popCommaOrField()
	return s.stream.Write(content)
}

// WriteRaw writes raw content to the stream
func (s *StackStream) WriteRaw(content string) {
	s.stream.WriteRaw(content)
	s.popCommaOrField()
}

// WriteNil writes a null value to the stream
func (s *StackStream) WriteNil() {
	s.stream.WriteNil()
	s.popCommaOrField()
}

// WriteTrue writes a true value to the stream
func (s *StackStream) WriteTrue() {
	s.stream.WriteTrue()
	s.popCommaOrField()
}

// WriteFalse writes a false value to the stream
func (s *StackStream) WriteFalse() {
	s.stream.WriteFalse()
	s.popCommaOrField()
}

// WriteBool writes a boolean value to the stream
func (s *StackStream) WriteBool(val bool) {
	s.stream.WriteBool(val)
	s.popCommaOrField()
}

// WriteInt writes an int value to the stream
func (s *StackStream) WriteInt(val int) {
	s.stream.WriteInt(val)
	s.popCommaOrField()
}

// WriteInt8 writes an int8 value to the stream
func (s *StackStream) WriteInt8(val int8) {
	s.stream.WriteInt8(val)
	s.popCommaOrField()
}

// WriteInt16 writes an int16 value to the stream
func (s *StackStream) WriteInt16(val int16) {
	s.stream.WriteInt16(val)
	s.popCommaOrField()
}

// WriteInt32 writes an int32 value to the stream
func (s *StackStream) WriteInt32(val int32) {
	s.stream.WriteInt32(val)
	s.popCommaOrField()
}

// WriteInt64 writes an int64 value to the stream
func (s *StackStream) WriteInt64(val int64) {
	s.stream.WriteInt64(val)
	s.popCommaOrField()
}

// WriteUint writes an uint value to the stream
func (s *StackStream) WriteUint(val uint) {
	s.stream.WriteUint(val)
	s.popCommaOrField()
}

// WriteUint8 writes an uint8 value to the stream
func (s *StackStream) WriteUint8(val uint8) {
	s.stream.WriteUint8(val)
	s.popCommaOrField()
}

// WriteUint16 writes an uint16 value to the stream
func (s *StackStream) WriteUint16(val uint16) {
	s.stream.WriteUint16(val)
	s.popCommaOrField()
}

// WriteUint32 writes an uint32 value to the stream
func (s *StackStream) WriteUint32(val uint32) {
	s.stream.WriteUint32(val)
	s.popCommaOrField()
}

// WriteUint64 writes an uint64 value to the stream
func (s *StackStream) WriteUint64(val uint64) {
	s.stream.WriteUint64(val)
	s.popCommaOrField()
}

// WriteFloat32 writes a float32 value to the stream
func (s *StackStream) WriteFloat32(val float32) {
	s.stream.WriteFloat32(val)
	s.popCommaOrField()
}

// WriteFloat64 writes a float64 value to the stream
func (s *StackStream) WriteFloat64(val float64) {
	s.stream.WriteFloat64(val)
	s.popCommaOrField()
}

// WriteString writes a string value to the stream
func (s *StackStream) WriteString(val string) {
	s.stream.WriteString(val)
	s.popCommaOrField()
}

// WriteObjectStart writes the start of an object and adds it to the stack
func (s *StackStream) WriteObjectStart() {
	s.stream.WriteObjectStart()
	s.popCommaOrField()
	s.push(ItemObject)
}

// WriteObjectEnd writes the end of an object and removes it from the stack
func (s *StackStream) WriteObjectEnd() {
	s.stream.WriteObjectEnd()
	s.pop(ItemObject)
}

// WriteArrayStart writes the start of an array and adds it to the stack
func (s *StackStream) WriteArrayStart() {
	s.stream.WriteArrayStart()
	s.popCommaOrField()
	s.push(ItemArray)
}

// WriteArrayEnd writes the end of an array and removes it from the stack
func (s *StackStream) WriteArrayEnd() {
	s.stream.WriteArrayEnd()
	s.pop(ItemArray)
}

// WriteMore writes a comma for arrays and objects
func (s *StackStream) WriteMore() {
	s.stream.WriteMore()
	s.push(ItemComma)
}

// WriteObjectField writes a field name for an object and adds it to the stack
func (s *StackStream) WriteObjectField(fieldName string) {
	s.stream.WriteObjectField(fieldName)
	s.pop(ItemComma)
	s.push(ItemField)
}

// Flush flushes the underlying stream
func (s *StackStream) Flush() error {
	err := s.closePendingObjects()
	if err != nil {
		return err
	}
	return s.stream.Flush()
}

// Error returns any error from the underlying stream
func (s *StackStream) Error() error {
	return s.stream.Error
}

// BufferAsString returns the content as a string after flushing any incomplete structures
func (s *StackStream) BufferAsString() (string, error) {
	err := s.closePendingObjects()
	if err != nil {
		return "", err
	}
	return string(s.stream.Buffer()), nil
}

// WriteEmptyArray writes the end of an array
func (s *StackStream) WriteEmptyArray() {
	s.stream.WriteEmptyArray()
}

// Size returns the size of the buffer
func (s *StackStream) Size() int {
	return len(s.stream.Buffer())
}

// IsComplete checks if the JSON structure is currently complete without open elements
func (s *StackStream) IsComplete() bool {
	return len(s.stack) == 0
}

// GetCurrentDepth returns the current nesting depth
func (s *StackStream) GetCurrentDepth() int {
	return len(s.stack)
}

// GetStackSummary returns a summary of the current stack state for debugging
func (s *StackStream) GetStackSummary() string {
	if len(s.stack) == 0 {
		return "Empty"
	}

	result := ""
	for i, item := range s.stack {
		switch item {
		case ItemObject:
			result += fmt.Sprintf("[%d] Object\n", i)
		case ItemArray:
			result += fmt.Sprintf("[%d] Array\n", i)
		case ItemField:
			result += fmt.Sprintf("[%d] Field\n", i)
		case ItemComma:
			result += fmt.Sprintf("[%d] Comma (expecting value)\n", i)
		}
	}
	return result
}

// closePendingObjects properly closes all pending JSON constructs on the stack
// to ensure valid JSON, even when an error occurs during writing
// This method is optimized for performance
func (s *StackStream) closePendingObjects() error {
	stackLen := len(s.stack)
	if stackLen == 0 {
		return s.stream.Error
	}

	// Process the stack in reverse order
	for i := stackLen - 1; i >= 0; i-- {
		item := s.stack[i]

		// Unrolled switch for better performance
		if item == ItemField || item == ItemComma {
			// Fields and commas are handled the same way - write null
			s.stream.WriteNil()
		} else if item == ItemArray {
			// Close array
			s.stream.WriteArrayEnd()
		} else if item == ItemObject {
			// Close object
			s.stream.WriteObjectEnd()
		}
	}

	// Clear the stack - more efficient than creating a new one
	s.stack = s.stack[:0]

	// Return any error from the underlying stream
	return s.stream.Error
}

// push adds an item to the stack
func (s *StackStream) push(item stackItem) {
	s.stack = append(s.stack, item)
}

// pop removes the top item from the stack
func (s *StackStream) pop(item stackItem) {
	if len(s.stack) > 0 && s.stack[len(s.stack)-1] == item {
		s.stack = s.stack[:len(s.stack)-1]
	}
}

// popCommaOrField is a helper method for the common case of popping ItemComma or ItemField from the stack
func (s *StackStream) popCommaOrField() {
	if len(s.stack) > 0 {
		top := s.stack[len(s.stack)-1]
		if top == ItemComma || top == ItemField {
			s.stack = s.stack[:len(s.stack)-1]
		}
	}
}
