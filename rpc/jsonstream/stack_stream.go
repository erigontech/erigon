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
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"strings"

	jsoniter "github.com/json-iterator/go"
)

// InitialStackSize is the initial capacity of the stack
const InitialStackSize = 16

// stackItem represents the type of item on the stack
type stackItem int8

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
	// out is the stream's own writer, kept because jsoniter does not expose it.
	// Nil means the caller reads the response back out of Buffer instead.
	out io.Writer
}

// newStackStream creates a new StackStream writing to out. Building the
// jsoniter.Stream here rather than taking one is what pins jsoniter's
// IndentionStep at zero.
func newStackStream(out io.Writer, bufSize int) *StackStream {
	return &StackStream{
		stream: jsoniter.NewStream(jsoniter.ConfigDefault, out, bufSize),
		stack:  make([]stackItem, 0, InitialStackSize),
		out:    out,
	}
}

// Buffer returns the underlying jsoniter.Stream's buffer
func (s *StackStream) Buffer() []byte {
	return s.stream.Buffer()
}

// Reset resets the underlying jsoniter.Stream and clears the stack
func (s *StackStream) Reset(out io.Writer) {
	s.stream.Reset(out)
	s.out = out
	// jsoniter latches the error on the stream, so a reused one would fail every
	// later Flush without draining.
	s.stream.Error = nil
	s.stack = s.stack[:0]
}

// WriteRawBytes writes already-encoded JSON held as bytes. A payload at or above
// FlushThreshold goes straight to the writer. Such a response commits the HTTP
// status either way, since flushIfFull drains the buffer the moment this returns.
func (s *StackStream) WriteRawBytes(content []byte) {
	if s.out != nil && len(content) >= FlushThreshold {
		s.writeThrough(content)
		s.popCommaOrField()
		return
	}
	s.stream.SetBuffer(append(s.stream.Buffer(), content...))
	s.popCommaOrField()
}

// writeThrough drains what is buffered and hands content to the writer. The
// empty-buffer check only skips a pointless zero-length Write; content is large
// by the time we get here, so it is written either way.
func (s *StackStream) writeThrough(content []byte) {
	if len(s.stream.Buffer()) > 0 && s.stream.Flush() != nil {
		// Same as flushIfFull: jsoniter latches the error, so these bytes can never
		// reach the client and holding them only pins memory.
		s.stream.SetBuffer(s.stream.Buffer()[:0])
		return
	}
	if s.stream.Error != nil {
		return
	}
	if _, err := s.out.Write(content); err != nil {
		s.stream.Error = err
	}
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
	writeStringFast(s.stream, val)
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
	s.closeInside(ItemObject)
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
	s.closeInside(ItemArray)
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
	writeObjectFieldFast(s.stream, fieldName)
	s.pop(ItemComma)
	s.push(ItemField)
}

// Flush flushes the underlying stream
func (s *StackStream) Flush() error {
	return s.stream.Flush()
}

// BufferAsString returns the content as a string after flushing any incomplete structures
func (s *StackStream) BufferAsString() (string, error) {
	err := s.ClosePending(0)
	if err != nil {
		return "", err
	}
	return string(s.stream.Buffer()), nil
}

// WriteEmptyArray writes an empty array into the underlying stream
func (s *StackStream) WriteEmptyArray() {
	s.stream.WriteEmptyArray()
	s.popCommaOrField()
}

// WriteEmptyObject writes an empty object into the underlying stream
func (s *StackStream) WriteEmptyObject() {
	s.stream.WriteEmptyObject()
	s.popCommaOrField()
}

// IsComplete checks if the JSON structure is currently complete without open elements
func (s *StackStream) IsComplete() bool {
	return len(s.stack) == 0
}

// StackSummary returns a summary of the current stack state for debugging
func (s *StackStream) StackSummary() string {
	if len(s.stack) == 0 {
		return "Empty"
	}

	var result strings.Builder
	for i, item := range s.stack {
		switch item {
		case ItemObject:
			result.WriteString(fmt.Sprintf("[%d] Object\n", i))
		case ItemArray:
			result.WriteString(fmt.Sprintf("[%d] Array\n", i))
		case ItemField:
			result.WriteString(fmt.Sprintf("[%d] Field\n", i))
		case ItemComma:
			result.WriteString(fmt.Sprintf("[%d] Comma\n", i))
		}
	}
	return result.String()
}

// ClosePending closes all open JSON structures above targetDepth, leaving the first targetDepth
// stack entries intact so subsequent writes continue inside that nesting level.
func (s *StackStream) ClosePending(targetDepth uint) error {
	stackLen := len(s.stack)
	if stackLen == 0 {
		return s.stream.Error
	}
	if targetDepth > uint(stackLen) {
		targetDepth = uint(stackLen)
	}

	for i := stackLen - 1; i >= int(targetDepth); i-- {
		switch s.stack[i] {
		case ItemField:
			s.stream.WriteNil()
		case ItemComma:
			if i > 0 && s.stack[i-1] == ItemObject {
				// a trailing comma inside an object needs a placeholder field to stay valid
				writeObjectFieldFast(s.stream, "")
				writeStringFast(s.stream, "")
			} else {
				s.stream.WriteNil()
			}
		case ItemArray:
			s.stream.WriteArrayEnd()
		case ItemObject:
			s.stream.WriteObjectEnd()
		}
	}

	s.stack = s.stack[:targetDepth]
	return s.stream.Error
}

func (s *StackStream) Depth() int { return len(s.stack) }

// push adds an item to the stack
func (s *StackStream) push(item stackItem) {
	s.stack = append(s.stack, item)
}

// closeInside completes whatever the caller left open inside the innermost
// container of this kind, so ending it yields valid JSON rather than a dangling
// comma or field. It does nothing once that container is already the top.
func (s *StackStream) closeInside(kind stackItem) {
	for i, item := range slices.Backward(s.stack) {
		if item == kind {
			_ = s.ClosePending(uint(i + 1))
			return
		}
	}
}

// pop removes the specified item from the top of the stack, if present
// @param item the item to pop from the stack
func (s *StackStream) pop(item stackItem) {
	if len(s.stack) > 0 && s.stack[len(s.stack)-1] == item {
		s.stack = s.stack[:len(s.stack)-1]
	}
}

// popCommaOrField pops ItemComma or ItemField after a value was written, and
// hands the buffer over if that value filled it. Every writer goes through here,
// so the bound holds for numbers and raw bytes as much as for strings.
func (s *StackStream) popCommaOrField() {
	if len(s.stack) > 0 {
		top := s.stack[len(s.stack)-1]
		if top == ItemComma || top == ItemField {
			s.stack = s.stack[:len(s.stack)-1]
		}
	}
	flushIfFull(s.stream)
}

// streamBuf appends to the stream's own buffer, so encoding/json writes into the
// pooled buffer instead of returning a freshly allocated one.
type streamBuf struct{ s *StackStream }

func (w streamBuf) Write(p []byte) (int, error) {
	w.s.stream.SetBuffer(append(w.s.stream.Buffer(), p...))
	return len(p), nil
}

// WriteJSONValue encodes v into the stream. Output matches json.Marshal(v) — the
// same encoder, the same HTML escaping — but the bytes land straight in the
// stream's buffer rather than in a per-response allocation.
func (s *StackStream) WriteJSONValue(v any) error {
	start := len(s.stream.Buffer())
	if err := json.NewEncoder(streamBuf{s}).Encode(v); err != nil {
		s.stream.SetBuffer(s.stream.Buffer()[:start]) // leave no partial value behind
		return err
	}
	b := s.stream.Buffer()
	s.stream.SetBuffer(b[:len(b)-1]) // Encode terminates with a newline; the envelope must not
	s.popCommaOrField()
	return nil
}

// Mark records a position the stream can be rewound to. Encoding a value can
// fail after an enclosing object is already open, and the only correct recovery
// is to discard the partial message and write a different one.
func (s *StackStream) Mark() (buffered, depth int) {
	return len(s.stream.Buffer()), len(s.stack)
}

// Rewind discards everything written since the matching Mark. It is only valid
// while those bytes are still buffered; a caller that may have crossed
// FlushThreshold in between cannot rely on it.
func (s *StackStream) Rewind(buffered, depth int) {
	s.stream.SetBuffer(s.stream.Buffer()[:buffered])
	s.stack = s.stack[:depth]
}
