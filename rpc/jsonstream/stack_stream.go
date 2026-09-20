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
	"encoding"
	"fmt"
	"github.com/erigontech/erigon/rpc/jsonstream/jsonw"
	"io"
	"slices"
	"strings"

	jsoniter "github.com/json-iterator/go"

	"github.com/erigontech/erigon/common/hexutil"
)

// InitialStackSize is the initial capacity of the stack
const InitialStackSize = 16

// stackItem represents the type of item on the stack
type stackItem int8

const (
	ItemObject stackItem = iota
	ItemArray
	ItemField
)

// StackStream wraps jsoniter.Stream with a stack to track unclosed JSON elements
// It implements the Stream interface
type StackStream struct {
	separatorPending bool
	stream           *jsoniter.Stream
	stack            []stackItem
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
	s.separatorPending = false
}

// WriteRawBytes writes already-encoded JSON held as bytes. A payload at or above
// FlushThreshold goes straight to the writer. Such a response commits the HTTP
// status either way, since flushIfFull drains the buffer the moment this returns.
func (s *StackStream) WriteRawBytes(content []byte) {
	s.beforeValue()
	if s.out != nil && len(content) >= FlushThreshold {
		s.writeThrough(content)
		s.afterValue()
		return
	}
	s.stream.SetBuffer(append(s.stream.Buffer(), content...))
	s.afterValue()
}

// AppendJSON lets a value write itself straight into the buffer, so the whole value costs
// one call rather than one per field.
func (s *StackStream) AppendJSON(v jsonw.JSONAppender) {
	s.beforeValue()
	s.stream.SetBuffer(v.AppendJSON(s.stream.Buffer()))
	s.afterValue()
}

func (s *StackStream) WriteHex(b []byte) {
	s.beforeValue()
	buf := s.stream.Buffer()
	start := len(buf)
	buf = hexutil.AppendQuoted(slices.Grow(buf, hexutil.QuotedLen(len(b))), b)
	if s.out != nil && len(buf)-start >= FlushThreshold {
		s.stream.SetBuffer(buf[:start])
		s.writeThrough(buf[start:])
	} else {
		s.stream.SetBuffer(buf)
	}
	s.afterValue()
}

// WriteQuotedText writes v.AppendText's output as a JSON string, without an escape scan: it is
// for hex quantities, which never need escaping.
func (s *StackStream) WriteQuotedText(v encoding.TextAppender) {
	s.beforeValue()
	buf, err := v.AppendText(append(s.stream.Buffer(), '"'))
	if err != nil {
		// An empty string keeps the JSON well-formed; the latched error stops it reaching the client.
		buf = append(s.stream.Buffer(), '"')
		if s.stream.Error == nil {
			s.stream.Error = err
		}
	}
	s.stream.SetBuffer(append(buf, '"'))
	s.afterValue()
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
	s.beforeValue()
	s.stream.WriteRaw(content)
	s.afterValue()
}

// WriteNil writes a null value to the stream
func (s *StackStream) WriteNil() {
	s.beforeValue()
	s.stream.WriteNil()
	s.afterValue()
}

// WriteTrue writes a true value to the stream
func (s *StackStream) WriteTrue() {
	s.beforeValue()
	s.stream.WriteTrue()
	s.afterValue()
}

// WriteFalse writes a false value to the stream
func (s *StackStream) WriteFalse() {
	s.beforeValue()
	s.stream.WriteFalse()
	s.afterValue()
}

// WriteBool writes a boolean value to the stream
func (s *StackStream) WriteBool(val bool) {
	s.beforeValue()
	s.stream.WriteBool(val)
	s.afterValue()
}

// WriteInt writes an int value to the stream
func (s *StackStream) WriteInt(val int) {
	s.beforeValue()
	s.stream.WriteInt(val)
	s.afterValue()
}

// WriteInt8 writes an int8 value to the stream
func (s *StackStream) WriteInt8(val int8) {
	s.beforeValue()
	s.stream.WriteInt8(val)
	s.afterValue()
}

// WriteInt16 writes an int16 value to the stream
func (s *StackStream) WriteInt16(val int16) {
	s.beforeValue()
	s.stream.WriteInt16(val)
	s.afterValue()
}

// WriteInt32 writes an int32 value to the stream
func (s *StackStream) WriteInt32(val int32) {
	s.beforeValue()
	s.stream.WriteInt32(val)
	s.afterValue()
}

// WriteInt64 writes an int64 value to the stream
func (s *StackStream) WriteInt64(val int64) {
	s.beforeValue()
	s.stream.WriteInt64(val)
	s.afterValue()
}

// WriteUint writes an uint value to the stream
func (s *StackStream) WriteUint(val uint) {
	s.beforeValue()
	s.stream.WriteUint(val)
	s.afterValue()
}

// WriteUint8 writes an uint8 value to the stream
func (s *StackStream) WriteUint8(val uint8) {
	s.beforeValue()
	s.stream.WriteUint8(val)
	s.afterValue()
}

// WriteUint16 writes an uint16 value to the stream
func (s *StackStream) WriteUint16(val uint16) {
	s.beforeValue()
	s.stream.WriteUint16(val)
	s.afterValue()
}

// WriteUint32 writes an uint32 value to the stream
func (s *StackStream) WriteUint32(val uint32) {
	s.beforeValue()
	s.stream.WriteUint32(val)
	s.afterValue()
}

// WriteUint64 writes an uint64 value to the stream
func (s *StackStream) WriteUint64(val uint64) {
	s.beforeValue()
	s.stream.WriteUint64(val)
	s.afterValue()
}

// WriteFloat32 writes a float32 value to the stream
func (s *StackStream) WriteFloat32(val float32) {
	s.beforeValue()
	s.stream.WriteFloat32(val)
	s.afterValue()
}

// WriteFloat64 writes a float64 value to the stream
func (s *StackStream) WriteFloat64(val float64) {
	s.beforeValue()
	s.stream.WriteFloat64(val)
	s.afterValue()
}

// WriteString writes a string value to the stream
func (s *StackStream) WriteString(val string) {
	s.beforeValue()
	writeStringFast(s.stream, val)
	s.afterValue()
}

// WriteObjectStart writes the start of an object and adds it to the stack
func (s *StackStream) WriteObjectStart() {
	s.beforeValue()
	s.stream.WriteObjectStart()
	s.consumeField()
	s.push(ItemObject)
}

// WriteObjectEnd writes the end of an object and removes it from the stack
func (s *StackStream) WriteObjectEnd() {
	s.closeInside(ItemObject)
	s.stream.WriteObjectEnd()
	s.pop(ItemObject)
	s.afterValue()
}

// WriteArrayStart writes the start of an array and adds it to the stack
func (s *StackStream) WriteArrayStart() {
	s.beforeValue()
	s.stream.WriteArrayStart()
	s.consumeField()
	s.push(ItemArray)
}

// WriteArrayEnd writes the end of an array and removes it from the stack
func (s *StackStream) WriteArrayEnd() {
	s.closeInside(ItemArray)
	s.stream.WriteArrayEnd()
	s.pop(ItemArray)
	s.afterValue()
}

// WriteMore is a no-op: the stream emits the separator each value needs. It stays so a
// caller written against the manual API still produces valid JSON.
func (s *StackStream) WriteMore() {}

// WriteObjectField writes a field name for an object and adds it to the stack
func (s *StackStream) WriteObjectField(fieldName string) jsonw.JSONWriter {
	s.beforeValue()
	writeObjectFieldFast(s.stream, fieldName)
	s.separatorPending = false
	s.push(ItemField)
	return s
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
	s.beforeValue()
	s.stream.WriteEmptyArray()
	s.afterValue()
}

// WriteEmptyObject writes an empty object into the underlying stream
func (s *StackStream) WriteEmptyObject() {
	s.beforeValue()
	s.stream.WriteEmptyObject()
	s.afterValue()
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
		case ItemArray:
			s.stream.WriteArrayEnd()
		case ItemObject:
			s.stream.WriteObjectEnd()
		}
	}

	s.stack = s.stack[:targetDepth]
	// What was closed is a finished value in the surviving container, and these writes
	// bypass afterValue, so record it here.
	if targetDepth < uint(stackLen) && targetDepth > 0 {
		s.separatorPending = true
	}
	return s.stream.Error
}

// Err reports a write error the stream latched. Flush cannot stand in for it on a stream with no
// writer: jsoniter returns nil for that case before it looks at the latched error.
func (s *StackStream) Err() error { return s.stream.Error }

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

// beforeValue writes the separator the next member needs.
func (s *StackStream) beforeValue() {
	if s.separatorPending {
		s.stream.WriteMore()
		s.separatorPending = false
	}
}

// markSeparatorPending states that a sibling value precedes what is written next. It is for
// a fragment written into a container this stream did not open, where the stack cannot say.
func (s *StackStream) markSeparatorPending() { s.separatorPending = true }

// consumeField drops the pending field name once its value has been written. A container
// consumes it when it opens, not when it closes: otherwise the field outlives its own
// value and ClosePending fills it with a second null.
func (s *StackStream) consumeField() {
	if n := len(s.stack); n > 0 && s.stack[n-1] == ItemField {
		s.stack = s.stack[:n-1]
	}
}

// afterValue separates the next member, but only inside a container: consecutive
// top-level values in one stream get no comma between them.
func (s *StackStream) afterValue() {
	s.consumeField()
	s.separatorPending = len(s.stack) > 0
	flushIfFull(s.stream)
}
