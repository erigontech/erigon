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
	"io"
	"slices"
	"strconv"
	"strings"

	jsoniter "github.com/json-iterator/go"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/length"
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

// WriteHexUint64 writes 0x and the shortest lowercase hex of v. The digits go straight into
// the buffer, so no hexutil value is built for the call and nothing can escape. Which fields
// are written this way is the caller's rule, not the stream's: see rpc/jsonstream/ethjson.
func (s *StackStream) WriteHexUint64(v uint64) {
	s.beforeValue()
	buf := s.stream.Buffer()
	start := len(buf)
	buf = strconv.AppendUint(append(buf, '"', '0', 'x'), v, 16)
	s.commit(append(buf, '"'), start)
	s.afterValue()
}

// WriteHexUint256 does the same for a 256-bit value, null for a nil one.
func (s *StackStream) WriteHexUint256(v *uint256.Int) {
	if v == nil {
		s.WriteNil()
		return
	}
	s.beforeValue()
	buf := s.stream.Buffer()
	start := len(buf)
	buf, _ = hexutil.U256(*v).AppendText(append(buf, '"'))
	s.commit(append(buf, '"'), start)
	s.afterValue()
}

func (s *StackStream) WriteHex(b []byte) {
	s.beforeValue()
	buf := s.stream.Buffer()
	start := len(buf)
	buf = hexutil.AppendQuoted(slices.Grow(buf, hexutil.QuotedLen(len(b))), b)
	s.commit(buf, start)
	s.afterValue()
}

// HexesField writes fixed-size values as one array field: one buffer growth for the whole
// array, where a value write per element grows once per element. A nil slice is null.
func HexesField[S ~[]E, E ~[length.Hash]byte](s *StackStream, name string, items S) {
	s.Field(name)
	if items == nil {
		s.WriteNil()
		return
	}
	s.beforeValue()
	buf := slices.Grow(s.stream.Buffer(), 2+len(items)*(hexutil.QuotedLen(length.Hash)+1))
	buf = append(buf, '[')
	for i := range items {
		if i > 0 {
			buf = append(buf, ',')
		}
		buf = hexutil.AppendQuoted(buf, items[i][:])
	}
	s.stream.SetBuffer(append(buf, ']'))
	s.afterValue()
}

// WriteHexBytes is WriteHexes for elements that are already byte slices, whose lengths vary
// and so are summed before the single growth.
func WriteHexBytes[S ~[]E, E ~[]byte](s *StackStream, items S) {
	s.beforeValue()
	size := 2 + len(items)
	for i := range items {
		size += hexutil.QuotedLen(len(items[i]))
	}
	buf := slices.Grow(s.stream.Buffer(), size)
	buf = append(buf, '[')
	for i := range items {
		if i > 0 {
			buf = append(buf, ',')
		}
		buf = hexutil.AppendQuoted(buf, items[i])
	}
	s.stream.SetBuffer(append(buf, ']'))
	s.afterValue()
}

// Open returns the stream the value goes to, which is this one.
func (s *StackStream) Open() *StackStream { return s }

// WriteQuotedText writes v.AppendText's output as a JSON string, without an escape scan: it is
// for hex quantities, which never need escaping.
func (s *StackStream) WriteQuotedText(v encoding.TextAppender) {
	s.beforeValue()
	start := len(s.stream.Buffer())
	buf, err := v.AppendText(append(s.stream.Buffer(), '"'))
	if err != nil {
		// An empty string keeps the JSON well-formed; the latched error stops it reaching the client.
		buf = append(s.stream.Buffer(), '"')
		if s.stream.Error == nil {
			s.stream.Error = err
		}
	}
	assertNoEscapes(buf[start+1:])
	buf = append(buf, '"')
	s.commit(buf, start)
	s.afterValue()
}

// assertNoEscapes holds WriteQuotedText's caller to its side of the bargain: the text goes out
// unscanned, so a byte JSON would escape would leave the response malformed. Every appender the
// RPC can answer with today is a hex quantity; this is what catches the next one that is not.
func assertNoEscapes(text []byte) {
	if !dbg.AssertEnabled {
		return
	}
	for _, c := range text {
		if c == '"' || c == '\\' || c < 0x20 {
			panic(fmt.Sprintf("jsonstream: quoted text holds %q, which JSON escapes", c))
		}
	}
}

// commit takes the buffer a value was appended to, handing anything past FlushThreshold
// straight to the writer rather than holding a whole large value in memory.
func (s *StackStream) commit(buf []byte, start int) {
	if s.out != nil && len(buf)-start >= FlushThreshold {
		s.stream.SetBuffer(buf[:start])
		s.writeThrough(buf[start:])
		return
	}
	s.stream.SetBuffer(buf)
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

// Int writes a signed integer. Narrower signed types widen to it without changing the digits.
func (s *StackStream) Int(val int64) {
	s.beforeValue()
	s.stream.WriteInt64(val)
	s.afterValue()
}

// Uint writes an unsigned integer. Narrower unsigned types widen to it without changing the digits.
func (s *StackStream) Uint(val uint64) {
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

// Field writes a field name for an object and adds it to the stack.
func (s *StackStream) Field(fieldName string) *StackStream {
	s.beforeValue()
	writeObjectFieldFast(s.stream, fieldName)
	s.push(ItemField)
	return s
}

// rewindField drops a field name whose value never arrived, with the separator written before
// it. Only bytes still in the buffer can go back, so the caller checks that nothing was written
// after the field name.
func (s *StackStream) rewindField(buf, depth int) {
	b := s.stream.Buffer()
	s.separatorPending = b[buf] == ','
	s.stream.SetBuffer(b[:buf])
	s.stack = s.stack[:depth]
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
	// What was closed is a finished value, and these writes bypass afterValue, so record it
	// here: a separator for the container that survives, none once the root is reached. At
	// depth 0 a fragment in someone else's container asserts its separator after this call.
	if targetDepth < uint(stackLen) {
		s.separatorPending = targetDepth > 0
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
