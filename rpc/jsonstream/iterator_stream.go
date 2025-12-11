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
	"io"

	jsoniter "github.com/json-iterator/go"
)

// JsoniterStream implements the Stream interface by wrapping jsoniter.Stream
type JsoniterStream struct {
	stream *jsoniter.Stream
}

// NewJsoniterStream creates a new JsoniterStream wrapped around the given jsoniter.Stream.
func NewJsoniterStream(stream *jsoniter.Stream) *JsoniterStream {
	return &JsoniterStream{
		stream: stream,
	}
}

func (s *JsoniterStream) Buffer() []byte {
	return s.stream.Buffer()
}

func (s *JsoniterStream) Reset(out io.Writer) {
	s.stream.Reset(out)
	s.stream.Error = nil
}

func (s *JsoniterStream) Write(content []byte) (int, error) {
	return s.stream.Write(content)
}

func (s *JsoniterStream) WriteRaw(content string) {
	s.stream.WriteRaw(content)
}

func (s *JsoniterStream) WriteNil() {
	s.stream.WriteNil()
}

func (s *JsoniterStream) WriteTrue() {
	s.stream.WriteTrue()
}

func (s *JsoniterStream) WriteFalse() {
	s.stream.WriteFalse()
}

func (s *JsoniterStream) WriteBool(val bool) {
	s.stream.WriteBool(val)
}

func (s *JsoniterStream) WriteInt(val int) {
	s.stream.WriteInt(val)
}

func (s *JsoniterStream) WriteInt8(val int8) {
	s.stream.WriteInt8(val)
}

func (s *JsoniterStream) WriteInt16(val int16) {
	s.stream.WriteInt16(val)
}

func (s *JsoniterStream) WriteInt32(val int32) {
	s.stream.WriteInt32(val)
}

func (s *JsoniterStream) WriteInt64(val int64) {
	s.stream.WriteInt64(val)
}

func (s *JsoniterStream) WriteUint(val uint) {
	s.stream.WriteUint(val)
}

func (s *JsoniterStream) WriteUint8(val uint8) {
	s.stream.WriteUint8(val)
}

func (s *JsoniterStream) WriteUint16(val uint16) {
	s.stream.WriteUint16(val)
}

func (s *JsoniterStream) WriteUint32(val uint32) {
	s.stream.WriteUint32(val)
}

func (s *JsoniterStream) WriteUint64(val uint64) {
	s.stream.WriteUint64(val)
}

func (s *JsoniterStream) WriteFloat32(val float32) {
	s.stream.WriteFloat32(val)
}

func (s *JsoniterStream) WriteFloat64(val float64) {
	s.stream.WriteFloat64(val)
}

func (s *JsoniterStream) WriteString(val string) {
	s.stream.WriteString(val)
}

func (s *JsoniterStream) WriteObjectStart() {
	s.stream.WriteObjectStart()
}

func (s *JsoniterStream) WriteObjectEnd() {
	s.stream.WriteObjectEnd()
}

func (s *JsoniterStream) WriteArrayStart() {
	s.stream.WriteArrayStart()
}

func (s *JsoniterStream) WriteArrayEnd() {
	s.stream.WriteArrayEnd()
}

func (s *JsoniterStream) WriteMore() {
	s.stream.WriteMore()
}

func (s *JsoniterStream) WriteObjectField(fieldName string) {
	s.stream.WriteObjectField(fieldName)
}

func (s *JsoniterStream) Flush() error {
	return s.stream.Flush()
}

func (s *JsoniterStream) Error() error {
	return s.stream.Error
}

func (s *JsoniterStream) BufferAsString() (string, error) {
	return string(s.Buffer()), nil
}

func (s *JsoniterStream) WriteEmptyArray() {
	s.stream.WriteEmptyArray()
}

func (s *JsoniterStream) WriteEmptyObject() {
	s.stream.WriteEmptyObject()
}

// Size returns the size of the buffer
func (s *JsoniterStream) Size() int {
	return len(s.stream.Buffer())
}

// ClosePending does nothing because JsoniterStream wrapper does not support closing pending elements
func (s *JsoniterStream) ClosePending( /*skipLast*/ uint) error {
	return nil
}
