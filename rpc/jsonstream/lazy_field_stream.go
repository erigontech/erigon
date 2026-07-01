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

// LazyFieldStream lazily writes fieldName: on the first value write; nothing is written if no
// value is written. When prependSeparator is true, a comma is emitted before the field name (for
// fields that follow other already-written fields in the same JSON object).
type LazyFieldStream struct {
	Stream
	written          bool
	openDepth        uint
	field            string
	prependSeparator bool
}

// NewLazyFieldStream creates a LazyFieldStream wrapping s. On the first value write it emits
// (optionally) a comma separator followed by fieldName: before forwarding the write.
func NewLazyFieldStream(s Stream, field string, prependSeparator bool) *LazyFieldStream {
	return &LazyFieldStream{Stream: s, field: field, prependSeparator: prependSeparator}
}

// Written reports whether a value has been written to this field.
func (s *LazyFieldStream) Written() bool { return s.written }

// ResetField clears the Written flag so the stream can be reused for a new field in the same object.
func (s *LazyFieldStream) ResetField() { s.written = false }

// CloseIfOpen closes any partial value written to this field back to the enclosing object level.
// It is a no-op when nothing has been written.
func (s *LazyFieldStream) CloseIfOpen() {
	if s.written {
		_ = s.Stream.ClosePending(s.openDepth)
	}
}

func (s *LazyFieldStream) ensure() {
	if !s.written {
		s.written = true
		if s.prependSeparator {
			s.Stream.WriteMore()
		}
		s.Stream.WriteObjectField(s.field)
		s.openDepth = uint(s.Stream.Depth() - 1)
	}
}

func (s *LazyFieldStream) WriteNil()                   { s.ensure(); s.Stream.WriteNil() }
func (s *LazyFieldStream) WriteTrue()                  { s.ensure(); s.Stream.WriteTrue() }
func (s *LazyFieldStream) WriteFalse()                 { s.ensure(); s.Stream.WriteFalse() }
func (s *LazyFieldStream) WriteBool(v bool)            { s.ensure(); s.Stream.WriteBool(v) }
func (s *LazyFieldStream) WriteInt(v int)              { s.ensure(); s.Stream.WriteInt(v) }
func (s *LazyFieldStream) WriteInt8(v int8)            { s.ensure(); s.Stream.WriteInt8(v) }
func (s *LazyFieldStream) WriteInt16(v int16)          { s.ensure(); s.Stream.WriteInt16(v) }
func (s *LazyFieldStream) WriteInt32(v int32)          { s.ensure(); s.Stream.WriteInt32(v) }
func (s *LazyFieldStream) WriteInt64(v int64)          { s.ensure(); s.Stream.WriteInt64(v) }
func (s *LazyFieldStream) WriteUint(v uint)            { s.ensure(); s.Stream.WriteUint(v) }
func (s *LazyFieldStream) WriteUint8(v uint8)          { s.ensure(); s.Stream.WriteUint8(v) }
func (s *LazyFieldStream) WriteUint16(v uint16)        { s.ensure(); s.Stream.WriteUint16(v) }
func (s *LazyFieldStream) WriteUint32(v uint32)        { s.ensure(); s.Stream.WriteUint32(v) }
func (s *LazyFieldStream) WriteUint64(v uint64)        { s.ensure(); s.Stream.WriteUint64(v) }
func (s *LazyFieldStream) WriteFloat32(v float32)      { s.ensure(); s.Stream.WriteFloat32(v) }
func (s *LazyFieldStream) WriteFloat64(v float64)      { s.ensure(); s.Stream.WriteFloat64(v) }
func (s *LazyFieldStream) WriteString(v string)        { s.ensure(); s.Stream.WriteString(v) }
func (s *LazyFieldStream) WriteObjectStart()           { s.ensure(); s.Stream.WriteObjectStart() }
func (s *LazyFieldStream) WriteArrayStart()            { s.ensure(); s.Stream.WriteArrayStart() }
func (s *LazyFieldStream) WriteEmptyArray()            { s.ensure(); s.Stream.WriteEmptyArray() }
func (s *LazyFieldStream) WriteEmptyObject()           { s.ensure(); s.Stream.WriteEmptyObject() }
func (s *LazyFieldStream) Write(p []byte) (int, error) { s.ensure(); return s.Stream.Write(p) }
func (s *LazyFieldStream) WriteRaw(v string)           { s.ensure(); s.Stream.WriteRaw(v) }
