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

import "io"

var (
	_ Stream = (*StackStream)(nil)
	_ Stream = (*LazyFieldStream)(nil)
)

// LazyFieldStream lazily writes fieldName: on the first value write; nothing is written if no
// value is written. When prependSeparator is true, a comma is emitted before the field name (for
// fields that follow other already-written fields in the same JSON object).
//
// It forwards to inner explicitly rather than embedding Stream: a method that
// reached inner without ensure would put the value's bytes at the enclosing
// object's level, and embedding would grant exactly that to every method later
// added to the interface.
type LazyFieldStream struct {
	inner            Stream
	written          bool
	openDepth        uint
	field            string
	prependSeparator bool
}

// NewLazyFieldStream creates a LazyFieldStream wrapping s. On the first value write it emits
// (optionally) a comma separator followed by fieldName: before forwarding the write.
func NewLazyFieldStream(s Stream, field string, prependSeparator bool) *LazyFieldStream {
	return &LazyFieldStream{inner: s, field: field, prependSeparator: prependSeparator}
}

// Written reports whether a value has been written to this field.
func (s *LazyFieldStream) Written() bool { return s.written }

// ResetField clears the Written flag so the stream can be reused for a new field in the same object.
func (s *LazyFieldStream) ResetField() { s.written = false }

// CloseIfOpen closes any partial value written to this field back to the enclosing object level.
// It is a no-op when nothing has been written.
func (s *LazyFieldStream) CloseIfOpen() {
	if s.written {
		_ = s.inner.ClosePending(s.openDepth)
	}
}

func (s *LazyFieldStream) ensure() {
	if !s.written {
		s.written = true
		if s.prependSeparator {
			s.inner.WriteMore()
		}
		s.inner.WriteObjectField(s.field)
		s.openDepth = uint(s.inner.Depth() - 1)
	}
}

func (s *LazyFieldStream) WriteNil()              { s.ensure(); s.inner.WriteNil() }
func (s *LazyFieldStream) WriteTrue()             { s.ensure(); s.inner.WriteTrue() }
func (s *LazyFieldStream) WriteFalse()            { s.ensure(); s.inner.WriteFalse() }
func (s *LazyFieldStream) WriteBool(v bool)       { s.ensure(); s.inner.WriteBool(v) }
func (s *LazyFieldStream) WriteInt(v int)         { s.ensure(); s.inner.WriteInt(v) }
func (s *LazyFieldStream) WriteInt8(v int8)       { s.ensure(); s.inner.WriteInt8(v) }
func (s *LazyFieldStream) WriteInt16(v int16)     { s.ensure(); s.inner.WriteInt16(v) }
func (s *LazyFieldStream) WriteInt32(v int32)     { s.ensure(); s.inner.WriteInt32(v) }
func (s *LazyFieldStream) WriteInt64(v int64)     { s.ensure(); s.inner.WriteInt64(v) }
func (s *LazyFieldStream) WriteUint(v uint)       { s.ensure(); s.inner.WriteUint(v) }
func (s *LazyFieldStream) WriteUint8(v uint8)     { s.ensure(); s.inner.WriteUint8(v) }
func (s *LazyFieldStream) WriteUint16(v uint16)   { s.ensure(); s.inner.WriteUint16(v) }
func (s *LazyFieldStream) WriteUint32(v uint32)   { s.ensure(); s.inner.WriteUint32(v) }
func (s *LazyFieldStream) WriteUint64(v uint64)   { s.ensure(); s.inner.WriteUint64(v) }
func (s *LazyFieldStream) WriteFloat32(v float32) { s.ensure(); s.inner.WriteFloat32(v) }
func (s *LazyFieldStream) WriteFloat64(v float64) { s.ensure(); s.inner.WriteFloat64(v) }
func (s *LazyFieldStream) WriteString(v string)   { s.ensure(); s.inner.WriteString(v) }
func (s *LazyFieldStream) WriteRaw(v string)      { s.ensure(); s.inner.WriteRaw(v) }
func (s *LazyFieldStream) WriteRawBytes(v []byte) { s.ensure(); s.inner.WriteRawBytes(v) }
func (s *LazyFieldStream) WriteObjectStart()      { s.ensure(); s.inner.WriteObjectStart() }
func (s *LazyFieldStream) WriteArrayStart()       { s.ensure(); s.inner.WriteArrayStart() }
func (s *LazyFieldStream) WriteEmptyArray()       { s.ensure(); s.inner.WriteEmptyArray() }
func (s *LazyFieldStream) WriteEmptyObject()      { s.ensure(); s.inner.WriteEmptyObject() }

// A separator and a field name carry no value bytes, so opening the field for
// them would emit `"result":` with nothing to follow it. They belong to a
// container a value write already opened.
func (s *LazyFieldStream) WriteMore()                   { s.inner.WriteMore() }
func (s *LazyFieldStream) WriteObjectField(name string) { s.inner.WriteObjectField(name) }

// The ends close what a value opened, so the field is already there.
func (s *LazyFieldStream) WriteObjectEnd() { s.inner.WriteObjectEnd() }
func (s *LazyFieldStream) WriteArrayEnd()  { s.inner.WriteArrayEnd() }

func (s *LazyFieldStream) Buffer() []byte                 { return s.inner.Buffer() }
func (s *LazyFieldStream) Flush() error                   { return s.inner.Flush() }
func (s *LazyFieldStream) ClosePending(target uint) error { return s.inner.ClosePending(target) }
func (s *LazyFieldStream) Depth() int                     { return s.inner.Depth() }

func (s *LazyFieldStream) Reset(out io.Writer) {
	s.inner.Reset(out)
	s.written = false
}
