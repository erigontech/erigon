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
)

// Stream is an interface that defines the common functionality between
// jsoniter.Stream and StackStream for JSON serialization.
type Stream interface {
	// Basic operations

	Buffer() []byte
	Reset(out io.Writer)
	// WriteRawBytes and WriteRaw write already-encoded JSON. Nothing is escaped
	// or validated, so the caller owns that: a value that is not yet valid JSON
	// — any unencoded string — must go through WriteString.
	WriteRawBytes(content []byte)
	WriteRaw(content string)
	// Flush is where a delivery failure surfaces: value writers cannot fail and
	// the automatic flush drops the error, so a handler that has to notice the
	// client leaving checks this one.
	Flush() error

	// Value writing methods

	WriteNil()
	WriteTrue()
	WriteFalse()
	WriteBool(val bool)
	WriteInt(val int)
	WriteInt8(val int8)
	WriteInt16(val int16)
	WriteInt32(val int32)
	WriteInt64(val int64)
	WriteUint(val uint)
	WriteUint8(val uint8)
	WriteUint16(val uint16)
	WriteUint32(val uint32)
	WriteUint64(val uint64)
	WriteFloat32(val float32)
	WriteFloat64(val float64)
	// WriteString and WriteObjectField must consume val before returning:
	// callers pass views over reusable buffers.
	WriteString(val string)

	// JSON structure methods

	WriteObjectStart()
	WriteObjectEnd()
	WriteArrayStart()
	WriteArrayEnd()
	WriteMore()
	WriteObjectField(fieldName string)

	// Utility methods

	WriteEmptyArray()
	WriteEmptyObject()

	// Extended functionality

	ClosePending(targetDepth uint) error
	// Depth counts the entries ClosePending would unwind, which is not the
	// container nesting: a field name or a comma still waiting for its value
	// counts too. Pass it back as targetDepth to return to this point.
	Depth() int
}
