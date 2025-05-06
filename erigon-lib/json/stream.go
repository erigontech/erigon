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
	"io"
)

// Stream is an interface that defines the common functionality between
// jsoniter.Stream and StackStream for JSON serialization.
type Stream interface {
	// Basic operations

	Buffer() []byte
	Reset(out io.Writer)
	Write(content []byte) (int, error)
	WriteRaw(content string)
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
}
