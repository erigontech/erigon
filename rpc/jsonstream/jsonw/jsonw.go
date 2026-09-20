// Copyright 2026 The Erigon Authors
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

package jsonw

import "encoding"

// JSONAppender encodes itself by appending to dst, with no call per field.
type JSONAppender interface{ AppendJSON(dst []byte) []byte }

// JSONWriter is the JSON stream a MarshalFastJSONTo writes into, in the manner of json/v2's jsontext.Encoder.
type JSONWriter interface {
	// WriteHex writes b as a 0x-prefixed hex string.
	WriteHex(b []byte)
	// WriteQuotedText writes v.AppendText's output as a JSON string. The text must need no
	// escaping: callers pass hex quantities.
	WriteQuotedText(v encoding.TextAppender)
	// WriteRawBytes writes already-encoded JSON verbatim. It is the escape hatch for a
	// value the fast path has no shape for, so it must not be emulated by quoting.
	WriteRawBytes(content []byte)
	// WriteString writes s as an escaped JSON string.
	WriteString(s string)
	WriteBool(v bool)
	// AppendJSON hands the stream's buffer to a value that encodes itself with plain
	// appends, so a whole value costs one call instead of one per field.
	AppendJSON(v JSONAppender)
	WriteNil()
	WriteObjectStart()
	// WriteObjectField returns the writer, so a field and its value can be chained.
	WriteObjectField(name string) JSONWriter
	WriteObjectEnd()
	WriteArrayStart()
	WriteMore()
	WriteArrayEnd()
}

// Field writes the comma a following field needs, then the field name. The first field of
// an object uses WriteObjectField directly.
func Field(w JSONWriter, name string) JSONWriter {
	return w.WriteObjectField(name)
}

// Hex writes b as a hex string field, or null when b is nil.
func Hex(w JSONWriter, name string, b []byte) {
	if b == nil {
		Field(w, name).WriteNil()
		return
	}
	Field(w, name).WriteHex(b)
}

type textPtr[T any] interface {
	*T
	encoding.TextAppender
}

// Text writes v's text as a JSON string field, or null when v is nil.
func Text[T any, P textPtr[T]](w JSONWriter, name string, v P) {
	if v == nil {
		Field(w, name).WriteNil()
		return
	}
	Field(w, name).WriteQuotedText(v)
}

// Array writes a JSON array field exactly as the reflection encoder would: the pointer
// decides whether the field appears, the slice decides its shape. A nil pointer omits the
// field, a nil slice is null, an empty slice is []. So a field declared without omitempty
// passes &field and is always present, and a *[]T with omitempty passes itself.
func Array[S ~[]E, E any](w JSONWriter, name string, items *S, elem func(JSONWriter, *E)) {
	if items == nil {
		return
	}
	Field(w, name)
	ArrayValue(w, *items, elem)
}

// ArrayValue writes the array itself, with no field name, for a result that is a bare
// array. A nil slice is null and an empty one is [].
func ArrayValue[S ~[]E, E any](w JSONWriter, items S, elem func(JSONWriter, *E)) {
	if items == nil {
		w.WriteNil()
		return
	}
	w.WriteArrayStart()
	for i := range items {
		elem(w, &items[i])
	}
	w.WriteArrayEnd()
}
