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

import "encoding"

// Field writes the comma a following field needs, then the field name. The first field of
// an object uses WriteObjectField directly.
func Field(s *StackStream, name string) *StackStream {
	return s.WriteObjectField(name)
}

// Hex writes b as a hex string field, or null when b is nil.
func Hex(s *StackStream, name string, b []byte) {
	if b == nil {
		Field(s, name).WriteNil()
		return
	}
	Field(s, name).WriteHex(b)
}

type textPtr[T any] interface {
	*T
	encoding.TextAppender
}

// Text writes v's text as a JSON string field, or null when v is nil.
func Text[T any, P textPtr[T]](s *StackStream, name string, v P) {
	if v == nil {
		Field(s, name).WriteNil()
		return
	}
	Field(s, name).WriteQuotedText(v)
}

// Array writes a JSON array field exactly as the reflection encoder would: the pointer
// decides whether the field appears, the slice decides its shape. A nil pointer omits the
// field, a nil slice is null, an empty slice is []. So a field declared without omitempty
// passes &field and is always present, and a *[]T with omitempty passes itself.
func Array[S ~[]E, E any](s *StackStream, name string, items *S, elem func(*StackStream, *E)) {
	if items == nil {
		return
	}
	Field(s, name)
	ArrayValue(s, *items, elem)
}

// ArrayValue writes the array itself, with no field name, for a result that is a bare
// array. A nil slice is null and an empty one is [].
func ArrayValue[S ~[]E, E any](s *StackStream, items S, elem func(*StackStream, *E)) {
	if items == nil {
		s.WriteNil()
		return
	}
	s.WriteArrayStart()
	for i := range items {
		elem(s, &items[i])
	}
	s.WriteArrayEnd()
}
