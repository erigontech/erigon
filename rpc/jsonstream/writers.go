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

// Field writes the separator a following field needs, then the field name. On a stream
// that writes its own separators WriteMore is a no-op, so this is correct either way.
func Field(s *StackStream, name string) *StackStream {
	s.WriteMore()
	return s.WriteObjectField(name)
}

// Hex writes b as a hex string field. A field that can be absent writes its own null.
func (s *StackStream) Hex(name string, b []byte) {
	Field(s, name)
	s.WriteHex(b)
}

type textPtr[T any] interface {
	*T
	encoding.TextAppender
}

// Text writes v's text as a JSON string field, or null when v is nil.
func Text[T any, P textPtr[T]](s *StackStream, name string, v P) {
	Field(s, name)
	if v == nil {
		s.WriteNil()
		return
	}
	s.WriteQuotedText(v)
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
		if i > 0 {
			s.WriteMore()
		}
		elem(s, &items[i])
	}
	s.WriteArrayEnd()
}
