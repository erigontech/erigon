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

	"github.com/holiman/uint256"
)

type textPtr[T any] interface {
	*T
	encoding.TextAppender
}

// Text writes v's text as a JSON string field, or null when v is nil.
func Text[T any, P textPtr[T]](s *StackStream, name string, v P) {
	s.Field(name)
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
		elem(s, &items[i])
	}
	s.WriteArrayEnd()
}

// HexUint64 writes 0x and the shortest lowercase hex of v, with no field name. Which fields
// are written this way is the caller's rule, not the stream's: see rpc/jsonstream/ethjson.
func HexUint64(s *StackStream, v uint64) { writeHexUint64(s, v) }

// HexUint256 does the same for a 256-bit value, null for a nil one.
func HexUint256(s *StackStream, v *uint256.Int) { writeHexUint256(s, v) }
