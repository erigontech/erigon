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
	"slices"
	"unsafe"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
)

// hexType lists the types whose AppendText output needs no JSON escaping, which is what lets
// their text go out unscanned. Add a type only if its text is hex.
type hexType interface {
	hexutil.Uint64 | hexutil.Uint | hexutil.Int64 | hexutil.U256 | hexutil.Big | hexutil.Bytes |
		common.Hash | common.Address |
		~[8]byte | ~[256]byte // types.BlockNonce, types.Bloom: execution/types imports this package
}

type hexPtr[T hexType] interface {
	*T
	encoding.TextAppender
}

// Hex writes a field, null for a nil v.
func Hex[T hexType, P hexPtr[T]](s *StackStream, name string, v P) {
	var t encoding.TextAppender
	if v != nil {
		t = v
	}
	s.hexField(name, t)
}

// HexOmitempty writes a pointer field tagged omitempty: a nil v is left out. A value field
// tagged omitempty is left out when zero, which the caller checks.
func HexOmitempty[T hexType, P hexPtr[T]](s *StackStream, name string, v P) {
	if v != nil {
		s.hexField(name, v)
	}
}

func (s *StackStream) hexField(name string, v encoding.TextAppender) {
	s.Field(name)
	if v == nil {
		s.WriteNil()
		return
	}
	s.writeQuotedText(v)
}

// Hexes writes a slice field, null for a nil slice.
func Hexes[S ~[]E, E hexType, P hexPtr[E]](s *StackStream, name string, items S) {
	hexesField[S, E, P](s, name, items)
}

// HexesOmitempty writes a slice field tagged omitempty: an empty slice is left out.
func HexesOmitempty[S ~[]E, E hexType, P hexPtr[E]](s *StackStream, name string, items S) {
	if len(items) > 0 {
		hexesField[S, E, P](s, name, items)
	}
}

func hexesField[S ~[]E, E hexType, P hexPtr[E]](s *StackStream, name string, items S) {
	s.Field(name)
	HexesValue[S, E, P](s, items)
}

// HexesValue writes the array itself, with no field name, null for a nil slice.
func HexesValue[S ~[]E, E hexType, P hexPtr[E]](s *StackStream, items S) {
	if items == nil {
		s.WriteNil()
		return
	}
	s.beforeValue()
	// exact for the fixed-size types, a first guess for Bytes and Big; no further than the flush
	size := 2 + len(items)*(hexutil.QuotedLen(int(unsafe.Sizeof(items[0])))+1)
	buf := slices.Grow(s.stream.Buffer(), min(size, FlushThreshold))
	s.stream.SetBuffer(append(buf, '['))
	for i := range items {
		buf = s.stream.Buffer()
		if i > 0 {
			buf = append(buf, ',')
		}
		buf = append(buf, '"')
		text := s.appendText(buf, P(&items[i]))
		s.stream.SetBuffer(append(text, '"'))
		flushIfFull(s.stream) // blob arrays reach megabytes
	}
	s.stream.SetBuffer(append(s.stream.Buffer(), ']'))
	s.afterValue()
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
