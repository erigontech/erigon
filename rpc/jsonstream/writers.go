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

// hexType lists the types written as hex strings. The hexutil numbers write their own text; Bytes
// and a fixed-size byte array are written as their bytes, whatever their own text, so none needs
// escaping.
// The array must still have text, as encoding/json writes a plain array as a list of numbers.
type hexType interface {
	hexutil.Uint64 | hexutil.Uint | hexutil.Int64 | hexutil.U256 | hexutil.Big | hexutil.Bytes |
		common.Hash | common.Address |
		~[8]byte | ~[256]byte // types.BlockNonce, types.Bloom: execution/types imports this package
	encoding.TextAppender
}

// emptyHexType lists the hexTypes that omitempty can leave out: a zero number, or Bytes with no
// bytes. encoding/json never leaves out an array or a big number.
type emptyHexType interface {
	hexutil.Uint64 | hexutil.Bytes
	encoding.TextAppender
}

// Hex writes a value field.
func Hex[T hexType](s *StackStream, name string, v T) {
	hexField(s, name, &v)
}

// HexOmitempty writes a value field tagged omitempty.
func HexOmitempty[T emptyHexType](s *StackStream, name string, v T) {
	hexFieldOmitempty(s, name, &v)
}

// HexPtr writes a pointer field, null for nil.
func HexPtr[T hexType](s *StackStream, name string, v *T) {
	hexField(s, name, v)
}

// HexPtrOmitempty writes a pointer field tagged omitempty: nil is left out.
func HexPtrOmitempty[T hexType](s *StackStream, name string, v *T) {
	if v != nil {
		hexField(s, name, v)
	}
}

func hexFieldOmitempty[T emptyHexType](s *StackStream, name string, v *T) {
	switch x := any(v).(type) {
	case *hexutil.Bytes:
		if len(*x) == 0 {
			return
		}
	case *hexutil.Uint64:
		if *x == 0 {
			return
		}
	}
	hexField(s, name, v)
}

func hexField[T hexType](s *StackStream, name string, v *T) {
	s.Field(name)
	if v == nil {
		s.WriteNil()
		return
	}
	s.beforeValue()
	buf := s.stream.Buffer()
	start := len(buf)
	if b, ok := bytesOf(v); ok {
		buf = hexutil.AppendQuoted(slices.Grow(buf, hexutil.QuotedLen(len(b))), b)
	} else {
		buf = append(appendOwnText(s, append(buf, '"'), v), '"')
	}
	s.commit(buf, start)
	s.afterValue()
}

// appendOwnText appends v's text. On failure it appends nothing and latches the error, which
// keeps the JSON well-formed and stops it reaching the client.
func appendOwnText[T hexType](s *StackStream, buf []byte, v *T) []byte {
	text, err := (*v).AppendText(buf)
	if err != nil {
		if s.stream.Error == nil {
			s.stream.Error = err
		}
		return buf
	}
	return text
}

// bytesOf returns the bytes of a byte array or Bytes; ok is false for a hexutil number, which
// writes its own text.
func bytesOf[T hexType](v *T) (b []byte, ok bool) {
	if isByteArray[T]() {
		return unsafe.Slice((*byte)(unsafe.Pointer(v)), unsafe.Sizeof(*v)), true
	}
	if b, ok := any(v).(*hexutil.Bytes); ok {
		return *b, true
	}
	return nil, false
}

// isByteArray is fixed for each instantiation, so the compiler drops the branch it guards: of
// the hexTypes only the byte arrays have an alignment of 1.
func isByteArray[T hexType]() bool {
	var v T
	return unsafe.Alignof(v) == 1
}

// Hexes writes a slice field, null for a nil slice.
func Hexes[S ~[]E, E hexType](s *StackStream, name string, items S) {
	hexesField(s, name, items)
}

// HexesOmitempty writes a slice field tagged omitempty: an empty slice is left out.
func HexesOmitempty[S ~[]E, E hexType](s *StackStream, name string, items S) {
	if len(items) > 0 {
		hexesField(s, name, items)
	}
}

func hexesField[S ~[]E, E hexType](s *StackStream, name string, items S) {
	s.Field(name)
	HexesValue(s, items)
}

// HexesValue writes the array itself, with no field name, null for a nil slice.
func HexesValue[S ~[]E, E hexType](s *StackStream, items S) {
	if items == nil {
		s.WriteNil()
		return
	}
	s.beforeValue()
	// exact for the fixed-size types, a first guess for Bytes and Big; no further than the flush
	size := 2 + len(items)*(hexutil.QuotedLen(int(unsafe.Sizeof(*new(E))))+1)
	buf := append(slices.Grow(s.stream.Buffer(), min(size, FlushThreshold)), '[')
	if isByteArray[E]() && size <= FlushThreshold {
		for i := range items {
			if i > 0 {
				buf = append(buf, ',')
			}
			b, _ := bytesOf(&items[i])
			buf = hexutil.AppendQuoted(buf, b)
		}
		s.stream.SetBuffer(append(buf, ']'))
		s.afterValue()
		return
	}
	for i := range items {
		if i > 0 {
			buf = append(buf, ',')
		}
		if b, ok := bytesOf(&items[i]); ok {
			buf = hexutil.AppendQuoted(slices.Grow(buf, hexutil.QuotedLen(len(b))), b)
		} else {
			buf = append(appendOwnText(s, append(buf, '"'), &items[i]), '"')
		}
		if len(buf) >= FlushThreshold { // blob arrays reach megabytes
			s.stream.SetBuffer(buf)
			flushFull(s.stream)
			buf = s.stream.Buffer()
		}
	}
	s.stream.SetBuffer(append(buf, ']'))
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
