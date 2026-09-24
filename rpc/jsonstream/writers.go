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
	"encoding/hex"
	"slices"
	"unsafe"

	"github.com/erigontech/erigon/common/hexutil"
)

// hexType lists the types written as hex strings. The hexutil types write their own text; a
// fixed-size byte array is written as its bytes, whatever its own text, so none needs escaping.
// The array must still have text, as encoding/json writes a plain array as a list of numbers.
type hexType interface {
	hexutil.Uint64 | hexutil.Uint | hexutil.Int64 | hexutil.U256 | hexutil.Big | hexutil.Bytes |
		~[8]byte | ~[20]byte | ~[32]byte | ~[256]byte
	encoding.TextAppender
}

// emptyHexType lists the hexTypes that omitempty can leave out: a zero number, or Bytes with no
// bytes. encoding/json never leaves out an array or a big number.
type emptyHexType interface {
	hexutil.Uint64 | hexutil.Uint | hexutil.Int64 | hexutil.Bytes
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
	case *hexutil.Uint:
		if *x == 0 {
			return
		}
	case *hexutil.Int64:
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
	start := len(s.stream.Buffer())
	s.commit(append(appendHex(s, append(s.stream.Buffer(), '"'), v), '"'), start)
	s.afterValue()
}

// appendHex appends v's hex text. A failing AppendText appends nothing and latches its error,
// which keeps the JSON well-formed and stops it reaching the client.
func appendHex[T hexType](s *StackStream, buf []byte, v *T) []byte {
	var text []byte
	var err error
	switch x := any(v).(type) {
	case *hexutil.Uint64:
		text, err = x.AppendText(buf)
	case *hexutil.Uint:
		text, err = x.AppendText(buf)
	case *hexutil.Int64:
		text, err = x.AppendText(buf)
	case *hexutil.U256:
		text, err = x.AppendText(buf)
	case *hexutil.Big:
		text, err = x.AppendText(buf)
	case *hexutil.Bytes:
		text, err = x.AppendText(buf)
	default: // a byte array
		text = hex.AppendEncode(append(buf, "0x"...), unsafe.Slice((*byte)(unsafe.Pointer(v)), unsafe.Sizeof(*v)))
	}
	if err != nil {
		if s.stream.Error == nil {
			s.stream.Error = err
		}
		return buf
	}
	return text
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
	s.stream.SetBuffer(append(slices.Grow(s.stream.Buffer(), min(size, FlushThreshold)), '['))
	for i := range items {
		buf := s.stream.Buffer()
		if i > 0 {
			buf = append(buf, ',')
		}
		s.stream.SetBuffer(append(appendHex(s, append(buf, '"'), &items[i]), '"'))
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
