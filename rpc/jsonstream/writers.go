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

	"github.com/erigontech/erigon/common/hexutil"
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

// A JSON-RPC value is either a quantity, `^0x(0|[1-9a-f][0-9a-f]*)$`, or data,
// `^0x[0-9a-f]*$`, as
// https://github.com/ethereum/execution-apis/blob/main/src/schemas/base-types.yaml defines
// them. Which one a field uses follows from its Go type, so these writers take the domain
// type and hold that mapping in one place: an encoder cannot pass a byte slice where the
// spec wants a quantity, and no encoder needs to name a hexutil type.
func Quantity[T ~uint64 | ~uint](s *StackStream, name string, v T) {
	q := hexutil.Uint64(v)
	Text(s, name, &q)
}

// QuantityOrNull writes null for a field the header or receipt does not carry.
func QuantityOrNull[T ~uint64 | ~uint](s *StackStream, name string, v *T) {
	if v == nil {
		s.Field(name).WriteNil()
		return
	}
	Quantity(s, name, *v)
}

// Quantity256 writes a 256-bit quantity, null when the field is absent.
func Quantity256(s *StackStream, name string, v *uint256.Int) {
	Text(s, name, (*hexutil.U256)(v))
}

// Data writes a byte string whose length is its own, such as extraData or a log's data.
func Data(s *StackStream, name string, b []byte) {
	s.Field(name).WriteHex(b)
}

// QuantityOmitZero leaves the field out when the value is zero, which is what a json tag's
// omitempty asks for.
func QuantityOmitZero[T ~uint64 | ~uint](s *StackStream, name string, v T) {
	if v == 0 {
		return
	}
	Quantity(s, name, v)
}

// QuantityOmitNil leaves the field out when the header or receipt does not carry it, for a
// field whose json tag says omitempty rather than null.
func QuantityOmitNil[T ~uint64 | ~uint](s *StackStream, name string, v *T) {
	if v == nil {
		return
	}
	Quantity(s, name, *v)
}

// DataOmitEmpty leaves the field out when there are no bytes.
func DataOmitEmpty(s *StackStream, name string, b []byte) {
	if len(b) == 0 {
		return
	}
	Data(s, name, b)
}
