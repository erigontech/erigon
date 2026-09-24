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

// Package ethjson writes the two hex forms the Ethereum JSON-RPC spec uses. It needs nothing
// but jsonstream's exported writers, so the spec's rules live here rather than in the stream.
package ethjson

import (
	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/rpc/jsonstream"
)

// A JSON-RPC value is either a quantity, `^0x(0|[1-9a-f][0-9a-f]*)$`, or data,
// `^0x[0-9a-f]*$`, as
// https://github.com/ethereum/execution-apis/blob/main/src/schemas/base-types.yaml defines
// them. Which one a field uses follows from its Go type, so these writers take the domain
// type and hold that mapping in one place: an encoder cannot pass a byte slice where the
// spec wants a quantity, and no encoder needs to name a hexutil type.
func Quantity[T ~uint64 | ~uint](s *jsonstream.StackStream, name string, v T) {
	s.Field(name)
	jsonstream.HexUint64(s, uint64(v))
}

// QuantityOrNull writes null for a field the header or receipt does not carry.
func QuantityOrNull[T ~uint64 | ~uint](s *jsonstream.StackStream, name string, v *T) {
	if v == nil {
		s.Field(name).WriteNil()
		return
	}
	Quantity(s, name, *v)
}

// Quantity256 writes a 256-bit quantity, null when the field is absent.
func Quantity256(s *jsonstream.StackStream, name string, v *uint256.Int) {
	s.Field(name)
	jsonstream.HexUint256(s, v)
}

// Data writes a byte string whose length is its own, such as extraData or a log's data.
func Data(s *jsonstream.StackStream, name string, b []byte) {
	s.Field(name)
	s.WriteHex(b)
}

// DataList writes fixed-size values as one array field, growing the buffer once for the whole
// array rather than once per element. A nil slice is null.
func DataList[S ~[]E, E ~[length.Hash]byte](s *jsonstream.StackStream, name string, items S) {
	jsonstream.HexesField(s, name, items)
}
