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

import "github.com/holiman/uint256"

// JSONWriter is the JSON stream a MarshalFastJSONTo writes into, in the manner of json/v2's jsontext.Encoder.
type JSONWriter interface {
	// WriteHex writes b as a 0x-prefixed hex string.
	WriteHex(b []byte)
	// WriteHexUint64 writes v as a 0x-prefixed hex quantity, without leading zeros.
	WriteHexUint64(v uint64)
	// WriteHexU256 writes v as a 0x-prefixed hex quantity, without leading zeros.
	WriteHexU256(v uint256.Int)
	// WriteString writes s as an escaped JSON string.
	WriteString(s string)
	WriteNil()
	WriteObjectStart()
	WriteObjectField(name string)
	WriteObjectEnd()
	WriteArrayStart()
	WriteMore()
	WriteArrayEnd()
}
