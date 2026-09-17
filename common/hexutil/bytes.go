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

package hexutil

import (
	"encoding/hex"
	"encoding/json"
	"reflect"
)

var bytesT = reflect.TypeFor[Bytes]()

// Bytes marshals/unmarshals as a JSON string with 0x prefix.
// The empty slice marshals as "0x".
type Bytes []byte

// HexPrefix is the "0x" prefix used by all hex-encoded Ethereum values.
const HexPrefix = `0x`

// MarshalText implements encoding.TextMarshaler
func (b Bytes) MarshalText() ([]byte, error) {
	result := make([]byte, len(b)*2+2)
	copy(result, HexPrefix)
	hex.Encode(result[2:], b)
	return result, nil
}

// AppendText implements encoding.TextAppender: the alloc-free, byte-identical
// counterpart to MarshalText. Only encoding/json/v2 consults it today.
func (b Bytes) AppendText(dst []byte) ([]byte, error) {
	dst = append(dst, HexPrefix...)
	return hex.AppendEncode(dst, b), nil
}

// JSONWriter is the JSON stream a MarshalFastJSONTo writes into, in the manner of json/v2's jsontext.Encoder.
type JSONWriter interface {
	// AvailableBuffer returns an empty buffer with at least sizeHint spare capacity. The stream owns it:
	// append one value and pass it to WriteRawBytes.
	AvailableBuffer(sizeHint int) []byte
	WriteRawBytes([]byte)
}

// WriteRawJSON writes already-encoded JSON.
func WriteRawJSON(w JSONWriter, raw string) {
	w.WriteRawBytes(append(w.AvailableBuffer(len(raw)), raw...))
}

func MarshalFastJSONArrayTo(w JSONWriter, items []Bytes) {
	MarshalFastJSONElemsTo(w, items, bytesJSONLen, appendBytesJSON)
}

// MarshalFastJSONElemsTo writes items as a JSON array one element at a time, so a large array never sits in one buffer.
func MarshalFastJSONElemsTo[T any](w JSONWriter, items []T, jsonLen func(T) int, appendJSON func([]byte, T) []byte) {
	if items == nil {
		WriteRawJSON(w, "null")
		return
	}
	if len(items) == 0 {
		WriteRawJSON(w, "[]")
		return
	}
	for i, item := range items {
		sep := byte(',')
		if i == 0 {
			sep = '['
		}
		enc := appendJSON(append(w.AvailableBuffer(jsonLen(item)+len("[]")), sep), item)
		if i == len(items)-1 {
			enc = append(enc, ']')
		}
		w.WriteRawBytes(enc)
	}
}

// MarshalFastJSONTo writes b as a JSON string without the escape scan json does: hex never needs escaping.
func (b Bytes) MarshalFastJSONTo(w JSONWriter) error {
	w.WriteRawBytes(appendBytesJSON(w.AvailableBuffer(bytesJSONLen(b)), b))
	return nil
}

func bytesJSONLen(b Bytes) int { return len(b)*2 + len(`"0x"`) }

func appendBytesJSON(dst []byte, b Bytes) []byte {
	return append(hex.AppendEncode(append(dst, `"`+HexPrefix...), b), '"')
}

// UnmarshalJSON implements json.Unmarshaler.
func (b *Bytes) UnmarshalJSON(input []byte) error {
	if !isString(input) {
		return &json.UnmarshalTypeError{Value: "non-string", Type: bytesT}
	}
	return wrapTypeError(b.UnmarshalText(input[1:len(input)-1]), bytesT)
}

// UnmarshalText implements encoding.TextUnmarshaler.
func (b *Bytes) UnmarshalText(input []byte) error {
	raw, err := checkText(input, true)
	if err != nil {
		return err
	}
	dec := make([]byte, len(raw)/2)
	if _, err = hex.Decode(dec, raw); err != nil {
		err = mapError(err)
	} else {
		*b = dec
	}
	return err
}

// String returns the hex encoding of b.
func (b Bytes) String() string {
	return Encode(b)
}
