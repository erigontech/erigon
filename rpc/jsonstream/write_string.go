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
	"encoding/binary"

	jsoniter "github.com/json-iterator/go"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/bitutil"
)

// ctrlEscape holds the escape jsoniter emits for each byte below 0x20.
var ctrlEscape = func() (t [0x20]string) {
	const hexDigits = "0123456789abcdef"
	for b := range t {
		switch byte(b) {
		case '\n':
			t[b] = `\n`
		case '\r':
			t[b] = `\r`
		case '\t':
			t[b] = `\t`
		default:
			t[b] = `\u00` + string([]byte{hexDigits[b>>4], hexDigits[b&0xf]})
		}
	}
	return t
}()

// escapeIndex returns the offset of the first byte a JSON string must escape,
// or len(val) if there is none. It reads eight bytes at a time: the stdlib has
// nothing for a set of bytes, IndexByte is SIMD but takes one, and ContainsAny
// rebuilds its ASCII set on every call.
func escapeIndex(val string) int {
	b := common.ToBytesZeroCopy(val)
	i := 0
	for ; i+8 <= len(b); i += 8 {
		x := binary.LittleEndian.Uint64(b[i:])
		if bitutil.HasLess(x, 0x20)|bitutil.HasByte(x, '"')|bitutil.HasByte(x, '\\') != 0 {
			break
		}
	}
	for ; i < len(val); i++ {
		if c := val[i]; c < 0x20 || c == '"' || c == '\\' {
			break
		}
	}
	return i
}

// writeStringFast writes val as a JSON string, copying each escape-free run in
// one go where jsoniter's WriteString appends a byte at a time. The output is
// byte for byte what jsoniter produces, including the HTML characters it leaves
// alone: Erigon writes through WriteString, not WriteStringWithHTMLEscaped.
func writeStringFast(stream *jsoniter.Stream, val string) {
	stream.WriteRaw(`"`)
	for {
		i := escapeIndex(val)
		stream.WriteRaw(val[:i])
		if i == len(val) {
			break
		}
		switch c := val[i]; c {
		case '"':
			stream.WriteRaw(`\"`)
		case '\\':
			stream.WriteRaw(`\\`)
		default:
			stream.WriteRaw(ctrlEscape[c])
		}
		val = val[i+1:]
	}
	stream.WriteRaw(`"`)
}

// writeObjectFieldFast writes a field name and its colon. jsoniter follows the
// colon with a space while indenting, which no stream this package builds ever
// does -- every config here has an indention step of zero.
func writeObjectFieldFast(stream *jsoniter.Stream, fieldName string) {
	writeStringFast(stream, fieldName)
	stream.WriteRaw(":")
}
