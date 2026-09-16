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

package jsonstream

import (
	"encoding/binary"
	"math/bits"

	jsoniter "github.com/json-iterator/go"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/bitutil"
)

const hexDigits = "0123456789abcdef"

// escapeIndex returns the offset of the first byte a JSON string must escape,
// or len(val) if there is none. It reads eight bytes at a time: the stdlib has
// nothing for a set of bytes, IndexByte is SIMD but takes one, and ContainsAny
// rebuilds its ASCII set on every call.
func escapeIndex(val string) int {
	b := common.ToBytesZeroCopy(val)
	i := 0
	for ; i+8 <= len(b); i += 8 {
		x := binary.LittleEndian.Uint64(b[i:])
		if z := bitutil.HasLess(x, 0x20) | bitutil.HasByte(x, '"') | bitutil.HasByte(x, '\\'); z != 0 {
			// the lowest set bit of each mask is a real hit, so the lowest of
			// the three is too, and it names the lane
			return i + bits.TrailingZeros64(z)>>3
		}
	}
	for ; i < len(val); i++ {
		if isEscape(val[i]) {
			break
		}
	}
	return i
}

func isEscape(c byte) bool { return c < 0x20 || c == '"' || c == '\\' }

// appendJSONString appends val as a JSON string, copying each escape-free run in
// one go where jsoniter's WriteString appends a byte at a time. The output is
// byte for byte what jsoniter produces, HTML characters included: Erigon writes
// through WriteString, not WriteStringWithHTMLEscaped.
func appendJSONString(buf []byte, val string) []byte {
	buf = append(buf, '"')
	for len(val) > 0 {
		// Escapes come in runs, so test the next byte before paying for the word
		// scan: on dense input the scanner costs a load and three broadword ops
		// to report a byte already in hand.
		c := val[0]
		if !isEscape(c) {
			i := 1 + escapeIndex(val[1:])
			buf = append(buf, val[:i]...)
			val = val[i:]
			continue
		}
		switch c {
		case '"', '\\':
			buf = append(buf, '\\', c)
		case '\n':
			buf = append(buf, '\\', 'n')
		case '\r':
			buf = append(buf, '\\', 'r')
		case '\t':
			buf = append(buf, '\\', 't')
		default:
			buf = append(buf, '\\', 'u', '0', '0', hexDigits[c>>4], hexDigits[c&0xf])
		}
		val = val[1:]
	}
	return append(buf, '"')
}

func writeStringFast(stream *jsoniter.Stream, val string) {
	stream.SetBuffer(appendJSONString(stream.Buffer(), val))
}

// writeObjectFieldFast writes a field name and its colon. The bare colon is
// correct only at jsoniter's IndentionStep 0, which newStackStream pins.
func writeObjectFieldFast(stream *jsoniter.Stream, fieldName string) {
	stream.SetBuffer(append(appendJSONString(stream.Buffer(), fieldName), ':'))
}
