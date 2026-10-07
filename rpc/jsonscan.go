// Copyright 2026 The go-ethereum Authors
// (original work)
// Copyright 2026 The Erigon Authors
// (modifications)
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

package rpc

import (
	"bytes"
	"encoding/binary"

	"github.com/erigontech/erigon/common/bitutil"
)

// Helpers for finding the bounds of JSON values without parsing them, so a
// request is not walked by encoding/json once per layer. They require input
// encoding/json has already accepted, and hand out sub-slices of it which are
// still decoded afterwards. Do not use these on unchecked input.

func isJSONSpace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\r'
}

// skipJSONSpace returns the offset of the first byte at or after i that is not
// insignificant whitespace.
func skipJSONSpace(data []byte, i int) int {
	for i < len(data) && isJSONSpace(data[i]) {
		i++
	}
	return i
}

// scanJSONString returns the offset just past the string beginning at data[i],
// which must be its opening quote.
func scanJSONString(data []byte, i int) int {
	i++ // opening quote
	for i < len(data) {
		j := bytes.IndexByte(data[i:], '"')
		if j < 0 {
			return len(data)
		}
		// The quote ends the string unless an odd number of backslashes run up
		// to it, in which case it is escaped.
		k, n := i+j-1, 0
		for k >= i && data[k] == '\\' {
			n++
			k--
		}
		i += j + 1
		if n%2 == 0 {
			return i
		}
	}
	return len(data)
}

// scanJSONValue returns the offset just past the JSON value beginning at
// data[i], skipping any whitespace in front of it.
func scanJSONValue(data []byte, i int) int {
	i = skipJSONSpace(data, i)
	if i >= len(data) {
		return len(data)
	}
	switch data[i] {
	case '"':
		return scanJSONString(data, i)
	case '{', '[':
		depth := 0
		for i < len(data) {
			switch data[i] {
			case '"':
				i = scanJSONString(data, i)
				continue
			case '{', '[':
				depth++
			case '}', ']':
				depth--
				if depth == 0 {
					return i + 1
				}
			}
			i++
		}
		return len(data)
	default:
		// A number, true, false or null, ending at the next structural byte.
		for ; i < len(data); i++ {
			c := data[i]
			if c == ',' || c == '}' || c == ']' || isJSONSpace(c) {
				return i
			}
		}
		return len(data)
	}
}

// forEachJSONField calls fn with the key and raw value of every member of the
// JSON object in data. Nothing is called if data does not hold an object.
func forEachJSONField(data []byte, fn func(key, value []byte)) {
	i := skipJSONSpace(data, 0)
	if i >= len(data) || data[i] != '{' {
		return
	}
	i++
	for {
		i = skipJSONSpace(data, i)
		if i >= len(data) || data[i] == '}' {
			return
		}
		if data[i] == ',' {
			i++
			continue
		}
		if data[i] != '"' {
			return
		}
		keyStart := i
		keyEnd := scanJSONString(data, i)
		i = skipJSONSpace(data, keyEnd)
		if i >= len(data) || data[i] != ':' {
			return
		}
		valStart := skipJSONSpace(data, i+1)
		valEnd := scanJSONValue(data, valStart)
		i = valEnd
		if keyEnd-1 <= keyStart {
			return
		}
		fn(data[keyStart+1:keyEnd-1], data[valStart:valEnd])
	}
}

// forEachJSONElement calls fn with the raw value of every element of the JSON
// array in data, until fn returns false. Nothing is called if data does not
// hold an array.
func forEachJSONElement(data []byte, fn func(value []byte) bool) {
	i := skipJSONSpace(data, 0)
	if i >= len(data) || data[i] != '[' {
		return
	}
	i++
	for {
		i = skipJSONSpace(data, i)
		if i >= len(data) || data[i] == ']' {
			return
		}
		if data[i] == ',' {
			i++
			continue
		}
		start := i
		i = scanJSONValue(data, i)
		if i == start {
			// A value that scans to nothing would spin here. Valid JSON never does.
			return
		}
		if !fn(data[start:i]) {
			return
		}
	}
}

// maxJSONDepth matches the nesting limit encoding/json enforces.
const maxJSONDepth = 10000

// validJSON reports whether data holds exactly one JSON value. It accepts and
// rejects exactly what json.Valid does, but walks string contents a word at a
// time instead of a byte at a time, which is most of the cost of a body whose
// bulk is one long string.
func validJSON(data []byte) bool {
	i, ok := scanValidJSON(data, skipJSONSpace(data, 0), 1)
	return ok && skipJSONSpace(data, i) == len(data)
}

// scanValidJSON validates the value beginning at data[i] and returns the offset
// just past it.
func scanValidJSON(data []byte, i, depth int) (int, bool) {
	if i >= len(data) {
		return i, false
	}
	switch data[i] {
	case '"':
		return scanValidJSONString(data, i)
	case '{':
		return scanValidJSONComposite(data, i, depth, '}')
	case '[':
		return scanValidJSONComposite(data, i, depth, ']')
	case 't':
		return scanJSONLiteral(data, i, "true")
	case 'f':
		return scanJSONLiteral(data, i, "false")
	case 'n':
		return scanJSONLiteral(data, i, "null")
	default:
		return scanValidJSONNumber(data, i)
	}
}

func scanJSONLiteral(data []byte, i int, lit string) (int, bool) {
	if len(data)-i < len(lit) || string(data[i:i+len(lit)]) != lit {
		return i, false
	}
	return i + len(lit), true
}

// scanValidJSONString validates the string whose opening quote is at data[i] and
// returns the offset just past its closing quote.
//
// Almost every string carries no escape, so bytes.IndexByte finds the closing
// quote and one word-wise pass rejects raw control characters. Invalid UTF-8 is
// content either way: json.Valid accepts it, because the v1 decoder substitutes
// U+FFFD rather than failing.
func scanValidJSONString(data []byte, i int) (int, bool) {
	i++ // opening quote
	n := bytes.IndexByte(data[i:], '"')
	if n < 0 {
		return len(data), false
	}
	// A backslash may escape that quote, so such a string is walked byte-wise.
	if span := data[i : i+n]; bytes.IndexByte(span, '\\') < 0 {
		if !controlFree(span) {
			return i, false
		}
		return i + n + 1, true
	}
	return scanValidJSONStringEscaped(data, i)
}

// controlFree reports whether b holds no byte below 0x20, eight bytes per test.
func controlFree(b []byte) bool {
	i := 0
	for ; i+8 <= len(b); i += 8 {
		if bitutil.HasLess(binary.NativeEndian.Uint64(b[i:]), 0x20) != 0 {
			return false
		}
	}
	for ; i < len(b); i++ {
		if b[i] < 0x20 {
			return false
		}
	}
	return true
}

func scanValidJSONStringEscaped(data []byte, i int) (int, bool) {
	for i < len(data) {
		switch c := data[i]; {
		case c == '"':
			return i + 1, true
		case c < 0x20:
			return i, false
		case c == '\\':
			var ok bool
			if i, ok = scanJSONEscape(data, i); !ok {
				return i, false
			}
		default:
			i++
		}
	}
	return len(data), false
}

// scanJSONEscape validates the escape sequence starting at the backslash at
// data[i] and returns the offset just past it.
func scanJSONEscape(data []byte, i int) (int, bool) {
	i++ // backslash
	if i >= len(data) {
		return i, false
	}
	switch data[i] {
	case '"', '\\', '/', 'b', 'f', 'n', 'r', 't':
		return i + 1, true
	case 'u':
		if i+5 > len(data) {
			return i, false
		}
		for _, c := range data[i+1 : i+5] {
			if !isHexDigit(c) {
				return i, false
			}
		}
		return i + 5, true
	default:
		return i, false
	}
}

func isHexDigit(c byte) bool {
	return isJSONDigit(c) || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')
}

// scanValidJSONComposite validates the object or array whose opening bracket is
// at data[i] and returns the offset just past close. Members carry a quoted key
// and a colon, elements do not.
func scanValidJSONComposite(data []byte, i, depth int, close byte) (int, bool) {
	// Only a composite is a level of nesting, so only it is counted.
	if depth > maxJSONDepth {
		return i, false
	}
	i = skipJSONSpace(data, i+1)
	if i < len(data) && data[i] == close {
		return i + 1, true
	}
	for {
		var ok bool
		if close == '}' {
			if i >= len(data) || data[i] != '"' {
				return i, false
			}
			if i, ok = scanValidJSONString(data, i); !ok {
				return i, false
			}
			i = skipJSONSpace(data, i)
			if i >= len(data) || data[i] != ':' {
				return i, false
			}
			i = skipJSONSpace(data, i+1)
		}
		if i, ok = scanValidJSON(data, i, depth+1); !ok {
			return i, false
		}
		i = skipJSONSpace(data, i)
		if i >= len(data) {
			return i, false
		}
		switch data[i] {
		case close:
			return i + 1, true
		case ',':
			i = skipJSONSpace(data, i+1)
		default:
			return i, false
		}
	}
}

// scanValidJSONNumber validates the number grammar of RFC 8259, section 6.
func scanValidJSONNumber(data []byte, i int) (int, bool) {
	if i < len(data) && data[i] == '-' {
		i++
	}
	switch {
	case i >= len(data):
		return i, false
	case data[i] == '0':
		i++
	case data[i] >= '1' && data[i] <= '9':
		i = skipJSONDigits(data, i)
	default:
		return i, false
	}
	if i < len(data) && data[i] == '.' {
		if i+1 >= len(data) || !isJSONDigit(data[i+1]) {
			return i, false
		}
		i = skipJSONDigits(data, i+1)
	}
	if i < len(data) && (data[i] == 'e' || data[i] == 'E') {
		i++
		if i < len(data) && (data[i] == '+' || data[i] == '-') {
			i++
		}
		if i >= len(data) || !isJSONDigit(data[i]) {
			return i, false
		}
		i = skipJSONDigits(data, i)
	}
	return i, true
}

func isJSONDigit(c byte) bool { return c >= '0' && c <= '9' }

func skipJSONDigits(data []byte, i int) int {
	for i < len(data) && isJSONDigit(data[i]) {
		i++
	}
	return i
}
