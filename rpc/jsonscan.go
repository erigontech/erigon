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
	i, ok := scanValidJSON(data, skipJSONSpace(data, 0))
	return ok && skipJSONSpace(data, i) == len(data)
}

// scanValidJSON validates the value beginning at data[i] and returns the offset
// just past it. Open composites are kept as closing brackets in a byte stack, not
// in call frames: a body at the nesting limit is ~20 KB and would otherwise grow
// the request goroutine's stack by megabytes.
func scanValidJSON(data []byte, i int) (int, bool) {
	var buf [32]byte
	closers := buf[:0]
	for {
		if i >= len(data) {
			return i, false
		}
		var ok bool
		switch c := data[i]; c {
		case '{', '[':
			// Only a composite is a level of nesting, so only it is counted.
			if len(closers) >= maxJSONDepth {
				return i, false
			}
			closer := byte(']')
			if c == '{' {
				closer = '}'
			}
			i = skipJSONSpace(data, i+1)
			if i < len(data) && data[i] == closer {
				i, ok = i+1, true
				break
			}
			closers = append(closers, closer)
			if closer == '}' {
				if i, ok = scanJSONMemberKey(data, i); !ok {
					return i, false
				}
			}
			continue
		case '"':
			i, ok = scanValidJSONString(data, i)
		case 't':
			i, ok = scanJSONLiteral(data, i, "true")
		case 'f':
			i, ok = scanJSONLiteral(data, i, "false")
		case 'n':
			i, ok = scanJSONLiteral(data, i, "null")
		default:
			i, ok = scanValidJSONNumber(data, i)
		}
		if !ok {
			return i, false
		}
		// A value ended at i: close the composites it completes, or move on to the
		// next member of the innermost one.
		for {
			if len(closers) == 0 {
				return i, true
			}
			i = skipJSONSpace(data, i)
			if i >= len(data) {
				return i, false
			}
			closer := closers[len(closers)-1]
			if data[i] == closer {
				closers = closers[:len(closers)-1]
				i++
				continue
			}
			if data[i] != ',' {
				return i, false
			}
			i = skipJSONSpace(data, i+1)
			if closer == '}' {
				if i, ok = scanJSONMemberKey(data, i); !ok {
					return i, false
				}
			}
			break
		}
	}
}

// scanJSONMemberKey validates an object member's quoted key and colon at data[i]
// and returns the offset of its value.
func scanJSONMemberKey(data []byte, i int) (int, bool) {
	if i >= len(data) || data[i] != '"' {
		return i, false
	}
	i, ok := scanValidJSONString(data, i)
	if !ok {
		return i, false
	}
	i = skipJSONSpace(data, i)
	if i >= len(data) || data[i] != ':' {
		return i, false
	}
	return skipJSONSpace(data, i+1), true
}

func scanJSONLiteral(data []byte, i int, lit string) (int, bool) {
	if len(data)-i < len(lit) || string(data[i:i+len(lit)]) != lit {
		return i, false
	}
	return i + len(lit), true
}

// Invalid UTF-8 counts as string content below: json.Valid accepts it, because
// the v1 decoder substitutes U+FFFD rather than failing.
const (
	lowBits  = 0x0101010101010101
	highBits = 0x8080808080808080
)

// wordInteresting reports whether any of the eight bytes in v needs a closer
// look. The three masks are joined before the test so the loop branches once.
func wordInteresting(v uint64) bool {
	q := v ^ ('"' * lowBits)
	b := v ^ ('\\' * lowBits)
	return ((v-0x20*lowBits)&^v|(q-lowBits)&^q|(b-lowBits)&^b)&highBits != 0
}

// scanValidJSONString validates the string whose opening quote is at data[i] and
// returns the offset just past its closing quote.
func scanValidJSONString(data []byte, i int) (int, bool) {
	i++ // opening quote
	for i < len(data) {
		if i+8 <= len(data) {
			if !wordInteresting(binary.NativeEndian.Uint64(data[i:])) {
				i += 8
				continue
			}
		}
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
