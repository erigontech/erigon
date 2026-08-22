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
	"io"

	jsoniter "github.com/json-iterator/go"
)

const AutoCloseOnError = true
const InitialBufferSize = 4096

const (
	escapeLo = ^uint64(0) / 255
	escapeHi = ^uint64(0) / 255 * 128
)

func hasLess(x, n uint64) uint64 { return (x - escapeLo*n) &^ x & escapeHi }
func hasByte(x, n uint64) uint64 { v := x ^ (escapeLo * n); return (v - escapeLo) &^ v & escapeHi }

// needsEscape reports whether val contains a byte jsoniter would escape, reading
// eight at a time. The stdlib has no primitive for this: IndexByte is SIMD but
// takes one byte, and ContainsAny rebuilds its ASCII set on every call.
func needsEscape(val string) bool {
	for len(val) >= 8 {
		x := binary.LittleEndian.Uint64([]byte(val[:8]))
		if hasLess(x, 0x20)|hasByte(x, '"')|hasByte(x, '\\') != 0 {
			return true
		}
		val = val[8:]
	}
	for i := 0; i < len(val); i++ {
		if c := val[i]; c < 0x20 || c == '"' || c == '\\' {
			return true
		}
	}
	return false
}

// writeStringFast copies a string that needs no escaping in one go. jsoniter
// appends it a byte at a time, testing each one, so its cost grows with length
// where a scan plus a copy does not. Under eight bytes there is no word to scan
// and its loop is already the cheaper option.
func writeStringFast(stream *jsoniter.Stream, val string) {
	if len(val) < 8 || needsEscape(val) {
		stream.WriteString(val)
		return
	}
	buf := append(stream.Buffer(), '"')
	buf = append(buf, val...)
	stream.SetBuffer(append(buf, '"'))
}

func New(out io.Writer) Stream {
	stream := jsoniter.NewStream(jsoniter.ConfigDefault, out, InitialBufferSize)
	if AutoCloseOnError {
		return NewStackStream(stream)
	}
	return NewJsoniterStream(stream)
}

func Wrap(stream *jsoniter.Stream) Stream {
	if AutoCloseOnError {
		return NewStackStream(stream)
	}
	return NewJsoniterStream(stream)
}
