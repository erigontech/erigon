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
	"slices"
	"strconv"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common/hexutil"
)

type textPtr[T any] interface {
	*T
	encoding.TextAppender
}

// Text writes v's text as a JSON string field, or null when v is nil.
func Text[T any, P textPtr[T]](s *Stream, name string, v P) {
	s.Field(name)
	if v == nil {
		s.WriteNil()
		return
	}
	s.WriteQuotedText(v)
}

// ArrayValue writes the array itself, with no field name, for a result that is a bare
// array. A nil slice is null and an empty one is [].
func ArrayValue[S ~[]E, E any](s *Stream, items S, elem func(*Stream, *E)) {
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

// HexUint64 writes 0x and the shortest lowercase hex of v. The digits go straight into
// the buffer, so no hexutil value is built for the call and nothing can escape. Which fields
// are written this way is the caller's rule, not the stream's: see rpc/jsonstream/ethjson.
func HexUint64(s *Stream, v uint64) {
	s.beforeValue()
	buf := s.stream.Buffer()
	start := len(buf)
	buf = strconv.AppendUint(append(buf, '"', '0', 'x'), v, 16)
	s.commit(append(buf, '"'), start)
	s.afterValue()
}

// HexUint64Field writes a field name and its HexUint64 value in one step, so the name is never
// left on the stack waiting for a value.
func HexUint64Field(s *Stream, name string, v uint64) {
	s.beforeValue()
	writeObjectFieldFast(s.stream, name)
	buf := s.stream.Buffer()
	start := len(buf)
	buf = strconv.AppendUint(append(buf, '"', '0', 'x'), v, 16)
	s.commit(append(buf, '"'), start)
	s.separatorPending = true
	flushIfFull(s.stream)
}

// HexField writes a field name and the 0x-prefixed hex of b in one step, as HexUint64Field does.
func HexField(s *Stream, name string, b []byte) {
	s.beforeValue()
	writeObjectFieldFast(s.stream, name)
	buf := s.stream.Buffer()
	start := len(buf)
	buf = hexutil.AppendQuoted(slices.Grow(buf, hexutil.QuotedLen(len(b))), b)
	s.commit(buf, start)
	s.separatorPending = true
	flushIfFull(s.stream)
}

// HexUint256Field writes a field name and its HexUint256 value in one step, null for a nil one.
func HexUint256Field(s *Stream, name string, v *uint256.Int) {
	s.beforeValue()
	writeObjectFieldFast(s.stream, name)
	buf := s.stream.Buffer()
	start := len(buf)
	if v == nil {
		buf = append(buf, "null"...)
	} else {
		buf, _ = hexutil.U256(*v).AppendText(append(buf, '"'))
		buf = append(buf, '"')
	}
	s.commit(buf, start)
	s.separatorPending = true
	flushIfFull(s.stream)
}

// HexUint256 does the same for a 256-bit value, null for a nil one.
func HexUint256(s *Stream, v *uint256.Int) {
	if v == nil {
		s.WriteNil()
		return
	}
	s.beforeValue()
	buf := s.stream.Buffer()
	start := len(buf)
	buf, _ = hexutil.U256(*v).AppendText(append(buf, '"'))
	s.commit(append(buf, '"'), start)
	s.afterValue()
}
