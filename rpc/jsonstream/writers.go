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

// HexUint64Field writes a field name and v as a 0x-prefixed hex quantity in one step, so the name
// is never left on the stack waiting for a value.
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

// HexUint256Field writes a field name and v as a 0x-prefixed hex quantity in one step, or null
// when v is nil.
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

// HexKey writes the 0x-prefixed hex of b as a field name, for an object keyed by hashes or
// addresses, without building the name as a string first.
func HexKey(s *Stream, b []byte) {
	s.beforeValue()
	buf := hexutil.AppendQuoted(slices.Grow(s.stream.Buffer(), hexutil.QuotedLen(len(b))+1), b)
	s.stream.SetBuffer(append(buf, ':'))
	s.push(ItemField)
}
