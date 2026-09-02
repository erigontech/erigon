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

// Package snappypool pools the stream codecs of github.com/golang/snappy.
package snappypool

import (
	"io"
	"sync"

	"github.com/golang/snappy"
)

var (
	readers = sync.Pool{New: func() any { return snappy.NewReader(nil) }}
	writers = sync.Pool{New: func() any { return snappy.NewBufferedWriter(nil) }}
)

func Reader(r io.Reader) *snappy.Reader {
	sr := readers.Get().(*snappy.Reader)
	sr.Reset(r)
	return sr
}

func PutReader(sr *snappy.Reader) {
	sr.Reset(nil)
	readers.Put(sr)
}

func Writer(w io.Writer) *snappy.Writer {
	sw := writers.Get().(*snappy.Writer)
	sw.Reset(w)
	return sw
}

func PutWriter(sw *snappy.Writer) {
	sw.Reset(nil)
	writers.Put(sw)
}
