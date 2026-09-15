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

// Package snappypool pools the stream codecs of github.com/golang/snappy. Each
// NewReader or NewBufferedWriter allocates ~139 KiB of scratch buffers, which is
// worth reusing on paths that codec one message at a time.
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

// Reader returns a pooled reader bound to r. Reset it again for every frame
// stream, since a wire response carries a separate stream per chunk.
func Reader(r io.Reader) *snappy.Reader {
	sr := readers.Get().(*snappy.Reader)
	sr.Reset(r)
	return sr
}

// PutReader parks sr in the pool. Reset(nil) keeps it from pinning the caller's
// reader while parked.
func PutReader(sr *snappy.Reader) {
	sr.Reset(nil)
	readers.Put(sr)
}

func Writer(w io.Writer) *snappy.Writer {
	sw := writers.Get().(*snappy.Writer)
	sw.Reset(w)
	return sw
}

// PutWriter parks sw in the pool. Flush first: Reset drops whatever is still
// buffered.
func PutWriter(sw *snappy.Writer) {
	sw.Reset(nil)
	writers.Put(sw)
}
