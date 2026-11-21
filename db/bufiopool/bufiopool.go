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

// Package bufiopool pools 512KB bufio readers and writers. Erigon doesn't
// create tons of bufio readers/writers, but it has tons of parallel small
// unit-tests which each create many small files and bufio readers/writers —
// pooling avoids the allocation pressure in that scenario.
package bufiopool

import (
	"bufio"
	"io"
	"sync"

	"github.com/c2h5oh/datasize"
)

var (
	writers = sync.Pool{New: func() any { return bufio.NewWriterSize(nil, int(512*datasize.KB)) }}
	readers = sync.Pool{New: func() any { return bufio.NewReaderSize(nil, int(512*datasize.KB)) }}
)

func Writer(w io.Writer) *bufio.Writer {
	bw := writers.Get().(*bufio.Writer)
	bw.Reset(w)
	return bw
}

// Reset(nil) before Put is required: without it the pool entry retains a
// reference to the underlying io.Writer/io.Reader, keeping it alive until the
// next GC cycle or until the entry is reused — whichever comes first.
func PutWriter(w *bufio.Writer) { w.Reset(nil); writers.Put(w) }

func Reader(r io.Reader) *bufio.Reader {
	br := readers.Get().(*bufio.Reader)
	br.Reset(r)
	return br
}

func PutReader(r *bufio.Reader) { r.Reset(nil); readers.Put(r) }
