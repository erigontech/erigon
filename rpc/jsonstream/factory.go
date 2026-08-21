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
	"io"
	"sync"

	"github.com/c2h5oh/datasize"
	jsoniter "github.com/json-iterator/go"
)

const AutoCloseOnError = true
const InitialBufferSize = 4096

// FlushThreshold is how much a response buffers before it is handed to the
// writer. Flushing in smaller pieces costs a socket write each, because bufio
// only bypasses its own buffer for writes larger than it holds.
const FlushThreshold = int(64 * datasize.KB)

// maxPooledBuffer caps what goes back into the pool: a trace of an attack
// transaction reaches gigabytes, and pooling it would pin that for the process.
const maxPooledBuffer = int(1 * datasize.MB)

// Pooled streams start at the size they flush at, so a response never grows.
var streamPool = sync.Pool{New: func() any {
	return NewStackStream(jsoniter.NewStream(jsoniter.ConfigDefault, nil, FlushThreshold))
}}

// New returns a stream writing to out. Not calling Release is safe, it only
// forfeits the reuse. A nil out is never pooled: that stream has nowhere to
// flush, so the caller takes its buffer via Buffer().
func New(out io.Writer) Stream {
	if out == nil || !AutoCloseOnError {
		return Wrap(jsoniter.NewStream(jsoniter.ConfigDefault, out, InitialBufferSize))
	}
	s := streamPool.Get().(*StackStream)
	s.reset(out)
	s.pooled = true
	return s
}

// Release hands the stream back for reuse. It must not be used afterwards, and
// nothing may still hold a slice of its buffer.
func Release(s Stream) {
	ss, ok := s.(*StackStream)
	if !ok || !ss.pooled {
		return
	}
	ss.pooled = false
	if c := cap(ss.stream.Buffer()); c < InitialBufferSize || c > maxPooledBuffer {
		return
	}
	ss.reset(nil)
	streamPool.Put(ss)
}

func Wrap(stream *jsoniter.Stream) Stream {
	if AutoCloseOnError {
		return NewStackStream(stream)
	}
	return NewJsoniterStream(stream)
}
