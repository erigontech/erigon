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
	"bytes"
	"io"
	"sync"

	"github.com/c2h5oh/datasize"

	jsoniter "github.com/json-iterator/go"
)

const (
	InitialBufferSize = 4096

	// FlushThreshold is where a stream hands its buffer over, so it bounds write syscalls at
	// one per this many bytes. It does not bound the buffer: flushIfFull checks the length
	// after the write that crossed it, and Get keeps whatever capacity the stream came back
	// with, so an in-flight response can hold up to maxPooledBufferSize.
	FlushThreshold = int(256 * datasize.KB)

	// maxPooledBufferSize bounds what a stream carries back into the pool, and with it how much
	// an in-flight response may hold. It needs headroom over FlushThreshold: a stream that
	// crossed the threshold has already grown past it, so at cap == threshold exactly the
	// streaming responses are dropped and only small ones stay pooled.
	maxPooledBufferSize = int(1 * datasize.MB)
)

// MaxPooledBufferSize is what Put admits back into the pool.
func MaxPooledBufferSize() int { return maxPooledBufferSize }

const _ = uint(maxPooledBufferSize - 2*FlushThreshold)

// flushIfFull hands the buffer over once it is full, so a large response streams
// instead of being held whole.
func flushIfFull(stream *jsoniter.Stream) {
	if len(stream.Buffer()) >= FlushThreshold {
		flushFull(stream)
	}
}

// flushFull is outlined so the threshold check stays inlinable into every value write.
//
//go:noinline
func flushFull(stream *jsoniter.Stream) {
	if err := stream.Flush(); err != nil {
		// Discarded, not retried: jsoniter latches err on the stream, so every
		// later Flush returns it without draining and these bytes can never
		// reach the client. Keeping them would hold the whole response for a
		// client that stopped reading, which is what this bound exists to prevent.
		stream.SetBuffer(stream.Buffer()[:0])
	}
}

// New builds an unpooled stream. Request paths use Get.
func New(out io.Writer) Stream {
	return newStackStream(out, InitialBufferSize)
}

var streamPool = sync.Pool{New: func() any { return newStackStream(nil, InitialBufferSize) }}

// Get is New over a pool. Put the stream back once its bytes have left it;
// skipping Put only costs the recycling.
func Get(out io.Writer) *StackStream {
	s := streamPool.Get().(*StackStream)
	s.Reset(out)
	return s
}

// Put returns a stream to the pool. The caller must hold no view of Buffer()
// afterwards, and must not write to the stream again.
func Put(s Stream) {
	ss, ok := s.(*StackStream)
	if !ok || cap(ss.stream.Buffer()) > maxPooledBufferSize {
		return
	}
	ss.Reset(nil) // the writer goes too, so an idle stream pins no connection
	streamPool.Put(ss)
}

// Marshal encodes v into a byte slice the caller owns.
func Marshal(v interface{ MarshalFastJSONTo(*StackStream) error }) ([]byte, error) {
	s := Get(nil)
	defer Put(s)
	if err := v.MarshalFastJSONTo(s); err != nil {
		return nil, err
	}
	if err := s.Err(); err != nil { // a latched write error left a placeholder in the buffer
		return nil, err
	}
	return bytes.Clone(s.Buffer()), nil
}
