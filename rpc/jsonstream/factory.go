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

	"github.com/c2h5oh/datasize"
	jsoniter "github.com/json-iterator/go"
)

const AutoCloseOnError = true
const InitialBufferSize = 4096

// FlushThreshold bounds how much a response buffers. Chosen for memory, not
// speed: under --http.compression, which defaults on, gzhttp buffers on its own
// terms and the size makes no difference; uncompressed it does, but flattens
// past 256KiB. BenchmarkStreamFlushThreshold sweeps it.
const FlushThreshold = int(64 * datasize.KB)

// appendAndFlush appends instead of calling jsoniter.Stream.Write, which
// implements io.Writer and so writes through on every call, reslicing the buffer
// forward and leaving nothing to append into or to hand over. It reports the
// latched error so callers that abandon expensive work on a failure still see one.
func appendAndFlush(stream *jsoniter.Stream, content []byte) (int, error) {
	stream.SetBuffer(append(stream.Buffer(), content...))
	flushIfFull(stream)
	return len(content), stream.Error
}

// flushIfFull hands the buffer over once it is full, so a large response streams
// instead of being held whole. Undeliverable bytes are dropped: a later Flush
// short-circuits on stream.Error without draining, so keeping them would buffer
// the whole failed response. stream.Error keeps the failure.
func flushIfFull(stream *jsoniter.Stream) {
	if len(stream.Buffer()) < FlushThreshold {
		return
	}
	if stream.Flush() != nil {
		stream.SetBuffer(stream.Buffer()[:0])
	}
}

func New(out io.Writer) Stream {
	return Wrap(jsoniter.NewStream(jsoniter.ConfigDefault, out, InitialBufferSize))
}

func Wrap(stream *jsoniter.Stream) Stream {
	if AutoCloseOnError {
		return NewStackStream(stream)
	}
	return NewJsoniterStream(stream)
}
