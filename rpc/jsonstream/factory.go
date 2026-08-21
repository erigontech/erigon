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

// FlushThreshold bounds how much a response buffers. It is chosen for memory,
// not speed: with --http.compression, which defaults on, the size makes no
// difference at all - gzhttp buffers on its own terms, and sweeping 4KiB to 1MiB
// gives the same 774 socket writes and the same 25.6ms for an 8MB response,
// which gzip itself dominates. Uncompressed it does matter, 2035 socket writes
// and 5.9ms at 4KiB against 257 and 1.3ms here, flattening past 256KiB - not
// worth four times the memory per concurrent stream for a non-default path.
const FlushThreshold = int(64 * datasize.KB)

// appendAndFlush appends raw bytes to the buffer instead of calling
// jsoniter.Stream.Write, which exists to satisfy io.Writer: it writes through on
// every call and reslices the buffer forward to keep whatever a short write left,
// leaving nothing to append into. On a response written item by item that costs a
// realloc each time, and defeats flushing on a full buffer. The latched error is
// reported so callers that abandon expensive work on a write failure keep working.
func appendAndFlush(stream *jsoniter.Stream, content []byte) (int, error) {
	stream.SetBuffer(append(stream.Buffer(), content...))
	flushIfFull(stream)
	return len(content), stream.Error
}

// flushIfFull hands the buffer over once it is full, so a large response streams
// instead of being held whole. Bytes that fail to reach the client are dropped:
// they can never be delivered, and every later Flush short-circuits on
// stream.Error without draining, so keeping them would buffer the whole failed
// response. stream.Error keeps the failure itself.
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
