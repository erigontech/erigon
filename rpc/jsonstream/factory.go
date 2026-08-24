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

// FlushThreshold bounds how much of a response is held in memory at once. A
// trace can run to gigabytes, and nothing above this layer flushes inside one.
const FlushThreshold = int(64 * datasize.KB)

// flushIfFull hands the buffer over once it is full, so a large response streams
// instead of being held whole.
func flushIfFull(stream *jsoniter.Stream) {
	if len(stream.Buffer()) < FlushThreshold {
		return
	}
	if err := stream.Flush(); err != nil {
		// Discarded, not retried: jsoniter latches err on the stream, so every
		// later Flush returns it without draining and these bytes can never
		// reach the client. Keeping them would hold the whole response for a
		// client that stopped reading, which is what this bound exists to prevent.
		stream.SetBuffer(stream.Buffer()[:0])
	}
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
