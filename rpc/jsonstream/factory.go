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

// FlushThreshold is how much a response may buffer before it is handed to the
// writer. net/http buffers behind us, and bufio only bypasses its 4KB buffer for
// writes larger than it holds, so flushing in small pieces turns every flush into
// its own socket write. Measured on a 64MB response: 4KB gives 16417 socket
// writes and 59ms, 64KB gives 2049 and 20ms, 1MB gives 129 and 12ms. 64KB keeps
// the per-request cost bounded at high concurrency.
const FlushThreshold = int(64 * datasize.KB)

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
