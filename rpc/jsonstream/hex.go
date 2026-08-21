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

package jsonstream

import (
	"encoding/hex"

	jsoniter "github.com/json-iterator/go"
)

// hexWriter encodes hex for a stream. The scratch buffer belongs to the stream
// so it never escapes per call, and going through Stream.Write keeps draining
// to the underlying writer — encoding straight into the stream's buffer would
// let a long trace accumulate the whole response in memory.
type hexWriter struct {
	scratch []byte
}

func (h *hexWriter) writeBody(stream *jsoniter.Stream, b []byte) {
	need := len(b) * 2
	if cap(h.scratch) < need {
		h.scratch = make([]byte, need)
	}
	h.scratch = h.scratch[:need]
	hex.Encode(h.scratch, b)
	stream.Write(h.scratch) //nolint:errcheck
}
