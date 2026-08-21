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
	"slices"

	jsoniter "github.com/json-iterator/go"
)

// writeHexBody encodes b as hex into the stream's own buffer. Going through an
// intermediate array would push that array to the heap, since Write takes the
// slice through an interface.
func writeHexBody(stream *jsoniter.Stream, b []byte) {
	buf := stream.Buffer()
	n := len(buf)
	buf = slices.Grow(buf, len(b)*2)[:n+len(b)*2]
	hex.Encode(buf[n:], b)
	stream.SetBuffer(buf)
}
