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

package pool

import (
	"bytes"
	"sync"
)

// Buffers that grew beyond maxBufferCap are dropped on Put so that one oversized use cannot pin memory in the pool.
const maxBufferCap = 1 << 20

var buffers = sync.Pool{New: func() any { return new(bytes.Buffer) }}

// GetBuffer returns an empty buffer from the shared pool.
func GetBuffer() *bytes.Buffer {
	b := buffers.Get().(*bytes.Buffer)
	b.Reset()
	return b
}

func PutBuffer(b *bytes.Buffer) {
	if b.Cap() > maxBufferCap {
		return
	}
	buffers.Put(b)
}
