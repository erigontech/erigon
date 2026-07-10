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

package commitment

// byteArena hands out stable copies of byte slices. A full chunk is replaced,
// never rewound, so slices handed out earlier stay intact even after reset —
// consumers may retain them for as long as they need (GC keeps the chunk
// alive). reset only stops further appends into the current chunk.
type byteArena struct {
	buf []byte
}

const byteArenaChunk = 64 * 1024

func (a *byteArena) intern(b []byte) []byte {
	if len(b) > byteArenaChunk {
		out := make([]byte, len(b))
		copy(out, b)
		return out
	}
	if cap(a.buf)-len(a.buf) < len(b) {
		a.buf = make([]byte, 0, byteArenaChunk)
	}
	off := len(a.buf)
	a.buf = append(a.buf, b...)
	return a.buf[off : off+len(b) : off+len(b)]
}

// copyBytes is intern with common.Copy's contract: nil in, nil out; empty in, empty out.
func (a *byteArena) copyBytes(b []byte) []byte {
	if len(b) == 0 {
		if b == nil {
			return nil
		}
		return []byte{}
	}
	return a.intern(b)
}

func (a *byteArena) reset() { a.buf = nil }
