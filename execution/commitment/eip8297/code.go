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

package eip8297

// EIP-8297's code embedding (eip:"Code").
const (
	// ChunkDataLen is how much code one chunk holds; byte 0 of the 32-byte
	// value carries the PUSHDATA count instead.
	ChunkDataLen = ValueLength - 1

	PushOffset = 95
	Push1      = PushOffset + 1
	Push32     = PushOffset + 32
)

// ChunkifyCode splits code into the tree's chunk values (eip:"Code"). The
// PUSHDATA scan runs over the whole code, so residual PUSHDATA carries across
// chunk boundaries. Padding to a multiple of 31 happens before the scan, which
// is what makes a PUSH whose data runs off the end count against the padded tail.
func ChunkifyCode(code []byte) [][ValueLength]byte {
	var s ChunkScratch
	return s.Chunkify(code)
}

type ChunkScratch struct {
	padded     []byte
	pushdataAt []byte
	chunks     [][ValueLength]byte
}

func (s *ChunkScratch) Chunkify(code []byte) [][ValueLength]byte {
	if len(code) == 0 {
		return nil
	}
	padded := code
	if rem := len(code) % ChunkDataLen; rem != 0 {
		s.padded = append(s.padded[:0], code...)
		for range ChunkDataLen - rem {
			s.padded = append(s.padded, 0)
		}
		padded = s.padded
	}

	// pushdataAt[i] is how many bytes from i on are still PUSHDATA. It runs a whole
	// chunk past the code so a PUSH32 on the last byte has room.
	if cap(s.pushdataAt) < len(padded)+ValueLength {
		s.pushdataAt = make([]byte, len(padded)+ValueLength)
	}
	pushdataAt := s.pushdataAt[:len(padded)+ValueLength]
	clear(pushdataAt)
	for pos := 0; pos < len(padded); {
		var pushdata int
		if padded[pos] >= Push1 && padded[pos] <= Push32 {
			pushdata = int(padded[pos]) - PushOffset
		}
		pos++
		for x := range pushdata {
			pushdataAt[pos+x] = byte(pushdata - x)
		}
		pos += pushdata
	}

	chunks := s.chunks[:0]
	for pos := 0; pos < len(padded); pos += ChunkDataLen {
		var chunk [ValueLength]byte
		chunk[0] = min(pushdataAt[pos], ChunkDataLen)
		copy(chunk[1:], padded[pos:pos+ChunkDataLen])
		chunks = append(chunks, chunk)
	}
	s.chunks = chunks
	return chunks
}
