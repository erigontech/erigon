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

import "fmt"

// EIP-8297's code embedding (eip:349-397).
const (
	// pbinChunkDataLen is how much code one chunk holds; byte 0 of the 32-byte
	// value carries the PUSHDATA count instead.
	pbinChunkDataLen = pbinValueLength - 1

	// pbinHeaderCodeChunks are the chunks the account header holds, at
	// sub-indices CODE_OFFSET..255. Higher chunks live in the code zone.
	pbinHeaderCodeChunks = pbinStemSubtreeWidth - pbinCodeOffset

	pbinPushOffset = 95
	pbinPush1      = pbinPushOffset + 1
	pbinPush32     = pbinPushOffset + 32
)

// pbinChunkifyCode splits code into the tree's chunk values (eip:374-397). Byte
// 0 of a chunk counts how many of its leading bytes are PUSHDATA, so the scan
// runs over the whole code and residual PUSHDATA carries across chunk
// boundaries. Padding to a multiple of 31 happens before the scan, which is what
// makes a PUSH whose data runs off the end count against the padded tail.
func pbinChunkifyCode(code []byte) [][pbinValueLength]byte {
	if len(code) == 0 {
		return nil
	}
	padded := code
	if rem := len(code) % pbinChunkDataLen; rem != 0 {
		padded = make([]byte, len(code)+pbinChunkDataLen-rem)
		copy(padded, code)
	}

	// pushdataAt[i] is how many bytes from i on are still PUSHDATA. The spec sizes
	// it a whole chunk past the code so a PUSH32 on the last byte has room.
	pushdataAt := make([]byte, len(padded)+pbinValueLength)
	for pos := 0; pos < len(padded); {
		var pushdata int
		if padded[pos] >= pbinPush1 && padded[pos] <= pbinPush32 {
			pushdata = int(padded[pos]) - pbinPushOffset
		}
		pos++
		for x := range pushdata {
			pushdataAt[pos+x] = byte(pushdata - x)
		}
		pos += pushdata
	}

	chunks := make([][pbinValueLength]byte, 0, len(padded)/pbinChunkDataLen)
	for pos := 0; pos < len(padded); pos += pbinChunkDataLen {
		var chunk [pbinValueLength]byte
		chunk[0] = min(pushdataAt[pos], pbinChunkDataLen)
		copy(chunk[1:], padded[pos:pos+pbinChunkDataLen])
		chunks = append(chunks, chunk)
	}
	return chunks
}

// pbinRecordLeafValue is the value a leaf carries itself rather than deriving
// from state — a code chunk, or a sub-index the embedding reserves and defines
// no packing for. Unlike a storage value it is not left-padded into place: a
// chunk is positional, byte 0 being the PUSHDATA count, so a short value is an
// error.
func pbinRecordLeafValue(u *Update) ([pbinValueLength]byte, error) {
	if u.StorageLen != pbinValueLength {
		return [pbinValueLength]byte{}, fmt.Errorf("%w: record-resident leaf holds %d value bytes, want %d",
			errPBinCellHash, u.StorageLen, pbinValueLength)
	}
	return u.Storage, nil
}
