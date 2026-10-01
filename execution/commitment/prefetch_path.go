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

import (
	"encoding/binary"

	"github.com/erigontech/erigon/execution/commitment/nibbles"
)

func PrefetchBranchPath(hashedKey []byte, read func(prefix []byte) []byte) {
	var compactBuf [maxCompactKeyLen]byte
	for depth := 0; depth < len(hashedKey); {
		data := read(nibbles.HexToCompactInto(compactBuf[:], hashedKey[:depth]))
		if len(data) < 4 {
			return
		}
		data = data[2:]
		bitmap := binary.BigEndian.Uint16(data)
		nib := int(hashedKey[depth])
		if bitmap&(1<<nib) == 0 {
			return
		}
		pos := 2
		for n := range nib {
			if bitmap&(1<<n) != 0 {
				if pos >= len(data) {
					return
				}
				pos = skipCellFields(data, pos+1, data[pos])
			}
		}
		if pos >= len(data) {
			return
		}
		fields := cellFields(data[pos])
		ext := 0
		if fields&fieldExtension != 0 {
			if l, n := binary.Uvarint(data[pos+1:]); n > 0 {
				ext = int(l)
			}
		}
		switch {
		case fields&fieldAccountAddr != 0 && depth < 64:
			depth = 64 + ext
		case fields&(fieldAccountAddr|fieldStorageAddr) != 0:
			return
		default:
			depth += 1 + ext
		}
	}
}
