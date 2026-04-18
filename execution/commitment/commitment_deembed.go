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

package commitment

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math/bits"

	"github.com/erigontech/erigon/common/dbg"
)

// DeEmbedCommitment controls whether branch nodes are stored with each of the
// 16 possible children split into its own key. With this flag on, changing one
// child's encoding only rewrites that child's row in history rather than the
// entire branch blob. The flag is a package-level variable so tests can toggle
// it; production callers should set COMMITMENT_DEEMBED in the environment.
var DeEmbedCommitment = dbg.EnvBool("COMMITMENT_DEEMBED", false)

// DeEmbedMetaMarker is the byte appended to a compact branch prefix to form
// its metadata key in de-embedded mode. It is chosen > 0x0F so that it can
// never collide with a child key, which appends a nibble (0x00-0x0F).
const DeEmbedMetaMarker byte = 0xFF

// DeEmbedMetaKey returns compact(P) || [DeEmbedMetaMarker].
// buf is reused for the result; caller owns the returned slice.
func DeEmbedMetaKey(compactPrefix []byte, buf []byte) []byte {
	buf = append(buf[:0], compactPrefix...)
	buf = append(buf, DeEmbedMetaMarker)
	return buf
}

// DeEmbedChildKey returns compact(P) || [nibble] for nibble in 0x00-0x0F.
// buf is reused for the result; caller owns the returned slice.
func DeEmbedChildKey(compactPrefix []byte, nibble byte, buf []byte) []byte {
	buf = append(buf[:0], compactPrefix...)
	buf = append(buf, nibble)
	return buf
}

// BuildDeEmbedMetaValue serialises a branch's touchMap and afterMap into the
// 4-byte metadata value.
func BuildDeEmbedMetaValue(touchMap, afterMap uint16, buf []byte) []byte {
	if cap(buf) < 4 {
		buf = make([]byte, 4)
	} else {
		buf = buf[:4]
	}
	binary.BigEndian.PutUint16(buf[0:2], touchMap)
	binary.BigEndian.PutUint16(buf[2:4], afterMap)
	return buf
}

// ParseDeEmbedMetaValue decodes the 4-byte metadata value.
func ParseDeEmbedMetaValue(data []byte) (touchMap, afterMap uint16, err error) {
	if len(data) < 4 {
		return 0, 0, fmt.Errorf("de-embed metadata too short: %d bytes", len(data))
	}
	touchMap = binary.BigEndian.Uint16(data[0:2])
	afterMap = binary.BigEndian.Uint16(data[2:4])
	return
}

// SplitBranchDataIntoChildren parses a full branch blob and returns its
// touchMap, afterMap, and per-child cell byte slices. For each bit set in
// touchMap&afterMap, cells[nibble] is a slice into data covering that cell's
// [fields:1][field_data...]. Other slots are nil.
func SplitBranchDataIntoChildren(data []byte) (touchMap, afterMap uint16, cells [16][]byte, err error) {
	if len(data) < 4 {
		return 0, 0, cells, fmt.Errorf("branch data too short: %d bytes", len(data))
	}
	touchMap = binary.BigEndian.Uint16(data[0:2])
	afterMap = binary.BigEndian.Uint16(data[2:4])
	pos := 4
	for bitset := touchMap; bitset != 0; {
		bit := bitset & -bitset
		nibble := bits.TrailingZeros16(bit)
		bitset ^= bit
		if afterMap&bit == 0 {
			continue
		}
		if pos >= len(data) {
			return 0, 0, cells, fmt.Errorf("branch data truncated at nibble %d pos %d/%d", nibble, pos, len(data))
		}
		start := pos
		fieldBits := data[pos]
		pos++
		pos, err = advancePastCellFields(data, pos, fieldBits)
		if err != nil {
			return 0, 0, cells, fmt.Errorf("split at nibble %d: %w", nibble, err)
		}
		cells[nibble] = data[start:pos]
	}
	return touchMap, afterMap, cells, nil
}

// ReassembleBranchData rebuilds a BranchData blob from metadata maps and
// per-child cell byte slices. The output byte layout matches the embedded
// encoding produced by BranchEncoder.EncodeBranch.
func ReassembleBranchData(touchMap, afterMap uint16, cells [16][]byte, buf []byte) (BranchData, error) {
	bitmap := touchMap & afterMap
	buf = buf[:0]
	var hdr [4]byte
	binary.BigEndian.PutUint16(hdr[0:2], touchMap)
	binary.BigEndian.PutUint16(hdr[2:4], afterMap)
	buf = append(buf, hdr[:]...)
	for bitset := bitmap; bitset != 0; {
		bit := bitset & -bitset
		nibble := bits.TrailingZeros16(bit)
		bitset ^= bit
		cellBytes := cells[nibble]
		if len(cellBytes) == 0 {
			return nil, fmt.Errorf("missing cell data for nibble %d while reassembling branch", nibble)
		}
		buf = append(buf, cellBytes...)
	}
	return buf, nil
}

// advancePastCellFields advances pos past the fields indicated by fieldBits,
// returning the new position or an error if the data is truncated or malformed.
// Field layout per flag: [varint length][data]. Flags are:
//
//	1=extension, 2=accountAddr, 4=storageAddr, 8=hash, 16=stateHash
func advancePastCellFields(data []byte, pos int, fieldBits byte) (int, error) {
	for flag := byte(1); flag <= 16; flag <<= 1 {
		if fieldBits&flag == 0 {
			continue
		}
		if pos >= len(data) {
			return pos, fmt.Errorf("truncated before varint for field flag %d", flag)
		}
		l, n := binary.Uvarint(data[pos:])
		if n == 0 {
			return pos, errors.New("varint: buffer too small for length")
		}
		if n < 0 {
			return pos, errors.New("varint: value overflow for length")
		}
		pos += n
		if l > 0 {
			if len(data) < pos+int(l) {
				return pos, fmt.Errorf("truncated field flag %d: need %d more bytes, have %d", flag, l, len(data)-pos)
			}
			pos += int(l)
		}
	}
	return pos, nil
}
