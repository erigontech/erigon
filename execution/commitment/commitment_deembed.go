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

// DeEmbedHashSize is the fixed size of a cell hash stored in the meta entry.
// HexPatricia cells always produce either a 32-byte hash or no hash at all
// (hashLen == 0), so the packed hash vector uses a fixed stride of 32 bytes.
const DeEmbedHashSize = 32

// Bit position of the fieldHash flag in the cell encoding (flag == 8).
const fieldHashFlag = 8

// BuildDeEmbedMetaValue serialises a branch's touchMap, afterMap, hashMap and
// the 32-byte hashes for every bit set in hashMap into the metadata value.
// Value layout: [touchMap:2][afterMap:2][hashMap:2][hash_0 : 32]...[hash_k : 32]
// where hashes appear in ascending-nibble order over the set bits of hashMap.
func BuildDeEmbedMetaValue(touchMap, afterMap, hashMap uint16, hashes [16][]byte, buf []byte) []byte {
	n := bits.OnesCount16(hashMap)
	size := 6 + n*DeEmbedHashSize
	if cap(buf) < size {
		buf = make([]byte, size)
	} else {
		buf = buf[:size]
	}
	binary.BigEndian.PutUint16(buf[0:2], touchMap)
	binary.BigEndian.PutUint16(buf[2:4], afterMap)
	binary.BigEndian.PutUint16(buf[4:6], hashMap)
	pos := 6
	for bitset := hashMap; bitset != 0; {
		bit := bitset & -bitset
		nibble := bits.TrailingZeros16(bit)
		bitset ^= bit
		h := hashes[nibble]
		if len(h) != DeEmbedHashSize {
			// hashMap is expected to match exactly which cells contributed a
			// 32-byte hash; if not, zero-fill the slot for safety.
			for i := 0; i < DeEmbedHashSize; i++ {
				buf[pos+i] = 0
			}
			copy(buf[pos:pos+DeEmbedHashSize], h)
		} else {
			copy(buf[pos:pos+DeEmbedHashSize], h)
		}
		pos += DeEmbedHashSize
	}
	return buf
}

// ParseDeEmbedMetaValue decodes the metadata value into its maps and hashes.
// hashes[nibble] aliases data when the corresponding hashMap bit is set.
func ParseDeEmbedMetaValue(data []byte) (touchMap, afterMap, hashMap uint16, hashes [16][]byte, err error) {
	if len(data) < 6 {
		err = fmt.Errorf("de-embed metadata too short: %d bytes", len(data))
		return
	}
	touchMap = binary.BigEndian.Uint16(data[0:2])
	afterMap = binary.BigEndian.Uint16(data[2:4])
	hashMap = binary.BigEndian.Uint16(data[4:6])
	expected := 6 + bits.OnesCount16(hashMap)*DeEmbedHashSize
	if len(data) != expected {
		err = fmt.Errorf("de-embed metadata length mismatch: got %d want %d (hashMap=%04x)", len(data), expected, hashMap)
		return
	}
	pos := 6
	for bitset := hashMap; bitset != 0; {
		bit := bitset & -bitset
		nibble := bits.TrailingZeros16(bit)
		bitset ^= bit
		hashes[nibble] = data[pos : pos+DeEmbedHashSize]
		pos += DeEmbedHashSize
	}
	return
}

// IsDeEmbedMetaValue returns true when data is shaped like a meta entry
// ([touchMap:2][afterMap:2][hashMap:2][packed_hashes]). It validates the
// length matches 6 + 32*popcount(hashMap). Used by the squeeze/merge
// transform path to distinguish meta from cell fragments when the key is
// not available.
func IsDeEmbedMetaValue(data []byte) bool {
	if len(data) < 6 {
		return false
	}
	hashMap := binary.BigEndian.Uint16(data[4:6])
	return len(data) == 6+bits.OnesCount16(hashMap)*DeEmbedHashSize
}

// SplitBranchDataIntoChildren parses a canonical embedded branch blob into
// the pieces stored under the de-embed format:
//   - touchMap / afterMap as in the source blob;
//   - hashMap: bit set for each cell whose fieldHash was present;
//   - cells[nibble]: the cell fragment with the fieldHash field removed and
//     the fieldHash bit cleared in fieldBits;
//   - hashes[nibble]: the raw 32-byte hash for that cell (aliases source data).
//
// Cells whose fieldHash was absent are stored as-is and hashMap has the
// corresponding bit cleared.
func SplitBranchDataIntoChildren(data []byte) (touchMap, afterMap, hashMap uint16, cells [16][]byte, hashes [16][]byte, err error) {
	if len(data) < 4 {
		err = fmt.Errorf("branch data too short: %d bytes", len(data))
		return
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
			err = fmt.Errorf("branch data truncated at nibble %d pos %d/%d", nibble, pos, len(data))
			return
		}
		cellStart := pos
		fieldBits := data[pos]
		pos++
		hashFieldStart, hashValueStart, hashValueEnd, newPos, pErr := walkCellFieldsLocatingHash(data, pos, fieldBits)
		if pErr != nil {
			err = fmt.Errorf("split at nibble %d: %w", nibble, pErr)
			return
		}
		pos = newPos
		if hashFieldStart >= 0 && hashValueEnd-hashValueStart == DeEmbedHashSize {
			hashes[nibble] = data[hashValueStart:hashValueEnd]
			hashMap |= bit
			// Build stored fragment: clear fieldHash bit, drop the hash field bytes.
			clearedBits := fieldBits &^ byte(fieldHashFlag)
			fragSize := (pos - cellStart) - (hashValueEnd - hashFieldStart)
			frag := make([]byte, 0, fragSize)
			frag = append(frag, clearedBits)
			frag = append(frag, data[cellStart+1:hashFieldStart]...)
			frag = append(frag, data[hashValueEnd:pos]...)
			cells[nibble] = frag
		} else {
			cells[nibble] = data[cellStart:pos]
		}
	}
	return
}

// ReassembleBranchData rebuilds the canonical embedded branch blob from the
// stored pieces: the three bitmaps, the cell fragments (fieldHash stripped
// for entries in hashMap) and the 32-byte hashes vector.
func ReassembleBranchData(touchMap, afterMap, hashMap uint16, cells [16][]byte, hashes [16][]byte, buf []byte) (BranchData, error) {
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
		if hashMap&bit != 0 {
			h := hashes[nibble]
			if len(h) != DeEmbedHashSize {
				return nil, fmt.Errorf("missing or malformed hash for nibble %d", nibble)
			}
			var err error
			buf, err = spliceHashIntoStoredCell(buf, cellBytes, h)
			if err != nil {
				return nil, fmt.Errorf("splice hash for nibble %d: %w", nibble, err)
			}
		} else {
			buf = append(buf, cellBytes...)
		}
	}
	return buf, nil
}

// spliceHashIntoStoredCell appends to dst the canonical cell fragment rebuilt
// from a stored fragment (cell — which has the fieldHash bit cleared) and the
// raw hash bytes. The hash is inserted just before stateHash in encoding
// order (i.e., after extension / accountAddr / storageAddr).
func spliceHashIntoStoredCell(dst, cell, hash []byte) ([]byte, error) {
	if len(cell) == 0 {
		return dst, errors.New("spliceHashIntoStoredCell: empty cell")
	}
	fieldBits := cell[0]
	if fieldBits&byte(fieldHashFlag) != 0 {
		return dst, fmt.Errorf("spliceHashIntoStoredCell: fieldHash bit unexpectedly set (fieldBits=%02x)", fieldBits)
	}
	pos := 1
	// Walk past extension, accountAddr, storageAddr (flags 1,2,4) — hash goes
	// before stateHash (flag 16) in the canonical encoding order.
	for _, flag := range [3]byte{1, 2, 4} {
		if fieldBits&flag == 0 {
			continue
		}
		if pos >= len(cell) {
			return dst, fmt.Errorf("spliceHashIntoStoredCell: truncated before flag %d", flag)
		}
		l, n := binary.Uvarint(cell[pos:])
		if n == 0 {
			return dst, fmt.Errorf("spliceHashIntoStoredCell: bad varint for flag %d", flag)
		}
		if n < 0 {
			return dst, fmt.Errorf("spliceHashIntoStoredCell: overflow varint for flag %d", flag)
		}
		pos += n
		if l > 0 {
			if len(cell) < pos+int(l) {
				return dst, fmt.Errorf("spliceHashIntoStoredCell: truncated flag %d", flag)
			}
			pos += int(l)
		}
	}
	dst = append(dst, fieldBits|byte(fieldHashFlag))
	dst = append(dst, cell[1:pos]...)
	var lenBuf [binary.MaxVarintLen64]byte
	ln := binary.PutUvarint(lenBuf[:], uint64(len(hash)))
	dst = append(dst, lenBuf[:ln]...)
	dst = append(dst, hash...)
	dst = append(dst, cell[pos:]...)
	return dst, nil
}

// walkCellFieldsLocatingHash walks the cell fields starting at pos (first
// field byte, right after fieldBits) and returns the position of the hash
// field if present. All indices are into data. hashFieldStart points at the
// varint-length byte; hashValueStart at the first hash byte; hashValueEnd at
// the end of the hash data. When no hash field is present, all three are -1.
// newPos is the byte immediately after the last field.
func walkCellFieldsLocatingHash(data []byte, pos int, fieldBits byte) (hashFieldStart, hashValueStart, hashValueEnd, newPos int, err error) {
	hashFieldStart, hashValueStart, hashValueEnd = -1, -1, -1
	for flag := byte(1); flag <= 16; flag <<= 1 {
		if fieldBits&flag == 0 {
			continue
		}
		if pos >= len(data) {
			err = fmt.Errorf("truncated before varint for field flag %d", flag)
			return
		}
		fieldStart := pos
		l, n := binary.Uvarint(data[pos:])
		if n == 0 {
			err = errors.New("varint: buffer too small for length")
			return
		}
		if n < 0 {
			err = errors.New("varint: value overflow for length")
			return
		}
		pos += n
		if l > 0 {
			if len(data) < pos+int(l) {
				err = fmt.Errorf("truncated field flag %d: need %d more bytes, have %d", flag, l, len(data)-pos)
				return
			}
		}
		valueStart := pos
		if l > 0 {
			pos += int(l)
		}
		if flag == fieldHashFlag {
			hashFieldStart = fieldStart
			hashValueStart = valueStart
			hashValueEnd = pos
		}
	}
	newPos = pos
	return
}

// ReplacePlainKeysInCell applies fn to the accountAddr / storageAddr fields of
// a single de-embedded cell fragment (layout: [fieldBits:1][fields...]). The
// extension, hash, and stateHash fields are walked but not transformed.
// Returns the original cell slice when no replacement happens.
func ReplacePlainKeysInCell(cell []byte, buf []byte, fn func(key []byte, isStorage bool) ([]byte, error)) ([]byte, error) {
	if len(cell) == 0 {
		return cell, nil
	}
	var numBuf [binary.MaxVarintLen64]byte
	fields := cell[0]
	pos := 1
	anyChanged := false
	spanStart := 0
	out := buf[:0]

	skipField := func(errLabel string) error {
		if pos >= len(cell) {
			return fmt.Errorf("replacePlainKeysInCell: truncated before %s len", errLabel)
		}
		l, n := binary.Uvarint(cell[pos:])
		if n == 0 {
			return fmt.Errorf("replacePlainKeysInCell: buffer too small for %s len", errLabel)
		}
		if n < 0 {
			return fmt.Errorf("replacePlainKeysInCell: value overflow for %s len", errLabel)
		}
		pos += n
		if l > 0 {
			if len(cell) < pos+int(l) {
				return fmt.Errorf("replacePlainKeysInCell: buffer too small for %s: expected %d got %d", errLabel, pos+int(l), len(cell))
			}
			pos += int(l)
		}
		return nil
	}

	replaceField := func(isStorage bool, errLabel string) error {
		keyFieldStart := pos
		if pos >= len(cell) {
			return fmt.Errorf("replacePlainKeysInCell: truncated before %s len", errLabel)
		}
		l, n := binary.Uvarint(cell[pos:])
		if n == 0 {
			return fmt.Errorf("replacePlainKeysInCell: buffer too small for %s len", errLabel)
		}
		if n < 0 {
			return fmt.Errorf("replacePlainKeysInCell: value overflow for %s len", errLabel)
		}
		pos += n
		if len(cell) < pos+int(l) {
			return fmt.Errorf("replacePlainKeysInCell: buffer too small for %s: expected %d got %d", errLabel, pos+int(l), len(cell))
		}
		if l > 0 {
			pos += int(l)
		}
		newKey, err := fn(cell[pos-int(l):pos], isStorage)
		if err != nil {
			return err
		}
		if newKey != nil {
			if !anyChanged {
				if cap(out) < len(cell) {
					out = make([]byte, 0, len(cell))
				}
				anyChanged = true
			}
			out = append(out, cell[spanStart:keyFieldStart]...)
			ln := binary.PutUvarint(numBuf[:], uint64(len(newKey)))
			out = append(out, numBuf[:ln]...)
			out = append(out, newKey...)
			spanStart = pos
		}
		return nil
	}

	if fields&byte(fieldExtension) != 0 {
		if err := skipField("extension"); err != nil {
			return nil, err
		}
	}
	if fields&byte(fieldAccountAddr) != 0 {
		if err := replaceField(false, "accountAddr"); err != nil {
			return nil, err
		}
	}
	if fields&byte(fieldStorageAddr) != 0 {
		if err := replaceField(true, "storageAddr"); err != nil {
			return nil, err
		}
	}
	if fields&byte(fieldHash) != 0 {
		if err := skipField("hash"); err != nil {
			return nil, err
		}
	}
	if fields&byte(fieldStateHash) != 0 {
		if err := skipField("stateHash"); err != nil {
			return nil, err
		}
	}

	if !anyChanged {
		return cell, nil
	}
	out = append(out, cell[spanStart:]...)
	return out, nil
}
