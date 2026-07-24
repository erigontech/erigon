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

// Deembedded branch persistence.
//
// The default (embedded) layout stores a whole branch node — every present
// child's hash, extension and leaf plain key — inside one value keyed by the
// branch prefix. Branches whose children are leaves (52-byte storage plain keys
// + hashes) therefore grow large.
//
// The deembedded layout splits a branch's leaf children out into their own
// keys: the leaf child at nibble n under branch prefix P is stored at
// compact(P||n), holding exactly the per-cell fields the embedded layout would
// have inlined. The parent value at compact(P) then keeps only the small
// per-child data needed to rehash and descend: sub-branch children keep their
// {hash, extension} inline (unchanged); leaf children contribute nothing to the
// parent body beyond a bit in leafMap.
//
// The in-memory fold/hash engine is untouched, so the trie root is byte-for-byte
// identical to the embedded layout — only what is written to / read from the
// commitment domain changes.
//
// Parent value layout (before the 2-byte touch-map prefix is stripped on read):
//
//	[touchMap:2][afterMap:2][leafMap:2]
//	for each nibble set in afterMap, ascending:
//	    if leaf  (leafMap bit set):  (nothing — data lives at compact(P||nibble))
//	    if branch(leafMap bit clear): [hashLen:1][hash][extLen:1][extension]
//
// Leaf child value layout at compact(P||nibble):
//
//	[fields:1] then, per set field, uvarint(len)+bytes — identical field framing
//	to EncodeBranch's per-cell encoding, decoded by cell.fillFromFields.

import (
	"encoding/binary"
	"fmt"
	"math/bits"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/commitment/nibbles"
)

// deembedChildKey returns the commitment-domain key for the child at nibble
// under the branch reached by prefixNibbles.
func (hph *HexPatriciaHashed) deembedChildKey(prefixNibbles []byte, nibble int) []byte {
	child := make([]byte, len(prefixNibbles)+1)
	copy(child, prefixNibbles)
	child[len(prefixNibbles)] = byte(nibble)
	return nibbles.HexToCompact(child)
}

func appendUvarVal(buf, val []byte) []byte {
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], uint64(len(val)))
	buf = append(buf, tmp[:n]...)
	return append(buf, val...)
}

// encodeDeembedLeafChild serialises a leaf cell for its standalone child key,
// mirroring EncodeBranch's per-cell field selection so cell.fillFromFields
// reconstructs an identical cell.
func encodeDeembedLeafChild(cell *cell, buf []byte) []byte {
	var fields cellFields
	if cell.extLen > 0 && cell.storageAddrLen == 0 {
		fields |= fieldExtension
	}
	if cell.accountAddrLen > 0 {
		fields |= fieldAccountAddr
	}
	if cell.storageAddrLen > 0 {
		fields |= fieldStorageAddr
	}
	if cell.hashLen > 0 {
		fields |= fieldHash
	}
	if cell.stateHashLen == 32 && (cell.accountAddrLen > 0 || cell.storageAddrLen > 0) {
		fields |= fieldStateHash
	}
	buf = append(buf, byte(fields))
	if fields&fieldExtension != 0 {
		buf = appendUvarVal(buf, cell.extension[:cell.extLen])
	}
	if fields&fieldAccountAddr != 0 {
		buf = appendUvarVal(buf, cell.accountAddr[:cell.accountAddrLen])
	}
	if fields&fieldStorageAddr != 0 {
		buf = appendUvarVal(buf, cell.storageAddr[:cell.storageAddrLen])
	}
	if fields&fieldHash != 0 {
		buf = appendUvarVal(buf, cell.hash[:cell.hashLen])
	}
	if fields&fieldStateHash != 0 {
		buf = appendUvarVal(buf, cell.stateHash[:cell.stateHashLen])
	}
	return buf
}

// collectDeembedBranch writes a branch in the deembedded layout: touched leaf
// children go to their own keys, and the parent keeps only sub-branch children
// inline plus the leafMap. It replaces branchEncoder.CollectUpdate for the
// deembed trie and needs no merge-with-prev — the full present row is written.
func (hph *HexPatriciaHashed) collectDeembedBranch(updateKey []byte, row int, touchMap, afterMap uint16) error {
	prefix := hph.currentKey[:hph.currentKeyLen]

	var leafMap uint16
	for bitset := afterMap; bitset != 0; {
		bit := bitset & -bitset
		nibble := bits.TrailingZeros16(bit)
		cell := &hph.grid[row][nibble]
		if cell.accountAddrLen > 0 || cell.storageAddrLen > 0 {
			leafMap |= uint16(1) << nibble
		}
		bitset ^= bit
	}

	var hdr [6]byte
	binary.BigEndian.PutUint16(hdr[0:], touchMap)
	binary.BigEndian.PutUint16(hdr[2:], afterMap)
	binary.BigEndian.PutUint16(hdr[4:], leafMap)
	buf := append([]byte{}, hdr[:]...)

	var childBuf []byte
	for bitset := afterMap; bitset != 0; {
		bit := bitset & -bitset
		nibble := bits.TrailingZeros16(bit)
		cell := &hph.grid[row][nibble]

		if leafMap&bit != 0 {
			// Rewrite the child key only when the leaf was touched; an untouched
			// leaf child keeps its existing (still-current) record.
			if touchMap&bit != 0 {
				childKey := hph.deembedChildKey(prefix, nibble)
				childBuf = encodeDeembedLeafChild(cell, childBuf[:0])
				if err := hph.ctx.PutBranch(childKey, common.Copy(childBuf), nil); err != nil {
					return err
				}
			}
		} else {
			buf = append(buf, byte(cell.hashLen))
			buf = append(buf, cell.hash[:cell.hashLen]...)
			buf = append(buf, byte(cell.extLen))
			buf = append(buf, cell.extension[:cell.extLen]...)
		}
		bitset ^= bit
	}

	return hph.ctx.PutBranch(common.Copy(updateKey), buf, nil)
}

// deleteDeembedBranch writes the empty-branch marker for a collapsed/deleted
// branch. Leaf child keys are left as harmless orphans (never referenced by a
// live parent); they are overwritten if the prefix is reused.
func (hph *HexPatriciaHashed) deleteDeembedBranch(updateKey []byte, touchMap uint16) error {
	var hdr [6]byte
	binary.BigEndian.PutUint16(hdr[0:], touchMap)
	// afterMap = 0, leafMap = 0
	return hph.ctx.PutBranch(common.Copy(updateKey), common.Copy(hdr[:]), nil)
}

// decodeDeembedBranchIntoRow reverses collectDeembedBranch. branch has the
// leading 2-byte touch map already stripped by the caller, so it starts at
// afterMap. Leaf children are loaded eagerly from their own keys, producing an
// in-memory row identical to the embedded decode path.
func (hph *HexPatriciaHashed) decodeDeembedBranchIntoRow(row int, depth int16, branch []byte, deleted bool) error {
	if len(branch) < 4 {
		return fmt.Errorf("deembed branch too short: %d bytes", len(branch))
	}
	afterMap := binary.BigEndian.Uint16(branch[0:])
	leafMap := binary.BigEndian.Uint16(branch[2:])
	if deleted {
		hph.touchMap[row], hph.afterMap[row] = afterMap, 0
	} else {
		hph.touchMap[row], hph.afterMap[row] = 0, afterMap
	}

	prefix := hph.currentKey[:hph.currentKeyLen]
	pos := 4
	for bitset := afterMap; bitset != 0; {
		bit := bitset & -bitset
		nibble := bits.TrailingZeros16(bit)
		cell := &hph.grid[row][nibble]
		cell.reset()

		if leafMap&bit != 0 {
			childKey := hph.deembedChildKey(prefix, nibble)
			childVal, _, err := hph.ctx.Branch(childKey)
			if err != nil {
				return err
			}
			if len(childVal) == 0 {
				return fmt.Errorf("missing deembed leaf child at nibble %x under prefix %x", nibble, prefix)
			}
			fieldBits := cellFields(childVal[0])
			if _, err := cell.fillFromFields(childVal, 1, fieldBits); err != nil {
				return fmt.Errorf("deembed leaf child fillFromFields nibble %x: %w", nibble, err)
			}
		} else {
			if pos >= len(branch) {
				return fmt.Errorf("deembed branch truncated before sub-branch child at nibble %x", nibble)
			}
			hashLen := int(branch[pos])
			pos++
			if pos+hashLen > len(branch) {
				return fmt.Errorf("deembed branch truncated in hash at nibble %x", nibble)
			}
			if hashLen > 0 {
				copy(cell.hash[:], branch[pos:pos+hashLen])
				cell.hashLen = int16(hashLen)
				pos += hashLen
			}
			if pos >= len(branch) {
				return fmt.Errorf("deembed branch truncated before extension at nibble %x", nibble)
			}
			extLen := int(branch[pos])
			pos++
			if pos+extLen > len(branch) {
				return fmt.Errorf("deembed branch truncated in extension at nibble %x", nibble)
			}
			if extLen > 0 {
				copy(cell.hashedExtension[:], branch[pos:pos+extLen])
				cell.hashedExtLen = int16(extLen)
				copy(cell.extension[:], branch[pos:pos+extLen])
				cell.extLen = int16(extLen)
				pos += extLen
			}
		}

		if err := cell.deriveHashedKeys(depth, hph.keccak, hph.accountKeyLen, hph.cellHashBuf[:]); err != nil {
			return err
		}
		bitset ^= bit
	}
	return nil
}
