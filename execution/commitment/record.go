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
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math/bits"
	"sync"

	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/length"
)

const (
	recordFlagLeaf         byte = 1 << 0
	recordFlagExtensionOdd byte = 1 << 1
	recordFlagStorageLeaf  byte = 1 << 2
	recordFlagHasStorage   byte = 1 << 3
	recordFlagHash         byte = 1 << 4
	recordFlagStorageAddr  byte = 1 << 5
	recordFlagsAll              = recordFlagLeaf | recordFlagExtensionOdd | recordFlagStorageLeaf | recordFlagHasStorage | recordFlagHash | recordFlagStorageAddr
)

var (
	ErrMalformedRecord = errors.New("commitment: malformed edge record")
	ErrEdgeRecord      = errors.New("commitment: edge record is not a legacy branch row")
)

type BranchRecordRead struct {
	Data            BranchData
	ChildMasks      [16]uint16
	ChildMasksKnown uint16
}

func SynthesizeBranchRow(mask uint16, maskKnown bool, records [16][]byte, recordsPresent uint16, legacy []byte) (BranchRecordRead, error) {
	if recordsPresent == 0 && !maskKnown {
		return BranchRecordRead{Data: bytes.Clone(legacy)}, nil
	}

	var legacyCells [16]cell
	var legacyMaps BranchMaps
	if len(legacy) >= 4 {
		var err error
		legacyMaps, err = DecodeBranchInto(legacy[2:], false, &legacyCells)
		if err != nil {
			return BranchRecordRead{}, err
		}
	} else if len(legacy) > 0 && recordsPresent == 0 {
		return BranchRecordRead{Data: bytes.Clone(legacy)}, nil
	}

	var tombstoneMask uint16
	for bitset := recordsPresent; bitset != 0; bitset &= bitset - 1 {
		bit := bitset & -bitset
		nibble := bits.TrailingZeros16(bit)
		if len(records[nibble]) == 0 {
			tombstoneMask |= bit
		}
	}

	effectiveMask := mask
	if !maskKnown {
		effectiveMask = (recordsPresent | legacyMaps.AfterMap) &^ tombstoneMask
	} else {
		effectiveMask &^= tombstoneMask
	}
	if effectiveMask == 0 {
		return BranchRecordRead{}, nil
	}

	var decoded [16]cell
	var result BranchRecordRead
	childMasks, childMasksKnown, decErr := decodeRecordsIntoCells(
		effectiveMask, records, recordsPresent, &legacyCells, legacyMaps.AfterMap, &decoded)
	if decErr != nil {
		return BranchRecordRead{}, decErr
	}
	result.ChildMasks, result.ChildMasksKnown = childMasks, childMasksKnown
	var cells [16]cellEncodeData
	for bitset := effectiveMask; bitset != 0; {
		bit := bitset & -bitset
		nibble := bits.TrailingZeros16(bit)
		cells[nibble] = cellEncodeDataFromCell(&decoded[nibble])
		bitset ^= bit
	}

	enc := branchRowEncoders.Get().(*BranchEncoder)
	encoded, err := enc.EncodeBranch(effectiveMask, effectiveMask, effectiveMask, &cells)
	if err != nil {
		branchRowEncoders.Put(enc)
		return BranchRecordRead{}, err
	}
	result.Data = bytes.Clone(encoded)
	branchRowEncoders.Put(enc)
	return result, nil
}

// EncodeBranch touches only buf and bitmapBuf, and the result is cloned before the encoder
// goes back, so the pooled buffer never reaches a caller. A merger is never allocated here:
// nothing on this path merges.
var branchRowEncoders = sync.Pool{
	New: func() any { return &BranchEncoder{buf: bytes.NewBuffer(make([]byte, 0, 1024))} },
}

// decodeRecordsIntoCells fills out[nibble] for every bit of effectiveMask, from this node's edge
// records where present and from a bundled legacy row otherwise, and reports each child's own
// bitmap. Sole decoder for both the direct read and the legacy row synthesis. legacyCells may be
// nil when legacyAfterMap is 0, since no bit then resolves to it.
func decodeRecordsIntoCells(effectiveMask uint16, records [16][]byte, recordsPresent uint16,
	legacyCells *[16]cell, legacyAfterMap uint16, out *[16]cell) (childMasks [16]uint16, childMasksKnown uint16, err error) {
	for bitset := effectiveMask; bitset != 0; {
		bit := bitset & -bitset
		nibble := bits.TrailingZeros16(bit)
		switch {
		case recordsPresent&bit != 0:
			if len(records[nibble]) == 0 {
				return childMasks, childMasksKnown, malformedRecord("empty record at nibble %d", nibble)
			}
			recordMask, decErr := DecodeRecordInto(records[nibble], &out[nibble])
			if decErr != nil {
				return childMasks, childMasksKnown, fmt.Errorf("decode edge record at nibble %d: %w", nibble, decErr)
			}
			if recordMask != 0 {
				childMasks[nibble] = recordMask
				childMasksKnown |= bit
			}
		case legacyAfterMap&bit != 0:
			out[nibble] = legacyCells[nibble]
		default:
			return childMasks, childMasksKnown, fmt.Errorf("missing record for mask bit %d", nibble)
		}
		bitset ^= bit
	}
	return childMasks, childMasksKnown, nil
}

func EncodeBranchChild(mask uint16, cell *cellEncodeData) []byte {
	extLen := recordExtensionLength(cell)
	flags := recordFlagHash
	if extLen&1 != 0 {
		flags |= recordFlagExtensionOdd
	}

	record := make([]byte, 0, 1+2+length.Hash+(extLen+1)/2)
	record = append(record, flags)
	var encodedMask [2]byte
	binary.BigEndian.PutUint16(encodedMask[:], mask)
	record = append(record, encodedMask[:]...)
	record = append(record, cell.hash[:]...)
	return appendRecordExtension(record, cell, extLen)
}

func EncodeLeafChild(cell *cellEncodeData) []byte {
	storageLeaf := cell.accountAddrLen == 0 && cell.storageAddrLen > 0
	hasStorage := cell.accountAddrLen > 0 && (cell.hashLen > 0 || cell.storageAddrLen > 0 || cell.storageMask != 0)
	// A hoisted slot is the account's whole storage subtree and has no root of its own: the root is
	// the collapsed leaf, which only exists once the slot is hashed at the account boundary. Record
	// the slot instead, as legacy does with fieldStorageAddr.
	hoistedSlot := hasStorage && cell.hashLen == 0 && cell.storageAddrLen > 0
	stateHashPresent := cell.stateHashLen == length.Hash

	flags := recordFlagLeaf
	if storageLeaf {
		flags |= recordFlagStorageLeaf
	} else if hasStorage {
		flags |= recordFlagHasStorage
	}
	if hoistedSlot {
		flags |= recordFlagStorageAddr
	}
	if stateHashPresent {
		flags |= recordFlagHash
	}
	extLen := 0
	if !storageLeaf {
		extLen = recordExtensionLength(cell)
		if extLen&1 != 0 {
			flags |= recordFlagExtensionOdd
		}
	}

	baseLen := 1
	if stateHashPresent {
		baseLen += length.Hash
	}
	switch {
	case storageLeaf:
		baseLen += length.Hash
	case hoistedSlot:
		baseLen += 2 + length.Addr + length.Hash
	case hasStorage:
		baseLen += length.Hash + 2 + length.Addr
	default:
		baseLen += length.Addr
	}
	record := make([]byte, 0, baseLen+(extLen+1)/2)
	record = append(record, flags)
	if stateHashPresent {
		record = append(record, cell.stateHash[:]...)
	}

	switch {
	case storageLeaf:
		record = append(record, recordStorageSlot(cell)...)
	case hoistedSlot:
		var encodedMask [2]byte
		binary.BigEndian.PutUint16(encodedMask[:], cell.storageMask)
		record = append(record, encodedMask[:]...)
		record = append(record, cell.accountAddr[:]...)
		record = append(record, recordStorageSlot(cell)...)
		record = appendRecordExtension(record, cell, extLen)
	case hasStorage:
		record = append(record, cell.hash[:]...)
		var encodedMask [2]byte
		binary.BigEndian.PutUint16(encodedMask[:], cell.storageMask)
		record = append(record, encodedMask[:]...)
		record = append(record, cell.accountAddr[:]...)
		record = appendRecordExtension(record, cell, extLen)
	default:
		record = append(record, cell.accountAddr[:]...)
		if extLen > 0 {
			record = appendRecordExtension(record, cell, extLen)
		}
	}
	return record
}

func DecodeRecordInto(record []byte, c *cell) (mask uint16, err error) {
	if c == nil {
		return 0, malformedRecord("nil destination cell")
	}
	if len(record) == 0 {
		return 0, malformedRecord("empty record")
	}

	c.reset()
	c.CodeHash = empty.CodeHash
	flags := record[0]
	if flags&^recordFlagsAll != 0 {
		return 0, malformedRecord("unknown flags 0x%x", flags)
	}

	if flags&recordFlagLeaf == 0 {
		if flags&(recordFlagStorageLeaf|recordFlagHasStorage|recordFlagStorageAddr) != 0 {
			return 0, malformedRecord("branch record has leaf-only flags 0x%x", flags)
		}
		if flags&recordFlagHash == 0 {
			return 0, malformedRecord("branch record has no hash")
		}
		const fixedLen = 1 + 2 + length.Hash
		if len(record) < fixedLen || len(record) > fixedLen+len(c.extension)/2 {
			return 0, malformedRecord("branch record length %d", len(record))
		}
		mask = binary.BigEndian.Uint16(record[1:3])
		copy(c.hash[:], record[3:fixedLen])
		c.hashLen = length.Hash
		if err := decodeRecordExtension(flags, record[fixedLen:], c); err != nil {
			return 0, err
		}
		return mask, nil
	}

	if flags&recordFlagStorageLeaf != 0 {
		if flags&(recordFlagHasStorage|recordFlagExtensionOdd|recordFlagStorageAddr) != 0 {
			return 0, malformedRecord("storage leaf has incompatible flags 0x%x", flags)
		}
		pos := 1
		if flags&recordFlagHash != 0 {
			if len(record) < pos+length.Hash {
				return 0, malformedRecord("storage leaf is missing its hash")
			}
			copy(c.stateHash[:], record[pos:pos+length.Hash])
			c.stateHashLen = length.Hash
			pos += length.Hash
		}
		if len(record) != pos+length.Hash {
			return 0, malformedRecord("storage leaf record length %d", len(record))
		}
		copy(c.storageAddr[:length.Hash], record[pos:])
		c.storageAddrLen = length.Hash
		return 0, nil
	}

	pos := 1
	if flags&recordFlagHash != 0 {
		if len(record) < pos+length.Hash {
			return 0, malformedRecord("account leaf is missing its hash")
		}
		copy(c.stateHash[:], record[pos:pos+length.Hash])
		c.stateHashLen = length.Hash
		pos += length.Hash
	}

	if flags&recordFlagStorageAddr != 0 {
		if flags&recordFlagHasStorage == 0 {
			return 0, malformedRecord("storage address without the storage flag 0x%x", flags)
		}
		const fixedLen = 2 + length.Addr + length.Hash
		if len(record) < pos+fixedLen {
			return 0, malformedRecord("account storage address fields are truncated")
		}
		mask = binary.BigEndian.Uint16(record[pos : pos+2])
		c.storageMask = mask
		pos += 2
		copy(c.accountAddr[:], record[pos:pos+length.Addr])
		c.accountAddrLen = length.Addr
		copy(c.storageAddr[:length.Addr], record[pos:pos+length.Addr])
		pos += length.Addr
		copy(c.storageAddr[length.Addr:length.Addr+length.Hash], record[pos:pos+length.Hash])
		c.storageAddrLen = length.Addr + length.Hash
		pos += length.Hash
		if err := decodeRecordExtension(flags, record[pos:], c); err != nil {
			return 0, err
		}
		return mask, nil
	}

	if flags&recordFlagHasStorage != 0 {
		const storagePrefixLen = length.Hash + 2
		if len(record) < pos+storagePrefixLen+length.Addr {
			return 0, malformedRecord("account storage fields are truncated")
		}
		copy(c.hash[:], record[pos:pos+length.Hash])
		c.hashLen = length.Hash
		pos += length.Hash
		mask = binary.BigEndian.Uint16(record[pos : pos+2])
		c.storageMask = mask
		pos += 2
	}

	if len(record) < pos+length.Addr {
		return 0, malformedRecord("account address is truncated")
	}
	copy(c.accountAddr[:], record[pos:pos+length.Addr])
	c.accountAddrLen = length.Addr
	pos += length.Addr

	if flags&recordFlagHasStorage == 0 {
		if len(record) > pos+len(c.extension)/2 {
			return 0, malformedRecord("account leaf record length %d", len(record))
		}
		if err := decodeRecordExtension(flags, record[pos:], c); err != nil {
			return 0, err
		}
		return 0, nil
	}
	if len(record) > pos+len(c.extension)/2 {
		return 0, malformedRecord("account storage record length %d", len(record))
	}
	if err := decodeRecordExtension(flags, record[pos:], c); err != nil {
		return 0, err
	}
	return mask, nil
}

func recordExtensionLength(cell *cellEncodeData) int {
	if cell == nil || cell.extLen <= 0 {
		return 0
	}
	if cell.extLen > int16(len(cell.extension)) {
		return len(cell.extension)
	}
	return int(cell.extLen)
}

func appendRecordExtension(record []byte, cell *cellEncodeData, extLen int) []byte {
	for i := 0; i < extLen; i += 2 {
		packed := (cell.extension[i] & 0x0f) << 4
		if i+1 < extLen {
			packed |= cell.extension[i+1] & 0x0f
		}
		record = append(record, packed)
	}
	return record
}

func recordStorageSlot(cell *cellEncodeData) []byte {
	storageLen := int(cell.storageAddrLen)
	if storageLen < length.Hash {
		return cell.storageAddr[:length.Hash]
	}
	if storageLen > len(cell.storageAddr) {
		storageLen = len(cell.storageAddr)
	}
	return cell.storageAddr[storageLen-length.Hash : storageLen]
}

func decodeRecordExtension(flags byte, tail []byte, c *cell) error {
	if len(tail) > len(c.extension)/2 {
		return malformedRecord("extension tail length %d", len(tail))
	}
	odds := flags&recordFlagExtensionOdd != 0
	if odds {
		if len(tail) == 0 {
			return malformedRecord("odd extension has no tail")
		}
		if tail[len(tail)-1]&0x0f != 0 {
			return malformedRecord("odd extension has a non-zero pad nibble")
		}
	}

	extLen := 2 * len(tail)
	if odds {
		extLen--
	}
	for i, packed := range tail {
		c.extension[2*i] = packed >> 4
		if 2*i+1 < extLen {
			c.extension[2*i+1] = packed & 0x0f
		}
	}
	c.extLen = int16(extLen)
	c.hashedExtLen = int16(extLen)
	copy(c.hashedExtension[:extLen], c.extension[:extLen])
	return nil
}

func malformedRecord(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrMalformedRecord, fmt.Sprintf(format, args...))
}
