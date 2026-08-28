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
	"errors"
	"fmt"

	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/length"
)

const (
	recordFlagLeaf         byte = 1 << 0
	recordFlagExtensionOdd byte = 1 << 1
	recordFlagStorageLeaf  byte = 1 << 2
	recordFlagHasStorage   byte = 1 << 3
	recordFlagHash         byte = 1 << 4
	recordFlagsAll              = recordFlagLeaf | recordFlagExtensionOdd | recordFlagStorageLeaf | recordFlagHasStorage | recordFlagHash
)

var ErrMalformedRecord = errors.New("commitment: malformed edge record")

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
	stateHashPresent := cell.stateHashLen == length.Hash

	flags := recordFlagLeaf
	if storageLeaf {
		flags |= recordFlagStorageLeaf
	} else if hasStorage {
		flags |= recordFlagHasStorage
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
	switch {
	case storageLeaf:
		baseLen += length.Hash
	case hasStorage:
		baseLen += length.Hash + length.Hash + 2 + length.Addr
	default:
		baseLen += length.Hash + length.Addr
	}
	record := make([]byte, 0, baseLen+(extLen+1)/2)
	record = append(record, flags)
	if stateHashPresent {
		record = append(record, cell.stateHash[:]...)
	}

	switch {
	case storageLeaf:
		record = append(record, recordStorageSlot(cell)...)
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
		if flags&(recordFlagStorageLeaf|recordFlagHasStorage) != 0 {
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
		if flags&(recordFlagHasStorage|recordFlagExtensionOdd) != 0 {
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
