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

package pbt

import (
	"bytes"
	"encoding/binary"
	"math/bits"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
)

const (
	recordFormat  byte = 0
	hdrFormatMask      = 0x0f
	hdrIsExtRoot       = 1 << 4
	hdrHasExt          = 1 << 5
	hdrReserved        = 1 << 6
	hdrIsLeafRoot      = 1 << 7
	maxCells           = 16
)

type RecordErrorRule uint8

const (
	FormatError RecordErrorRule = iota + 1
	ReservedError
	RootBitsError
	RootKeyError
	HasExtError
	MaskError
	CellCountError
	ExtensionLengthError
	SelfExtensionLengthError
	PaddingError
	LengthError
	CompactValueError
	SuffixLengthError
	ZoneError
	KeyError
)

type RecordError struct {
	Rule   RecordErrorRule
	Detail string
}

func (e *RecordError) Error() string { return e.Detail }

func (e *RecordError) Is(target error) bool {
	other, ok := target.(*RecordError)
	return ok && e.Rule == other.Rule
}

type RootForm uint8

const (
	RowRoot RootForm = iota
	ExtRoot
	LeafRoot
)

type CellKind uint8

const (
	EmptyCell CellKind = iota
	LeafCell
	BranchCell
)

type Cell struct {
	Kind   CellKind
	Key    []byte
	Value  [eip8297.ValueLength]byte
	Prefix eip8297.Bitpath
	Left   common.Hash
	Right  common.Hash
}

type Record struct {
	Form    RootForm
	SelfExt eip8297.Bitpath
	Left    common.Hash
	Right   common.Hash
	Cells   [maxCells]Cell
}

type recordKey struct {
	path   eip8297.Bitpath
	root   bool
	global bool
}

func GlobalRootKey() []byte { return []byte{0x08} }

func EncodeRowKey(path *eip8297.Bitpath) ([]byte, error) {
	if path == nil || path.BitLen < 0 || path.BitLen > eip8297.MaxPathBits || path.BitLen%4 != 0 {
		return nil, recordError(KeyError, "row path bit length must be a multiple of four")
	}
	return eip8297.AppendBitPath(nil, path), nil
}

func BucketRootKey(address []byte) ([]byte, error) {
	if len(address) != length.Addr {
		return nil, recordError(KeyError, "bucket root address must be 20 bytes")
	}
	slot := make([]byte, length.Hash)
	slot[len(slot)-1] = eip8297.HeaderStorageSlots
	treeKey := eip8297.TreeKeyStorage(address, slot)
	path := eip8297.PathFromBits(treeKey[:length.Hash+1], 264)
	return EncodeRowKey(&path)
}

func EncodeRecord(key []byte, record *Record) ([]byte, error) {
	if record == nil {
		return nil, recordError(FormatError, "nil record")
	}
	k, err := decodeRecordKey(key)
	if err != nil {
		return nil, err
	}
	switch record.Form {
	case RowRoot:
		return encodeRow(k, record)
	case ExtRoot:
		if !k.root {
			return nil, recordError(RootKeyError, "extension root must use a root key")
		}
		return encodeExtRoot(k, record)
	case LeafRoot:
		if !k.root {
			return nil, recordError(RootKeyError, "leaf root must use a root key")
		}
		return encodeLeafRoot(k, record)
	default:
		return nil, recordError(FormatError, "unknown record form")
	}
}

func DecodeRecord(key, data []byte) (Record, error) {
	if len(data) != 0 && data[0]&hdrFormatMask != recordFormat {
		return Record{}, recordError(FormatError, "unsupported record format")
	}
	k, err := decodeRecordKey(key)
	if err != nil {
		return Record{}, err
	}
	if len(data) == 0 {
		return Record{}, nil
	}
	if data[0]&hdrReserved != 0 {
		return Record{}, recordError(ReservedError, "record reserved header bit is set")
	}
	if data[0] == hdrIsExtRoot && !k.root && len(data) < 3 {
		return Record{}, recordError(FormatError, "legacy format marker requires rebuild")
	}
	if data[0]&hdrIsExtRoot != 0 && data[0]&hdrIsLeafRoot != 0 {
		return Record{}, recordError(RootBitsError, "record has both root forms")
	}
	if data[0]&hdrIsLeafRoot != 0 {
		if !k.root {
			return Record{}, recordError(RootKeyError, "leaf root must use a root key")
		}
		return decodeLeafRoot(k, data)
	}
	if data[0]&hdrIsExtRoot != 0 {
		if !k.root {
			return Record{}, recordError(RootKeyError, "extension root must use a root key")
		}
		return decodeExtRoot(k, data)
	}
	return decodeRow(k, data)
}

func encodeRow(k recordKey, record *Record) ([]byte, error) {
	var childMask, leafMask, extMask uint16
	for slot := range record.Cells {
		cell := &record.Cells[slot]
		if cell.Kind == EmptyCell {
			continue
		}
		if cell.Kind != LeafCell && cell.Kind != BranchCell {
			return nil, recordError(FormatError, "unknown cell form")
		}
		bit := uint16(1) << slot
		childMask |= bit
		if cell.Kind == LeafCell {
			leafMask |= bit
			if _, err := rowLeafSuffix(k.path, slot, cell.Key); err != nil {
				return nil, err
			}
		} else {
			keyLen, err := rowKeyLength(&k.path, slot)
			if err != nil {
				return nil, err
			}
			if int(cell.Prefix.BitLen)+int(k.path.BitLen)+4 >= keyLen*8 {
				return nil, recordError(ExtensionLengthError, "branch prefix exceeds the key length")
			}
			if cell.Prefix.BitLen != 0 {
				extMask |= bit
			}
		}
	}
	if childMask == 0 {
		return nil, nil
	}
	if bits.OnesCount16(childMask) < 2 {
		return nil, recordError(CellCountError, "row must contain at least two cells")
	}

	out := []byte{recordFormat}
	if extMask != 0 {
		out[0] |= hdrHasExt
	}
	out = binary.BigEndian.AppendUint16(out, childMask)
	out = binary.BigEndian.AppendUint16(out, leafMask)
	if extMask != 0 {
		out = binary.BigEndian.AppendUint16(out, extMask)
	}
	for slot := range record.Cells {
		cell := &record.Cells[slot]
		if cell.Kind != BranchCell {
			continue
		}
		out = append(out, cell.Left[:]...)
		out = append(out, cell.Right[:]...)
	}
	for slot := range record.Cells {
		cell := &record.Cells[slot]
		if cell.Kind != BranchCell || cell.Prefix.BitLen == 0 {
			continue
		}
		out = binary.BigEndian.AppendUint16(out, uint16(cell.Prefix.BitLen))
		out = cell.Prefix.AppendPackedBits(out)
	}
	for slot := range record.Cells {
		cell := &record.Cells[slot]
		if cell.Kind != LeafCell {
			continue
		}
		suffix, err := rowLeafSuffix(k.path, slot, cell.Key)
		if err != nil {
			return nil, err
		}
		value, err := eip8297.EncodeLeafValue(cell.Key, &cell.Value)
		if err != nil {
			return nil, recordError(CompactValueError, err.Error())
		}
		if len(value) > 255 {
			return nil, recordError(CompactValueError, "compact value exceeds one-byte length")
		}
		out = suffix.AppendPackedBits(out)
		out = append(out, byte(len(value)))
		out = append(out, value...)
	}
	return out, nil
}

func encodeExtRoot(k recordKey, record *Record) ([]byte, error) {
	if record.SelfExt.BitLen < 4 {
		return nil, recordError(SelfExtensionLengthError, "root extension bit length must be at least four")
	}
	maxLen, err := rootExtensionKeyLength(k, &record.SelfExt)
	if err != nil {
		return nil, err
	}
	if int(record.SelfExt.BitLen) >= maxLen {
		return nil, recordError(SelfExtensionLengthError, "root extension ends at the key length")
	}
	out := []byte{recordFormat | hdrIsExtRoot}
	out = binary.BigEndian.AppendUint16(out, uint16(record.SelfExt.BitLen))
	out = record.SelfExt.AppendPackedBits(out)
	out = append(out, record.Left[:]...)
	out = append(out, record.Right[:]...)
	return out, nil
}

func encodeLeafRoot(k recordKey, record *Record) ([]byte, error) {
	var cell Cell
	count := 0
	for slot := range record.Cells {
		candidate := &record.Cells[slot]
		if candidate.Kind == EmptyCell {
			continue
		}
		count++
		cell = *candidate
	}
	if count != 1 || cell.Kind != LeafCell {
		return nil, recordError(CellCountError, "leaf root must contain one leaf")
	}
	suffix, err := rootLeafSuffix(k, cell.Key)
	if err != nil {
		return nil, err
	}
	value, err := eip8297.EncodeLeafValue(cell.Key, &cell.Value)
	if err != nil {
		return nil, recordError(CompactValueError, err.Error())
	}
	if len(value) > 255 {
		return nil, recordError(CompactValueError, "compact value exceeds one-byte length")
	}
	out := []byte{recordFormat | hdrIsLeafRoot}
	out = suffix.AppendPackedBits(out)
	out = append(out, byte(len(value)))
	out = append(out, value...)
	return out, nil
}

func decodeRow(k recordKey, data []byte) (Record, error) {
	if len(data) < 5 {
		return Record{}, recordError(LengthError, "row header is truncated")
	}
	childMask := binary.BigEndian.Uint16(data[1:3])
	leafMask := binary.BigEndian.Uint16(data[3:5])
	if leafMask&^childMask != 0 {
		return Record{}, recordError(MaskError, "leaf mask is not a child subset")
	}
	extMask := uint16(0)
	off := 5
	if data[0]&hdrHasExt != 0 {
		if len(data) < off+2 {
			return Record{}, recordError(LengthError, "extension mask is truncated")
		}
		extMask = binary.BigEndian.Uint16(data[off : off+2])
		off += 2
		if extMask == 0 {
			return Record{}, recordError(HasExtError, "extension header bit requires a non-empty extension mask")
		}
	}
	if extMask&leafMask != 0 || extMask&^childMask != 0 {
		return Record{}, recordError(MaskError, "extension mask is not a branch subset")
	}
	if bits.OnesCount16(childMask) < 2 {
		return Record{}, recordError(CellCountError, "row must contain at least two cells")
	}
	var record Record
	record.Form = RowRoot
	for slot := range maxCells {
		bit := uint16(1) << slot
		if childMask&bit == 0 || leafMask&bit != 0 {
			continue
		}
		if len(data) < off+64 {
			return Record{}, recordError(LengthError, "branch cells are truncated")
		}
		copy(record.Cells[slot].Left[:], data[off:off+32])
		copy(record.Cells[slot].Right[:], data[off+32:off+64])
		record.Cells[slot].Kind = BranchCell
		off += 64
	}
	for slot := range maxCells {
		bit := uint16(1) << slot
		if extMask&bit == 0 {
			continue
		}
		prefix, next, err := decodePathField(data, off)
		if err != nil {
			return Record{}, err
		}
		keyLen, keyErr := rowKeyLength(&k.path, slot)
		if keyErr != nil {
			return Record{}, keyErr
		}
		if int(prefix.BitLen)+int(k.path.BitLen)+4 >= keyLen*8 {
			return Record{}, recordError(ExtensionLengthError, "branch extension exceeds the key length")
		}
		record.Cells[slot].Prefix = prefix
		record.Cells[slot].Kind = BranchCell
		off = next
	}
	for slot := range maxCells {
		bit := uint16(1) << slot
		if leafMask&bit == 0 {
			continue
		}
		suffixBits, err := rowSuffixBits(&k.path, slot)
		if err != nil {
			return Record{}, err
		}
		packed := packedLen(suffixBits)
		if len(data) < off+packed+1 {
			return Record{}, recordError(LengthError, "leaf suffix is truncated")
		}
		if !canonicalPadding(data[off:off+packed], suffixBits) {
			return Record{}, recordError(PaddingError, "leaf suffix has non-zero padding")
		}
		suffix := eip8297.PathFromBits(data[off:off+packed], suffixBits)
		off += packed
		valueLen := int(data[off])
		off++
		if len(data) < off+valueLen {
			return Record{}, recordError(LengthError, "compact leaf value is truncated")
		}
		fullKey, err := rowLeafKey(k.path, slot, suffix)
		if err != nil {
			return Record{}, err
		}
		value, decodeErr := eip8297.DecodeLeafValue(fullKey, data[off:off+valueLen])
		if decodeErr != nil {
			return Record{}, recordError(CompactValueError, decodeErr.Error())
		}
		record.Cells[slot] = Cell{Kind: LeafCell, Key: fullKey, Value: value}
		off += valueLen
	}
	if off != len(data) {
		return Record{}, recordError(LengthError, "record has trailing bytes")
	}
	return record, nil
}

func decodeExtRoot(k recordKey, data []byte) (Record, error) {
	if data[0]&hdrHasExt != 0 {
		return Record{}, recordError(HasExtError, "extension root cannot carry a child extension mask")
	}
	if len(data) < 3 {
		return Record{}, recordError(LengthError, "root extension is truncated")
	}
	bitLen := int(binary.BigEndian.Uint16(data[1:3]))
	if bitLen < 4 {
		return Record{}, recordError(SelfExtensionLengthError, "root extension bit length must be at least four")
	}
	if bitLen > eip8297.MaxPathBits {
		return Record{}, recordError(SelfExtensionLengthError, "root extension exceeds the key length")
	}
	packed := packedLen(int16(bitLen))
	if len(data) != 3+packed+64 {
		return Record{}, recordError(LengthError, "root extension length is not exact")
	}
	if !canonicalPadding(data[3:3+packed], int16(bitLen)) {
		return Record{}, recordError(PaddingError, "root extension has non-zero padding")
	}
	selfExt := eip8297.PathFromBits(data[3:3+packed], int16(bitLen))
	maxLen, keyErr := rootExtensionKeyLength(k, &selfExt)
	if keyErr != nil {
		return Record{}, keyErr
	}
	if bitLen >= maxLen {
		return Record{}, recordError(SelfExtensionLengthError, "root extension ends at the key length")
	}
	var record Record
	record.Form = ExtRoot
	record.SelfExt = eip8297.PathFromBits(data[3:3+packed], int16(bitLen))
	copy(record.Left[:], data[3+packed:3+packed+32])
	copy(record.Right[:], data[3+packed+32:])
	return record, nil
}

func decodeLeafRoot(k recordKey, data []byte) (Record, error) {
	if data[0]&hdrHasExt != 0 {
		return Record{}, recordError(HasExtError, "leaf root cannot carry extensions")
	}
	suffixBits, err := rootSuffixBits(k, data[1:])
	if err != nil {
		return Record{}, err
	}
	packed := packedLen(suffixBits)
	if len(data) < 1+packed+1 {
		return Record{}, recordError(LengthError, "leaf root is truncated")
	}
	if !canonicalPadding(data[1:1+packed], suffixBits) {
		return Record{}, recordError(PaddingError, "leaf root suffix has non-zero padding")
	}
	suffix := eip8297.PathFromBits(data[1:1+packed], suffixBits)
	off := 1 + packed
	valueLen := int(data[off])
	off++
	if len(data) != off+valueLen {
		return Record{}, recordError(LengthError, "leaf root length is not exact")
	}
	fullKey, err := rootLeafKey(k, suffix)
	if err != nil {
		return Record{}, err
	}
	value, decodeErr := eip8297.DecodeLeafValue(fullKey, data[off:])
	if decodeErr != nil {
		return Record{}, recordError(CompactValueError, decodeErr.Error())
	}
	var record Record
	record.Form = LeafRoot
	record.Cells[0] = Cell{Kind: LeafCell, Key: fullKey, Value: value}
	return record, nil
}

func decodePathField(data []byte, off int) (eip8297.Bitpath, int, error) {
	if len(data) < off+2 {
		return eip8297.Bitpath{}, 0, recordError(LengthError, "extension length is truncated")
	}
	bitLen := int(binary.BigEndian.Uint16(data[off : off+2]))
	if bitLen < 1 || bitLen > eip8297.MaxPathBits {
		return eip8297.Bitpath{}, 0, recordError(ExtensionLengthError, "extension bit length is outside the canonical range")
	}
	off += 2
	packed := packedLen(int16(bitLen))
	if len(data) < off+packed {
		return eip8297.Bitpath{}, 0, recordError(LengthError, "extension bits are truncated")
	}
	if !canonicalPadding(data[off:off+packed], int16(bitLen)) {
		return eip8297.Bitpath{}, 0, recordError(PaddingError, "extension has non-zero padding")
	}
	return eip8297.PathFromBits(data[off:off+packed], int16(bitLen)), off + packed, nil
}

func decodeRecordKey(key []byte) (recordKey, error) {
	if bytes.Equal(key, GlobalRootKey()) {
		return recordKey{root: true, global: true}, nil
	}
	path, err := eip8297.DecodeBitPath(key)
	if err != nil {
		return recordKey{}, recordError(KeyError, err.Error())
	}
	if path.BitLen%4 != 0 {
		return recordKey{}, recordError(KeyError, "row key bit length must be a multiple of four")
	}
	if path.BitLen == 0 {
		return recordKey{}, recordError(KeyError, "ordinary row key cannot be empty")
	}
	root := path.BitLen == 264 && pathByte(&path, 0) == eip8297.StorageZone
	if !root {
		if err := validateRowPath(&path); err != nil {
			return recordKey{}, err
		}
	}
	return recordKey{path: path, root: root}, nil
}

func rowLeafSuffix(path eip8297.Bitpath, slot int, key []byte) (eip8297.Bitpath, error) {
	suffixBits, err := rowSuffixBits(&path, slot)
	if err != nil {
		return eip8297.Bitpath{}, err
	}
	if len(key)*8 != int(path.BitLen)+4+int(suffixBits) {
		return eip8297.Bitpath{}, recordError(SuffixLengthError, "leaf suffix length does not match the row key")
	}
	full := eip8297.PathFromBits(key, int16(len(key)*8))
	if eip8297.CommonPrefixBitsAt(&full, 0, &path) != path.BitLen {
		return eip8297.Bitpath{}, recordError(SuffixLengthError, "leaf key does not match the row path")
	}
	keyBytes, ok := eip8297.ZoneKeyLength(key[0])
	if !ok || len(key) != keyBytes {
		return eip8297.Bitpath{}, recordError(ZoneError, "leaf key uses a reserved zone")
	}
	for i := range 4 {
		if full.Bit(path.BitLen+int16(i)) != uint64((slot>>(3-i))&1) {
			return eip8297.Bitpath{}, recordError(SuffixLengthError, "leaf key does not match the row slot")
		}
	}
	return full.Slice(path.BitLen+4, full.BitLen), nil
}

func rowLeafKey(path eip8297.Bitpath, slot int, suffix eip8297.Bitpath) ([]byte, error) {
	full := path
	var slotPath eip8297.Bitpath
	for i := range 4 {
		slotPath.AppendBit(uint64((slot >> (3 - i)) & 1))
	}
	full.Append(&slotPath)
	full.Append(&suffix)
	key := full.AppendPackedBits(nil)
	if len(key) == 0 {
		return nil, recordError(ZoneError, "leaf key is empty")
	}
	keyLen, ok := eip8297.ZoneKeyLength(key[0])
	if !ok || len(key) != keyLen {
		return nil, recordError(ZoneError, "leaf key uses a reserved zone")
	}
	return key, nil
}

func rowSuffixBits(path *eip8297.Bitpath, slot int) (int16, error) {
	keyBytes, err := rowKeyLength(path, slot)
	if err != nil {
		return 0, err
	}
	value := keyBytes*8 - int(path.BitLen) - 4
	if value < 0 || value > eip8297.MaxPathBits {
		return 0, recordError(SuffixLengthError, "leaf suffix length is outside the key")
	}
	return int16(value), nil
}

func rowZone(path *eip8297.Bitpath, slot int) byte {
	if path.BitLen == 0 {
		return byte(slot) << 4
	}
	if path.BitLen < 8 {
		return pathNibble(path, 0)<<4 | byte(slot)
	}
	return pathByte(path, 0)
}

func rowKeyLength(path *eip8297.Bitpath, slot int) (int, error) {
	if path.BitLen == 0 {
		switch slot {
		case 0:
			return eip8297.AccountKeyLength, nil
		case 15:
			return eip8297.StorageKeyLength, nil
		default:
			return 0, recordError(ZoneError, "row slot uses a reserved zone")
		}
	}
	zone := rowZone(path, slot)
	keyBytes, ok := eip8297.ZoneKeyLength(zone)
	if !ok {
		return 0, recordError(ZoneError, "row slot uses a reserved zone")
	}
	return keyBytes, nil
}

func validateRowPath(path *eip8297.Bitpath) error {
	if path.BitLen < 8 {
		zone := pathNibble(path, 0)
		if zone != 0 && zone != 0xf {
			return recordError(ZoneError, "row path uses a reserved zone")
		}
		return nil
	}
	if _, ok := eip8297.ZoneKeyLength(pathByte(path, 0)); !ok {
		return recordError(ZoneError, "row path uses a reserved zone")
	}
	return nil
}

func rootExtensionKeyLength(k recordKey, path *eip8297.Bitpath) (int, error) {
	if !k.global {
		return eip8297.StorageKeyLength*8 - int(k.path.BitLen), nil
	}
	if path.BitLen < 4 {
		return 0, recordError(ZoneError, "root extension does not identify a zone")
	}
	zone := pathNibble(path, 0)
	if zone == 0 {
		if path.BitLen >= 8 {
			zone = pathByte(path, 0)
			if zone != eip8297.AccountZone && zone != eip8297.CodeZone {
				return 0, recordError(ZoneError, "root extension uses a reserved zone")
			}
		}
		return eip8297.AccountKeyLength * 8, nil
	}
	if zone == 0xf {
		if path.BitLen >= 8 && pathByte(path, 0) != eip8297.StorageZone {
			return 0, recordError(ZoneError, "root extension uses a reserved zone")
		}
		return eip8297.StorageKeyLength * 8, nil
	}
	return 0, recordError(ZoneError, "root extension uses a reserved zone")
}

func rootLeafSuffix(k recordKey, key []byte) (eip8297.Bitpath, error) {
	keyBytes, ok := eip8297.ZoneKeyLength(firstByte(key))
	if !ok || len(key) != keyBytes {
		return eip8297.Bitpath{}, recordError(ZoneError, "root suffix key uses a reserved zone")
	}
	full := eip8297.PathFromBits(key, int16(keyBytes*8))
	start := int16(0)
	if !k.global {
		if keyBytes*8 != eip8297.MaxPathBits || eip8297.CommonPrefixBitsAt(&full, 0, &k.path) != k.path.BitLen {
			return eip8297.Bitpath{}, recordError(SuffixLengthError, "bucket leaf key does not match the bucket root")
		}
		start = k.path.BitLen
	}
	return full.Slice(start, full.BitLen), nil
}

func rootLeafKey(k recordKey, suffix eip8297.Bitpath) ([]byte, error) {
	full := suffix
	if !k.global {
		full = k.path
		full.Append(&suffix)
	}
	key := full.AppendPackedBits(nil)
	keyBits, ok := eip8297.ZoneKeyLength(firstByte(key))
	if !ok || len(key) != keyBits {
		return nil, recordError(ZoneError, "root key uses a reserved zone")
	}
	return key, nil
}

func rootSuffixBits(k recordKey, data []byte) (int16, error) {
	if !k.global {
		return int16(eip8297.MaxPathBits - k.path.BitLen), nil
	}
	if len(data) == 0 {
		return 0, recordError(SuffixLengthError, "global leaf root has no key prefix")
	}
	keyBytes, ok := eip8297.ZoneKeyLength(data[0])
	if !ok {
		return 0, recordError(ZoneError, "leaf key uses a reserved zone")
	}
	return int16(keyBytes * 8), nil
}

func pathByte(path *eip8297.Bitpath, byteIndex int) byte {
	var out byte
	for i := range 8 {
		out |= byte(path.Bit(int16(byteIndex*8+i))) << uint(7-i)
	}
	return out
}

func pathNibble(path *eip8297.Bitpath, from int16) byte {
	var out byte
	for i := range 4 {
		out |= byte(path.Bit(from+int16(i))) << uint(3-i)
	}
	return out
}

func firstByte(key []byte) byte {
	if len(key) == 0 {
		return 0
	}
	return key[0]
}

func packedLen(bitLen int16) int { return (int(bitLen) + 7) / 8 }

func canonicalPadding(data []byte, bitLen int16) bool {
	if bitLen%8 == 0 {
		return true
	}
	return len(data) != 0 && data[len(data)-1]&(0xff>>uint(bitLen%8)) == 0
}

func recordError(rule RecordErrorRule, detail string) *RecordError {
	if rule == FormatError {
		detail = "record format requires rebuild: " + detail
	}
	return &RecordError{Rule: rule, Detail: detail}
}
