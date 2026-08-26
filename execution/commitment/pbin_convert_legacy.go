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
	"fmt"
	"math/bits"

	"github.com/erigontech/erigon/common/length"
)

// Reading and writing the record format that predates pbinRecordFormat. The
// decoder serves the one-way datadir conversion, while the encoders also build
// legacy test corpora. A legacy record spells its cells with a touchMap/afterMap
// header and a uvarint length on every field; the current one spells neither.

// PBinRecordConverter rewrites legacy records. It is not safe for concurrent use.
type PBinRecordConverter struct {
	enc  pbinBranchEncoder
	keys pbinDigestCache
}

func NewPBinRecordConverter() *PBinRecordConverter {
	return &PBinRecordConverter{keys: pbinDigestCache{sum: pbinSelectedSum}}
}

// PBinEncodeLegacyRecord rewrites a current branch record in the pre-version
// format. The key is needed to restore storage prefixes omitted by the current
// record format.
func PBinEncodeLegacyRecord(key, current []byte) ([]byte, error) {
	if len(current) > 0 && current[0] == 0 {
		return nil, fmt.Errorf("pbin encode legacy: input is already a legacy record")
	}

	path, err := pbinDecodeBitPath(key)
	if err != nil {
		return nil, fmt.Errorf("pbin encode legacy: record key %x: %w", key, err)
	}
	converter := NewPBinRecordConverter()
	var cells [2]pbinCell
	if _, err = pbinDecodeBranch(current, &cells, path.bitLen+1, &converter.keys); err != nil {
		return nil, fmt.Errorf("pbin encode legacy: record at %x: %w", key, err)
	}

	out := binary.BigEndian.AppendUint16(nil, pbinCellBits)
	out = binary.BigEndian.AppendUint16(out, pbinCellBits)
	for bit := range cells {
		if out, err = pbinEncodeLegacyCell(out, &cells[bit]); err != nil {
			return nil, fmt.Errorf("pbin encode legacy: record at %x: %w", key, err)
		}
	}
	return out, nil
}

// PBinEncodeLegacyState rewrites a current trie state blob in the pre-version
// format. The root cell keeps its flags and prefix, but its fields gain lengths.
func PBinEncodeLegacyState(current []byte) ([]byte, error) {
	if len(current) >= 2 && current[0] == pbinStateMarker && current[1] != pbinRecordFormat &&
		current[1] <= pbinStateFlagsAll {
		return nil, fmt.Errorf("pbin encode legacy: input is already a legacy state blob")
	}
	if err := ValidatePBinStateFormat(current); err != nil {
		return nil, fmt.Errorf("pbin encode legacy: %w", err)
	}
	if len(current) < 5 {
		return nil, fmt.Errorf("pbin encode legacy: %w: header is %d bytes, want at least 5", errPBinStateBlob, len(current))
	}
	flags := current[2]
	if flags&^byte(pbinStateFlagsAll) != 0 {
		return nil, fmt.Errorf("pbin encode legacy: %w: unknown flags %08b", errPBinStateBlob, flags)
	}
	rootLen := int(binary.BigEndian.Uint16(current[3:5]))
	if len(current) != 5+rootLen {
		return nil, fmt.Errorf("pbin encode legacy: %w: root cell of %d bytes in a %d-byte blob", errPBinStateBlob, rootLen, len(current))
	}

	out := []byte{pbinStateMarker, flags, 0, 0}
	if rootLen == 0 {
		return out, nil
	}
	var root pbinCell
	pos, err := pbinDecodeCell(current, 5, &root, 0, nil, false)
	if err != nil {
		return nil, fmt.Errorf("pbin encode legacy: state root cell: %w", err)
	}
	if pos != len(current) {
		return nil, fmt.Errorf("pbin encode legacy: %w: %d trailing bytes after the root cell", errPBinStateBlob, len(current)-pos)
	}
	if out, err = pbinEncodeLegacyCell(out, &root); err != nil {
		return nil, fmt.Errorf("pbin encode legacy: state root cell: %w", err)
	}
	binary.BigEndian.PutUint16(out[2:4], uint16(len(out)-4))
	return out, nil
}

// PBinEncodeLegacyRootRecord rewrites a current root-cell record in the
// pre-version format. The record is a bare cell: no branch header, and a prefix
// that is never omitted.
func PBinEncodeLegacyRootRecord(current []byte) ([]byte, error) {
	if len(current) == 0 {
		return nil, nil
	}
	var root pbinCell
	pos, err := pbinDecodeCell(current, 0, &root, 0, nil, false)
	if err != nil {
		return nil, fmt.Errorf("pbin encode legacy: root record: %w", err)
	}
	if pos != len(current) {
		return nil, fmt.Errorf("%w: %d trailing bytes after the root cell", errPBinMalformedBranch, len(current)-pos)
	}
	return pbinEncodeLegacyCell(nil, &root)
}

// PBinRootRecordIsLegacy reports whether a root-cell record still spells its
// fields with lengths. The current format gives every field a fixed width, so a
// legacy record always leaves its length bytes over.
func PBinRootRecordIsLegacy(data []byte) bool {
	if len(data) == 0 {
		return false
	}
	var root pbinCell
	pos, err := pbinDecodeCell(data, 0, &root, 0, nil, false)
	return err != nil || pos != len(data)
}

func pbinEncodeLegacyCell(dst []byte, c *pbinCell) ([]byte, error) {
	var fields pbinCellFields
	switch c.kind {
	case pbinNodeLeaf:
		fields = pbinFieldLeaf
	case pbinNodeBranch:
		fields = pbinFieldBranch
	default:
		return nil, fmt.Errorf("%w: cell has no node kind", errPBinMalformedBranch)
	}
	if c.accountAddrLen > 0 {
		fields |= pbinFieldAccountAddr
	}
	if c.storageAddrLen > 0 {
		fields |= pbinFieldStorageAddr
	}
	if c.kind == pbinNodeLeaf && fields&pbinFieldValue == 0 {
		fields |= pbinFieldLeafValue
	}
	if c.hashLen > 0 {
		fields |= pbinFieldHash
	}

	dst = append(dst, byte(fields))
	dst = binary.AppendUvarint(dst, uint64(c.prefix.bitLen))
	dst = c.prefix.appendPackedBits(dst)
	appendValue := func(value []byte) {
		dst = binary.AppendUvarint(dst, uint64(len(value)))
		dst = append(dst, value...)
	}
	if fields&pbinFieldAccountAddr != 0 {
		appendValue(c.accountAddr[:c.accountAddrLen])
	}
	if fields&pbinFieldStorageAddr != 0 {
		appendValue(c.storageAddr[:c.storageAddrLen])
	}
	if fields&pbinFieldLeafValue != 0 {
		value, err := pbinRecordLeafValue(&c.Update)
		if err != nil {
			return nil, err
		}
		appendValue(value[:])
	}
	if fields&pbinFieldHash != 0 {
		appendValue(c.hash[:c.hashLen])
	}
	return dst, nil
}

// ConvertBranch rewrites one legacy branch record. key is the record's own DB
// key, which carries the node path and therefore the depth the current format
// reconstructs omitted storage prefixes from.
//
// A legacy record naming one cell panics. The fold collapses a single survivor
// into its parent (foldPropagate) and only foldBranch writes a record, so such a
// record cannot come from this algorithm — it means the input was written by
// something else, and converting it would invent a node.
func (c *PBinRecordConverter) ConvertBranch(key, data []byte) ([]byte, error) {
	path, err := pbinDecodeBitPath(key)
	if err != nil {
		return nil, fmt.Errorf("pbin convert: record key %x: %w", key, err)
	}
	depth := path.bitLen + 1

	var cells [2]pbinCell
	touchMap, afterMap, err := pbinLegacyDecodeBranch(data, &cells)
	if err != nil {
		return nil, fmt.Errorf("pbin convert: record at %x: %w", key, err)
	}
	if n := bits.OnesCount16(afterMap); n != 2 {
		panic(fmt.Sprintf("pbin convert: record at %x names %d cells (afterMap %04b); "+
			"a one-cell node is collapsed by foldPropagate and never stored", key, n, afterMap))
	}

	out, err := c.enc.encode(touchMap, afterMap, &cells)
	if err != nil {
		return nil, fmt.Errorf("pbin convert: re-encode at %x: %w", key, err)
	}
	out = append([]byte(nil), out...)

	// The current format drops a storage leaf's prefix and rebuilds it from the
	// address and this depth. Reading the result back is the only thing that
	// proves the dropped bits were the derivable ones.
	var got [2]pbinCell
	if _, err = pbinDecodeBranch(out, &got, depth, &c.keys); err != nil {
		return nil, fmt.Errorf("pbin convert: verify at %x: %w", key, err)
	}
	for bit := range cells {
		if got[bit] != cells[bit] {
			return nil, fmt.Errorf("pbin convert: record at %x cell %d does not round-trip", key, bit)
		}
	}
	return out, nil
}

// ConvertRootRecord rewrites the bare cell stored under the root key. It has no
// branch header, so the leading zero that marks a legacy branch record is absent
// and the key is the only thing that names it.
func (c *PBinRecordConverter) ConvertRootRecord(data []byte) ([]byte, error) {
	if len(data) == 0 {
		return nil, nil
	}
	var root pbinCell
	pos, err := pbinLegacyDecodeCell(data, 0, &root)
	if err != nil {
		return nil, fmt.Errorf("pbin convert: root record: %w", err)
	}
	if pos != len(data) {
		return nil, fmt.Errorf("%w: %d trailing bytes after the root cell", errPBinMalformedBranch, len(data)-pos)
	}
	out, err := pbinAppendCell(nil, &root, false)
	if err != nil {
		return nil, fmt.Errorf("pbin convert: root record: %w", err)
	}
	var got pbinCell
	if pos, err = pbinDecodeCell(out, 0, &got, 0, &c.keys, false); err != nil {
		return nil, fmt.Errorf("pbin convert: verify root record: %w", err)
	}
	if pos != len(out) || got != root {
		return nil, fmt.Errorf("pbin convert: root record does not round-trip")
	}
	return out, nil
}

// CompareLegacy checks that a current record preserves the cells in its legacy
// spelling. key supplies the depth needed to reconstruct omitted storage prefixes.
func (c *PBinRecordConverter) CompareLegacy(key, legacy, current []byte) error {
	path, err := pbinDecodeBitPath(key)
	if err != nil {
		return fmt.Errorf("pbin compare: record key %x: %w", key, err)
	}

	var legacyCells [2]pbinCell
	if _, _, err = pbinLegacyDecodeBranch(legacy, &legacyCells); err != nil {
		return fmt.Errorf("pbin compare: legacy record at %x: %w", key, err)
	}

	var currentCells [2]pbinCell
	if _, err = pbinDecodeBranch(current, &currentCells, path.bitLen+1, &c.keys); err != nil {
		return fmt.Errorf("pbin compare: current record at %x: %w", key, err)
	}
	for bit := range legacyCells {
		if legacyCells[bit] != currentCells[bit] {
			return fmt.Errorf("pbin compare: record at %x cell %d does not match", key, bit)
		}
	}
	return nil
}

// ConvertState rewrites the trie state blob, which gains the format byte and
// loses the field lengths inside its root cell.
func (c *PBinRecordConverter) ConvertState(blob []byte) ([]byte, error) {
	if len(blob) == 0 {
		return nil, nil
	}
	if len(blob) < 4 || blob[0] != pbinStateMarker {
		return nil, fmt.Errorf("%w: not a legacy pbin blob", errPBinStateBlob)
	}
	flags := blob[1]
	if flags&^byte(pbinStateFlagsAll) != 0 {
		return nil, fmt.Errorf("%w: unknown flags %08b", errPBinStateBlob, flags)
	}
	rootLen := int(binary.BigEndian.Uint16(blob[2:4]))
	if len(blob) != 4+rootLen {
		return nil, fmt.Errorf("%w: root cell of %d bytes in a %d-byte blob", errPBinStateBlob, rootLen, len(blob))
	}

	out := []byte{pbinStateMarker, pbinRecordFormat, flags, 0, 0}
	if rootLen > 0 {
		var root pbinCell
		pos, err := pbinLegacyDecodeCell(blob, 4, &root)
		if err != nil {
			return nil, fmt.Errorf("pbin convert: state root cell: %w", err)
		}
		if pos != len(blob) {
			return nil, fmt.Errorf("%w: %d trailing bytes after the root cell", errPBinStateBlob, len(blob)-pos)
		}
		if out, err = pbinAppendCell(out, &root, false); err != nil {
			return nil, fmt.Errorf("pbin convert: state root cell: %w", err)
		}
	}
	binary.BigEndian.PutUint16(out[3:5], uint16(len(out)-5))
	return out, nil
}

// LegacyStateRoot hashes the root cell in a pre-version state blob without
// restoring it into an engine that only accepts the current format.
func (c *PBinRecordConverter) LegacyStateRoot(blob []byte) ([]byte, error) {
	if len(blob) < 4 || blob[0] != pbinStateMarker {
		return nil, fmt.Errorf("%w: not a legacy pbin blob", errPBinStateBlob)
	}
	flags := blob[1]
	if flags&^byte(pbinStateFlagsAll) != 0 {
		return nil, fmt.Errorf("%w: unknown flags %08b", errPBinStateBlob, flags)
	}
	rootLen := int(binary.BigEndian.Uint16(blob[2:4]))
	if len(blob) != 4+rootLen {
		return nil, fmt.Errorf("%w: root cell of %d bytes in a %d-byte blob", errPBinStateBlob, rootLen, len(blob))
	}

	var root pbinCell
	if rootLen > 0 {
		pos, err := pbinLegacyDecodeCell(blob, 4, &root)
		if err != nil {
			return nil, fmt.Errorf("pbin compare: state root cell: %w", err)
		}
		if pos != len(blob) {
			return nil, fmt.Errorf("%w: %d trailing bytes after the root cell", errPBinStateBlob, len(blob)-pos)
		}
	}

	hasher := pbinHasher{sum: c.keys.sum}
	hash, err := hasher.cellHash(&root, new(pbinBitpath))
	if err != nil {
		return nil, fmt.Errorf("pbin compare: state root: %w", err)
	}
	return hash[:], nil
}

// CurrentStateRoot hashes the root cell in a current-format state blob without
// restoring it into an engine that needs a database context.
func (c *PBinRecordConverter) CurrentStateRoot(blob []byte) ([]byte, error) {
	if err := ValidatePBinStateFormat(blob); err != nil {
		return nil, err
	}
	if len(blob) < 5 {
		return nil, fmt.Errorf("%w: header is %d bytes, want at least 5", errPBinStateBlob, len(blob))
	}
	flags := blob[2]
	if flags&^byte(pbinStateFlagsAll) != 0 {
		return nil, fmt.Errorf("%w: unknown flags %08b", errPBinStateBlob, flags)
	}
	rootLen := int(binary.BigEndian.Uint16(blob[3:5]))
	if len(blob) != 5+rootLen {
		return nil, fmt.Errorf("%w: root cell of %d bytes in a %d-byte blob", errPBinStateBlob, rootLen, len(blob))
	}

	var root pbinCell
	if rootLen > 0 {
		pos, err := pbinDecodeCell(blob, 5, &root, 0, &c.keys, false)
		if err != nil {
			return nil, fmt.Errorf("pbin state root: %w", err)
		}
		if pos != len(blob) {
			return nil, fmt.Errorf("%w: %d trailing bytes after the root cell", errPBinStateBlob, len(blob)-pos)
		}
	}

	hasher := pbinHasher{sum: c.keys.sum}
	hash, err := hasher.cellHash(&root, new(pbinBitpath))
	if err != nil {
		return nil, fmt.Errorf("pbin state root: %w", err)
	}
	return hash[:], nil
}

func pbinLegacyDecodeBranch(data []byte, cells *[2]pbinCell) (touchMap, afterMap uint16, err error) {
	cells[0].reset()
	cells[1].reset()

	if len(data) < 4 {
		return 0, 0, fmt.Errorf("%w: %d bytes is shorter than the legacy header", errPBinMalformedBranch, len(data))
	}
	touchMap, afterMap = binary.BigEndian.Uint16(data), binary.BigEndian.Uint16(data[2:])
	if err := pbinCheckCellMaps(touchMap, afterMap); err != nil {
		return 0, 0, err
	}

	pos := 4
	for bitset := afterMap; bitset != 0; {
		bit := bitset & -bitset
		if pos, err = pbinLegacyDecodeCell(data, pos, &cells[bits.TrailingZeros16(bit)]); err != nil {
			return 0, 0, err
		}
		bitset ^= bit
	}
	if pos != len(data) {
		return 0, 0, fmt.Errorf("%w: %d trailing bytes", errPBinMalformedBranch, len(data)-pos)
	}
	return touchMap, afterMap, nil
}

func pbinLegacyDecodeCell(data []byte, pos int, c *pbinCell) (int, error) {
	if pos >= len(data) {
		return 0, fmt.Errorf("%w: no cell body at offset %d", errPBinMalformedBranch, pos)
	}
	fields := pbinCellFields(data[pos])
	pos++
	if fields&^pbinFieldsAll != 0 {
		return 0, fmt.Errorf("%w: unknown cell fields %08b", errPBinMalformedBranch, fields)
	}
	switch fields & pbinFieldKind {
	case pbinFieldLeaf:
		c.kind = pbinNodeLeaf
	case pbinFieldBranch:
		c.kind = pbinNodeBranch
	default:
		return 0, fmt.Errorf("%w: cell fields %08b name no single node kind", errPBinMalformedBranch, fields)
	}

	var err error
	if pos, err = pbinDecodePrefix(data, pos, c); err != nil {
		return 0, err
	}
	if fields&pbinFieldAccountAddr != 0 {
		if pos, err = pbinLegacyDecodeVal(data, pos, c.accountAddr[:], length.Addr); err != nil {
			return 0, err
		}
		c.accountAddrLen = length.Addr
	}
	if fields&pbinFieldStorageAddr != 0 {
		if pos, err = pbinLegacyDecodeVal(data, pos, c.storageAddr[:], length.Addr+length.Hash); err != nil {
			return 0, err
		}
		c.storageAddrLen = length.Addr + length.Hash
	}
	if fields&pbinFieldLeafValue != 0 {
		if pos, err = pbinLegacyDecodeVal(data, pos, c.Storage[:], pbinValueLength); err != nil {
			return 0, err
		}
		c.Flags, c.StorageLen = StorageUpdate, pbinValueLength
	}
	if fields&pbinFieldHash != 0 {
		if pos, err = pbinLegacyDecodeVal(data, pos, c.hash[:], length.Hash); err != nil {
			return 0, err
		}
		c.hashLen = length.Hash
	}
	return pos, nil
}

func pbinLegacyDecodeVal(data []byte, pos int, dst []byte, want int) (int, error) {
	n, read := binary.Uvarint(data[pos:])
	if read <= 0 {
		return 0, fmt.Errorf("%w: unreadable field length at offset %d", errPBinMalformedBranch, pos)
	}
	pos += read
	if int(n) != want {
		return 0, fmt.Errorf("%w: field of %d bytes, want %d", errPBinMalformedBranch, n, want)
	}
	if pos+want > len(data) {
		return 0, fmt.Errorf("%w: field of %d bytes needs more than the %d left", errPBinMalformedBranch, want, len(data)-pos)
	}
	copy(dst, data[pos:pos+want])
	return pos + want, nil
}
