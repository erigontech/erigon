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

// Reading the record format that predates pbinRecordFormat, for the one-way
// conversion of a datadir built before it. A legacy record spells its cells
// with a touchMap/afterMap header and a uvarint length on every field; the
// current one spells neither. Nothing outside the converter may use this.

// PBinRecordConverter rewrites legacy records. It is not safe for concurrent use.
type PBinRecordConverter struct {
	enc  pbinBranchEncoder
	keys pbinDigestCache
}

func NewPBinRecordConverter() *PBinRecordConverter {
	return &PBinRecordConverter{keys: pbinDigestCache{sum: pbinSelectedSum}}
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
