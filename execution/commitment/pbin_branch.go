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
	"math/bits"

	"github.com/erigontech/erigon/common/length"
)

// pbinCellBits are the only child slots a binary node has.
const pbinCellBits = 0b11

type pbinCellFields uint8

const (
	pbinFieldLeaf        pbinCellFields = 1
	pbinFieldBranch      pbinCellFields = 2
	pbinFieldAccountAddr pbinCellFields = 4
	pbinFieldStorageAddr pbinCellFields = 8
	pbinFieldHash        pbinCellFields = 16

	pbinFieldsAll = pbinFieldLeaf | pbinFieldBranch | pbinFieldAccountAddr | pbinFieldStorageAddr | pbinFieldHash
	pbinFieldKind = pbinFieldLeaf | pbinFieldBranch
)

var (
	errPBinMalformedBranch = errors.New("pbin: malformed branch record")
	errPBinCellMaps        = errors.New("pbin: branch maps address more than two cells")
)

// pbinBranchData is one serialised binary node. It is deliberately not
// BranchData: a 66-byte tree-key prefix does not fit the shared codec's cell
// fields, and PatriciaContext moves branch payloads as opaque bytes.
type pbinBranchData []byte

// pbinBranchEncoder serialises a binary node. Every record carries both child
// cells, so a record read back replaces its predecessor outright and no
// merge-with-previous path exists — at arity 2 the untouched sibling is the
// whole other half of the subtree, and merging is what loses it.
type pbinBranchEncoder struct {
	buf []byte
}

func (e *pbinBranchEncoder) encode(touchMap, afterMap uint16, cells *[2]pbinCell) (pbinBranchData, error) {
	if err := pbinCheckCellMaps(touchMap, afterMap); err != nil {
		return nil, err
	}
	e.buf = binary.BigEndian.AppendUint16(e.buf[:0], touchMap)
	e.buf = binary.BigEndian.AppendUint16(e.buf, afterMap)

	var err error
	for bitset := afterMap; bitset != 0; {
		bit := bitset & -bitset
		if e.buf, err = pbinAppendCell(e.buf, &cells[bits.TrailingZeros16(bit)]); err != nil {
			return nil, err
		}
		bitset ^= bit
	}
	return e.buf, nil
}

func pbinAppendCell(dst []byte, c *pbinCell) ([]byte, error) {
	var fields pbinCellFields
	switch c.kind {
	case pbinNodeLeaf:
		fields = pbinFieldLeaf
	case pbinNodeBranch:
		fields = pbinFieldBranch
	default:
		return nil, fmt.Errorf("%w: cell present in afterMap has no node kind", errPBinMalformedBranch)
	}
	if c.accountAddrLen > 0 {
		fields |= pbinFieldAccountAddr
	}
	if c.storageAddrLen > 0 {
		fields |= pbinFieldStorageAddr
	}
	if c.hashLen > 0 {
		fields |= pbinFieldHash
	}

	dst = append(dst, byte(fields))
	dst = binary.AppendUvarint(dst, uint64(c.prefix.bitLen))
	dst = c.prefix.appendPackedBits(dst)

	if fields&pbinFieldAccountAddr != 0 {
		dst = pbinAppendLenAndVal(dst, c.accountAddr[:c.accountAddrLen])
	}
	if fields&pbinFieldStorageAddr != 0 {
		dst = pbinAppendLenAndVal(dst, c.storageAddr[:c.storageAddrLen])
	}
	if fields&pbinFieldHash != 0 {
		dst = pbinAppendLenAndVal(dst, c.hash[:c.hashLen])
	}
	return dst, nil
}

func pbinAppendLenAndVal(dst, val []byte) []byte {
	return append(binary.AppendUvarint(dst, uint64(len(val))), val...)
}

// pbinDecodeBranch fills both cells from a record, rejecting every spelling the
// encoder would not produce so a record has one canonical form.
func pbinDecodeBranch(data []byte, cells *[2]pbinCell) (touchMap, afterMap uint16, err error) {
	cells[0].reset()
	cells[1].reset()

	if len(data) < 4 {
		return 0, 0, fmt.Errorf("%w: %d bytes is shorter than the header", errPBinMalformedBranch, len(data))
	}
	touchMap, afterMap = binary.BigEndian.Uint16(data), binary.BigEndian.Uint16(data[2:])
	if err = pbinCheckCellMaps(touchMap, afterMap); err != nil {
		return 0, 0, err
	}

	pos := 4
	for bitset := afterMap; bitset != 0; {
		bit := bitset & -bitset
		if pos, err = pbinDecodeCell(data, pos, &cells[bits.TrailingZeros16(bit)]); err != nil {
			return 0, 0, err
		}
		bitset ^= bit
	}
	if pos != len(data) {
		return 0, 0, fmt.Errorf("%w: %d trailing bytes", errPBinMalformedBranch, len(data)-pos)
	}
	return touchMap, afterMap, nil
}

func pbinDecodeCell(data []byte, pos int, c *pbinCell) (int, error) {
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

	pos, err := pbinDecodePrefix(data, pos, c)
	if err != nil {
		return 0, err
	}
	if fields&pbinFieldAccountAddr != 0 {
		if pos, err = pbinDecodeFixedVal(data, pos, c.accountAddr[:], length.Addr); err != nil {
			return 0, err
		}
		c.accountAddrLen = length.Addr
	}
	if fields&pbinFieldStorageAddr != 0 {
		if pos, err = pbinDecodeFixedVal(data, pos, c.storageAddr[:], length.Addr+length.Hash); err != nil {
			return 0, err
		}
		c.storageAddrLen = length.Addr + length.Hash
	}
	if fields&pbinFieldHash != 0 {
		if pos, err = pbinDecodeFixedVal(data, pos, c.hash[:], length.Hash); err != nil {
			return 0, err
		}
		c.hashLen = length.Hash
	}
	return pos, nil
}

// pbinDecodePrefix reads the explicit bit count and exactly the bytes it
// implies. The count is the authority: a byte length left to speak for itself
// would carry up to seven pad bits into the branch hash.
func pbinDecodePrefix(data []byte, pos int, c *pbinCell) (int, error) {
	bitLen, n := binary.Uvarint(data[pos:])
	if n <= 0 {
		return 0, fmt.Errorf("%w: unreadable prefix bit count at offset %d", errPBinMalformedBranch, pos)
	}
	pos += n
	if bitLen > pbinMaxPathBits {
		return 0, fmt.Errorf("%w: prefix of %d bits exceeds %d", errPBinMalformedBranch, bitLen, pbinMaxPathBits)
	}
	byteLen := (int(bitLen) + 7) / 8
	if pos+byteLen > len(data) {
		return 0, fmt.Errorf("%w: prefix of %d bits needs %d bytes, %d left", errPBinMalformedBranch, bitLen, byteLen, len(data)-pos)
	}
	if used := bitLen % 8; used != 0 && data[pos+byteLen-1]&(0xFF>>used) != 0 {
		return 0, fmt.Errorf("%w: non-zero pad bits after a %d-bit prefix", errPBinMalformedBranch, bitLen)
	}
	c.prefix = pbinPathFromBits(data[pos:pos+byteLen], int16(bitLen))
	return pos + byteLen, nil
}

func pbinDecodeFixedVal(data []byte, pos int, dst []byte, want int) (int, error) {
	l, n := binary.Uvarint(data[pos:])
	if n <= 0 {
		return 0, fmt.Errorf("%w: unreadable value length at offset %d", errPBinMalformedBranch, pos)
	}
	pos += n
	if l != uint64(want) {
		return 0, fmt.Errorf("%w: value of %d bytes, want %d", errPBinMalformedBranch, l, want)
	}
	if pos+want > len(data) {
		return 0, fmt.Errorf("%w: value of %d bytes needs more than the %d left", errPBinMalformedBranch, want, len(data)-pos)
	}
	copy(dst, data[pos:pos+want])
	return pos + want, nil
}

// pbinCheckCellMaps enforces the arity: a binary node has cells 0 and 1 and
// nothing else, in either map.
func pbinCheckCellMaps(touchMap, afterMap uint16) error {
	if (touchMap|afterMap)&^pbinCellBits != 0 {
		return fmt.Errorf("%w: touch %016b after %016b", errPBinCellMaps, touchMap, afterMap)
	}
	return nil
}
