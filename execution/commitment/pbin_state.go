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
)

// The pbin state blob is the root cell plus the three root flags — nothing per
// row. State is only encoded with every row folded, and unfold fully
// initializes a row before anything reads it, so the grid arrays restore as
// zero. Depths in particular are never serialized, and no depth ever meets a
// one-byte encoding.
const (
	// pbinStateMarker opens every pbin blob. A hex blob opens with a root-flags
	// byte ≤ 0x07, so the marker also refuses a cross-variant restore outright.
	pbinStateMarker = 0xB1

	pbinStateRootPresent = 1
	pbinStateRootChecked = 2
	pbinStateRootTouched = 4

	pbinStateFlagsAll = pbinStateRootPresent | pbinStateRootChecked | pbinStateRootTouched
)

var (
	errPBinStateBlob = errors.New("pbin: malformed state blob")
	errPBinStateOpen = errors.New("pbin: trie state unavailable with rows open")

	_ StatefulTrie = (*PBinPatriciaHashed)(nil)
)

func (pph *PBinPatriciaHashed) EncodeCurrentState(buf []byte) ([]byte, error) {
	if pph.grid.activeRows != 0 || pph.currentKey.bitLen != 0 {
		return nil, fmt.Errorf("%w: %d rows, %d-bit key", errPBinStateOpen, pph.grid.activeRows, pph.currentKey.bitLen)
	}
	var flags byte
	if pph.rootPresent {
		flags |= pbinStateRootPresent
	}
	if pph.rootChecked {
		flags |= pbinStateRootChecked
	}
	if pph.rootTouched {
		flags |= pbinStateRootTouched
	}
	buf = append(buf, pbinStateMarker, flags, 0, 0)
	lenAt := len(buf) - 2
	if pph.grid.root.kind != pbinNodeEmpty {
		var err error
		if buf, err = pbinAppendCell(buf, &pph.grid.root); err != nil {
			return nil, err
		}
	}
	binary.BigEndian.PutUint16(buf[lenAt:], uint16(len(buf)-lenAt-2))
	return buf, nil
}

// SetState is the inverse of EncodeCurrentState; nil or empty resets the engine,
// and the tree is then found again through the stored root record.
func (pph *PBinPatriciaHashed) SetState(buf []byte) error {
	if pph.grid.activeRows != 0 {
		return fmt.Errorf("%w: cannot restore over %d rows", errPBinStateOpen, pph.grid.activeRows)
	}
	pph.Reset()
	if len(buf) == 0 {
		return nil
	}
	if len(buf) < 4 || buf[0] != pbinStateMarker {
		return fmt.Errorf("%w: not a pbin blob", errPBinStateBlob)
	}
	flags := buf[1]
	if flags&^byte(pbinStateFlagsAll) != 0 {
		return fmt.Errorf("%w: unknown flags %08b", errPBinStateBlob, flags)
	}
	if rootLen := int(binary.BigEndian.Uint16(buf[2:4])); len(buf) != 4+rootLen {
		return fmt.Errorf("%w: root cell of %d bytes in a %d-byte blob", errPBinStateBlob, rootLen, len(buf))
	}
	if len(buf) > 4 {
		pos, err := pbinDecodeCell(buf, 4, &pph.grid.root)
		if err == nil && pos != len(buf) {
			err = fmt.Errorf("%w: %d trailing bytes after the root cell", errPBinStateBlob, len(buf)-pos)
		}
		if err != nil {
			pph.grid.root.reset()
			return err
		}
	}
	pph.rootPresent = flags&pbinStateRootPresent != 0
	pph.rootChecked = flags&pbinStateRootChecked != 0
	pph.rootTouched = flags&pbinStateRootTouched != 0
	return nil
}
