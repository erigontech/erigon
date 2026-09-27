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
	"fmt"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
)

type leafRefSource interface {
	LeafRefs(key, data []byte) *commitment.LeafRefs
}

func leafRefsOf(ctx commitment.PatriciaContext, key, data []byte) *commitment.LeafRefs {
	if source, ok := ctx.(leafRefSource); ok {
		return source.LeafRefs(key, data)
	}
	return nil
}

func ComputeLeafRefs(key, data []byte) *commitment.LeafRefs {
	parsed, err := decodeRecordKey(key)
	if err != nil || len(data) == 0 {
		return nil
	}
	record, err := DecodeRecord(key, data)
	if err != nil || record.Form != RowRoot {
		return nil
	}
	slots := occupiedSlots(&record)
	if len(slots) == 0 {
		return nil
	}
	hashes := [maxCells]common.Hash{}
	mask := uint16(0)
	if err := computeCellRefs(parsed.path, &record, slots, 0, len(slots), parsed.path.BitLen, &hashes, &mask); err != nil {
		return nil
	}
	refs := &commitment.LeafRefs{Mask: mask, Refs: make([][32]byte, 0, len(slots))}
	for slot := range maxCells {
		if mask&(uint16(1)<<slot) != 0 {
			refs.Refs = append(refs.Refs, hashes[slot])
		}
	}
	return refs
}

func PrefetchPath(read func([]byte) []byte, key []byte) {
	read(GlobalRootKey())
	for bitLen := int16(4); bitLen <= int16(len(key)*8); bitLen += 4 {
		path := eip8297.PathFromBits(key, bitLen)
		rowKey, err := EncodeRowKey(&path)
		if err != nil {
			return
		}
		read(rowKey)
	}
}

func computeCellRefs(path eip8297.Bitpath, record *Record, slots []int, from, to int, parentSplit int16, hashes *[maxCells]common.Hash, mask *uint16) error {
	if to-from > 1 {
		split := firstSlotSplit(slots[from], slots[to-1], path.BitLen)
		middle := from
		for middle < to && slotBit(slots[middle], int(split-path.BitLen)) == 0 {
			middle++
		}
		if middle == from || middle == to {
			return fmt.Errorf("row cells do not split at bit %d", split)
		}
		if err := computeCellRefs(path, record, slots, from, middle, split, hashes, mask); err != nil {
			return err
		}
		return computeCellRefs(path, record, slots, middle, to, split, hashes, mask)
	}
	slot := slots[from]
	cell := &record.Cells[slot]
	var hash common.Hash
	switch cell.Kind {
	case LeafCell:
		hash = leafHash(cell)
	case BranchCell:
		prefix := rowPrefix(&path, slot, parentSplit+1, path.BitLen+4)
		prefix.Append(&cell.Prefix)
		hash = branchHash(&prefix, &cell.Left, &cell.Right)
	default:
		return fmt.Errorf("row cell %d is empty", slot)
	}
	hashes[slot] = hash
	*mask |= uint16(1) << slot
	return nil
}

func sameCell(a, b Cell) bool {
	return a.Kind == b.Kind && bytes.Equal(a.Key, b.Key) && a.Value == b.Value && a.Prefix == b.Prefix && a.Left == b.Left && a.Right == b.Right
}

func (n *rowNode) cachedCellHash(slot int) (common.Hash, bool) {
	if n.refs == nil || n.refMask&(uint16(1)<<slot) == 0 || !sameCell(n.cells[slot].Cell, n.refCells[slot]) {
		return common.Hash{}, false
	}
	index := 0
	for bit := range slot {
		if n.refMask&(uint16(1)<<bit) != 0 {
			index++
		}
	}
	return common.Hash(n.refs.Refs[index]), true
}
