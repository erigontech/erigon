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
	"fmt"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
)

type foldNode struct {
	split       int16
	left, right common.Hash
}

type FoldResult struct {
	Split       int16
	Left, Right common.Hash
}

func Fold(key []byte, record *Record) (common.Hash, error) {
	k, err := decodeRecordKey(key)
	if err != nil {
		return common.Hash{}, err
	}
	if record == nil || (record.Form == RowRoot && !hasCells(record)) {
		return eip8297.EmptyTreeHash, nil
	}
	switch record.Form {
	case LeafRoot:
		if !k.root {
			return common.Hash{}, fmt.Errorf("leaf root must use a root key")
		}
		cell, ok := singleLeaf(record)
		if !ok {
			return common.Hash{}, fmt.Errorf("leaf root must contain one leaf")
		}
		return leafHash(&cell), nil
	case ExtRoot:
		if !k.root {
			return common.Hash{}, fmt.Errorf("extension root must use a root key")
		}
		return branchHash(&record.SelfExt, &record.Left, &record.Right), nil
	case RowRoot:
		if !k.root {
			return common.Hash{}, fmt.Errorf("row root must use a root key")
		}
		node, err := foldRow(k.path, record)
		if err != nil {
			return common.Hash{}, err
		}
		slots := occupiedSlots(record)
		prefix := rowPrefix(&k.path, slots[0], k.path.BitLen, node.split)
		return branchHash(&prefix, &node.left, &node.right), nil
	default:
		return common.Hash{}, fmt.Errorf("unknown record form %d", record.Form)
	}
}

func FoldRow(key []byte, record *Record) (FoldResult, error) {
	k, err := decodeRecordKey(key)
	if err != nil {
		return FoldResult{}, err
	}
	if record == nil || record.Form != RowRoot {
		return FoldResult{}, fmt.Errorf("row fold requires a row record")
	}
	node, err := foldRow(k.path, record)
	if err != nil {
		return FoldResult{}, err
	}
	return FoldResult{Split: node.split, Left: node.left, Right: node.right}, nil
}

func foldRow(path eip8297.Bitpath, record *Record) (foldNode, error) {
	slots := occupiedSlots(record)
	if len(slots) < 2 {
		return foldNode{}, fmt.Errorf("row must contain at least two cells")
	}
	node, err := foldRange(path, record, slots, 0, len(slots), path.BitLen)
	if err != nil {
		return foldNode{}, err
	}
	return node, nil
}

func foldRange(path eip8297.Bitpath, record *Record, slots []int, from, to int, parentSplit int16) (foldNode, error) {
	split := firstSlotSplit(slots[from], slots[to-1], path.BitLen)
	middle := from
	for middle < to && slotBit(slots[middle], int(split-path.BitLen)) == 0 {
		middle++
	}
	if middle == from || middle == to {
		return foldNode{}, fmt.Errorf("row cells do not split at bit %d", split)
	}
	left, err := foldChild(path, record, slots, from, middle, split)
	if err != nil {
		return foldNode{}, err
	}
	right, err := foldChild(path, record, slots, middle, to, split)
	if err != nil {
		return foldNode{}, err
	}
	return foldNode{split: split, left: left, right: right}, nil
}

func foldChild(path eip8297.Bitpath, record *Record, slots []int, from, to int, parentSplit int16) (common.Hash, error) {
	if to-from > 1 {
		node, err := foldRange(path, record, slots, from, to, parentSplit)
		if err != nil {
			return common.Hash{}, err
		}
		prefix := rowPrefix(&path, slots[from], parentSplit+1, node.split)
		return branchHash(&prefix, &node.left, &node.right), nil
	}
	cell := &record.Cells[slots[from]]
	switch cell.Kind {
	case LeafCell:
		return leafHash(cell), nil
	case BranchCell:
		if int(path.BitLen)+4+int(cell.Prefix.BitLen) > eip8297.MaxPathBits {
			return common.Hash{}, fmt.Errorf("branch cell prefix exceeds the key length")
		}
		prefix := rowPrefix(&path, slots[from], parentSplit+1, path.BitLen+4)
		prefix.Append(&cell.Prefix)
		return branchHash(&prefix, &cell.Left, &cell.Right), nil
	default:
		return common.Hash{}, fmt.Errorf("row cell %d is empty", slots[from])
	}
}

func branchHash(prefix *eip8297.Bitpath, left, right *common.Hash) common.Hash {
	return eip8297.HashBytes(eip8297.BranchPreimage(nil, prefix, left, right))
}

func leafHash(cell *Cell) common.Hash {
	return eip8297.HashBytes(eip8297.LeafPreimage(nil, cell.Key, cell.Value[:]))
}

func occupiedSlots(record *Record) []int {
	slots := make([]int, 0, len(record.Cells))
	for slot := range record.Cells {
		if record.Cells[slot].Kind != EmptyCell {
			slots = append(slots, slot)
		}
	}
	return slots
}

func hasCells(record *Record) bool {
	for slot := range record.Cells {
		if record.Cells[slot].Kind != EmptyCell {
			return true
		}
	}
	return false
}

func singleLeaf(record *Record) (Cell, bool) {
	var leaf Cell
	count := 0
	for slot := range record.Cells {
		if record.Cells[slot].Kind == EmptyCell {
			continue
		}
		if record.Cells[slot].Kind != LeafCell {
			return Cell{}, false
		}
		count++
		leaf = record.Cells[slot]
	}
	return leaf, count == 1
}

func firstSlotSplit(left, right int, start int16) int16 {
	for offset := range 4 {
		if slotBit(left, offset) != slotBit(right, offset) {
			return start + int16(offset)
		}
	}
	return start + 4
}

func slotBit(slot, offset int) uint64 {
	return uint64(slot >> uint(3-offset) & 1)
}

func rowPrefix(path *eip8297.Bitpath, slot int, from, to int16) eip8297.Bitpath {
	var prefix eip8297.Bitpath
	for bit := from; bit < to; bit++ {
		if bit < path.BitLen {
			prefix.AppendBit(path.Bit(bit))
			continue
		}
		prefix.AppendBit(slotBit(slot, int(bit-path.BitLen)))
	}
	return prefix
}
