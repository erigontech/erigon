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
	"math/bits"
	"sort"
	"sync"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
)

type foldNode struct {
	split       int16
	left, right common.Hash
	hash        common.Hash
}

type FoldResult struct {
	Split       int16
	Left, Right common.Hash
}

var (
	hashHook        func([]byte)
	foldRefHook     func(bool)
	hashScratchPool sync.Pool
)

type hashScratchBuffer struct {
	bytes []byte
}

func rowRoutingResult(row *rowNode) (FoldResult, error) {
	var occupied [maxCells]int
	slots := row.occupiedInto(occupied[:0])
	if len(slots) < 2 {
		return FoldResult{}, fmt.Errorf("row must contain at least two cells")
	}
	return FoldResult{Split: firstSlotSplit(slots[0], slots[len(slots)-1], row.path.BitLen)}, nil
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
		node, err := foldRowWithRefs(k.path, record, nil)
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

func (t *Trie) foldDirtyRows() error {
	if t.root != nil && t.root.form == ExtRoot && t.root.topRow == nil && !t.stopsUpperPath(&t.root.self) && !(t.upperOnly && t.root.self.BitLen >= t.rootRecordPath().BitLen+264 && isStoragePath(&t.root.self)) {
		row, err := t.extTopRow(t.root)
		if err != nil {
			return err
		}
		t.root.topRow = row
	}
	dirty := make([]*rowNode, 0, len(t.dirtyRows))
	for _, row := range t.dirtyRows {
		dirty = append(dirty, row)
	}
	sort.SliceStable(dirty, func(i, j int) bool { return dirty[i].path.BitLen > dirty[j].path.BitLen })
	for _, row := range dirty {
		if row.tombstone {
			continue
		}
		var occupied [maxCells]int
		slots := row.occupiedInto(occupied[:0])
		if len(slots) >= 2 {
			result, err := t.foldRowResult(row)
			if err != nil {
				return err
			}
			row.folded = true
			row.foldResult = result
		} else if len(slots) == 1 && row.cell(slots[0]).Kind == BranchCell && row.cell(slots[0]).child != nil {
			path, left, right, err := t.rowDescriptor(row.cell(slots[0]).child)
			if err != nil {
				return err
			}
			start := row.path.BitLen + 4
			if path.BitLen < start {
				return errInsertKey
			}
			cell := row.cell(slots[0])
			cell.Prefix = path.Slice(start, path.BitLen)
			cell.Left, cell.Right = left, right
			row.markCellDirty(slots[0])
		}
		if len(slots) == 0 {
			continue
		}
		path, left, right, err := t.rowDescriptor(row)
		if err != nil {
			return err
		}
		if row.parent != nil && row.parent.cell(row.parentSlot).child == row {
			cell := row.parent.cell(row.parentSlot)
			start := row.parent.path.BitLen + 4
			if path.BitLen < start {
				return errInsertKey
			}
			cell.Prefix = path.Slice(start, path.BitLen)
			cell.Left, cell.Right = left, right
			row.parent.markCellDirty(row.parentSlot)
		}
		if t.root != nil && t.root.row == row {
			t.foldedRoot = branchHash(&path, &left, &right)
			t.foldedRootReady = true
		}
		if t.root != nil && t.root.topRow == row && t.root.form == ExtRoot {
			t.root.self = path
			t.root.left, t.root.right = left, right
			t.foldedRoot = branchHash(&t.root.self, &t.root.left, &t.root.right)
			t.foldedRootReady = true
		}
	}
	if t.root == nil {
		return nil
	}
	switch t.root.form {
	case LeafRoot:
		t.foldedRoot = leafHash(&t.root.leaf)
		t.foldedRootReady = true
	case RowRoot:
		if t.root.row == nil {
			t.foldedRoot = eip8297.EmptyTreeHash
			t.foldedRootReady = true
		}
	case ExtRoot:
		if !t.foldedRootReady {
			t.foldedRoot = branchHash(&t.root.self, &t.root.left, &t.root.right)
			t.foldedRootReady = true
		}
	}
	return nil
}

func (t *Trie) rowDescriptor(row *rowNode) (eip8297.Bitpath, common.Hash, common.Hash, error) {
	var occupied [maxCells]int
	slots := row.occupiedInto(occupied[:0])
	switch len(slots) {
	case 0:
		return eip8297.Bitpath{}, common.Hash{}, common.Hash{}, errInsertKey
	case 1:
		cell := row.cell(slots[0])
		switch cell.Kind {
		case LeafCell:
			path, err := keyPath(cell.Key)
			if err != nil {
				return eip8297.Bitpath{}, common.Hash{}, common.Hash{}, err
			}
			return path, leafHash(cell.Cell), common.Hash{}, nil
		case BranchCell:
			return branchPath(row, slots[0], cell), cell.Left, cell.Right, nil
		default:
			return eip8297.Bitpath{}, common.Hash{}, common.Hash{}, errInsertKey
		}
	case 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16:
		if !row.folded {
			return eip8297.Bitpath{}, common.Hash{}, common.Hash{}, fmt.Errorf("row %x was not folded", row.key)
		}
		prefix, err := rowTopPrefix(row, row.foldResult.Split)
		if err != nil {
			return eip8297.Bitpath{}, common.Hash{}, common.Hash{}, err
		}
		return prefix, row.foldResult.Left, row.foldResult.Right, nil
	default:
		return eip8297.Bitpath{}, common.Hash{}, common.Hash{}, errInsertKey
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
	node, err := foldRowWithRefs(k.path, record, nil)
	if err != nil {
		return FoldResult{}, err
	}
	return FoldResult{Split: node.split, Left: node.left, Right: node.right}, nil
}

func foldRowWithRefs(path eip8297.Bitpath, record *Record, refs *rowNode) (foldNode, error) {
	slots := occupiedSlots(record)
	if len(slots) < 2 {
		return foldNode{}, fmt.Errorf("row must contain at least two cells")
	}
	split := firstSlotSplit(slots[0], slots[len(slots)-1], path.BitLen)
	prefix := rowPrefix(&path, slots[0], path.BitLen, split)
	if refs != nil {
		if node, ok := refs.cachedInternalHash(slotsMask(slots, 0, len(slots)), path.BitLen, split, &prefix); ok {
			return node, nil
		}
	}
	node, err := foldRange(path, record, slots, 0, len(slots), path.BitLen, refs)
	if err != nil {
		return foldNode{}, err
	}
	return node, nil
}

func foldRange(path eip8297.Bitpath, record *Record, slots []int, from, to int, parentSplit int16, refs *rowNode) (foldNode, error) {
	split := firstSlotSplit(slots[from], slots[to-1], path.BitLen)
	middle := from
	for middle < to && slotBit(slots[middle], int(split-path.BitLen)) == 0 {
		middle++
	}
	if middle == from || middle == to {
		return foldNode{}, fmt.Errorf("row cells do not split at bit %d", split)
	}
	left, err := foldChild(path, record, slots, from, middle, split, refs)
	if err != nil {
		return foldNode{}, err
	}
	right, err := foldChild(path, record, slots, middle, to, split, refs)
	if err != nil {
		return foldNode{}, err
	}
	return foldNode{split: split, left: left, right: right}, nil
}

func foldChild(path eip8297.Bitpath, record *Record, slots []int, from, to int, parentSplit int16, refs *rowNode) (common.Hash, error) {
	if to-from > 1 {
		split := firstSlotSplit(slots[from], slots[to-1], path.BitLen)
		prefix := rowPrefix(&path, slots[from], parentSplit+1, split)
		if refs != nil {
			if node, ok := refs.cachedInternalHash(slotsMask(slots, from, to), parentSplit, split, &prefix); ok {
				return node.hash, nil
			}
		}
		node, err := foldRange(path, record, slots, from, to, parentSplit, refs)
		if err != nil {
			return common.Hash{}, err
		}
		prefix = rowPrefix(&path, slots[from], parentSplit+1, node.split)
		return branchHash(&prefix, &node.left, &node.right), nil
	}
	cell := &record.Cells[slots[from]]
	switch cell.Kind {
	case LeafCell:
		if refs != nil {
			if hash, ok := refs.cachedCellHash(slots[from], nil); ok {
				return hash, nil
			}
		}
		return leafHash(cell), nil
	case BranchCell:
		if int(path.BitLen)+4+int(cell.Prefix.BitLen) > eip8297.MaxPathBits {
			return common.Hash{}, fmt.Errorf("branch cell prefix exceeds the key length")
		}
		prefix := rowPrefix(&path, slots[from], parentSplit+1, path.BitLen+4)
		prefix.Append(&cell.Prefix)
		if refs != nil {
			if hash, ok := refs.cachedCellHash(slots[from], &prefix); ok {
				return hash, nil
			}
		}
		return branchHash(&prefix, &cell.Left, &cell.Right), nil
	default:
		return common.Hash{}, fmt.Errorf("row cell %d is empty", slots[from])
	}
}

func branchHash(prefix *eip8297.Bitpath, left, right *common.Hash) common.Hash {
	scratch := getHashScratch(1 + 2 + (int(prefix.BitLen)+7)/8 + 64)
	preimage := eip8297.BranchPreimage(scratch.bytes[:0], prefix, left, right)
	hash := hashBytes(preimage)
	scratch.bytes = preimage[:0]
	hashScratchPool.Put(scratch)
	return hash
}

func leafHash(cell *Cell) common.Hash {
	scratch := getHashScratch(1 + len(cell.Key) + eip8297.ValueLength)
	preimage := eip8297.LeafPreimage(scratch.bytes[:0], cell.Key, cell.Value[:])
	hash := hashBytes(preimage)
	scratch.bytes = preimage[:0]
	hashScratchPool.Put(scratch)
	return hash
}

func getHashScratch(size int) *hashScratchBuffer {
	scratch, _ := hashScratchPool.Get().(*hashScratchBuffer)
	if scratch == nil {
		return &hashScratchBuffer{bytes: make([]byte, 0, size)}
	}
	if cap(scratch.bytes) < size {
		scratch.bytes = make([]byte, 0, size)
	}
	return scratch
}

func hashBytes(preimage []byte) common.Hash {
	if hashHook != nil {
		hashHook(preimage)
	}
	return eip8297.HashBytes(preimage)
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
	return start + int16(min(bits.LeadingZeros8(uint8(left^right)<<4), 4))
}

func slotBit(slot, offset int) uint64 {
	return uint64(slot >> uint(3-offset) & 1)
}

func rowPrefix(path *eip8297.Bitpath, slot int, from, to int16) eip8297.Bitpath {
	if from >= to {
		return eip8297.Bitpath{}
	}
	if from < path.BitLen {
		pathEnd := min(to, path.BitLen)
		prefix := path.Slice(from, pathEnd)
		if pathEnd == to {
			return prefix
		}
		slotPath := eip8297.PathFromBits([]byte{byte(slot) << 4}, 4)
		suffix := slotPath.Slice(0, to-pathEnd)
		prefix.Append(&suffix)
		return prefix
	}
	slotPath := eip8297.PathFromBits([]byte{byte(slot) << 4}, 4)
	return slotPath.Slice(from-path.BitLen, to-path.BitLen)
}
