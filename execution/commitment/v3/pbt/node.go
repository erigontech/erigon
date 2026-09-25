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
	"github.com/erigontech/erigon/execution/commitment/eip8297"
)

type rowCell struct {
	Cell
	child *rowNode
}

type rowNode struct {
	path  eip8297.Bitpath
	key   []byte
	raw   []byte
	prev  []byte
	dirty bool
	cells [maxCells]rowCell
}

type treeRoot struct {
	form   RootForm
	raw    []byte
	prev   []byte
	leaf   Cell
	self   eip8297.Bitpath
	left   common.Hash
	right  common.Hash
	row    *rowNode
	topRow *rowNode
}

func newRow(path eip8297.Bitpath, key, raw []byte) *rowNode {
	return &rowNode{path: path, key: bytes.Clone(key), raw: bytes.Clone(raw), prev: bytes.Clone(raw)}
}

func (n *rowNode) record() Record {
	var record Record
	record.Form = RowRoot
	for slot := range n.cells {
		record.Cells[slot] = n.cells[slot].Cell
	}
	return record
}

func (n *rowNode) occupied() []int {
	slots := make([]int, 0, maxCells)
	for slot := range n.cells {
		if n.cells[slot].Kind != EmptyCell {
			slots = append(slots, slot)
		}
	}
	return slots
}

func (n *rowNode) markDirty() {
	n.dirty = true
}

func (n *rowNode) cell(slot int) *rowCell {
	return &n.cells[slot]
}

func leafCell(key []byte, value [eip8297.ValueLength]byte) rowCell {
	return rowCell{Cell: Cell{Kind: LeafCell, Key: bytes.Clone(key), Value: value}}
}

func branchCell(prefix eip8297.Bitpath, left, right common.Hash) rowCell {
	return rowCell{Cell: Cell{Kind: BranchCell, Prefix: prefix, Left: left, Right: right}}
}

func rowKeyForPath(path *eip8297.Bitpath) ([]byte, error) {
	if path.BitLen == 0 {
		return bytes.Clone(GlobalRootKey()), nil
	}
	return EncodeRowKey(path)
}

func rowFromRecord(path eip8297.Bitpath, key, raw []byte, record *Record) *rowNode {
	n := newRow(path, key, raw)
	for slot := range n.cells {
		n.cells[slot].Cell = record.Cells[slot]
		n.cells[slot].Key = bytes.Clone(record.Cells[slot].Key)
	}
	return n
}

func setLeaf(n *rowNode, slot int, key []byte, value [eip8297.ValueLength]byte) {
	n.cells[slot] = leafCell(key, value)
	n.markDirty()
}

func setBranch(n *rowNode, slot int, cell rowCell) {
	n.cells[slot] = cell
	n.markDirty()
}

func rowFoldResult(n *rowNode) (FoldResult, error) {
	key, err := rowKeyForPath(&n.path)
	if err != nil {
		return FoldResult{}, err
	}
	return FoldRow(key, recordPointer(n))
}

func recordPointer(n *rowNode) *Record {
	record := n.record()
	return &record
}

func rowTopPrefix(n *rowNode, split int16) (eip8297.Bitpath, error) {
	slots := n.occupied()
	if len(slots) < 2 {
		return eip8297.Bitpath{}, fmt.Errorf("row has fewer than two cells")
	}
	return rowPrefix(&n.path, slots[0], 0, split), nil
}

func rowChildPath(parent *rowNode, slot int, prefix eip8297.Bitpath, split int16) (eip8297.Bitpath, error) {
	path := parent.path
	var slotPath eip8297.Bitpath
	for i := range 4 {
		slotPath.AppendBit(uint64((slot >> (3 - i)) & 1))
	}
	path.Append(&slotPath)
	window := (split / 4) * 4
	need := window - path.BitLen
	if need < 0 || need > prefix.BitLen || window > eip8297.MaxPathBits {
		return eip8297.Bitpath{}, fmt.Errorf("child row path is outside the key")
	}
	part := prefix.Slice(0, need)
	path.Append(&part)
	return path, nil
}

func branchSplit(n *rowNode, slot int, cell *rowCell) int16 {
	return n.path.BitLen + 4 + cell.Prefix.BitLen
}

func branchPath(n *rowNode, slot int, cell *rowCell) eip8297.Bitpath {
	path := n.path
	var slotPath eip8297.Bitpath
	for i := range 4 {
		slotPath.AppendBit(uint64((slot >> (3 - i)) & 1))
	}
	path.Append(&slotPath)
	path.Append(&cell.Prefix)
	return path
}

func firstDifference(a, b *eip8297.Bitpath) int16 {
	limit := min(a.BitLen, b.BitLen)
	for i := range limit {
		if a.Bit(i) != b.Bit(i) {
			return i
		}
	}
	return limit
}

func slotAt(path *eip8297.Bitpath, start int16) int {
	var slot int
	for i := range 4 {
		slot = slot<<1 | int(path.Bit(start+int16(i)))
	}
	return slot
}

func keyPath(key []byte) (eip8297.Bitpath, error) {
	keyLen, ok := eip8297.ZoneKeyLength(firstByte(key))
	if !ok || len(key) != keyLen {
		return eip8297.Bitpath{}, fmt.Errorf("invalid tree key")
	}
	return eip8297.PathFromBits(key, int16(keyLen*8)), nil
}
