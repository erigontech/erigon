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
	"math/bits"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
)

type rowCell struct {
	*Cell
	Kind  CellKind
	child *rowNode
}

type rowNode struct {
	path       eip8297.Bitpath
	key        []byte
	name       string
	raw        []byte
	prev       []byte
	dirty      bool
	routing    bool
	tombstone  bool
	folded     bool
	foldResult FoldResult
	refs       *commitment.LeafRefs
	refMask    uint16
	dirtyCells uint16
	parent     *rowNode
	parentSlot int
	cells      [maxCells]rowCell
}

const (
	rowChunkInitial = 4
	rowChunkMax     = 64
)

type rowChunk struct {
	rows []rowNode
	used int
}

type cellChunk struct {
	cells []Cell
	used  int
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
	row := new(rowNode)
	initRowOwned(row, path, key, raw)
	return row
}

func initRowOwned(row *rowNode, path eip8297.Bitpath, key, raw []byte) {
	rawCopy := bytes.Clone(raw)
	*row = rowNode{path: path, key: key, name: string(key), raw: rawCopy, prev: rawCopy}
}

func (n *rowNode) record() Record {
	var record Record
	record.Form = RowRoot
	for slot := range n.cells {
		if n.cells[slot].Kind != EmptyCell {
			record.Cells[slot] = *n.cells[slot].Cell
		}
	}
	return record
}

func (n *rowNode) occupiedInto(slots []int) []int {
	slots = slots[:0]
	for slot := range n.cells {
		if n.cells[slot].Kind != EmptyCell {
			slots = append(slots, slot)
		}
	}
	return slots
}

func (n *rowNode) occupiedCount() int {
	count := 0
	for slot := range n.cells {
		if n.cells[slot].Kind != EmptyCell {
			count++
		}
	}
	return count
}

func (n *rowNode) markDirty() {
	n.dirty = true
	n.folded = false
}

func (n *rowNode) markCellDirty(slot int) {
	n.dirtyCells |= uint16(1) << slot
}

func (n *rowNode) cell(slot int) *rowCell {
	return &n.cells[slot]
}

func leafCell(key []byte, value [eip8297.ValueLength]byte) rowCell {
	return rowCell{Cell: &Cell{Kind: LeafCell, Key: key, Value: value}, Kind: LeafCell}
}

func branchCell(prefix eip8297.Bitpath, left, right common.Hash) rowCell {
	return rowCell{Cell: &Cell{Kind: BranchCell, Prefix: prefix, Left: left, Right: right}, Kind: BranchCell}
}

func (t *Trie) newCell(cell Cell) *Cell {
	if t.cellChunkIndex == len(t.cellChunks) {
		size := rowChunkInitial
		if len(t.cellChunks) != 0 {
			size = min(len(t.cellChunks[len(t.cellChunks)-1].cells)*2, rowChunkMax)
		}
		t.cellChunks = append(t.cellChunks, &cellChunk{cells: make([]Cell, size)})
	}
	chunk := t.cellChunks[t.cellChunkIndex]
	if chunk.used == len(chunk.cells) {
		t.cellChunkIndex++
		if t.cellChunkIndex == len(t.cellChunks) {
			t.cellChunks = append(t.cellChunks, &cellChunk{cells: make([]Cell, min(len(chunk.cells)*2, rowChunkMax))})
		}
		chunk = t.cellChunks[t.cellChunkIndex]
	}
	cellCopy := &chunk.cells[chunk.used]
	chunk.used++
	*cellCopy = cell
	return cellCopy
}

func (t *Trie) leafCell(key []byte, value [eip8297.ValueLength]byte) rowCell {
	return rowCell{Cell: t.newCell(Cell{Kind: LeafCell, Key: key, Value: value}), Kind: LeafCell}
}

func (t *Trie) branchCell(prefix eip8297.Bitpath, left, right common.Hash) rowCell {
	return rowCell{Cell: t.newCell(Cell{Kind: BranchCell, Prefix: prefix, Left: left, Right: right}), Kind: BranchCell}
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
		if record.Cells[slot].Kind != EmptyCell {
			cell := record.Cells[slot]
			n.cells[slot] = rowCell{Cell: &cell, Kind: cell.Kind}
		}
	}
	return n
}

func (t *Trie) newRow(path eip8297.Bitpath, key, raw []byte) *rowNode {
	if t.rowChunkIndex == len(t.rowChunks) {
		size := rowChunkInitial
		if len(t.rowChunks) != 0 {
			size = min(len(t.rowChunks[len(t.rowChunks)-1].rows)*2, rowChunkMax)
		}
		t.rowChunks = append(t.rowChunks, &rowChunk{rows: make([]rowNode, size)})
	}
	chunk := t.rowChunks[t.rowChunkIndex]
	if chunk.used == len(chunk.rows) {
		t.rowChunkIndex++
		if t.rowChunkIndex == len(t.rowChunks) {
			t.rowChunks = append(t.rowChunks, &rowChunk{rows: make([]rowNode, min(len(chunk.rows)*2, rowChunkMax))})
		}
		chunk = t.rowChunks[t.rowChunkIndex]
	}
	row := &chunk.rows[chunk.used]
	chunk.used++
	initRowOwned(row, path, key, raw)
	return row
}

func (t *Trie) rowFromRecord(path eip8297.Bitpath, key, raw []byte, record *Record) *rowNode {
	n := t.newRow(path, key, raw)
	for slot := range n.cells {
		if record.Cells[slot].Kind != EmptyCell {
			cell := record.Cells[slot]
			n.cells[slot] = rowCell{Cell: &cell, Kind: cell.Kind}
		}
	}
	if refs := leafRefsOf(t.ctx, key, raw); refs != nil {
		n.refs = refs
		n.refMask = refs.Mask
	}
	return n
}

func (t *Trie) setLeaf(n *rowNode, slot int, key []byte, value [eip8297.ValueLength]byte) {
	n.cells[slot] = t.leafCell(key, value)
	n.markCellDirty(slot)
}

func setBranch(n *rowNode, slot int, cell rowCell) {
	n.cells[slot] = cell
	n.markCellDirty(slot)
}

func rowFoldResult(n *rowNode) (FoldResult, error) {
	key, err := rowKeyForPath(&n.path)
	if err != nil {
		return FoldResult{}, err
	}
	record := n.record()
	return FoldRow(key, &record)
}

func rowTopPrefix(n *rowNode, split int16) (eip8297.Bitpath, error) {
	var occupied [maxCells]int
	slots := n.occupiedInto(occupied[:0])
	if len(slots) < 2 {
		return eip8297.Bitpath{}, fmt.Errorf("row has fewer than two cells")
	}
	return rowPrefix(&n.path, slots[0], 0, split), nil
}

func rowChildPath(parent *rowNode, slot int, prefix eip8297.Bitpath, split int16) (eip8297.Bitpath, error) {
	path := parent.path
	slotPath := eip8297.PathFromBits([]byte{byte(slot) << 4}, 4)
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

func branchSplit(n *rowNode, cell *rowCell) int16 {
	return n.path.BitLen + 4 + cell.Prefix.BitLen
}

func branchPath(n *rowNode, slot int, cell *rowCell) eip8297.Bitpath {
	return branchPathFrom(n.path, slot, &cell.Prefix)
}

func branchPathFrom(path eip8297.Bitpath, slot int, prefix *eip8297.Bitpath) eip8297.Bitpath {
	slotPath := eip8297.PathFromBits([]byte{byte(slot) << 4}, 4)
	path.Append(&slotPath)
	path.Append(prefix)
	return path
}

func firstDifference(a, b *eip8297.Bitpath) int16 {
	limit := min(a.BitLen, b.BitLen)
	for word := 0; word*64 < int(limit); word++ {
		remaining := int(limit) - word*64
		xor := a.Words[word] ^ b.Words[word]
		if remaining < 64 {
			xor &= ^uint64(0) << uint(64-remaining)
		}
		if xor != 0 {
			return int16(word*64 + bits.LeadingZeros64(xor))
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
