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

var (
	errInsertKey   = fmt.Errorf("invalid insert key")
	errInsertValue = fmt.Errorf("zero insert value")
)

func (t *Trie) insert(key []byte, value [eip8297.ValueLength]byte) error {
	path, err := keyPath(key)
	if err != nil {
		return err
	}
	if value == ([eip8297.ValueLength]byte{}) {
		return errInsertValue
	}
	root, err := t.loadRoot()
	if err != nil {
		return err
	}
	if root.form == RowRoot && root.row == nil {
		root.form = LeafRoot
		root.leaf = leafCell(key, value).Cell
		root.raw = nil
		return nil
	}
	switch root.form {
	case RowRoot:
		return t.insertRow(root.row, path, key, value)
	case LeafRoot:
		oldPath, err := keyPath(root.leaf.Key)
		if err != nil {
			return err
		}
		d := firstDifference(&oldPath, &path)
		if oldPath.BitLen == path.BitLen && d == oldPath.BitLen {
			root.leaf = leafCell(key, value).Cell
			return nil
		}
		return t.splitRootLeaf(root, oldPath, root.leaf, path, key, value, d)
	case ExtRoot:
		return t.insertExtRoot(root, path, key, value)
	default:
		return fmt.Errorf("unknown root form %d", root.form)
	}
}

func (t *Trie) splitRootLeaf(root *treeRoot, oldPath eip8297.Bitpath, old Cell, newPath eip8297.Bitpath, newKey []byte, newValue [eip8297.ValueLength]byte, split int16) error {
	window := (split / 4) * 4
	path := oldPath.Slice(0, window)
	row, err := t.twoLeafRow(path, old.Key, old.Value, newKey, newValue)
	if err != nil {
		return err
	}
	if window == 0 {
		row.prev = bytes.Clone(root.raw)
		root.form = RowRoot
		root.row = row
		t.registerRow(row)
		return nil
	}
	result, err := rowFoldResult(row)
	if err != nil {
		return err
	}
	root.form = ExtRoot
	root.self = oldPath.Slice(0, split)
	root.left, root.right = result.Left, result.Right
	root.topRow = row
	t.registerRow(row)
	return nil
}

func (t *Trie) insertExtRoot(root *treeRoot, path eip8297.Bitpath, key []byte, value [eip8297.ValueLength]byte) error {
	d := firstDifference(&root.self, &path)
	split := root.self.BitLen
	if d >= split || d/4 == split/4 {
		row, err := t.extTopRow(root)
		if err != nil {
			return err
		}
		if err := t.insertRow(row, path, key, value); err != nil {
			return err
		}
		return t.refreshRootFromRow(root, row)
	}
	window := (d / 4) * 4
	oldPath := root.self
	if window == 0 {
		row := newRow(eip8297.Bitpath{}, GlobalRootKey(), root.raw)
		oldSlot := slotAt(&oldPath, 0)
		newSlot := slotAt(&path, 0)
		oldPrefix := oldPath.Slice(4, oldPath.BitLen)
		row.cells[oldSlot] = branchCell(oldPrefix, root.left, root.right)
		row.cells[newSlot] = leafCell(key, value)
		row.markDirty()
		root.form = RowRoot
		root.row = row
		t.registerRow(row)
		return nil
	}
	rowPath := path.Slice(0, window)
	row, err := newRowFromBranch(rowPath, key, value, oldPath, root.left, root.right)
	if err != nil {
		return err
	}
	result, err := rowFoldResult(row)
	if err != nil {
		return err
	}
	root.self = path.Slice(0, d)
	root.left, root.right = result.Left, result.Right
	root.topRow = row
	t.registerRow(row)
	return nil
}

func (t *Trie) extTopRow(root *treeRoot) (*rowNode, error) {
	if root.topRow != nil {
		return root.topRow, nil
	}
	window := (root.self.BitLen / 4) * 4
	path := root.self.Slice(0, window)
	row, err := t.loadRow(path)
	if err != nil {
		return nil, err
	}
	root.topRow = row
	return row, nil
}

func (t *Trie) refreshRootFromRow(root *treeRoot, row *rowNode) error {
	result, err := rowFoldResult(row)
	if err != nil {
		return err
	}
	self, err := rowTopPrefix(row, result.Split)
	if err != nil {
		return err
	}
	root.left, root.right = result.Left, result.Right
	root.self = self
	root.topRow = row
	return nil
}

func (t *Trie) insertRow(row *rowNode, path eip8297.Bitpath, key []byte, value [eip8297.ValueLength]byte) error {
	if path.BitLen <= row.path.BitLen || eip8297.CommonPrefixBitsAt(&path, 0, &row.path) != row.path.BitLen {
		return errInsertKey
	}
	slot := int(path.Bit(row.path.BitLen)*8 + path.Bit(row.path.BitLen+1)*4 + path.Bit(row.path.BitLen+2)*2 + path.Bit(row.path.BitLen+3))
	cell := row.cell(slot)
	switch cell.Kind {
	case EmptyCell:
		setLeaf(row, slot, key, value)
		t.markDirty(row)
		return nil
	case LeafCell:
		oldPath, err := keyPath(cell.Key)
		if err != nil {
			return err
		}
		d := firstDifference(&oldPath, &path)
		if oldPath.BitLen == path.BitLen && d == oldPath.BitLen {
			setLeaf(row, slot, key, value)
			t.markDirty(row)
			return nil
		}
		if d/4 == row.path.BitLen/4 {
			newSlot := int(path.Bit(row.path.BitLen)*8 + path.Bit(row.path.BitLen+1)*4 + path.Bit(row.path.BitLen+2)*2 + path.Bit(row.path.BitLen+3))
			setLeaf(row, newSlot, key, value)
			t.markDirty(row)
			return nil
		}
		window := (d / 4) * 4
		childPath := path.Slice(0, window)
		child, err := t.twoLeafRow(childPath, cell.Key, cell.Value, key, value)
		if err != nil {
			return err
		}
		result, err := rowFoldResult(child)
		if err != nil {
			return err
		}
		prefix := path.Slice(row.path.BitLen+4, result.Split)
		setBranch(row, slot, branchCell(prefix, result.Left, result.Right))
		row.cell(slot).child = child
		child.parent = row
		child.parentSlot = slot
		t.registerRow(child)
		t.markDirty(row)
		return nil
	case BranchCell:
		return t.insertBranch(row, slot, path, key, value)
	default:
		return errInsertKey
	}
}

func (t *Trie) insertBranch(row *rowNode, slot int, path eip8297.Bitpath, key []byte, value [eip8297.ValueLength]byte) error {
	cell := row.cell(slot)
	branchPath := branchPath(row, slot, cell)
	d := firstDifference(&branchPath, &path)
	split := branchSplit(row, slot, cell)
	if d >= split || d/4 == split/4 {
		child, err := t.loadBranchChild(row, slot)
		if err != nil {
			return err
		}
		if err := t.insertRow(child, path, key, value); err != nil {
			return err
		}
		return t.refreshBranch(row, slot, child)
	}
	window := (d / 4) * 4
	childPath := path.Slice(0, window)
	child, err := newRowFromBranch(childPath, key, value, branchPath, cell.Left, cell.Right)
	if err != nil {
		return err
	}
	result, err := rowFoldResult(child)
	if err != nil {
		return err
	}
	full, err := rowTopPrefix(child, result.Split)
	if err != nil {
		return err
	}
	prefix := full.Slice(row.path.BitLen+4, result.Split)
	setBranch(row, slot, branchCell(prefix, result.Left, result.Right))
	row.cell(slot).child = child
	child.parent = row
	child.parentSlot = slot
	t.registerRow(child)
	t.markDirty(row)
	return nil
}

func (t *Trie) remove(key []byte) error {
	path, err := keyPath(key)
	if err != nil {
		return err
	}
	root, err := t.loadRoot()
	if err != nil {
		return err
	}
	switch root.form {
	case RowRoot:
		if root.row == nil {
			return nil
		}
		if found, err := t.removeFromRow(root.row, path, key); err != nil {
			return err
		} else if !found {
			return nil
		}
		return nil
	case LeafRoot:
		if !bytes.Equal(root.leaf.Key, key) {
			return nil
		}
		root.form = RowRoot
		root.leaf = Cell{}
		return nil
	case ExtRoot:
		if firstDifference(&root.self, &path) < root.self.BitLen {
			return nil
		}
		row, err := t.extTopRow(root)
		if err != nil {
			return err
		}
		found, err := t.removeFromRow(row, path, key)
		if err != nil || !found {
			return err
		}
		return nil
	default:
		return fmt.Errorf("unknown root form %d", root.form)
	}
}

func (t *Trie) removeFromRow(row *rowNode, path eip8297.Bitpath, key []byte) (bool, error) {
	if path.BitLen <= row.path.BitLen || eip8297.CommonPrefixBitsAt(&path, 0, &row.path) != row.path.BitLen {
		return false, nil
	}
	slot := int(keyBit(row.path.BitLen, key))
	cell := row.cell(slot)
	switch cell.Kind {
	case EmptyCell:
		return false, nil
	case LeafCell:
		if !bytes.Equal(cell.Key, key) {
			return false, nil
		}
		row.cells[slot] = rowCell{}
		t.markDirty(row)
		return true, nil
	case BranchCell:
		branchPath := branchPath(row, slot, cell)
		if firstDifference(&branchPath, &path) < branchPath.BitLen {
			return false, nil
		}
		child, err := t.loadBranchChild(row, slot)
		if err != nil {
			return false, err
		}
		found, err := t.removeFromRow(child, path, key)
		return found, err
	default:
		return false, errInsertKey
	}
}

func (t *Trie) normalize() error {
	root := t.root
	switch root.form {
	case RowRoot:
		if root.row == nil {
			return nil
		}
		return t.normalizeRootRow(root, root.row)
	case ExtRoot:
		row, err := t.extTopRow(root)
		if err != nil {
			return err
		}
		return t.normalizeRootRow(root, row)
	default:
		return nil
	}
}

func (t *Trie) normalizeRootRow(root *treeRoot, row *rowNode) error {
	if err := t.normalizeChildren(row); err != nil {
		return err
	}
	slots := row.occupied()
	if len(slots) >= 2 {
		if root.form == ExtRoot {
			return t.refreshRootFromRow(root, row)
		}
		return nil
	}
	t.rootDirty = true
	if row.path.BitLen == 0 {
		delete(t.dirtyRows, string(GlobalRootKey()))
	} else {
		row.tombstone = true
		t.markDirty(row)
	}
	if len(slots) == 0 {
		root.form = RowRoot
		root.row = nil
		root.topRow = nil
		root.self = eip8297.Bitpath{}
		root.left, root.right = common.Hash{}, common.Hash{}
		return nil
	}
	cell := row.cell(slots[0])
	if cell.Kind == LeafCell {
		root.form = LeafRoot
		root.leaf = cell.Cell
	} else {
		full := branchPath(row, slots[0], cell)
		root.form = ExtRoot
		root.self = full
		root.left, root.right = cell.Left, cell.Right
	}
	root.row = nil
	root.topRow = nil
	return nil
}

func (t *Trie) normalizeChildren(row *rowNode) error {
	for slot := range row.cells {
		cell := row.cell(slot)
		if cell.Kind != BranchCell || cell.child == nil {
			continue
		}
		if err := t.normalizeRow(cell.child); err != nil {
			return err
		}
	}
	return nil
}

func (t *Trie) normalizeRow(row *rowNode) error {
	if err := t.normalizeChildren(row); err != nil {
		return err
	}
	if !row.dirty {
		return nil
	}
	slots := row.occupied()
	if len(slots) >= 2 {
		if row.parent == nil {
			return nil
		}
		return t.refreshBranch(row.parent, row.parentSlot, row)
	}
	row.tombstone = true
	t.markDirty(row)
	parent := row.parent
	if parent == nil {
		return nil
	}
	if len(slots) == 0 {
		parent.cells[row.parentSlot] = rowCell{}
	} else {
		cell := *row.cell(slots[0])
		cell.child = nil
		if cell.Kind == BranchCell {
			full := branchPath(row, slots[0], row.cell(slots[0]))
			start := parent.path.BitLen + 4
			if full.BitLen < start {
				return errInsertKey
			}
			cell.Prefix = full.Slice(start, full.BitLen)
		}
		parent.cells[row.parentSlot] = cell
	}
	t.markDirty(parent)
	return nil
}

func (t *Trie) dropPrefix(prefix []byte) error {
	if len(prefix) != 33 || prefix[0] != eip8297.StorageZone {
		return errInsertKey
	}
	keys, err := t.allKeys()
	if err != nil {
		return err
	}
	for _, key := range keys {
		if bytes.HasPrefix(key, prefix) {
			if err := t.remove(key); err != nil {
				return err
			}
		}
	}
	return nil
}

func (t *Trie) allKeys() ([][]byte, error) {
	root, err := t.loadRoot()
	if err != nil {
		return nil, err
	}
	var keys [][]byte
	var visit func(*rowNode) error
	visit = func(row *rowNode) error {
		for slot := range row.cells {
			cell := row.cell(slot)
			switch cell.Kind {
			case LeafCell:
				keys = append(keys, bytes.Clone(cell.Key))
			case BranchCell:
				child, err := t.loadBranchChild(row, slot)
				if err != nil {
					return err
				}
				if err := visit(child); err != nil {
					return err
				}
			}
		}
		return nil
	}
	switch root.form {
	case LeafRoot:
		keys = append(keys, bytes.Clone(root.leaf.Key))
	case ExtRoot:
		row, err := t.extTopRow(root)
		if err != nil {
			return nil, err
		}
		if err := visit(row); err != nil {
			return nil, err
		}
	case RowRoot:
		if root.row != nil {
			if err := visit(root.row); err != nil {
				return nil, err
			}
		}
	}
	return keys, nil
}

func (t *Trie) refreshBranch(parent *rowNode, slot int, child *rowNode) error {
	result, err := rowFoldResult(child)
	if err != nil {
		return err
	}
	full, err := rowTopPrefix(child, result.Split)
	if err != nil {
		return err
	}
	start := parent.path.BitLen + 4
	if full.BitLen < start {
		return errInsertKey
	}
	cell := parent.cell(slot)
	cell.Prefix = full.Slice(start, result.Split)
	cell.Left, cell.Right = result.Left, result.Right
	cell.child = child
	t.markDirty(parent)
	return nil
}

func (t *Trie) twoLeafRow(path eip8297.Bitpath, keyA []byte, valueA [eip8297.ValueLength]byte, keyB []byte, valueB [eip8297.ValueLength]byte) (*rowNode, error) {
	keyAPath, err := keyPath(keyA)
	if err != nil {
		return nil, err
	}
	keyBPath, err := keyPath(keyB)
	if err != nil {
		return nil, err
	}
	if keyAPath.BitLen <= path.BitLen || keyBPath.BitLen <= path.BitLen || eip8297.CommonPrefixBitsAt(&keyAPath, 0, &path) != path.BitLen || eip8297.CommonPrefixBitsAt(&keyBPath, 0, &path) != path.BitLen {
		return nil, errInsertKey
	}
	a := int(keyAPath.Bit(path.BitLen)*8 + keyAPath.Bit(path.BitLen+1)*4 + keyAPath.Bit(path.BitLen+2)*2 + keyAPath.Bit(path.BitLen+3))
	b := int(keyBPath.Bit(path.BitLen)*8 + keyBPath.Bit(path.BitLen+1)*4 + keyBPath.Bit(path.BitLen+2)*2 + keyBPath.Bit(path.BitLen+3))
	if a == b {
		return nil, errInsertKey
	}
	row := newRow(path, nil, nil)
	row.cells[a] = leafCell(keyA, valueA)
	row.cells[b] = leafCell(keyB, valueB)
	row.markDirty()
	return row, nil
}

func newRowFromBranch(path eip8297.Bitpath, key []byte, value [eip8297.ValueLength]byte, oldPath eip8297.Bitpath, left, right common.Hash) (*rowNode, error) {
	if oldPath.BitLen < path.BitLen+4 {
		return nil, errInsertKey
	}
	row := newRow(path, nil, nil)
	newSlot := int(keyBit(path.BitLen, key))
	oldSlot := int(oldPath.Bit(path.BitLen)*8 + oldPath.Bit(path.BitLen+1)*4 + oldPath.Bit(path.BitLen+2)*2 + oldPath.Bit(path.BitLen+3))
	row.cells[newSlot] = leafCell(key, value)
	oldPrefix := oldPath.Slice(path.BitLen+4, oldPath.BitLen)
	row.cells[oldSlot] = branchCell(oldPrefix, left, right)
	row.markDirty()
	return row, nil
}

func keyBit(bit int16, key []byte) uint64 {
	path, err := keyPath(key)
	if err != nil || bit >= path.BitLen {
		return 0
	}
	return path.Bit(bit)*8 + path.Bit(bit+1)*4 + path.Bit(bit+2)*2 + path.Bit(bit+3)
}
