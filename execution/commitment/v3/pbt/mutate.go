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
		t.registerRow(row)
		return nil
	case LeafCell:
		oldPath, err := keyPath(cell.Key)
		if err != nil {
			return err
		}
		d := firstDifference(&oldPath, &path)
		if oldPath.BitLen == path.BitLen && d == oldPath.BitLen {
			setLeaf(row, slot, key, value)
			return nil
		}
		if d/4 == row.path.BitLen/4 {
			newSlot := int(path.Bit(row.path.BitLen)*8 + path.Bit(row.path.BitLen+1)*4 + path.Bit(row.path.BitLen+2)*2 + path.Bit(row.path.BitLen+3))
			setLeaf(row, newSlot, key, value)
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
		t.registerRow(child)
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
	t.registerRow(child)
	return nil
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
	parent.markDirty()
	t.registerRow(parent)
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
