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
	"sort"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
)

type subtreeCell struct {
	path eip8297.Bitpath
	cell rowCell
}

func isStoragePath(path *eip8297.Bitpath) bool {
	return path.BitLen >= 8 && pathByte(path, 0) == eip8297.StorageZone
}

func (t *Trie) processUpperOps(ops []Op, changed map[string]phaseBucketResult) (common.Hash, error) {
	t.upperOnly = true
	defer func() { t.upperOnly = false }()
	for i := range ops {
		op := ops[i]
		var err error
		switch {
		case op.Value == ([eip8297.ValueLength]byte{}):
			err = t.remove(op.Key)
		default:
			err = t.insert(op.Key, op.Value)
		}
		if err != nil {
			return common.Hash{}, fmt.Errorf("apply upper op %x: %w", op.Key, err)
		}
		if err := t.refreshRouting(); err != nil {
			return common.Hash{}, fmt.Errorf("refresh upper op %x: %w", op.Key, err)
		}
		t.rootDirty = true
	}
	keys := make([]string, 0, len(changed))
	for key := range changed {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		result := changed[key]
		bucketPath, err := bucketPathForKey([]byte(key))
		if err != nil {
			return common.Hash{}, err
		}
		if _, err := t.removeSubtree(&bucketPath); err != nil {
			return common.Hash{}, fmt.Errorf("remove upper bucket %x: %w", []byte(key), err)
		}
		if result.present {
			subtree, err := subtreeForDescriptor([]byte(key), result.descriptor)
			if err != nil {
				return common.Hash{}, fmt.Errorf("descriptor bucket %x: %w", []byte(key), err)
			}
			if err := t.insertSubtree(subtree); err != nil {
				return common.Hash{}, fmt.Errorf("insert upper bucket %x: %w", []byte(key), err)
			}
		}
		if err := t.refreshRouting(); err != nil {
			return common.Hash{}, err
		}
		t.rootDirty = true
	}
	if err := t.normalize(); err != nil {
		return common.Hash{}, fmt.Errorf("normalize upper tree: %w", err)
	}
	if err := t.write(); err != nil {
		return common.Hash{}, err
	}
	return t.rootHash()
}

func subtreeForDescriptor(key []byte, descriptor bucketDescriptor) (subtreeCell, error) {
	bucketPath, err := bucketPathForKey(key)
	if err != nil {
		return subtreeCell{}, err
	}
	switch descriptor.form {
	case LeafRoot:
		path, err := keyPath(descriptor.leaf.Key)
		if err != nil {
			return subtreeCell{}, err
		}
		return subtreeCell{path: path, cell: leafCell(descriptor.leaf.Key, descriptor.leaf.Value)}, nil
	case RowRoot:
		result, err := rowFoldResult(descriptor.row)
		if err != nil {
			return subtreeCell{}, err
		}
		path, err := rowTopPrefix(descriptor.row, result.Split)
		if err != nil {
			return subtreeCell{}, err
		}
		return subtreeCell{path: path, cell: branchCell(eip8297.Bitpath{}, result.Left, result.Right)}, nil
	case ExtRoot:
		path := bucketPath
		path.Append(&descriptor.self)
		return subtreeCell{path: path, cell: branchCell(eip8297.Bitpath{}, descriptor.left, descriptor.right)}, nil
	default:
		return subtreeCell{}, fmt.Errorf("unknown bucket form %d", descriptor.form)
	}
}

func subtreeCellForRow(subtree subtreeCell, rowPath eip8297.Bitpath) (rowCell, error) {
	cell := subtree.cell
	cell.child = subtree.cell.child
	if cell.Kind == BranchCell {
		start := rowPath.BitLen + 4
		if subtree.path.BitLen < start {
			return rowCell{}, errInsertKey
		}
		cell.Prefix = subtree.path.Slice(start, subtree.path.BitLen)
	}
	return cell, nil
}

func subtreeRow(path eip8297.Bitpath, old, added subtreeCell) (*rowNode, error) {
	oldSlot := slotAt(&old.path, path.BitLen)
	newSlot := slotAt(&added.path, path.BitLen)
	if oldSlot == newSlot {
		return nil, errInsertKey
	}
	row := newRow(path, nil, nil)
	oldCell, err := subtreeCellForRow(old, path)
	if err != nil {
		return nil, err
	}
	newCell, err := subtreeCellForRow(added, path)
	if err != nil {
		return nil, err
	}
	row.cells[oldSlot] = oldCell
	row.cells[newSlot] = newCell
	if oldCell.child != nil {
		oldCell.child.parent = row
		oldCell.child.parentSlot = oldSlot
	}
	if newCell.child != nil {
		newCell.child.parent = row
		newCell.child.parentSlot = newSlot
	}
	row.markDirty()
	return row, nil
}

func (t *Trie) insertSubtree(subtree subtreeCell) error {
	root, err := t.loadRoot()
	if err != nil {
		return err
	}
	if root.form == RowRoot && root.row == nil {
		if subtree.cell.Kind == LeafCell {
			root.form = LeafRoot
			root.leaf = subtree.cell.Cell
		} else {
			root.form = ExtRoot
			root.self = subtree.path
			root.left = subtree.cell.Left
			root.right = subtree.cell.Right
		}
		return nil
	}
	switch root.form {
	case LeafRoot:
		oldPath, err := keyPath(root.leaf.Key)
		if err != nil {
			return err
		}
		d := firstDifference(&oldPath, &subtree.path)
		if oldPath.BitLen == subtree.path.BitLen && d == oldPath.BitLen {
			if subtree.cell.Kind != LeafCell {
				return errInsertKey
			}
			root.leaf = subtree.cell.Cell
			return nil
		}
		old := subtreeCell{path: oldPath, cell: leafCell(root.leaf.Key, root.leaf.Value)}
		return t.splitRootSubtree(root, old, subtree, d)
	case ExtRoot:
		return t.insertSubtreeExtRoot(root, subtree)
	case RowRoot:
		return t.insertSubtreeRow(root.row, subtree)
	default:
		return fmt.Errorf("unknown root form %d", root.form)
	}
}

func (t *Trie) splitRootSubtree(root *treeRoot, old, added subtreeCell, split int16) error {
	window := (split / 4) * 4
	rowPath := old.path.Slice(0, window)
	row, err := subtreeRow(rowPath, old, added)
	if err != nil {
		return err
	}
	if window == t.rootRecordPath().BitLen {
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
	root.self, err = rowTopPrefix(row, result.Split)
	if err != nil {
		return err
	}
	root.left, root.right = result.Left, result.Right
	root.topRow = row
	t.registerRow(row)
	return nil
}

func (t *Trie) insertSubtreeExtRoot(root *treeRoot, subtree subtreeCell) error {
	d := firstDifference(&root.self, &subtree.path)
	split := root.self.BitLen
	if d >= split || d/4 == split/4 {
		row, err := t.extTopRow(root)
		if err != nil {
			return err
		}
		return t.insertSubtreeRow(row, subtree)
	}
	old := subtreeCell{path: root.self, cell: branchCell(eip8297.Bitpath{}, root.left, root.right)}
	old.cell.child = root.topRow
	window := (d / 4) * 4
	if window == t.rootRecordPath().BitLen {
		rowPath := t.rootRecordPath()
		row, err := subtreeRow(rowPath, old, subtree)
		if err != nil {
			return err
		}
		root.form = RowRoot
		root.row = row
		root.topRow = nil
		t.registerRow(row)
		return nil
	}
	rowPath := subtree.path.Slice(0, window)
	row, err := subtreeRow(rowPath, old, subtree)
	if err != nil {
		return err
	}
	root.self = subtree.path.Slice(0, d)
	root.topRow = row
	t.registerRow(row)
	return nil
}

func (t *Trie) insertSubtreeRow(row *rowNode, subtree subtreeCell) error {
	if row == nil || subtree.path.BitLen <= row.path.BitLen || eip8297.CommonPrefixBitsAt(&subtree.path, 0, &row.path) != row.path.BitLen {
		return errInsertKey
	}
	slot := slotAt(&subtree.path, row.path.BitLen)
	cell := row.cell(slot)
	switch cell.Kind {
	case EmptyCell:
		newCell, err := subtreeCellForRow(subtree, row.path)
		if err != nil {
			return err
		}
		setBranchOrLeaf(row, slot, newCell)
		t.markDirty(row)
		return nil
	case LeafCell:
		oldPath, err := keyPath(cell.Key)
		if err != nil {
			return err
		}
		d := firstDifference(&oldPath, &subtree.path)
		if oldPath.BitLen == subtree.path.BitLen && d == oldPath.BitLen {
			newCell, err := subtreeCellForRow(subtree, row.path)
			if err != nil {
				return err
			}
			setBranchOrLeaf(row, slot, newCell)
			t.markDirty(row)
			return nil
		}
		old := subtreeCell{path: oldPath, cell: *cell}
		if d/4 == row.path.BitLen/4 {
			newSlot := slotAt(&subtree.path, row.path.BitLen)
			newCell, err := subtreeCellForRow(subtree, row.path)
			if err != nil {
				return err
			}
			setBranchOrLeaf(row, newSlot, newCell)
			t.markDirty(row)
			return nil
		}
		childPath := subtree.path.Slice(0, (d/4)*4)
		child, err := subtreeRow(childPath, old, subtree)
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
		setBranch(row, slot, branchCell(full.Slice(row.path.BitLen+4, result.Split), result.Left, result.Right))
		row.cell(slot).child = child
		child.parent = row
		child.parentSlot = slot
		t.registerRow(child)
		t.markDirty(row)
		return nil
	case BranchCell:
		full := branchPath(row, slot, cell)
		d := firstDifference(&full, &subtree.path)
		split := branchSplit(row, slot, cell)
		if d >= split || d/4 == split/4 {
			if t.upperOnly && split >= t.rootRecordPath().BitLen+264 && isStoragePath(&full) {
				return fmt.Errorf("upper mutation reached bucket split %d", split)
			}
			child, err := t.loadBranchChild(row, slot)
			if err != nil {
				return err
			}
			if err := t.insertSubtreeRow(child, subtree); err != nil {
				return err
			}
			t.markDirty(row)
			return nil
		}
		old := subtreeCell{path: full, cell: *cell}
		childPath := subtree.path.Slice(0, (d/4)*4)
		child, err := subtreeRow(childPath, old, subtree)
		if err != nil {
			return err
		}
		result, err := rowFoldResult(child)
		if err != nil {
			return err
		}
		newFull, err := rowTopPrefix(child, result.Split)
		if err != nil {
			return err
		}
		setBranch(row, slot, branchCell(newFull.Slice(row.path.BitLen+4, result.Split), result.Left, result.Right))
		row.cell(slot).child = child
		child.parent = row
		child.parentSlot = slot
		t.registerRow(child)
		t.markDirty(row)
		return nil
	default:
		return errInsertKey
	}
}

func setBranchOrLeaf(row *rowNode, slot int, cell rowCell) {
	row.cells[slot] = cell
}

func (t *Trie) removeSubtree(prefix *eip8297.Bitpath) (bool, error) {
	root, err := t.loadRoot()
	if err != nil {
		return false, err
	}
	switch root.form {
	case RowRoot:
		if root.row == nil {
			return false, nil
		}
		return t.removeSubtreeRow(root.row, prefix)
	case LeafRoot:
		path, err := keyPath(root.leaf.Key)
		if err != nil {
			return false, err
		}
		if !pathHasPrefix(&path, prefix) {
			return false, nil
		}
		t.emptyRoot()
		return true, nil
	case ExtRoot:
		if root.self.BitLen >= prefix.BitLen && pathHasPrefix(&root.self, prefix) {
			t.emptyRoot()
			return true, nil
		}
		if !pathHasPrefix(prefix, &root.self) {
			return false, nil
		}
		row, err := t.extTopRow(root)
		if err != nil {
			return false, err
		}
		return t.removeSubtreeRow(row, prefix)
	default:
		return false, errInsertKey
	}
}

func (t *Trie) removeSubtreeRow(row *rowNode, prefix *eip8297.Bitpath) (bool, error) {
	if row == nil || row.path.BitLen >= prefix.BitLen || eip8297.CommonPrefixBitsAt(&row.path, 0, prefix) != row.path.BitLen {
		return false, nil
	}
	slot := slotAt(prefix, row.path.BitLen)
	cell := row.cell(slot)
	switch cell.Kind {
	case EmptyCell:
		return false, nil
	case LeafCell:
		path, err := keyPath(cell.Key)
		if err != nil {
			return false, err
		}
		if !pathHasPrefix(&path, prefix) {
			return false, nil
		}
		row.cells[slot] = rowCell{}
		t.markDirty(row)
		return true, nil
	case BranchCell:
		full := branchPath(row, slot, cell)
		if full.BitLen >= prefix.BitLen && pathHasPrefix(&full, prefix) {
			row.cells[slot] = rowCell{}
			t.markDirty(row)
			return true, nil
		}
		if full.BitLen >= prefix.BitLen || !pathHasPrefix(prefix, &full) {
			return false, nil
		}
		child, err := t.loadBranchChild(row, slot)
		if err != nil {
			return false, err
		}
		return t.removeSubtreeRow(child, prefix)
	default:
		return false, errInsertKey
	}
}

func (t *Trie) emptyRoot() {
	t.root.form = RowRoot
	t.root.row = nil
	t.root.topRow = nil
	t.root.self = eip8297.Bitpath{}
	t.root.left = common.Hash{}
	t.root.right = common.Hash{}
	t.root.leaf = Cell{}
}
