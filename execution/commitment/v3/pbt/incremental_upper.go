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
	"maps"
	"slices"

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
	t.foldedRoot = common.Hash{}
	t.foldedRootReady = false
	if t.roundPrev == nil {
		t.roundPrev = make(map[string][]byte)
		t.bucketDirty = make(map[string][]byte)
		if _, err := t.loadRoot(); err != nil {
			return common.Hash{}, err
		}
		t.rememberPrev(t.rootRecordKey(), t.root.prev)
	}
	t.upperOnly = true
	previousStops := t.upperStops
	t.upperStops = make([]eip8297.Bitpath, 0, len(changed))
	for key := range changed {
		t.upperStops = append(t.upperStops, changed[key].prefix)
	}
	defer func() {
		t.upperOnly = false
		t.upperStops = previousStops
	}()
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
	keys := slices.Sorted(maps.Keys(changed))
	for _, key := range keys {
		result := changed[key]
		prefix := result.prefix
		if prefix.BitLen == 0 {
			var err error
			prefix, err = bucketPathForKey([]byte(key))
			if err != nil {
				return common.Hash{}, err
			}
		}
		if err := t.replaceSubtreeAt(prefix, result); err != nil {
			return common.Hash{}, fmt.Errorf("replace upper subtree %x: %w", []byte(key), err)
		}
		if err := t.refreshRouting(); err != nil {
			return common.Hash{}, err
		}
		t.rootDirty = true
	}
	if err := t.normalize(); err != nil {
		return common.Hash{}, fmt.Errorf("normalize upper tree: %w", err)
	}
	if err := t.foldDirtyRows(); err != nil {
		return common.Hash{}, err
	}
	if err := t.write(); err != nil {
		return common.Hash{}, err
	}
	return t.rootHash()
}

func (t *Trie) stopsUpperPath(path *eip8297.Bitpath) bool {
	if !t.upperOnly {
		return false
	}
	for i := range t.upperStops {
		if pathHasPrefix(path, &t.upperStops[i]) {
			return true
		}
	}
	return false
}

func (t *Trie) replaceSubtreeAt(prefix eip8297.Bitpath, result phaseBucketResult) error {
	root, err := t.loadRoot()
	if err != nil {
		return err
	}
	if prefix.BitLen == t.rootRecordPath().BitLen && prefix == t.rootRecordPath() {
		return t.replaceRootDescriptor(prefix, result)
	}
	var replaced bool
	switch root.form {
	case RowRoot:
		if root.row != nil {
			replaced, err = t.replaceSubtreeRow(root.row, &prefix, result)
		}
	case LeafRoot:
		path, pathErr := keyPath(root.leaf.Key)
		if pathErr != nil {
			return pathErr
		}
		replaced = pathHasPrefix(&path, &prefix)
		if replaced {
			err = t.replaceRootDescriptor(prefix, result)
		}
	case ExtRoot:
		if root.self.BitLen >= prefix.BitLen && pathHasPrefix(&root.self, &prefix) {
			replaced = true
			err = t.replaceRootDescriptor(prefix, result)
		} else if pathHasPrefix(&prefix, &root.self) {
			var row *rowNode
			row, err = t.extTopRow(root)
			if err == nil {
				replaced, err = t.replaceSubtreeRow(row, &prefix, result)
			}
		}
	default:
		return fmt.Errorf("unknown root form %d", root.form)
	}
	if err != nil {
		return err
	}
	if !replaced && result.present {
		subtree, err := subtreeForDescriptorAt(prefix, result.descriptor)
		if err != nil {
			return err
		}
		if err := t.insertSubtree(subtree); err != nil {
			return err
		}
	}
	return nil
}

func (t *Trie) replaceRootDescriptor(prefix eip8297.Bitpath, result phaseBucketResult) error {
	if !result.present {
		t.emptyRoot()
		t.rootDirty = true
		return nil
	}
	switch result.descriptor.form {
	case LeafRoot:
		t.root.form = LeafRoot
		t.root.leaf = result.descriptor.leaf
	case ExtRoot:
		t.root.form = ExtRoot
		t.root.self = prefix
		t.root.self.Append(&result.descriptor.self)
		t.root.left = result.descriptor.left
		t.root.right = result.descriptor.right
		t.root.topRow = nil
	case RowRoot:
		if result.descriptor.row == nil {
			return fmt.Errorf("subtree descriptor has no row")
		}
		subtree, err := subtreeForDescriptorAt(prefix, result.descriptor)
		if err != nil {
			return err
		}
		t.root.form = ExtRoot
		t.root.self = subtree.path
		t.root.left = subtree.cell.Left
		t.root.right = subtree.cell.Right
		t.root.topRow = nil
	default:
		return fmt.Errorf("unknown subtree descriptor form %d", result.descriptor.form)
	}
	t.rootDirty = true
	return nil
}

func (t *Trie) replaceSubtreeRow(row *rowNode, prefix *eip8297.Bitpath, result phaseBucketResult) (bool, error) {
	if row == nil || !pathHasPrefix(prefix, &row.path) {
		return false, nil
	}
	if row.path.BitLen == prefix.BitLen {
		if row.parent == nil {
			return t.replaceRootDescriptor(*prefix, result) == nil, nil
		}
		return t.replaceRowCell(row.parent, row.parentSlot, prefix, result)
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
		return t.replaceRowCell(row, slot, prefix, result)
	case BranchCell:
		full := branchPath(row, slot, cell)
		if pathHasPrefix(&full, prefix) {
			return t.replaceRowCell(row, slot, prefix, result)
		}
		if !pathHasPrefix(prefix, &full) {
			return false, nil
		}
		childPath, err := rowChildPath(row, slot, cell.Prefix, branchSplit(row, cell))
		if err != nil {
			return false, err
		}
		if childPath.BitLen >= prefix.BitLen {
			return t.replaceRowCell(row, slot, prefix, result)
		}
		child, err := t.loadBranchChild(row, slot)
		if err != nil {
			return false, err
		}
		replaced, err := t.replaceSubtreeRow(child, prefix, result)
		if replaced {
			row.markCellDirty(slot)
			t.markDirty(row)
		}
		return replaced, err
	default:
		return false, errInsertKey
	}
}

func (t *Trie) replaceRowCell(row *rowNode, slot int, prefix *eip8297.Bitpath, result phaseBucketResult) (bool, error) {
	if !result.present {
		row.cells[slot] = rowCell{}
		row.markCellDirty(slot)
		t.markDirty(row)
		return true, nil
	}
	subtree, err := subtreeForDescriptorAt(*prefix, result.descriptor)
	if err != nil {
		return false, err
	}
	cell, err := subtreeCellForRow(subtree, row.path)
	if err != nil {
		return false, err
	}
	row.cells[slot] = cell
	row.markCellDirty(slot)
	t.markDirty(row)
	return true, nil
}

func subtreeForDescriptorAt(prefix eip8297.Bitpath, descriptor bucketDescriptor) (subtreeCell, error) {
	switch descriptor.form {
	case LeafRoot:
		path, err := keyPath(descriptor.leaf.Key)
		if err != nil {
			return subtreeCell{}, err
		}
		return subtreeCell{path: path, cell: leafCell(descriptor.leaf.Key, descriptor.leaf.Value)}, nil
	case RowRoot:
		result := descriptor.row.foldResult
		if !descriptor.row.folded {
			var err error
			result, err = rowRoutingResult(descriptor.row)
			if err != nil {
				return subtreeCell{}, err
			}
		}
		path, err := rowTopPrefix(descriptor.row, result.Split)
		if err != nil {
			return subtreeCell{}, err
		}
		return subtreeCell{path: path, cell: branchCell(eip8297.Bitpath{}, descriptor.left, descriptor.right)}, nil
	case ExtRoot:
		path := prefix
		path.Append(&descriptor.self)
		return subtreeCell{path: path, cell: branchCell(eip8297.Bitpath{}, descriptor.left, descriptor.right)}, nil
	default:
		return subtreeCell{}, fmt.Errorf("unknown bucket form %d", descriptor.form)
	}
}

func (t *Trie) descriptorAtPrefix(prefix *eip8297.Bitpath) (bucketDescriptor, bool, error) {
	return t.descriptorAtPrefixMode(prefix, false)
}

func (t *Trie) descriptorAtPrefixMode(prefix *eip8297.Bitpath, bucket bool) (bucketDescriptor, bool, error) {
	root, err := t.loadRoot()
	if err != nil {
		return bucketDescriptor{}, false, err
	}
	switch root.form {
	case LeafRoot:
		path, err := keyPath(root.leaf.Key)
		if err != nil {
			return bucketDescriptor{}, false, err
		}
		if (!bucket || root.leaf.Key[0] == eip8297.StorageZone) && pathHasPrefix(&path, prefix) {
			return bucketDescriptor{form: LeafRoot, leaf: root.leaf}, true, nil
		}
		return bucketDescriptor{}, false, nil
	case ExtRoot:
		if pathHasPrefix(&root.self, prefix) {
			if bucket && root.self.BitLen < prefix.BitLen+4 {
				row, err := t.extTopRow(root)
				if err != nil {
					return bucketDescriptor{}, false, err
				}
				return bucketDescriptor{form: RowRoot, row: row}, true, nil
			}
			return bucketDescriptor{form: ExtRoot, self: root.self.Slice(prefix.BitLen, root.self.BitLen), left: root.left, right: root.right}, true, nil
		}
		if !pathHasPrefix(prefix, &root.self) {
			return bucketDescriptor{}, false, nil
		}
		row, err := t.extTopRow(root)
		if err != nil {
			return bucketDescriptor{}, false, err
		}
		return t.descriptorAtRowMode(row, prefix, bucket)
	case RowRoot:
		if root.row == nil {
			return bucketDescriptor{}, false, nil
		}
		return t.descriptorAtRowMode(root.row, prefix, bucket)
	default:
		return bucketDescriptor{}, false, fmt.Errorf("unknown root form %d", root.form)
	}
}

func (t *Trie) descriptorAtRowMode(row *rowNode, prefix *eip8297.Bitpath, bucket bool) (bucketDescriptor, bool, error) {
	if row.path.BitLen == prefix.BitLen && row.path == *prefix {
		return bucketDescriptor{form: RowRoot, row: row}, true, nil
	}
	if !pathHasPrefix(prefix, &row.path) {
		return bucketDescriptor{}, false, nil
	}
	if bucket && row.path.BitLen+4 > prefix.BitLen {
		return bucketDescriptor{}, false, nil
	}
	slot := slotAt(prefix, row.path.BitLen)
	cell := row.cell(slot)
	switch cell.Kind {
	case EmptyCell:
		return bucketDescriptor{}, false, nil
	case LeafCell:
		path, err := keyPath(cell.Key)
		if err != nil {
			return bucketDescriptor{}, false, err
		}
		if (!bucket || cell.Key[0] == eip8297.StorageZone) && pathHasPrefix(&path, prefix) {
			return bucketDescriptor{form: LeafRoot, leaf: *cell.Cell}, true, nil
		}
		return bucketDescriptor{}, false, nil
	case BranchCell:
		full := branchPath(row, slot, cell)
		if pathHasPrefix(&full, prefix) {
			if bucket && full.BitLen < prefix.BitLen+4 {
				child, err := t.loadBranchChild(row, slot)
				if err != nil {
					return bucketDescriptor{}, false, err
				}
				return bucketDescriptor{form: RowRoot, row: child}, true, nil
			}
			return bucketDescriptor{form: ExtRoot, self: full.Slice(prefix.BitLen, full.BitLen), left: cell.Left, right: cell.Right}, true, nil
		}
		if !pathHasPrefix(prefix, &full) {
			return bucketDescriptor{}, false, nil
		}
		child, err := t.loadBranchChild(row, slot)
		if err != nil {
			return bucketDescriptor{}, false, err
		}
		return t.descriptorAtRowMode(child, prefix, bucket)
	default:
		return bucketDescriptor{}, false, fmt.Errorf("unknown cell form %d", cell.Kind)
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

func (t *Trie) subtreeRow(path eip8297.Bitpath, old, added subtreeCell) (*rowNode, error) {
	oldSlot := slotAt(&old.path, path.BitLen)
	newSlot := slotAt(&added.path, path.BitLen)
	if oldSlot == newSlot {
		return nil, errInsertKey
	}
	row := t.newRow(path, nil, nil)
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
			root.leaf = *subtree.cell.Cell
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
			root.leaf = *subtree.cell.Cell
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
	row, err := t.subtreeRow(rowPath, old, added)
	if err != nil {
		return err
	}
	if window == t.rootRecordPath().BitLen {
		root.form = RowRoot
		root.row = row
		t.registerRow(row)
		return nil
	}
	result, err := rowRoutingResult(row)
	if err != nil {
		return err
	}
	root.form = ExtRoot
	root.self, err = rowTopPrefix(row, result.Split)
	if err != nil {
		return err
	}
	root.left, root.right = common.Hash{}, common.Hash{}
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
		row, err := t.subtreeRow(rowPath, old, subtree)
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
	row, err := t.subtreeRow(rowPath, old, subtree)
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
		setBranch(row, slot, newCell)
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
			setBranch(row, slot, newCell)
			t.markDirty(row)
			return nil
		}
		old := subtreeCell{path: oldPath, cell: *cell}
		if d/4 == row.path.BitLen/4 {
			newCell, err := subtreeCellForRow(subtree, row.path)
			if err != nil {
				return err
			}
			setBranch(row, slot, newCell)
			t.markDirty(row)
			return nil
		}
		childPath := subtree.path.Slice(0, (d/4)*4)
		child, err := t.subtreeRow(childPath, old, subtree)
		if err != nil {
			return err
		}
		result, err := rowRoutingResult(child)
		if err != nil {
			return err
		}
		full, err := rowTopPrefix(child, result.Split)
		if err != nil {
			return err
		}
		t.attachChildRow(row, slot, child, branchCell(full.Slice(row.path.BitLen+4, result.Split), common.Hash{}, common.Hash{}), nil, 0)
		return nil
	case BranchCell:
		full := branchPath(row, slot, cell)
		d := firstDifference(&full, &subtree.path)
		split := branchSplit(row, cell)
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
		child, err := t.subtreeRow(childPath, old, subtree)
		if err != nil {
			return err
		}
		result, err := rowRoutingResult(child)
		if err != nil {
			return err
		}
		newFull, err := rowTopPrefix(child, result.Split)
		if err != nil {
			return err
		}
		t.attachChildRow(row, slot, child, branchCell(newFull.Slice(row.path.BitLen+4, result.Split), common.Hash{}, common.Hash{}), nil, 0)
		return nil
	default:
		return errInsertKey
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
