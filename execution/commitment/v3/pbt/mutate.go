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
	"encoding/binary"
	"errors"
	"fmt"
	"sort"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
)

var (
	errInsertKey    = fmt.Errorf("invalid insert key")
	errInsertValue  = fmt.Errorf("zero insert value")
	errFeedCodeSize = errors.New("pbin: code size unavailable for non-empty code hash")
)

type mergeKind uint8

const (
	mergeBasicData mergeKind = iota + 1
	mergeCodeHash
	mergeBasicDataPresent
	mergeBasicDataRetain
	mergeBasicDataPreserveEmpty
)

type feedMerge struct {
	kind     mergeKind
	nonce    uint64
	balance  uint256.Int
	codeHash common.Hash
}

func mergeKeepsZero(merge *feedMerge) bool {
	return merge != nil && merge.kind == mergeBasicDataPresent
}

func mergeRetainsExisting(merge *feedMerge) bool {
	return merge != nil && merge.kind == mergeBasicDataRetain
}

func mergeKeepsExistingEmpty(merge *feedMerge, value [eip8297.ValueLength]byte) bool {
	return merge != nil && merge.kind == mergeBasicDataPreserveEmpty && value == ([eip8297.ValueLength]byte{})
}

func (t *Trie) applyMerge(op Op) error {
	merge := op.merge
	switch merge.kind {
	case mergeBasicData, mergeBasicDataPresent, mergeBasicDataRetain, mergeBasicDataPreserveEmpty:
		existing, err := t.insertWithMerge(op.Key, [eip8297.ValueLength]byte{}, merge)
		if err == nil {
			if !existing {
				t.mergeCreatedStems[string(op.Key)] = struct{}{}
			} else {
				delete(t.mergeCreatedStems, string(op.Key))
			}
		}
		return err
	case mergeCodeHash:
		basicKey := bytes.Clone(op.Key)
		basicKey[len(basicKey)-1] = eip8297.BasicDataLeafKey
		if _, created := t.mergeCreatedStems[string(basicKey)]; created {
			return t.insert(op.Key, eip8297.CodeHashValue(merge.codeHash))
		}
		original, err := t.originalLeaf(basicKey)
		if err != nil {
			return err
		}
		if original != nil && !t.droppedLeaf(basicKey) {
			return nil
		}
		return t.insert(op.Key, eip8297.CodeHashValue(merge.codeHash))
	default:
		return fmt.Errorf("unknown feed merge %d", merge.kind)
	}
}

func (t *Trie) mergeValue(merge *feedMerge, existing *Cell, key []byte) ([eip8297.ValueLength]byte, error) {
	if merge == nil {
		return [eip8297.ValueLength]byte{}, nil
	}
	codeSize := uint64(0)
	if merge.kind == mergeBasicDataPreserveEmpty {
		return eip8297.EncodeBasicData(merge.nonce, &merge.balance, codeSize)
	}
	if existing != nil && !t.droppedLeaf(existing.Key) {
		codeSize = uint64(binary.BigEndian.Uint32(existing.Value[eip8297.BasicDataCodeSizeOffset:]))
	} else if !eip8297.IsEmptyCodeHash(merge.codeHash) {
		return [eip8297.ValueLength]byte{}, fmt.Errorf("%w for %x", errFeedCodeSize, key)
	}
	return eip8297.EncodeBasicData(merge.nonce, &merge.balance, codeSize)
}

func (t *Trie) droppedLeaf(key []byte) bool {
	_, ok := t.droppedLeafKeys[string(key)]
	return ok
}

func (t *Trie) originalLeaf(key []byte) (*Cell, error) {
	name := string(key)
	if _, ok := t.originalLeafSeen[name]; !ok {
		lookup := t.lookupLeafRaw
		if t.ownedPrefix != nil {
			lookup = t.lookupLeaf
		}
		cell, found, err := lookup(key)
		if err != nil {
			return nil, err
		}
		t.originalLeafSeen[name] = struct{}{}
		if found {
			copyCell := cell
			t.originalLeaves[name] = &copyCell
		}
	}
	return t.originalLeaves[name], nil
}

func (t *Trie) rememberOriginalLeaf(key []byte) error {
	_, err := t.originalLeaf(key)
	return err
}

func (t *Trie) lookupLeaf(key []byte) (Cell, bool, error) {
	path, err := keyPath(key)
	if err != nil {
		return Cell{}, false, err
	}
	root, err := t.loadRoot()
	if err != nil {
		return Cell{}, false, err
	}
	switch root.form {
	case RowRoot:
		if root.row == nil {
			return Cell{}, false, nil
		}
		return t.lookupRowLeaf(root.row, &path, key)
	case LeafRoot:
		return root.leaf, bytes.Equal(root.leaf.Key, key), nil
	case ExtRoot:
		if !pathHasPrefix(&path, &root.self) {
			return Cell{}, false, nil
		}
		row, err := t.extTopRow(root)
		if err != nil {
			return Cell{}, false, err
		}
		return t.lookupRowLeaf(row, &path, key)
	default:
		return Cell{}, false, fmt.Errorf("unknown root form %d", root.form)
	}
}

func (t *Trie) lookupLeafRaw(key []byte) (Cell, bool, error) {
	path, err := keyPath(key)
	if err != nil {
		return Cell{}, false, err
	}
	rootKey := t.rootRecordKey()
	raw, _, err := t.ctx.Branch(rootKey)
	if err != nil {
		return Cell{}, false, err
	}
	if len(raw) == 0 {
		return Cell{}, false, nil
	}
	record, err := DecodeRecord(rootKey, raw)
	if err != nil {
		return Cell{}, false, err
	}
	switch record.Form {
	case LeafRoot:
		return record.Cells[0], bytes.Equal(record.Cells[0].Key, key), nil
	case ExtRoot:
		prefix := t.rootRecordPath()
		prefix.Append(&record.SelfExt)
		if !pathHasPrefix(&path, &prefix) {
			return Cell{}, false, nil
		}
		rowPath := prefix.Slice(0, (prefix.BitLen/4)*4)
		rowKey, err := EncodeRowKey(&rowPath)
		if err != nil {
			return Cell{}, false, err
		}
		return t.lookupRawRow(rowKey, &rowPath, &path, key)
	case RowRoot:
		rowPath := t.rootRecordPath()
		return t.lookupRawRow(rootKey, &rowPath, &path, key)
	default:
		return Cell{}, false, fmt.Errorf("unknown root form %d", record.Form)
	}
}

func (t *Trie) lookupRawRow(rowKey []byte, rowPath, path *eip8297.Bitpath, key []byte) (Cell, bool, error) {
	raw, _, err := t.ctx.Branch(rowKey)
	if err != nil {
		return Cell{}, false, err
	}
	if len(raw) == 0 {
		return Cell{}, false, nil
	}
	record, err := DecodeRecord(rowKey, raw)
	if err != nil {
		return Cell{}, false, err
	}
	if !pathHasPrefix(path, rowPath) || path.BitLen < rowPath.BitLen+4 {
		return Cell{}, false, nil
	}
	slot := slotAt(path, rowPath.BitLen)
	cell := record.Cells[slot]
	switch cell.Kind {
	case EmptyCell:
		return Cell{}, false, nil
	case LeafCell:
		return cell, bytes.Equal(cell.Key, key), nil
	case BranchCell:
		full := rawBranchPath(rowPath, slot, &cell)
		if !pathHasPrefix(path, &full) {
			return Cell{}, false, nil
		}
		childPath := rawChildPath(rowPath, slot, &cell.Prefix)
		childKey, err := EncodeRowKey(&childPath)
		if err != nil {
			return Cell{}, false, err
		}
		return t.lookupRawRow(childKey, &childPath, path, key)
	default:
		return Cell{}, false, errInsertKey
	}
}

func rawBranchPath(rowPath *eip8297.Bitpath, slot int, cell *Cell) eip8297.Bitpath {
	path := *rowPath
	var slotPath eip8297.Bitpath
	for i := range 4 {
		slotPath.AppendBit(uint64((slot >> (3 - i)) & 1))
	}
	path.Append(&slotPath)
	path.Append(&cell.Prefix)
	return path
}

func rawChildPath(rowPath *eip8297.Bitpath, slot int, prefix *eip8297.Bitpath) eip8297.Bitpath {
	path := rawBranchPath(rowPath, slot, &Cell{Prefix: *prefix})
	window := (path.BitLen / 4) * 4
	return path.Slice(0, window)
}

func (t *Trie) lookupRowLeaf(row *rowNode, path *eip8297.Bitpath, key []byte) (Cell, bool, error) {
	if !pathHasPrefix(path, &row.path) || path.BitLen < row.path.BitLen+4 {
		return Cell{}, false, nil
	}
	cell := row.cell(slotAt(path, row.path.BitLen))
	switch cell.Kind {
	case EmptyCell:
		return Cell{}, false, nil
	case LeafCell:
		return *cell.Cell, bytes.Equal(cell.Key, key), nil
	case BranchCell:
		full := branchPath(row, slotAt(path, row.path.BitLen), cell)
		if !pathHasPrefix(path, &full) {
			return Cell{}, false, nil
		}
		child, err := t.loadBranchChild(row, slotAt(path, row.path.BitLen))
		if err != nil {
			return Cell{}, false, err
		}
		return t.lookupRowLeaf(child, path, key)
	default:
		return Cell{}, false, errInsertKey
	}
}

func (t *Trie) insert(key []byte, value [eip8297.ValueLength]byte) error {
	_, err := t.insertWithMerge(key, value, nil)
	return err
}

func (t *Trie) insertWithMerge(key []byte, value [eip8297.ValueLength]byte, mergeOp *feedMerge) (bool, error) {
	path, err := keyPath(key)
	if err != nil {
		return false, err
	}
	if mergeOp == nil && value == ([eip8297.ValueLength]byte{}) {
		return false, errInsertValue
	}
	root, err := t.loadRoot()
	if err != nil {
		return false, err
	}
	if root.form == RowRoot && root.row == nil {
		if mergeOp != nil {
			value, err = t.mergeValue(mergeOp, nil, key)
			if err != nil {
				return false, err
			}
		}
		if value == ([eip8297.ValueLength]byte{}) {
			if mergeOp != nil && !mergeKeepsZero(mergeOp) {
				if mergeRetainsExisting(mergeOp) {
					return true, nil
				}
				root.form = RowRoot
				root.leaf = Cell{}
				return true, nil
			}
			if mergeOp == nil {
				return false, errInsertValue
			}
		}
		root.form = LeafRoot
		root.leaf = *t.leafCell(key, value).Cell
		root.raw = nil
		return false, nil
	}
	switch root.form {
	case RowRoot:
		return t.insertRow(root.row, path, key, value, mergeOp)
	case LeafRoot:
		oldPath, err := keyPath(root.leaf.Key)
		if err != nil {
			return false, err
		}
		d := firstDifference(&oldPath, &path)
		if oldPath.BitLen == path.BitLen && d == oldPath.BitLen {
			if mergeOp != nil {
				value, err = t.mergeValue(mergeOp, &root.leaf, key)
				if err != nil {
					return false, err
				}
			}
			if value == ([eip8297.ValueLength]byte{}) {
				if mergeOp != nil && !mergeKeepsZero(mergeOp) {
					if mergeRetainsExisting(mergeOp) || mergeKeepsExistingEmpty(mergeOp, root.leaf.Value) {
						return true, nil
					}
					root.form = RowRoot
					root.leaf = Cell{}
					return true, nil
				}
				if mergeOp == nil {
					return false, errInsertValue
				}
			}
			root.leaf = *t.leafCell(key, value).Cell
			return true, nil
		}
		if mergeOp != nil {
			value, err = t.mergeValue(mergeOp, nil, key)
			if err != nil {
				return false, err
			}
		}
		if value == ([eip8297.ValueLength]byte{}) {
			if mergeOp != nil && !mergeKeepsZero(mergeOp) {
				return false, nil
			}
			if mergeOp == nil {
				return false, errInsertValue
			}
		}
		return false, t.splitRootLeaf(root, oldPath, root.leaf, path, key, value, d)
	case ExtRoot:
		return t.insertExtRoot(root, path, key, value, mergeOp)
	default:
		return false, fmt.Errorf("unknown root form %d", root.form)
	}
}

func (t *Trie) splitRootLeaf(root *treeRoot, oldPath eip8297.Bitpath, old Cell, newPath eip8297.Bitpath, newKey []byte, newValue [eip8297.ValueLength]byte, split int16) error {
	window := (split / 4) * 4
	path := oldPath.Slice(0, window)
	row, err := t.twoLeafRow(path, old.Key, old.Value, newKey, newValue)
	if err != nil {
		return err
	}
	if window == t.rootRecordPath().BitLen {
		row.prev = bytes.Clone(root.raw)
		root.form = RowRoot
		root.row = row
		t.registerRow(row)
		return nil
	}
	root.form = ExtRoot
	root.self = oldPath.Slice(0, split)
	root.left, root.right = common.Hash{}, common.Hash{}
	root.topRow = row
	t.registerRow(row)
	return nil
}

func (t *Trie) insertExtRoot(root *treeRoot, path eip8297.Bitpath, key []byte, value [eip8297.ValueLength]byte, merge *feedMerge) (bool, error) {
	d := firstDifference(&root.self, &path)
	split := root.self.BitLen
	if d >= split || d/4 == split/4 {
		row, err := t.extTopRow(root)
		if err != nil {
			return false, err
		}
		existing, err := t.insertRow(row, path, key, value, merge)
		if err != nil {
			return false, err
		}
		t.markDirty(row)
		return existing, nil
	}
	if merge != nil {
		var err error
		value, err = t.mergeValue(merge, nil, key)
		if err != nil {
			return false, err
		}
	}
	if value == ([eip8297.ValueLength]byte{}) {
		if merge != nil && !mergeKeepsZero(merge) {
			return false, nil
		}
		if merge == nil {
			return false, errInsertValue
		}
	}
	window := (d / 4) * 4
	oldPath := root.self
	if window == t.rootRecordPath().BitLen {
		rootPath := t.rootRecordPath()
		row := t.newRow(rootPath, t.rootRecordKey(), root.raw)
		oldSlot := slotAt(&oldPath, rootPath.BitLen)
		newSlot := slotAt(&path, rootPath.BitLen)
		oldPrefix := oldPath.Slice(rootPath.BitLen+4, oldPath.BitLen)
		row.cells[oldSlot] = t.branchCell(oldPrefix, root.left, root.right)
		if root.topRow != nil {
			row.cells[oldSlot].child = root.topRow
			root.topRow.parent = row
			root.topRow.parentSlot = oldSlot
		}
		row.cells[newSlot] = t.leafCell(key, value)
		row.markDirty()
		root.form = RowRoot
		root.row = row
		t.registerRow(row)
		return false, nil
	}
	rowPath := path.Slice(0, window)
	row, err := t.newRowFromBranch(rowPath, key, value, oldPath, root.left, root.right)
	if err != nil {
		return false, err
	}
	oldChild := root.topRow
	oldSlot := slotAt(&oldPath, rowPath.BitLen)
	root.self = path.Slice(0, d)
	root.topRow = row
	if oldChild != nil {
		row.cells[oldSlot].child = oldChild
		oldChild.parent = row
		oldChild.parentSlot = oldSlot
	}
	t.registerRow(row)
	return false, nil
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
	result, err := rowRoutingResult(row)
	if err != nil {
		return err
	}
	self, err := rowTopPrefix(row, result.Split)
	if err != nil {
		return err
	}
	root.self = self
	root.topRow = row
	return nil
}

func (t *Trie) insertRow(row *rowNode, path eip8297.Bitpath, key []byte, value [eip8297.ValueLength]byte, merge *feedMerge) (bool, error) {
	if path.BitLen <= row.path.BitLen || eip8297.CommonPrefixBitsAt(&path, 0, &row.path) != row.path.BitLen {
		return false, errInsertKey
	}
	slot := int(path.Bit(row.path.BitLen)*8 + path.Bit(row.path.BitLen+1)*4 + path.Bit(row.path.BitLen+2)*2 + path.Bit(row.path.BitLen+3))
	cell := row.cell(slot)
	switch cell.Kind {
	case EmptyCell:
		if merge != nil {
			var err error
			value, err = t.mergeValue(merge, nil, key)
			if err != nil {
				return false, err
			}
		}
		if value == ([eip8297.ValueLength]byte{}) {
			if merge != nil && !mergeKeepsZero(merge) {
				return false, nil
			}
			if merge == nil {
				return false, errInsertValue
			}
		}
		t.setLeaf(row, slot, key, value)
		t.markDirty(row)
		return false, nil
	case LeafCell:
		oldPath, err := keyPath(cell.Key)
		if err != nil {
			return false, err
		}
		d := firstDifference(&oldPath, &path)
		if oldPath.BitLen == path.BitLen && d == oldPath.BitLen {
			if merge != nil {
				value, err = t.mergeValue(merge, cell.Cell, key)
				if err != nil {
					return false, err
				}
			}
			if value == ([eip8297.ValueLength]byte{}) {
				if merge != nil && !mergeKeepsZero(merge) {
					if mergeRetainsExisting(merge) || mergeKeepsExistingEmpty(merge, cell.Cell.Value) {
						return true, nil
					}
					row.cells[slot] = rowCell{}
					row.markCellDirty(slot)
					t.markDirty(row)
					return true, nil
				}
				if merge == nil {
					return false, errInsertValue
				}
			}
			t.setLeaf(row, slot, key, value)
			t.markDirty(row)
			return true, nil
		}
		if d/4 == row.path.BitLen/4 {
			if merge != nil {
				var err error
				value, err = t.mergeValue(merge, nil, key)
				if err != nil {
					return false, err
				}
			}
			if value == ([eip8297.ValueLength]byte{}) {
				if merge != nil && !mergeKeepsZero(merge) {
					return false, nil
				}
				if merge == nil {
					return false, errInsertValue
				}
			}
			newSlot := int(path.Bit(row.path.BitLen)*8 + path.Bit(row.path.BitLen+1)*4 + path.Bit(row.path.BitLen+2)*2 + path.Bit(row.path.BitLen+3))
			t.setLeaf(row, newSlot, key, value)
			t.markDirty(row)
			return false, nil
		}
		if merge != nil {
			value, err = t.mergeValue(merge, nil, key)
			if err != nil {
				return false, err
			}
		}
		if value == ([eip8297.ValueLength]byte{}) {
			if merge != nil && !mergeKeepsZero(merge) {
				return false, nil
			}
			if merge == nil {
				return false, errInsertValue
			}
		}
		window := (d / 4) * 4
		childPath := path.Slice(0, window)
		child, err := t.twoLeafRow(childPath, cell.Key, cell.Value, key, value)
		if err != nil {
			return false, err
		}
		result, err := rowRoutingResult(child)
		if err != nil {
			return false, err
		}
		prefix := path.Slice(row.path.BitLen+4, result.Split)
		setBranch(row, slot, t.branchCell(prefix, common.Hash{}, common.Hash{}))
		row.cell(slot).child = child
		child.parent = row
		child.parentSlot = slot
		t.registerRow(child)
		t.markDirty(row)
		return false, nil
	case BranchCell:
		return t.insertBranch(row, slot, path, key, value, merge)
	default:
		return false, errInsertKey
	}
}

func (t *Trie) insertBranch(row *rowNode, slot int, path eip8297.Bitpath, key []byte, value [eip8297.ValueLength]byte, merge *feedMerge) (bool, error) {
	cell := row.cell(slot)
	oldChild := cell.child
	branchPath := branchPath(row, slot, cell)
	d := firstDifference(&branchPath, &path)
	split := branchSplit(row, slot, cell)
	if d >= split || d/4 == split/4 {
		child, err := t.loadBranchChild(row, slot)
		if err != nil {
			return false, err
		}
		existing, err := t.insertRow(child, path, key, value, merge)
		if err != nil {
			return false, err
		}
		t.markDirty(row)
		return existing, nil
	}
	if merge != nil {
		var err error
		value, err = t.mergeValue(merge, nil, key)
		if err != nil {
			return false, err
		}
	}
	if value == ([eip8297.ValueLength]byte{}) && merge == nil {
		return false, errInsertValue
	}
	window := (d / 4) * 4
	childPath := path.Slice(0, window)
	child, err := t.newRowFromBranch(childPath, key, value, branchPath, cell.Left, cell.Right)
	if err != nil {
		return false, err
	}
	result, err := rowRoutingResult(child)
	if err != nil {
		return false, err
	}
	full, err := rowTopPrefix(child, result.Split)
	if err != nil {
		return false, err
	}
	prefix := full.Slice(row.path.BitLen+4, result.Split)
	setBranch(row, slot, t.branchCell(prefix, common.Hash{}, common.Hash{}))
	row.cell(slot).child = child
	child.parent = row
	child.parentSlot = slot
	oldSlot := slotAt(&branchPath, child.path.BitLen)
	if oldChild != nil {
		child.cell(oldSlot).child = oldChild
		oldChild.parent = child
		oldChild.parentSlot = oldSlot
	}
	t.registerRow(child)
	t.markDirty(row)
	return false, nil
}

func (t *Trie) refreshRouting() error {
	if t.root == nil {
		return nil
	}
	sort.SliceStable(t.routingRows, func(i, j int) bool { return t.routingRows[i].path.BitLen > t.routingRows[j].path.BitLen })
	for _, row := range t.routingRows {
		if t.stopsUpperPath(&row.path) {
			continue
		}
		if row.parent == nil || row.parent.cell(row.parentSlot).child != row {
			continue
		}
		if err := t.refreshBranch(row.parent, row.parentSlot, row); err != nil {
			return err
		}
	}
	t.clearRoutingRows()
	switch t.root.form {
	case RowRoot:
		return nil
	case ExtRoot:
		if t.stopsUpperPath(&t.root.self) || t.upperOnly && t.root.self.BitLen >= t.rootRecordPath().BitLen+264 && isStoragePath(&t.root.self) {
			return nil
		}
		row, err := t.extTopRow(t.root)
		if err != nil {
			return err
		}
		if row.occupiedCount() >= 2 {
			return t.refreshRootFromRow(t.root, row)
		}
	}
	return nil
}

func (t *Trie) remove(key []byte) error {
	if len(key) == eip8297.AccountKeyLength && key[len(key)-1] == eip8297.BasicDataLeafKey {
		delete(t.mergeCreatedStems, string(key))
	}
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
		row.markCellDirty(slot)
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
		if t.stopsUpperPath(&root.self) || t.upperOnly && root.self.BitLen >= t.rootRecordPath().BitLen+264 && isStoragePath(&root.self) {
			return nil
		}
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
	var occupied [maxCells]int
	slots := row.occupiedInto(occupied[:0])
	if len(slots) >= 2 {
		if root.form == ExtRoot {
			return t.refreshRootFromRow(root, row)
		}
		return nil
	}
	t.rootDirty = true
	if row.path.BitLen == t.rootRecordPath().BitLen {
		delete(t.dirtyRows, string(t.rootRecordKey()))
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
		root.leaf = *cell.Cell
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
		if t.stopsUpperPath(&cell.child.path) || t.upperOnly && cell.child.path.BitLen >= t.rootRecordPath().BitLen+264 && isStoragePath(&cell.child.path) {
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
	var occupied [maxCells]int
	slots := row.occupiedInto(occupied[:0])
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
		if cell.Kind == BranchCell {
			full := branchPath(row, slots[0], row.cell(slots[0]))
			start := parent.path.BitLen + 4
			if full.BitLen < start {
				return errInsertKey
			}
			cell.Prefix = full.Slice(start, full.BitLen)
			if cell.child != nil {
				cell.child.parent = parent
				cell.child.parentSlot = row.parentSlot
			}
		}
		parent.cells[row.parentSlot] = cell
	}
	parent.markCellDirty(row.parentSlot)
	t.markDirty(parent)
	return nil
}

func (t *Trie) dropPrefix(prefix []byte) error {
	keys, err := t.keysUnderPrefix(prefix)
	if err != nil {
		return err
	}
	for _, key := range keys {
		if err := t.rememberOriginalLeaf(key); err != nil {
			return err
		}
		t.droppedLeafKeys[string(key)] = struct{}{}
		if err := t.remove(key); err != nil {
			return err
		}
	}
	return nil
}

func (t *Trie) refreshBranch(parent *rowNode, slot int, child *rowNode) error {
	start := parent.path.BitLen + 4
	if child.occupiedCount() == 0 {
		parent.cells[slot] = rowCell{}
		child.tombstone = true
		parent.markCellDirty(slot)
		t.markDirty(parent)
		return nil
	}
	cell := parent.cell(slot)
	split := child.path.BitLen
	if child.occupiedCount() >= 2 {
		result, err := rowRoutingResult(child)
		if err != nil {
			return err
		}
		full, err := rowTopPrefix(child, result.Split)
		if err != nil {
			return err
		}
		if full.BitLen < start {
			return errInsertKey
		}
		split = result.Split
		cell.Prefix = full.Slice(start, split)
	} else {
		if split < start {
			return errInsertKey
		}
		cell.Prefix = child.path.Slice(start, split)
	}
	cell.child = child
	parent.markCellDirty(slot)
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
	row := t.newRow(path, nil, nil)
	row.cells[a] = t.leafCell(keyA, valueA)
	row.cells[b] = t.leafCell(keyB, valueB)
	row.markDirty()
	return row, nil
}

func (t *Trie) newRowFromBranch(path eip8297.Bitpath, key []byte, value [eip8297.ValueLength]byte, oldPath eip8297.Bitpath, left, right common.Hash) (*rowNode, error) {
	if oldPath.BitLen < path.BitLen+4 {
		return nil, errInsertKey
	}
	row := t.newRow(path, nil, nil)
	newSlot := int(keyBit(path.BitLen, key))
	oldSlot := int(oldPath.Bit(path.BitLen)*8 + oldPath.Bit(path.BitLen+1)*4 + oldPath.Bit(path.BitLen+2)*2 + oldPath.Bit(path.BitLen+3))
	row.cells[newSlot] = t.leafCell(key, value)
	oldPrefix := oldPath.Slice(path.BitLen+4, oldPath.BitLen)
	row.cells[oldSlot] = t.branchCell(oldPrefix, left, right)
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
