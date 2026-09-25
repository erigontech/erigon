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

	"github.com/erigontech/erigon/execution/commitment/eip8297"
)

func (t *Trie) loadRoot() (*treeRoot, error) {
	if t.rootLoaded {
		return t.root, nil
	}
	t.rootLoaded = true
	rootKey := t.rootRecordKey()
	data, _, err := t.ctx.Branch(rootKey)
	if err != nil {
		return nil, err
	}
	t.root = &treeRoot{}
	t.rememberPrev(rootKey, data)
	if len(data) == 0 {
		return t.root, nil
	}
	record, err := DecodeRecord(rootKey, data)
	if err != nil {
		return nil, err
	}
	t.root.raw = bytes.Clone(data)
	t.root.prev = bytes.Clone(data)
	switch record.Form {
	case RowRoot:
		path := t.rootRecordPath()
		t.root.form = RowRoot
		t.root.row = rowFromRecord(path, rootKey, data, &record)
		t.rows[string(rootKey)] = t.root.row
	case LeafRoot:
		t.root.form = LeafRoot
		t.root.leaf = record.Cells[0]
	case ExtRoot:
		t.root.form = ExtRoot
		t.root.self = record.SelfExt
		if t.bucketMode {
			self := t.rootRecordPath()
			self.Append(&record.SelfExt)
			t.root.self = self
		}
		t.root.left = record.Left
		t.root.right = record.Right
	default:
		return nil, fmt.Errorf("unknown root form %d", record.Form)
	}
	return t.root, nil
}

func (t *Trie) loadRow(path eip8297.Bitpath) (*rowNode, error) {
	key, err := rowKeyForPath(&path)
	if err != nil {
		return nil, err
	}
	if row := t.rows[string(key)]; row != nil {
		return row, nil
	}
	data, _, err := t.ctx.Branch(key)
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return nil, fmt.Errorf("row %x is missing", key)
	}
	t.rememberPrev(key, data)
	record, err := DecodeRecord(key, data)
	if err != nil {
		return nil, err
	}
	if record.Form != RowRoot {
		return nil, fmt.Errorf("row %x has form %d", key, record.Form)
	}
	row := rowFromRecord(path, key, data, &record)
	t.rows[string(key)] = row
	return row, nil
}

func (t *Trie) loadBranchChild(parent *rowNode, slot int) (*rowNode, error) {
	cell := parent.cell(slot)
	if cell.child != nil {
		return cell.child, nil
	}
	split := branchSplit(parent, slot, cell)
	path, err := rowChildPath(parent, slot, cell.Prefix, split)
	if err != nil {
		return nil, err
	}
	child, err := t.loadRow(path)
	if err != nil {
		return nil, err
	}
	cell.child = child
	child.parent = parent
	child.parentSlot = slot
	return child, nil
}
