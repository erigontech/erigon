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
	"reflect"
)

func (t *Trie) Verify() error {
	if t.ctx == nil {
		return fmt.Errorf("nil Patricia context")
	}
	root, err := t.loadRoot()
	if err != nil {
		return err
	}
	if root.row == nil && root.form == RowRoot {
		return nil
	}
	switch root.form {
	case LeafRoot:
		return t.verifyRootRecord()
	case ExtRoot:
		if err := t.verifyRootRecord(); err != nil {
			return err
		}
		row, err := t.extTopRow(root)
		if err != nil {
			return err
		}
		result, err := t.verifyRow(row)
		if err != nil {
			return err
		}
		if result.Split != root.self.BitLen || result.Left != root.left || result.Right != root.right {
			return fmt.Errorf("root extension does not match its top row")
		}
		wantSelf, err := rowTopPrefix(row, result.Split)
		if err != nil {
			return err
		}
		if root.self != wantSelf {
			return fmt.Errorf("root extension prefix does not match its top row")
		}
		return nil
	case RowRoot:
		_, err := t.verifyRow(root.row)
		return err
	default:
		return fmt.Errorf("unknown root form %d", root.form)
	}
}

func (t *Trie) verifyRootRecord() error {
	record := t.rootRecord()
	data, err := EncodeRecord(GlobalRootKey(), &record)
	if err != nil {
		return err
	}
	if !bytes.Equal(data, t.root.raw) {
		return fmt.Errorf("root record is not canonical")
	}
	decoded, err := DecodeRecord(GlobalRootKey(), data)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(record, decoded) {
		return fmt.Errorf("root record round trip changed its value")
	}
	return nil
}

func (t *Trie) verifyRow(row *rowNode) (FoldResult, error) {
	record := row.record()
	data, err := EncodeRecord(row.key, &record)
	if err != nil {
		return FoldResult{}, err
	}
	if len(row.raw) != 0 && !bytes.Equal(data, row.raw) && !row.dirty {
		return FoldResult{}, fmt.Errorf("stored row %x changed without a mutation", row.key)
	}
	decoded, err := DecodeRecord(row.key, data)
	if err != nil {
		return FoldResult{}, err
	}
	if !reflect.DeepEqual(record, decoded) {
		return FoldResult{}, fmt.Errorf("row %x is not canonical", row.key)
	}
	for slot := range row.cells {
		cell := row.cell(slot)
		if cell.Kind != BranchCell {
			continue
		}
		child, err := t.loadBranchChild(row, slot)
		if err != nil {
			return FoldResult{}, err
		}
		childResult, err := t.verifyRow(child)
		if err != nil {
			return FoldResult{}, err
		}
		full, err := rowTopPrefix(child, childResult.Split)
		if err != nil {
			return FoldResult{}, err
		}
		wantPrefix := full.Slice(row.path.BitLen+4, childResult.Split)
		if cell.Prefix != wantPrefix || cell.Left != childResult.Left || cell.Right != childResult.Right {
			return FoldResult{}, fmt.Errorf("row %x cell %d does not match its child", row.key, slot)
		}
	}
	return rowFoldResult(row)
}
