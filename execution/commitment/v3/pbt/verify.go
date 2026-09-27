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
		return t.verifyBuckets()
	}
	var verifyErr error
	switch root.form {
	case LeafRoot:
		verifyErr = t.verifyRootRecord()
	case ExtRoot:
		if err := t.verifyRootRecord(); err != nil {
			verifyErr = err
			break
		}
		row, err := t.extTopRow(root)
		if err != nil {
			verifyErr = err
			break
		}
		result, err := t.verifyRow(row)
		if err != nil {
			verifyErr = err
			break
		}
		if result.Split != root.self.BitLen || result.Left != root.left || result.Right != root.right {
			verifyErr = fmt.Errorf("root extension does not match its top row")
			break
		}
		wantSelf, err := rowTopPrefix(row, result.Split)
		if err != nil {
			verifyErr = err
			break
		}
		if root.self != wantSelf {
			verifyErr = fmt.Errorf("root extension prefix does not match its top row")
		}
	case RowRoot:
		_, verifyErr = t.verifyRow(root.row)
	default:
		verifyErr = fmt.Errorf("unknown root form %d", root.form)
	}
	if verifyErr != nil {
		return verifyErr
	}
	return t.verifyBuckets()
}

func (t *Trie) verifyRootRecord() error {
	record := t.rootRecord()
	rootKey := t.rootRecordKey()
	data, err := EncodeRecord(rootKey, &record)
	if err != nil {
		return err
	}
	if !bytes.Equal(data, t.root.raw) {
		return fmt.Errorf("root record is not canonical")
	}
	decoded, err := DecodeRecord(rootKey, data)
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
	for slot := range maxCells {
		cell, ok := row.cellValue(slot)
		if !ok {
			continue
		}
		if cell.Kind == BranchCell {
			cell = *row.cell(slot)
		}
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

func (t *Trie) verifyBuckets() error {
	want, err := t.expectedBucketRecords()
	if err != nil {
		return err
	}
	for key := range want {
		descriptor := want[key]
		data, _, err := t.ctx.Branch([]byte(key))
		if err != nil {
			return err
		}
		if len(data) == 0 {
			return fmt.Errorf("bucket record %x is missing", []byte(key))
		}
		record := descriptor.record()
		wantData, err := EncodeRecord([]byte(key), &record)
		if err != nil {
			return err
		}
		if !bytes.Equal(data, wantData) {
			return fmt.Errorf("bucket record %x does not match its upper cell", []byte(key))
		}
		if _, err := DecodeRecord([]byte(key), data); err != nil {
			return err
		}
	}
	if lister, ok := t.ctx.(interface{ Records() map[string][]byte }); ok {
		for key, data := range lister.Records() {
			if len(data) == 0 {
				continue
			}
			if _, err := bucketPathForKey([]byte(key)); err == nil {
				if _, ok := want[key]; !ok {
					return fmt.Errorf("orphan bucket record %x", []byte(key))
				}
			}
		}
	}
	return nil
}
