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

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
)

func (t *Trie) newVerifier() *Trie {
	verifier := &Trie{
		ctx:                   t.ctx,
		rootKey:               bytes.Clone(t.rootKey),
		rootPath:              t.rootPath,
		bucketMode:            t.bucketMode,
		upperOnly:             t.upperOnly,
		suppressRoot:          t.suppressRoot,
		suppressBucketRecords: t.suppressBucketRecords,
		verifyOnly:            true,
		verifyProgress:        t.verifyProgress,
	}
	if t.ownedPrefix != nil {
		prefix := *t.ownedPrefix
		verifier.ownedPrefix = &prefix
	}
	if len(t.upperStops) != 0 {
		verifier.upperStops = append([]eip8297.Bitpath(nil), t.upperStops...)
	}
	return verifier
}

func (t *Trie) Verify() error {
	if t.ctx == nil {
		return fmt.Errorf("nil Patricia context")
	}
	return t.newVerifier().verify()
}

func (t *Trie) verify() error {
	root, err := t.loadRoot()
	if err != nil {
		return err
	}
	if _, ok := t.ctx.(interface{ Records() map[string][]byte }); ok {
		t.verifiedBucketKeys = make(map[string]struct{})
	} else {
		t.verifiedBucketKeys = nil
	}
	if root.row == nil && root.form == RowRoot {
		return t.verifyBuckets()
	}
	var verifyErr error
	switch root.form {
	case LeafRoot:
		verifyErr = t.verifyRootRecord()
		if verifyErr == nil && root.leaf.Key[0] == eip8297.StorageZone {
			path, err := keyPath(root.leaf.Key)
			if err != nil {
				verifyErr = err
			} else {
				bucketPath := path.Slice(0, 264)
				verifyErr = t.verifyBucketRecordPath(&bucketPath, bucketDescriptor{form: LeafRoot, leaf: root.leaf})
			}
		}
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
		if verifyErr == nil && root.self.BitLen >= 264 && pathByte(&root.self) == eip8297.StorageZone {
			bucketPath := root.self.Slice(0, 264)
			verifyErr = t.verifyBucketBranch(&bucketPath, &root.self, row, root.left, root.right)
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
	if t.verifyProgress != nil {
		t.verifyProgress(row.key)
	}
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
		if cell.Kind == LeafCell {
			if row.path.BitLen < 264 && cell.Key[0] == eip8297.StorageZone {
				path, err := keyPath(cell.Key)
				if err != nil {
					return FoldResult{}, err
				}
				bucketPath := path.Slice(0, 264)
				if err := t.verifyBucketRecordPath(&bucketPath, bucketDescriptor{form: LeafRoot, leaf: *cell.Cell}); err != nil {
					return FoldResult{}, err
				}
			}
			continue
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
		top, err := rowTopPrefix(child, childResult.Split)
		if err != nil {
			return FoldResult{}, err
		}
		wantPrefix := top.Slice(row.path.BitLen+4, childResult.Split)
		if cell.Prefix != wantPrefix || cell.Left != childResult.Left || cell.Right != childResult.Right {
			return FoldResult{}, fmt.Errorf("row %x cell %d does not match its child", row.key, slot)
		}
		full := branchPath(row, slot, cell)
		if row.path.BitLen < 264 && full.BitLen >= 264 && pathByte(&full) == eip8297.StorageZone {
			bucketPath := full.Slice(0, 264)
			if err := t.verifyBucketBranch(&bucketPath, &full, child, cell.Left, cell.Right); err != nil {
				return FoldResult{}, err
			}
		}
		cell.child = nil
		child.parent = nil
	}
	return rowFoldResult(row)
}

func (t *Trie) verifyBuckets() error {
	records, ok := t.ctx.(interface{ Records() map[string][]byte })
	if !ok {
		return nil
	}
	for key, data := range records.Records() {
		if len(data) == 0 {
			continue
		}
		if _, err := bucketPathForKey([]byte(key)); err == nil {
			if _, found := t.verifiedBucketKeys[key]; !found {
				return fmt.Errorf("orphan bucket record %x", []byte(key))
			}
		}
	}
	return nil
}

func (t *Trie) verifyBucketBranch(bucketPath, branchPath *eip8297.Bitpath, row *rowNode, left, right common.Hash) error {
	if branchPath.BitLen < bucketPath.BitLen+4 {
		return t.verifyBucketRecordPath(bucketPath, bucketDescriptor{form: RowRoot, row: row})
	}
	return t.verifyBucketRecordPath(bucketPath, bucketDescriptor{form: ExtRoot, self: branchPath.Slice(bucketPath.BitLen, branchPath.BitLen), left: left, right: right})
}

func (t *Trie) verifyBucketRecordPath(path *eip8297.Bitpath, descriptor bucketDescriptor) error {
	key, err := EncodeRowKey(path)
	if err != nil {
		return err
	}
	if t.verifiedBucketKeys != nil {
		if _, found := t.verifiedBucketKeys[string(key)]; found {
			return nil
		}
	}
	if err := t.verifyBucketRecord(key, descriptor); err != nil {
		return err
	}
	if t.verifiedBucketKeys != nil {
		t.verifiedBucketKeys[string(key)] = struct{}{}
	}
	return nil
}

func (t *Trie) verifyBucketRecord(key []byte, descriptor bucketDescriptor) error {
	data, _, err := t.ctx.Branch(key)
	if err != nil {
		return err
	}
	if len(data) == 0 {
		return fmt.Errorf("bucket record %x is missing", key)
	}
	record := descriptor.record()
	wantData, err := EncodeRecord(key, &record)
	if err != nil {
		return err
	}
	if !bytes.Equal(data, wantData) {
		return fmt.Errorf("bucket record %x does not match its upper cell", key)
	}
	if _, err := DecodeRecord(key, data); err != nil {
		return err
	}
	return nil
}
