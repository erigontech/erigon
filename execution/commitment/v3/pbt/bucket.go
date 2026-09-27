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

type bucketDescriptor struct {
	form        RootForm
	leaf        Cell
	self        eip8297.Bitpath
	left, right common.Hash
	split       int16
	row         *rowNode
}

func (d bucketDescriptor) record() Record {
	switch d.form {
	case LeafRoot:
		return Record{Form: LeafRoot, Cells: [maxCells]Cell{0: d.leaf}}
	case ExtRoot:
		return Record{Form: ExtRoot, SelfExt: d.self, Left: d.left, Right: d.right}
	case RowRoot:
		return d.row.record()
	default:
		return Record{}
	}
}

func bucketKeyForStorage(key []byte) ([]byte, error) {
	if len(key) != eip8297.StorageKeyLength || key[0] != eip8297.StorageZone {
		return nil, errInsertKey
	}
	return bucketKeyForPrefix(key[:33])
}

func bucketKeyForPrefix(prefix []byte) ([]byte, error) {
	if len(prefix) != 33 || prefix[0] != eip8297.StorageZone {
		return nil, errInsertKey
	}
	path := eip8297.PathFromBits(prefix, 264)
	return EncodeRowKey(&path)
}

func bucketPathForKey(key []byte) (eip8297.Bitpath, error) {
	path, err := eip8297.DecodeBitPath(key)
	if err != nil {
		return eip8297.Bitpath{}, err
	}
	if path.BitLen != 264 || pathByte(&path, 0) != eip8297.StorageZone {
		return eip8297.Bitpath{}, errInsertKey
	}
	return path, nil
}

func pathHasPrefix(path, prefix *eip8297.Bitpath) bool {
	return path.BitLen >= prefix.BitLen && eip8297.CommonPrefixBitsAt(path, 0, prefix) == prefix.BitLen
}

func (t *Trie) bucketDescriptor(key []byte) (bucketDescriptor, bool, error) {
	bucketPath, err := bucketPathForKey(key)
	if err != nil {
		return bucketDescriptor{}, false, err
	}
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
		if root.leaf.Key[0] == eip8297.StorageZone && pathHasPrefix(&path, &bucketPath) {
			return bucketDescriptor{form: LeafRoot, leaf: root.leaf}, true, nil
		}
	case ExtRoot:
		if root.self.BitLen >= bucketPath.BitLen && pathHasPrefix(&root.self, &bucketPath) {
			if root.self.BitLen < bucketPath.BitLen+4 {
				row, err := t.extTopRow(root)
				if err != nil {
					return bucketDescriptor{}, false, err
				}
				return bucketDescriptor{form: RowRoot, row: row}, true, nil
			}
			return bucketDescriptor{form: ExtRoot, self: root.self.Slice(bucketPath.BitLen, root.self.BitLen), left: root.left, right: root.right}, true, nil
		}
		row, err := t.extTopRow(root)
		if err != nil {
			return bucketDescriptor{}, false, err
		}
		return t.bucketDescriptorInRow(row, &bucketPath)
	case RowRoot:
		if root.row == nil {
			return bucketDescriptor{}, false, nil
		}
		return t.bucketDescriptorInRow(root.row, &bucketPath)
	default:
		return bucketDescriptor{}, false, fmt.Errorf("unknown root form %d", root.form)
	}
	return bucketDescriptor{}, false, nil
}

func (t *Trie) bucketDescriptorInRow(row *rowNode, bucketPath *eip8297.Bitpath) (bucketDescriptor, bool, error) {
	if row.path.BitLen >= bucketPath.BitLen {
		if row.path.BitLen == bucketPath.BitLen && row.path == *bucketPath {
			return bucketDescriptor{form: RowRoot, row: row}, true, nil
		}
		return bucketDescriptor{}, false, nil
	}
	if row.path.BitLen+4 > bucketPath.BitLen {
		return bucketDescriptor{}, false, nil
	}
	slot := slotAt(bucketPath, row.path.BitLen)
	cell := row.cell(slot)
	switch cell.Kind {
	case LeafCell:
		path, err := keyPath(cell.Key)
		if err != nil {
			return bucketDescriptor{}, false, err
		}
		if cell.Key[0] == eip8297.StorageZone && pathHasPrefix(&path, bucketPath) {
			return bucketDescriptor{form: LeafRoot, leaf: cell.Cell}, true, nil
		}
	case BranchCell:
		full := branchPath(row, slot, cell)
		if full.BitLen >= bucketPath.BitLen && pathHasPrefix(&full, bucketPath) {
			if full.BitLen < bucketPath.BitLen+4 {
				child, err := t.loadBranchChild(row, slot)
				if err != nil {
					return bucketDescriptor{}, false, err
				}
				return bucketDescriptor{form: RowRoot, row: child}, true, nil
			}
			return bucketDescriptor{form: ExtRoot, self: full.Slice(bucketPath.BitLen, full.BitLen), left: cell.Left, right: cell.Right}, true, nil
		}
		if full.BitLen < bucketPath.BitLen && pathHasPrefix(bucketPath, &full) {
			child, err := t.loadBranchChild(row, slot)
			if err != nil {
				return bucketDescriptor{}, false, err
			}
			return t.bucketDescriptorInRow(child, bucketPath)
		}
	}
	return bucketDescriptor{}, false, nil
}

func (t *Trie) bucketRecordPrevious(key []byte) ([]byte, error) {
	if data, ok := t.roundPrev[string(key)]; ok {
		return bytes.Clone(data), nil
	}
	data, err := t.bucketRecord(key)
	if err != nil {
		return nil, err
	}
	t.rememberPrev(key, data)
	return data, nil
}

func (t *Trie) bucketRecord(key []byte) ([]byte, error) {
	if data, ok := t.scheduledBucketRecords[string(key)]; ok {
		return bytes.Clone(data), nil
	}
	data, _, err := t.ctx.Branch(key)
	return data, err
}

func (t *Trie) bucketKeysFromRecord(key []byte) ([][]byte, error) {
	data, err := t.bucketRecord(key)
	if err != nil {
		return nil, err
	}
	t.rememberPrev(key, data)
	if len(data) == 0 {
		return nil, nil
	}
	record, err := DecodeRecord(key, data)
	if err != nil {
		return nil, err
	}
	path, err := bucketPathForKey(key)
	if err != nil {
		return nil, err
	}
	var row *rowNode
	switch record.Form {
	case LeafRoot:
		return [][]byte{bytes.Clone(record.Cells[0].Key)}, nil
	case RowRoot:
		row = t.rowFromRecord(path, key, data, &record)
		t.rows[string(key)] = row
	case ExtRoot:
		window := ((path.BitLen + record.SelfExt.BitLen) / 4) * 4
		childPath := path
		part := record.SelfExt.Slice(0, window-path.BitLen)
		childPath.Append(&part)
		row, err = t.loadRow(childPath)
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unknown bucket form %d", record.Form)
	}
	return t.rowKeys(row)
}

func (t *Trie) keysUnderPrefix(prefix []byte) ([][]byte, error) {
	if len(prefix) != 33 {
		return nil, errInsertKey
	}
	if prefix[0] == eip8297.StorageZone {
		bucketKey, err := bucketKeyForPrefix(prefix)
		if err != nil {
			return nil, err
		}
		return t.bucketKeysFromRecord(bucketKey)
	}
	if prefix[0] != eip8297.AccountZone {
		return nil, errInsertKey
	}
	path := eip8297.PathFromBits(prefix, 264)
	root, err := t.loadRoot()
	if err != nil {
		return nil, err
	}
	switch root.form {
	case LeafRoot:
		keyPath, err := keyPath(root.leaf.Key)
		if err != nil {
			return nil, err
		}
		if pathHasPrefix(&keyPath, &path) {
			return [][]byte{bytes.Clone(root.leaf.Key)}, nil
		}
		return nil, nil
	case ExtRoot:
		if !pathHasPrefix(&root.self, &path) && !pathHasPrefix(&path, &root.self) {
			return nil, nil
		}
		row, err := t.extTopRow(root)
		if err != nil {
			return nil, err
		}
		if pathHasPrefix(&root.self, &path) {
			return t.rowKeys(row)
		}
		return t.keysUnderRow(row, &path)
	case RowRoot:
		if root.row == nil {
			return nil, nil
		}
		return t.keysUnderRow(root.row, &path)
	default:
		return nil, fmt.Errorf("unknown root form %d", root.form)
	}
}

func (t *Trie) keysUnderRow(row *rowNode, prefix *eip8297.Bitpath) ([][]byte, error) {
	if row.path.BitLen >= prefix.BitLen {
		if pathHasPrefix(&row.path, prefix) {
			return t.rowKeys(row)
		}
		return nil, nil
	}
	if !pathHasPrefix(prefix, &row.path) {
		return nil, nil
	}
	slot := slotAt(prefix, row.path.BitLen)
	cell := row.cell(slot)
	switch cell.Kind {
	case EmptyCell:
		return nil, nil
	case LeafCell:
		keyPath, err := keyPath(cell.Key)
		if err != nil {
			return nil, err
		}
		if pathHasPrefix(&keyPath, prefix) {
			return [][]byte{bytes.Clone(cell.Key)}, nil
		}
		return nil, nil
	case BranchCell:
		full := branchPath(row, slot, cell)
		if !pathHasPrefix(&full, prefix) && !pathHasPrefix(prefix, &full) {
			return nil, nil
		}
		child, err := t.loadBranchChild(row, slot)
		if err != nil {
			return nil, err
		}
		if pathHasPrefix(&full, prefix) {
			return t.rowKeys(child)
		}
		return t.keysUnderRow(child, prefix)
	default:
		return nil, errInsertKey
	}
}

func (t *Trie) rowKeys(row *rowNode) ([][]byte, error) {
	keys := make([][]byte, 0)
	for slot := range row.cells {
		cell := row.cell(slot)
		switch cell.Kind {
		case LeafCell:
			keys = append(keys, bytes.Clone(cell.Key))
		case BranchCell:
			child, err := t.loadBranchChild(row, slot)
			if err != nil {
				return nil, err
			}
			childKeys, err := t.rowKeys(child)
			if err != nil {
				return nil, err
			}
			keys = append(keys, childKeys...)
		}
	}
	return keys, nil
}

func (t *Trie) expectedBucketRecords() (map[string]bucketDescriptor, error) {
	records := make(map[string]bucketDescriptor)
	add := func(path *eip8297.Bitpath, descriptor bucketDescriptor) error {
		key, err := EncodeRowKey(path)
		if err != nil {
			return err
		}
		records[string(key)] = descriptor
		return nil
	}
	var visit func(*rowNode) error
	visit = func(row *rowNode) error {
		for slot := range row.cells {
			cell := row.cell(slot)
			switch cell.Kind {
			case LeafCell:
				if cell.Key[0] != eip8297.StorageZone {
					continue
				}
				path, err := keyPath(cell.Key)
				if err != nil {
					return err
				}
				bucketPath := path.Slice(0, 264)
				if err := add(&bucketPath, bucketDescriptor{form: LeafRoot, leaf: cell.Cell}); err != nil {
					return err
				}
			case BranchCell:
				full := branchPath(row, slot, cell)
				if full.BitLen >= 264 && pathByte(&full, 0) == eip8297.StorageZone {
					bucketPath := full.Slice(0, 264)
					if full.BitLen < 268 {
						child, err := t.loadBranchChild(row, slot)
						if err != nil {
							return err
						}
						if err := add(&bucketPath, bucketDescriptor{form: RowRoot, row: child}); err != nil {
							return err
						}
					} else if err := add(&bucketPath, bucketDescriptor{form: ExtRoot, self: full.Slice(264, full.BitLen), left: cell.Left, right: cell.Right}); err != nil {
						return err
					}
					continue
				}
				if full.BitLen < 264 {
					child, err := t.loadBranchChild(row, slot)
					if err != nil {
						return err
					}
					if err := visit(child); err != nil {
						return err
					}
				}
			}
		}
		return nil
	}
	root, err := t.loadRoot()
	if err != nil {
		return nil, err
	}
	switch root.form {
	case LeafRoot:
		if root.leaf.Key[0] == eip8297.StorageZone {
			path, err := keyPath(root.leaf.Key)
			if err != nil {
				return nil, err
			}
			bucketPath := path.Slice(0, 264)
			if err := add(&bucketPath, bucketDescriptor{form: LeafRoot, leaf: root.leaf}); err != nil {
				return nil, err
			}
		}
	case ExtRoot:
		if root.self.BitLen >= 264 && pathByte(&root.self, 0) == eip8297.StorageZone {
			bucketPath := root.self.Slice(0, 264)
			if root.self.BitLen < 268 {
				row, err := t.extTopRow(root)
				if err != nil {
					return nil, err
				}
				if err := add(&bucketPath, bucketDescriptor{form: RowRoot, row: row}); err != nil {
					return nil, err
				}
			} else if err := add(&bucketPath, bucketDescriptor{form: ExtRoot, self: root.self.Slice(264, root.self.BitLen), left: root.left, right: root.right}); err != nil {
				return nil, err
			}
		} else {
			row, err := t.extTopRow(root)
			if err != nil {
				return nil, err
			}
			if err := visit(row); err != nil {
				return nil, err
			}
		}
	case RowRoot:
		if root.row != nil {
			if err := visit(root.row); err != nil {
				return nil, err
			}
		}
	}
	return records, nil
}
