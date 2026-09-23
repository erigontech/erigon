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

package v4

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/erigontech/erigon/execution/commitment"
)

var (
	errWipeRecord = errors.New("commitment v4: invalid wipe record")
	errWipePath   = errors.New("commitment v4: invalid wipe path")
)

func wipeStorageRecords(ctx commitment.PatriciaContext, addrHash [32]byte) error {
	records, err := enumerateStorageRecords(ctx, addrHash)
	if err != nil {
		return err
	}
	for _, r := range records {
		if err := applyDelta(newRecordDelta(r.key, nil, r.prev), ctx.PutBranch); err != nil {
			return err
		}
	}
	return nil
}

func enumerateStorageRecords(ctx commitment.PatriciaContext, addrHash [32]byte) ([]recordDelta, error) {
	rootKey := StorageRootKey(addrHash)
	data, _, err := ctx.Branch(rootKey)
	if err != nil {
		return nil, err
	}
	records := []recordDelta{{key: rootKey, prev: bytes.Clone(data)}}
	if len(data) == 0 {
		return records, nil
	}
	seen := map[string]struct{}{string(rootKey): {}}
	if err := enumerateRecordChildren(ctx, addrHash, nil, records[0].prev, 0, &records, seen); err != nil {
		return nil, err
	}
	return records, nil
}

func enumerateRecordChildren(ctx commitment.PatriciaContext, addrHash [32]byte, path, data []byte, depth int, records *[]recordDelta, seen map[string]struct{}) error {
	if err := Validate(data, depth); err != nil {
		return err
	}
	record := NewRecord(data, depth)
	if record.isLeafRoot() {
		return nil
	}
	l := record.layout()
	for nib := range 16 {
		bit := uint16(1) << nib
		if l.child&bit == 0 || l.leaf&bit != 0 {
			continue
		}
		childPath, err := childRecordPath(record, l, path, nib)
		if err != nil {
			return err
		}
		key := StorageNodeKey(addrHash, childPath, nil)
		if _, ok := seen[string(key)]; ok {
			return fmt.Errorf("%w: repeated child key", errWipeRecord)
		}
		seen[string(key)] = struct{}{}
		childData, _, err := ctx.Branch(key)
		if err != nil {
			return err
		}
		if len(childData) == 0 {
			return fmt.Errorf("%w: missing child at depth %d", errWipeRecord, len(childPath))
		}
		childData = bytes.Clone(childData)
		*records = append(*records, recordDelta{key: key, prev: childData})
		if err := enumerateRecordChildren(ctx, addrHash, childPath, childData, len(childPath), records, seen); err != nil {
			return err
		}
	}
	return nil
}

func childRecordPath(record Record, l layout, path []byte, nib int) ([]byte, error) {
	if nib < 0 || nib > 15 {
		return nil, errWipePath
	}
	if selfExt := record.SelfExt(); len(path) == 0 && len(selfExt) != 0 {
		return unpackPath(selfExt[1:], int(selfExt[0]), nil), nil
	}
	childPath := append(append([]byte(nil), path...), byte(nib))
	ext := record.extAt(l, nib)
	if len(ext) != 0 {
		decoded, err := decodeExtension(ext)
		if err != nil {
			return nil, err
		}
		childPath = append(childPath, decoded...)
	}
	if len(childPath) == 0 || len(childPath) > 63 {
		return nil, errWipePath
	}
	return childPath, nil
}
