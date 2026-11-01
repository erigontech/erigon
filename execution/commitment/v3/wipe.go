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

package v3

import (
	"errors"
	"fmt"

	"github.com/erigontech/erigon/execution/commitment"
)

var errWipeRecord = errors.New("commitment v3: invalid wipe record")

func enumerateStorageRecords(ctx commitment.PatriciaContext, addrHash [32]byte) ([]recordDelta, error) {
	rootKey := StorageNodeKey(addrHash, nil, nil)
	data, _, err := branchOwned(ctx, rootKey)
	if err != nil || len(data) == 0 {
		return nil, err
	}
	records := []recordDelta{{Key: rootKey, Data: []byte{}, Prev: data}}
	if err := enumerateRecordChildren(ctx, addrHash, nil, records[0].Prev, 0, &records); err != nil {
		return nil, err
	}
	return records, nil
}

func enumerateRecordChildren(ctx commitment.PatriciaContext, addrHash [32]byte, path, data []byte, depth int, records *[]recordDelta) error {
	if err := Validate(data, depth); err != nil {
		return err
	}
	record := Record{data: data, depth: depth}
	l := record.layout()
	for nib := range 16 {
		bit := uint16(1) << nib
		if l.child&bit == 0 || l.leaf&bit != 0 {
			continue
		}
		childPath := childRecordPath(record, l, path, nib)
		key := StorageNodeKey(addrHash, childPath, nil)
		childData, _, err := branchOwned(ctx, key)
		if err != nil {
			return err
		}
		if len(childData) == 0 {
			return fmt.Errorf("%w: missing child at depth %d", errWipeRecord, len(childPath))
		}
		*records = append(*records, recordDelta{Key: key, Data: []byte{}, Prev: childData})
		if err := enumerateRecordChildren(ctx, addrHash, childPath, childData, len(childPath), records); err != nil {
			return err
		}
	}
	return nil
}

func childRecordPath(record Record, l layout, path []byte, nib int) []byte {
	if selfExt := record.SelfExt(); len(path) == 0 && len(selfExt) != 0 {
		return unpackPath(selfExt[1:], int(selfExt[0]), nil)
	}
	childPath := append(append([]byte(nil), path...), byte(nib))
	return append(childPath, decodeExtension(record.extAt(l, nib))...)
}
