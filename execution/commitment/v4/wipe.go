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
	"errors"
	"fmt"

	"github.com/erigontech/erigon/execution/commitment"
)

var (
	errWipeRecord = errors.New("commitment v4: invalid wipe record")
	errWipePath   = errors.New("commitment v4: invalid wipe path")
)

func wipeStorageRecords(ctx commitment.PatriciaContext, addrHash [32]byte) error {
	keys, err := enumerateStorageRecords(ctx, addrHash)
	if err != nil {
		return err
	}
	for _, key := range keys {
		if err := putStorageRecord(ctx, key, nil); err != nil {
			return err
		}
	}
	return nil
}

func enumerateStorageRecords(ctx commitment.PatriciaContext, addrHash [32]byte) ([][]byte, error) {
	rootKey := StorageRootKey(addrHash)
	keys := make([][]byte, 0, 1)
	seen := make(map[string]struct{})
	keys = append(keys, append([]byte(nil), rootKey...))
	seen[string(rootKey)] = struct{}{}

	data, _, err := ctx.Branch(rootKey)
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return keys, nil
	}
	if err := enumerateRecordChildren(ctx, addrHash, nil, data, 0, &keys, seen); err != nil {
		return nil, err
	}
	return keys, nil
}

func enumerateRecordChildren(ctx commitment.PatriciaContext, addrHash [32]byte, path, data []byte, depth int, keys *[][]byte, seen map[string]struct{}) error {
	if err := Validate(data, depth); err != nil {
		return err
	}
	record := NewRecord(data, depth)
	if record.isLeafRoot() {
		return nil
	}
	for nib := range 16 {
		bit := uint16(1) << nib
		if record.ChildMask()&bit == 0 || record.LeafMask()&bit != 0 {
			continue
		}
		childPath, err := childRecordPath(record, path, nib)
		if err != nil {
			return err
		}
		key := StorageNodeKey(addrHash, childPath, nil)
		if _, ok := seen[string(key)]; ok {
			return fmt.Errorf("%w: repeated child key", errWipeRecord)
		}
		seen[string(key)] = struct{}{}
		*keys = append(*keys, key)
		childData, _, err := ctx.Branch(key)
		if err != nil {
			return err
		}
		if len(childData) == 0 {
			return fmt.Errorf("%w: missing child at depth %d", errWipeRecord, len(childPath))
		}
		if err := enumerateRecordChildren(ctx, addrHash, childPath, childData, len(childPath), keys, seen); err != nil {
			return err
		}
	}
	return nil
}

func childRecordPath(record Record, path []byte, nib int) ([]byte, error) {
	if nib < 0 || nib > 15 {
		return nil, errWipePath
	}
	if len(path) == 0 && len(record.SelfExt()) != 0 {
		return append([]byte(nil), unpackPath(record.SelfExt()[1:], int(record.SelfExt()[0]), nil)...), nil
	}
	childPath := append(append([]byte(nil), path...), byte(nib))
	ext := record.ExtAt(nib)
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
