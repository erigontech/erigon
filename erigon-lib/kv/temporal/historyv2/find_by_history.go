// Copyright 2022 The Erigon Authors
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

package historyv2

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/erigontech/erigon/erigon-lib/common/length"
	"github.com/erigontech/erigon/erigon-lib/kv"
	"github.com/erigontech/erigon/erigon-lib/kv/bitmapdb"
)

func FindByHistory(indexC kv.Cursor, changesC kv.CursorDupSort, storage bool, key []byte, timestamp uint64) ([]byte, bool, error) {
	var csBucket string
	if storage {
		csBucket = kv.StorageChangeSet
	} else {
		csBucket = kv.AccountChangeSet
	}

	k, v, seekErr := indexC.Seek(Mapper[csBucket].IndexChunkKey(key, timestamp))
	if seekErr != nil {
		return nil, false, seekErr
	}

	if k == nil {
		return nil, false, nil
	}
	if storage {
		if !bytes.Equal(k[:length.Addr], key[:length.Addr]) ||
			!bytes.Equal(k[length.Addr:length.Addr+length.Hash], key[length.Addr+length.Incarnation:]) {
			return nil, false, nil
		}
	} else {
		if !bytes.HasPrefix(k, key) {
			return nil, false, nil
		}
	}
	index := roaring64.New()
	if _, err := index.ReadFrom(bytes.NewReader(v)); err != nil {
		return nil, false, err
	}
	found, ok := bitmapdb.SeekInBitmap64(index, timestamp)
	changeSetBlock := found

	var data []byte
	var err error
	if ok {
		data, err = Mapper[csBucket].Find(changesC, changeSetBlock, key)
		if err != nil {
			if !errors.Is(err, ErrNotFound) {
				return nil, false, fmt.Errorf("finding %x in the changeset %d: %w", key, changeSetBlock, err)
			}
			return nil, false, nil
		}
	} else {
		return nil, false, nil
	}

	return data, true, nil
}
