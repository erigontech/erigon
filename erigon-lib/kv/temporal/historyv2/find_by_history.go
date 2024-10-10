/*
   Copyright 2022 Erigon contributors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package historyv2

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/bitmapdb"
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
