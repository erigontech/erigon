package historyv2read

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/bitmapdb"
	historyv22 "github.com/ledgerwatch/erigon-lib/kv/temporal/historyv2"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/ethdb"
)

const DefaultIncarnation = uint64(1)

func GetAsOf(tx kv.Tx, indexC kv.Cursor, changesC kv.CursorDupSort, storage bool, key []byte, timestamp uint64) ([]byte, error) {
	v, err := FindByHistory(tx, indexC, changesC, storage, key, timestamp)
	if err == nil {
		return v, nil
	}
	if !errors.Is(err, ethdb.ErrKeyNotFound) {
		return nil, err
	}
	return tx.GetOne(kv.PlainState, key)
}

func FindByHistory(tx kv.Tx, indexC kv.Cursor, changesC kv.CursorDupSort, storage bool, key []byte, timestamp uint64) ([]byte, error) {
	var csBucket string
	if storage {
		csBucket = kv.StorageChangeSet
	} else {
		csBucket = kv.AccountChangeSet
	}

	k, v, seekErr := indexC.Seek(historyv22.Mapper[csBucket].IndexChunkKey(key, timestamp))
	if seekErr != nil {
		return nil, seekErr
	}

	if k == nil {
		return nil, ethdb.ErrKeyNotFound
	}
	if storage {
		if !bytes.Equal(k[:length.Addr], key[:length.Addr]) ||
			!bytes.Equal(k[length.Addr:length.Addr+length.Hash], key[length.Addr+length.Incarnation:]) {
			return nil, ethdb.ErrKeyNotFound
		}
	} else {
		if !bytes.HasPrefix(k, key) {
			return nil, ethdb.ErrKeyNotFound
		}
	}
	index := roaring64.New()
	if _, err := index.ReadFrom(bytes.NewReader(v)); err != nil {
		return nil, err
	}
	found, ok := bitmapdb.SeekInBitmap64(index, timestamp)
	changeSetBlock := found

	var data []byte
	var err error
	if ok {
		data, err = historyv22.Mapper[csBucket].Find(changesC, changeSetBlock, key)
		if err != nil {
			if !errors.Is(err, historyv22.ErrNotFound) {
				return nil, fmt.Errorf("finding %x in the changeset %d: %w", key, changeSetBlock, err)
			}
			return nil, ethdb.ErrKeyNotFound
		}
	} else {
		return nil, ethdb.ErrKeyNotFound
	}

	//restore codehash
	if !storage {
		var acc accounts.Account
		if err := acc.DecodeForStorage(data); err != nil {
			return nil, err
		}
		if acc.Incarnation > 0 && acc.IsEmptyCodeHash() {
			var codeHash []byte
			var err error
			codeHash, err = tx.GetOne(kv.PlainContractCode, dbutils.PlainGenerateStoragePrefix(key, acc.Incarnation))
			if err != nil {
				return nil, err
			}
			if len(codeHash) > 0 {
				acc.CodeHash.SetBytes(codeHash)
			}
			data = make([]byte, acc.EncodingLengthForStorage())
			acc.EncodeForStorage(data)
		}
		return data, nil
	}

	return data, nil
}
