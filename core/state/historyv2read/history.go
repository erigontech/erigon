package historyv2read

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/RoaringBitmap/roaring/roaring64"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/bitmapdb"
	"github.com/ledgerwatch/erigon-lib/kv/temporal/historyv2"
	"github.com/ledgerwatch/erigon/core/bitmapdb2"
	"github.com/ledgerwatch/erigon/core/types/accounts"
)

const DefaultIncarnation = uint64(1)

func RestoreCodeHash(tx kv.Getter, key, v []byte, force *libcommon.Hash) ([]byte, error) {
	var acc accounts.Account
	if err := acc.DecodeForStorage(v); err != nil {
		return nil, err
	}
	if force != nil {
		acc.CodeHash = *force
	} else if acc.Incarnation > 0 && acc.IsEmptyCodeHash() {
		var codeHash []byte
		var err error

		prefix := make([]byte, length.Addr+length.BlockNum)
		copy(prefix, key)
		binary.BigEndian.PutUint64(prefix[length.Addr:], acc.Incarnation)

		codeHash, err = tx.GetOne(kv.PlainContractCode, prefix)
		if err != nil {
			return nil, err
		}
		if len(codeHash) > 0 {
			acc.CodeHash.SetBytes(codeHash)
		}
		v = make([]byte, acc.EncodingLengthForStorage())
		acc.EncodeForStorage(v)
	}
	return v, nil
}

type mergedSeeker func(key []byte, csBucket string, storage bool, block uint64) (uint64, bool, error)

func newV2MergedSeeker(indexC kv.Cursor, bitmapDB2 *bitmapdb2.DB) mergedSeeker {
	seekInDB := func(key []byte, csBucket string, storage bool, block uint64) (uint64, bool, error) {
		k, v, err := indexC.Seek(historyv2.Mapper[csBucket].IndexChunkKey(key, block))
		if err != nil {
			return 0, false, err
		}
		if k == nil {
			return 0, false, nil
		}
		if storage {
			if !bytes.Equal(k[:length.Addr], key[:length.Addr]) ||
				!bytes.Equal(k[length.Addr:length.Addr+length.Hash], key[length.Addr+length.Incarnation:]) {
				return 0, false, nil
			}
		} else {
			if !bytes.HasPrefix(k, key) {
				return 0, false, nil
			}
		}
		index := roaring64.New()
		if _, err := index.ReadFrom(bytes.NewReader(v)); err != nil {
			return 0, false, err
		}
		found, ok := bitmapdb.SeekInBitmap64(index, block)
		return found, ok, nil
	}
	seekInBitmapDB2 := func(key []byte, csBucket string, storage bool, block uint64) (uint64, bool, error) {
		if bitmapDB2 == nil {
			return 0, false, nil
		}
		v, err := bitmapDB2.SeekFirstGTE(historyv2.Mapper[csBucket].IndexBucket, key, uint32(block))
		if err != nil {
			return 0, false, err
		}
		return uint64(v), v != 0, nil
	}
	return func(key []byte, csBucket string, storage bool, block uint64) (uint64, bool, error) {
		found1, ok1, err := seekInBitmapDB2(key, csBucket, storage, block)
		if err != nil {
			return 0, false, err
		}
		found2, ok2, err := seekInDB(key, csBucket, storage, block)
		if err != nil {
			return 0, false, err
		}
		if ok1 && ok2 {
			if found1 < found2 {
				return found1, true, nil
			} else {
				return found2, true, nil
			}
		} else if ok1 {
			return found1, true, nil
		} else if ok2 {
			return found2, true, nil
		}
		return 0, false, nil
	}
}

func FindByHistoryV2(indexSeeker mergedSeeker, changesC kv.CursorDupSort, storage bool, key []byte, timestamp uint64) ([]byte, bool, error) {
	var csBucket string
	if storage {
		csBucket = kv.StorageChangeSet
	} else {
		csBucket = kv.AccountChangeSet
	}

	found, ok, err := indexSeeker(key, csBucket, storage, timestamp)
	if err != nil {
		return nil, false, fmt.Errorf("finding %x in the index: %w", key, err)
	}
	changeSetBlock := found

	var data []byte
	if ok {
		data, err = historyv2.Mapper[csBucket].Find(changesC, changeSetBlock, key)
		if err != nil {
			if !errors.Is(err, historyv2.ErrNotFound) {
				return nil, false, fmt.Errorf("finding %x in the changeset %d: %w", key, changeSetBlock, err)
			}
			return nil, false, nil
		}
	} else {
		return nil, false, nil
	}

	return data, true, nil
}

func GetAsOf(tx kv.Tx, indexC kv.Cursor, changesC kv.CursorDupSort, storage bool, key []byte, timestamp uint64,
	bitmapDB2 *bitmapdb2.DB) ([]byte, error) {
	indexSeeker := newV2MergedSeeker(indexC, bitmapDB2)
	v, ok, err := FindByHistoryV2(indexSeeker, changesC, storage, key, timestamp)
	if err != nil {
		return nil, err
	}
	if ok {
		//restore codehash
		if !storage {
			//restore codehash
			v, err = RestoreCodeHash(tx, key, v, nil)
			if err != nil {
				return nil, err
			}
		}

		return v, nil
	}
	return tx.GetOne(kv.PlainState, key)
}
