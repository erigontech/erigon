package state

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/changeset"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

func GetAsOf(db ethdb.KV, plain, storage bool, key []byte, timestamp uint64) ([]byte, error) {
	var dat []byte
	err := db.View(context.Background(), func(tx ethdb.Tx) error {
		v, err := FindByHistory(tx, plain, storage, key, timestamp)
		if err == nil {
			dat = make([]byte, len(v))
			copy(dat, v)
			return nil
		}
		if !errors.Is(err, ethdb.ErrKeyNotFound) {
			return err
		}
		{
			var bucket []byte
			if plain {
				bucket = dbutils.PlainStateBucket
			} else {
				bucket = dbutils.CurrentStateBucket
			}
			v, _ := tx.Bucket(bucket).Get(key)
			if v == nil {
				return ethdb.ErrKeyNotFound
			}

			dat = make([]byte, len(v))
			copy(dat, v)
			return nil
		}
	})
	return dat, err
}

func FindByHistory(tx ethdb.Tx, plain, storage bool, key []byte, timestamp uint64) ([]byte, error) {
	var hBucket []byte
	if storage {
		hBucket = dbutils.StorageHistoryBucket
	} else {
		hBucket = dbutils.AccountsHistoryBucket
	}
	hB := tx.Bucket(hBucket)
	if hB == nil {
		return nil, ethdb.ErrKeyNotFound
	}
	c := hB.Cursor()
	k, v, err := c.Seek(dbutils.IndexChunkKey(key, timestamp))
	if err != nil {
		return nil, err
	}
	if storage {
		if plain {
			if !bytes.Equal(k[:common.AddressLength], key[:common.AddressLength]) ||
				!bytes.Equal(k[common.AddressLength:common.AddressLength+common.HashLength], key[common.AddressLength+common.IncarnationLength:]) {
				return nil, ethdb.ErrKeyNotFound
			}
		} else {
			if !bytes.Equal(k[:common.HashLength], key[:common.HashLength]) ||
				!bytes.Equal(k[common.HashLength:common.HashLength+common.HashLength], key[common.HashLength+common.IncarnationLength:]) {
				return nil, ethdb.ErrKeyNotFound
			}
		}
	} else {
		if !bytes.HasPrefix(k, key) {
			return nil, ethdb.ErrKeyNotFound
		}
	}
	index := dbutils.WrapHistoryIndex(v)

	changeSetBlock, set, ok := index.Search(timestamp)
	var data []byte
	if ok {
		// set == true if this change was from empty record (non-existent account) to non-empty
		// In such case, we do not need to examine changeSet and return empty data
		if set {
			return []byte{}, nil
		}
		csBucket := dbutils.ChangeSetByIndexBucket(plain, storage)
		csB := tx.Bucket(csBucket)
		if csB == nil {
			return nil, fmt.Errorf("no changeset bucket %s", csB)
		}

		csKey := dbutils.EncodeTimestamp(changeSetBlock)
		changeSetData, _ := csB.Get(csKey)

		if plain {
			if storage {
				data, err = changeset.StorageChangeSetPlainBytes(changeSetData).FindWithoutIncarnation(key[:common.AddressLength], key[common.AddressLength+common.IncarnationLength:])
			} else {
				data, err = changeset.AccountChangeSetPlainBytes(changeSetData).FindLast(key)
			}
		} else if storage {
			data, err = changeset.StorageChangeSetBytes(changeSetData).FindWithoutIncarnation(key[:common.HashLength], key[common.HashLength+common.IncarnationLength:])
		} else {
			data, err = changeset.AccountChangeSetBytes(changeSetData).FindLast(key)
		}
		if err != nil {
			if !errors.Is(err, ethdb.ErrKeyNotFound) {
				return nil, fmt.Errorf("finding %x in the changeset %d: %w", key, changeSetBlock, err)
			}
			return nil, err
		}
	} else if plain {
		var lastChangesetBlock, lastIndexBlock uint64
		stageBucket := tx.Bucket(dbutils.SyncStageProgress)
		if stageBucket != nil {
			v, err := stageBucket.Get([]byte{byte(stages.Execution)})
			if err != nil && !errors.Is(err, ethdb.ErrKeyNotFound) {
				return nil, err
			}
			if len(v) > 0 {
				lastChangesetBlock = binary.BigEndian.Uint64(v[:8])
			}
			if storage {
				v, err = stageBucket.Get([]byte{byte(stages.AccountHistoryIndex)})
			} else {
				v, err = stageBucket.Get([]byte{byte(stages.StorageHistoryIndex)})
			}
			if err != nil && !errors.Is(err, ethdb.ErrKeyNotFound) {
				return nil, err
			}
			if len(v) > 0 {
				lastIndexBlock = binary.BigEndian.Uint64(v[:8])
			}
		}
		if lastChangesetBlock > lastIndexBlock {
			// iterate over changeset to compensate for lacking of the history index
			csBucket := dbutils.ChangeSetByIndexBucket(plain, storage)
			csB := tx.Bucket(csBucket)
			c := csB.Cursor()
			var startTimestamp uint64
			if timestamp < lastIndexBlock {
				startTimestamp = lastIndexBlock + 1
			} else {
				startTimestamp = timestamp + 1
			}
			startKey := dbutils.EncodeTimestamp(startTimestamp)
			err = nil
			for k, v, err1 := c.Seek(startKey); k != nil && err1 == nil; k, v, err1 = c.Next() {
				if storage {
					data, err = changeset.StorageChangeSetPlainBytes(v).FindWithoutIncarnation(key[:common.AddressLength], key[common.AddressLength+common.IncarnationLength:])
				} else {
					data, err = changeset.AccountChangeSetPlainBytes(v).FindLast(key)
				}
				if err == nil {
					break
				}
				if !errors.Is(err, changeset.ErrNotFound) {
					return nil, fmt.Errorf("finding %x in the changeset %d: %w", key, changeSetBlock, err)
				}
			}
			if err != nil {
				return nil, ethdb.ErrKeyNotFound
			}
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
			if plain {
				codeBucket := tx.Bucket(dbutils.PlainContractCodeBucket)
				codeHash, _ = codeBucket.Get(dbutils.PlainGenerateStoragePrefix(key, acc.Incarnation))
			} else {
				codeBucket := tx.Bucket(dbutils.ContractCodeBucket)
				codeHash, _ = codeBucket.Get(dbutils.GenerateStoragePrefix(key, acc.Incarnation))
			}
			if len(codeHash) > 0 {
				acc.CodeHash = common.BytesToHash(codeHash)
			}
			data = make([]byte, acc.EncodingLengthForStorage())
			acc.EncodeForStorage(data)
		}
		return data, nil
	}

	return data, nil
}
