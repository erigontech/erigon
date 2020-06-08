package ethdb

import (
	"bytes"
	"context"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/changeset"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/log"
)

func GetAsOf(db KV, bucket, hBucket, key []byte, timestamp uint64) ([]byte, error) {
	var dat []byte
	err := db.View(context.Background(), func(tx Tx) error {
		v, err := FindByHistory(tx, hBucket, key, timestamp)
		if err != nil {
			log.Debug("FindByHistory err", "err", err)
		} else {
			dat = make([]byte, len(v))
			copy(dat, v)
			return nil
		}
		{
			v, _ := tx.Bucket(bucket).Get(key)
			if v == nil {
				return ErrKeyNotFound
			}

			dat = make([]byte, len(v))
			copy(dat, v)
			return nil
		}
	})
	return dat, err
}

func FindByHistory(tx Tx, hBucket []byte, key []byte, timestamp uint64) ([]byte, error) {
	//check
	hB := tx.Bucket(hBucket)
	if hB == nil {
		return nil, ErrKeyNotFound
	}
	var keyF []byte
	if bytes.Equal(dbutils.StorageHistoryBucket, hBucket) {
		keyF = make([]byte, len(key)-common.IncarnationLength)
		copy(keyF, key[:common.AddressLength])
		copy(keyF[common.AddressLength:], key[common.AddressLength+common.IncarnationLength:])
	} else {
		keyF = common.CopyBytes(key)
	}

	c := hB.Cursor()
	k, v, err := c.Seek(dbutils.IndexChunkKey(key, timestamp))
	if err != nil {
		return nil, err
	}
	if !bytes.HasPrefix(k, keyF) {
		return nil, ErrKeyNotFound
	}
	index := dbutils.WrapHistoryIndex(v)

	changeSetBlock, set, ok := index.Search(timestamp)
	if !ok {
		return nil, ErrKeyNotFound
	}
	// set == true if this change was from empty record (non-existent account) to non-empty
	// In such case, we do not need to examine changeSet and return empty data
	if set {
		return []byte{}, nil
	}
	csB := tx.Bucket(dbutils.ChangeSetByIndexBucket(hBucket))
	if csB == nil {
		return nil, ErrKeyNotFound
	}

	csKey := dbutils.EncodeTimestamp(changeSetBlock)
	changeSetData, _ := csB.Get(csKey)

	var data []byte
	switch {
	case bytes.Equal(dbutils.AccountsHistoryBucket, hBucket):
		data, err = changeset.AccountChangeSetPlainBytes(changeSetData).FindLast(key)
	case bytes.Equal(dbutils.StorageHistoryBucket, hBucket):
		data, err = changeset.StorageChangeSetPlainBytes(changeSetData).FindWithoutIncarnation(key[:common.AddressLength], key[common.AddressLength+common.IncarnationLength:])
	}
	if err != nil {
		return nil, ErrKeyNotFound
	}

	//restore codehash
	if bytes.Equal(dbutils.AccountsHistoryBucket, hBucket) {
		var acc accounts.Account
		if err := acc.DecodeForStorage(data); err != nil {
			return nil, err
		}
		if acc.Incarnation > 0 && acc.IsEmptyCodeHash() {
			codeBucket := tx.Bucket(dbutils.PlainContractCodeBucket)
			codeHash, _ := codeBucket.Get(dbutils.PlainGenerateStoragePrefix(key, acc.Incarnation))
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
