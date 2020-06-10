package ethdb

import (
	"bytes"
	"context"
	"fmt"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/changeset"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
)

func GetAsOf(db KV, plain, storage bool, key []byte, timestamp uint64) ([]byte, error) {
	var dat []byte
	err := db.View(context.Background(), func(tx Tx) error {
		v, err := FindByHistory(tx, plain, storage, key, timestamp)
		if err == nil {
			dat = make([]byte, len(v))
			copy(dat, v)
			return nil
		}
		if err != ErrKeyNotFound {
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
				return ErrKeyNotFound
			}

			dat = make([]byte, len(v))
			copy(dat, v)
			return nil
		}
	})
	return dat, err
}

func FindByHistory(tx Tx, plain, storage bool, key []byte, timestamp uint64) ([]byte, error) {
	trace := bytes.Equal(key, common.FromHex("6090a6e47849629b7245dfa1ca21d94cd15878effffffffffffffffe31ef43972b7fdfacaf8d972253be3e91ed4682328ccc08a6728b4e8db755a931"))
	if trace {
		fmt.Printf("FindByHistory plain=%t storage=%t %x %d\n", plain, storage, key, timestamp)
	}
	//check
	var hBucket []byte
	if storage {
		hBucket = dbutils.StorageHistoryBucket
	} else {
		hBucket = dbutils.AccountsHistoryBucket
	}
	hB := tx.Bucket(hBucket)
	if hB == nil {
		return nil, ErrKeyNotFound
	}
	var keyF []byte
	if storage {
		keyF = make([]byte, len(key)-common.IncarnationLength)
		if plain {
			copy(keyF, key[:common.AddressLength])
			copy(keyF[common.AddressLength:], key[common.AddressLength+common.IncarnationLength:])
		} else {
			copy(keyF, key[:common.HashLength])
			copy(keyF[common.HashLength:], key[common.HashLength+common.IncarnationLength:])
		}
	} else {
		keyF = common.CopyBytes(key)
	}
	if trace {
		fmt.Printf("keyF %x\n", keyF)
	}

	c := hB.Cursor()
	k, v, err := c.Seek(dbutils.IndexChunkKey(key, timestamp))
	if err != nil {
		return nil, err
	}
	if trace {
		fmt.Printf("k %x\n", k)
	}
	if !bytes.HasPrefix(k, keyF) {
		return nil, ErrKeyNotFound
	}
	index := dbutils.WrapHistoryIndex(v)

	changeSetBlock, set, ok := index.Search(timestamp)
	if trace {
		fmt.Printf("%x: %d, %t, %t\n", v, changeSetBlock, set, ok)
		idx, idx1, _ := index.Decode()
		for i, id := range idx {
			fmt.Printf("%d, %t\n", id, idx1[i])
		}
	}
	if !ok {
		return nil, ErrKeyNotFound
	}
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

	var data []byte
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
		if err != ErrKeyNotFound {
			return nil, fmt.Errorf("finding %x in the changeset %d: %w", key, changeSetBlock, err)
		}
		return nil, err
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
