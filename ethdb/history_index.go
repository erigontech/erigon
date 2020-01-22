package ethdb

import (
	"bytes"
	"github.com/ledgerwatch/turbo-geth/common/changeset"
	"github.com/ledgerwatch/turbo-geth/common/debug"
	"sort"

	"github.com/ledgerwatch/bolt"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/rlp"
)

//
type HistoryIndex []uint64

func (hi *HistoryIndex) Encode() ([]byte, error) {
	return rlp.EncodeToBytes(hi)
}

func (hi *HistoryIndex) Decode(s []byte) error {
	if len(s) == 0 {
		return nil
	}
	return rlp.DecodeBytes(s, &hi)
}

func (hi *HistoryIndex) Append(v uint64) *HistoryIndex {
	*hi = append(*hi, v)
	if !sort.SliceIsSorted(*hi, func(i, j int) bool {
		return (*hi)[i] <= (*hi)[j]
	}) {
		sort.Slice(*hi, func(i, j int) bool {
			return (*hi)[i] <= (*hi)[j]
		})
	}

	return hi
}

//most common operation is remove one from the tail
func (hi *HistoryIndex) Remove(v uint64) *HistoryIndex {
	for i := len(*hi) - 1; i >= 0; i-- {
		if (*hi)[i] == v {
			*hi = append((*hi)[:i], (*hi)[i+1:]...)
		}
	}
	return hi
}

func (hi *HistoryIndex) Search(v uint64) (uint64, bool) {
	ln := len(*hi)
	if ln == 0 {
		return 0, false
	}

	if (*hi)[ln-1] < v {
		return 0, false
	}
	for i := ln - 1; i >= 0; i-- {
		if v == (*hi)[i] {
			return v, true
		}

		if (*hi)[i] < v {
			return (*hi)[i+1], true
		}
	}
	return (*hi)[0], true
}

func AppendToIndex(b []byte, timestamp uint64) ([]byte, error) {
	v := new(HistoryIndex)

	if err := v.Decode(b); err != nil {
		return nil, err
	}

	v.Append(timestamp)
	return v.Encode()
}
func RemoveFromIndex(b []byte, timestamp uint64) ([]byte, bool, error) {
	v := new(HistoryIndex)

	if err := v.Decode(b); err != nil {
		return nil, false, err
	}

	v.Remove(timestamp)
	res, err := v.Encode()
	if len(*v) == 0 {
		return res, true, err
	}
	return res, false, err
}

func BoltDBFindByHistory(tx *bolt.Tx, hBucket []byte, key []byte, timestamp uint64) ([]byte, error) {
	//check
	hB := tx.Bucket(hBucket)
	if hB == nil {
		return nil, ErrKeyNotFound
	}
	v, _ := hB.Get(key)
	index := new(HistoryIndex)

	err := index.Decode(v)
	if err != nil {
		return nil, err
	}

	changeSetBlock, ok := index.Search(timestamp)
	if !ok {
		return nil, ErrKeyNotFound
	}

	csB := tx.Bucket(dbutils.ChangeSetByIndexBucket(hBucket))
	if csB == nil {
		return nil, ErrKeyNotFound
	}
	changeSetData, _ := csB.Get(dbutils.CompositeChangeSetKey(dbutils.EncodeTimestamp(changeSetBlock), hBucket))

	var data []byte
	switch {
	case debug.IsThinHistory() && bytes.Equal(dbutils.AccountsHistoryBucket, hBucket):
		data, err = changeset.AccountChangeSetBytes(changeSetData).FindLast(key)
	case debug.IsThinHistory() && bytes.Equal(dbutils.StorageChangeSetBucket, hBucket):
		data, err = changeset.StorageChangeSetBytes(changeSetData).FindLast(key)
	default:
		data, err = changeset.FindLast(changeSetData, key)
	}

	if err != nil {
		return nil, ErrKeyNotFound
	}
	var acc accounts.Account
	if err := acc.DecodeForStorage(data); err != nil {
		return nil, err
	}

	if acc.Incarnation > 0 && acc.IsEmptyCodeHash() {
		codeBucket := tx.Bucket(dbutils.ContractCodeBucket)
		codeHash, _ := codeBucket.Get(dbutils.GenerateStoragePrefix(common.BytesToHash(key), acc.Incarnation))
		if len(codeHash) > 0 {
			acc.CodeHash = common.BytesToHash(codeHash)
		}
		data = make([]byte, acc.EncodingLengthForStorage())
		acc.EncodeForStorage(data)
	}
	return data, nil

}

func BoltDBFindStorageByHistory(tx *bolt.Tx, hBucket []byte, key []byte, timestamp uint64) ([]byte, error) {
	var k common.Hash
	copy(k[:], key[common.HashLength+common.IncarnationLength:])

	//check
	hB := tx.Bucket(hBucket)
	if hB == nil {
		return nil, ErrKeyNotFound
	}
	v, _ := hB.Get(key)
	index := NewStorageIndex()

	err := index.Decode(v)
	if err != nil {
		return nil, err
	}

	changeSetBlock, ok := index.Search(k, timestamp)
	if !ok {
		return nil, ErrKeyNotFound
	}

	csB := tx.Bucket(dbutils.ChangeSetByIndexBucket(hBucket))
	if csB == nil {
		return nil, ErrKeyNotFound
	}
	cs, _ := csB.Get(dbutils.CompositeChangeSetKey(dbutils.EncodeTimestamp(changeSetBlock), hBucket))
	if err != nil {
		return nil, err
	}

	var data []byte
	data, err = changeset.FindLast(cs, key)
	if err != nil {
		return nil, ErrKeyNotFound
	}
	var acc accounts.Account
	if err := acc.DecodeForStorage(data); err != nil {
		return nil, err
	}

	if acc.Incarnation > 0 && acc.IsEmptyCodeHash() {
		codeBucket := tx.Bucket(dbutils.ContractCodeBucket)
		codeHash, _ := codeBucket.Get(dbutils.GenerateStoragePrefix(common.BytesToHash(key), acc.Incarnation))
		if len(codeHash) > 0 {
			acc.CodeHash = common.BytesToHash(codeHash)
		}
		data = make([]byte, acc.EncodingLengthForStorage())
		acc.EncodeForStorage(data)
	}
	return data, nil

}
