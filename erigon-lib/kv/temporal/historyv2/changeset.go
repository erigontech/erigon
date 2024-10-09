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
	"encoding/binary"
	"fmt"
	math2 "math"
	"reflect"

	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/kv"
)

func NewChangeSet() *ChangeSet {
	return &ChangeSet{
		Changes: make([]Change, 0),
	}
}

type Change struct {
	Key   []byte
	Value []byte
}

// ChangeSet is a map with keys of the same size.
// Both keys and values are byte strings.
type ChangeSet struct {
	// Invariant: all keys are of the same size.
	Changes []Change
	keyLen  int
}

// BEGIN sort.Interface

func (s *ChangeSet) Len() int {
	return len(s.Changes)
}

func (s *ChangeSet) Swap(i, j int) {
	s.Changes[i], s.Changes[j] = s.Changes[j], s.Changes[i]
}

func (s *ChangeSet) Less(i, j int) bool {
	cmp := bytes.Compare(s.Changes[i].Key, s.Changes[j].Key)
	if cmp == 0 {
		cmp = bytes.Compare(s.Changes[i].Value, s.Changes[j].Value)
	}
	return cmp < 0
}

// END sort.Interface
func (s *ChangeSet) KeySize() int {
	if s.keyLen != 0 {
		return s.keyLen
	}
	for _, c := range s.Changes {
		return len(c.Key)
	}
	return 0
}

func (s *ChangeSet) checkKeySize(key []byte) error {
	if (s.Len() == 0 && s.KeySize() == 0) || (len(key) == s.KeySize() && len(key) > 0) {
		return nil
	}

	return fmt.Errorf("wrong key size in AccountChangeSet: expected %d, actual %d", s.KeySize(), len(key))
}

// Add adds a new entry to the AccountChangeSet.
// One must not add an existing key
// and may add keys only of the same size.
func (s *ChangeSet) Add(key []byte, value []byte) error {
	if err := s.checkKeySize(key); err != nil {
		return err
	}

	s.Changes = append(s.Changes, Change{
		Key:   key,
		Value: value,
	})
	return nil
}

func (s *ChangeSet) ChangedKeys() map[string]struct{} {
	m := make(map[string]struct{}, len(s.Changes))
	for i := range s.Changes {
		m[string(s.Changes[i].Key)] = struct{}{}
	}
	return m
}

func (s *ChangeSet) Equals(s2 *ChangeSet) bool {
	return reflect.DeepEqual(s.Changes, s2.Changes)
}

// Encoded Method
func FromDBFormat(dbKey, dbValue []byte) (uint64, []byte, []byte, error) {
	if len(dbKey) == 8 {
		return DecodeAccounts(dbKey, dbValue)
	} else {
		return DecodeStorage(dbKey, dbValue)
	}
}

func AvailableFrom(tx kv.Tx) (uint64, error) {
	c, err := tx.Cursor(kv.AccountChangeSet)
	if err != nil {
		return math2.MaxUint64, err
	}
	defer c.Close()
	k, _, err := c.First()
	if err != nil {
		return math2.MaxUint64, err
	}
	if len(k) == 0 {
		return math2.MaxUint64, nil
	}
	return binary.BigEndian.Uint64(k), nil
}
func AvailableStorageFrom(tx kv.Tx) (uint64, error) {
	c, err := tx.Cursor(kv.StorageChangeSet)
	if err != nil {
		return math2.MaxUint64, err
	}
	defer c.Close()
	k, _, err := c.First()
	if err != nil {
		return math2.MaxUint64, err
	}
	if len(k) == 0 {
		return math2.MaxUint64, nil
	}
	return binary.BigEndian.Uint64(k), nil
}

func ForEach(db kv.Tx, bucket string, startkey []byte, walker func(blockN uint64, k, v []byte) error) error {
	var blockN uint64
	return db.ForEach(bucket, startkey, func(k, v []byte) error {
		var err error
		blockN, k, v, err = FromDBFormat(k, v)
		if err != nil {
			return err
		}
		return walker(blockN, k, v)
	})
}
func ForPrefix(db kv.Tx, bucket string, startkey []byte, walker func(blockN uint64, k, v []byte) error) error {
	var blockN uint64
	return db.ForPrefix(bucket, startkey, func(k, v []byte) error {
		var err error
		blockN, k, v, err = FromDBFormat(k, v)
		if err != nil {
			return err
		}
		return walker(blockN, k, v)
	})
}

func Truncate(tx kv.RwTx, from uint64) error {
	keyStart := hexutility.EncodeTs(from)

	{
		c, err := tx.RwCursorDupSort(kv.AccountChangeSet)
		if err != nil {
			return err
		}
		defer c.Close()
		for k, _, err := c.Seek(keyStart); k != nil; k, _, err = c.NextNoDup() {
			if err != nil {
				return err
			}
			if err = tx.Delete(kv.AccountChangeSet, k); err != nil {
				return err
			}
			if err != nil {
				return err
			}
		}
	}
	{
		c, err := tx.RwCursorDupSort(kv.StorageChangeSet)
		if err != nil {
			return err
		}
		defer c.Close()
		for k, _, err := c.Seek(keyStart); k != nil; k, _, err = c.NextNoDup() {
			if err != nil {
				return err
			}
			if err = tx.Delete(kv.StorageChangeSet, k); err != nil {
				return err
			}
		}
	}
	return nil
}

type CSMapper struct {
	IndexBucket   string
	IndexChunkKey func([]byte, uint64) []byte
	Find          func(cursor kv.CursorDupSort, blockNumber uint64, key []byte) ([]byte, error)
	New           func() *ChangeSet
	Encode        Encoder
	Decode        Decoder
}

var Mapper = map[string]CSMapper{
	kv.AccountChangeSet: {
		IndexBucket:   kv.E2AccountsHistory,
		IndexChunkKey: AccountIndexChunkKey,
		New:           NewAccountChangeSet,
		Find:          FindAccount,
		Encode:        EncodeAccounts,
		Decode:        DecodeAccounts,
	},
	kv.StorageChangeSet: {
		IndexBucket:   kv.E2StorageHistory,
		IndexChunkKey: StorageIndexChunkKey,
		Find:          FindStorage,
		New:           NewStorageChangeSet,
		Encode:        EncodeStorage,
		Decode:        DecodeStorage,
	},
}

func AccountIndexChunkKey(key []byte, blockNumber uint64) []byte {
	blockNumBytes := make([]byte, length.Addr+8)
	copy(blockNumBytes, key)
	binary.BigEndian.PutUint64(blockNumBytes[length.Addr:], blockNumber)

	return blockNumBytes
}

func StorageIndexChunkKey(key []byte, blockNumber uint64) []byte {
	//remove incarnation and add block number
	blockNumBytes := make([]byte, length.Addr+length.Hash+8)
	copy(blockNumBytes, key[:length.Addr])
	copy(blockNumBytes[length.Addr:], key[length.Addr+length.Incarnation:])
	binary.BigEndian.PutUint64(blockNumBytes[length.Addr+length.Hash:], blockNumber)

	return blockNumBytes
}
