// Copyright 2019 The turbo-geth authors
// This file is part of the turbo-geth library.
//
// The turbo-geth library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The turbo-geth library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the turbo-geth library. If not, see <http://www.gnu.org/licenses/>.

package ethdb

import (
	"bytes"

	"github.com/dgraph-io/badger"

	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/log"
)

// BadgerDatabase is a wrapper over BadgerDb,
// compatible with the Database interface.
type BadgerDatabase struct {
	db *badger.DB // BadgerDB instance

	log log.Logger // Contextual logger tracking the database path
}

// NewBadgerDatabase returns a BadgerDB wrapper.
func NewBadgerDatabase(dir string) (*BadgerDatabase, error) {
	logger := log.New("database", dir)

	db, err := badger.Open(badger.DefaultOptions(dir))
	if err != nil {
		return nil, err
	}

	return &BadgerDatabase{
		db:  db,
		log: logger,
	}, nil
}

// Close closes the database.
func (db *BadgerDatabase) Close() {
	if err := db.db.Close(); err == nil {
		db.log.Info("Database closed")
	} else {
		db.log.Error("Failed to close database", "err", err)
	}
}

const bucketSeparator = byte(0xA6) // broken bar '¦'

func bucketKey(bucket, key []byte) []byte {
	var composite []byte
	composite = append(composite, bucket...)
	composite = append(composite, bucketSeparator)
	composite = append(composite, key...)
	return composite
}

func keyWithoutBucket(key, bucket []byte) []byte {
	if len(key) <= len(bucket) || !bytes.HasPrefix(key, bucket) || key[len(bucket)] != bucketSeparator {
		return nil
	}
	return key[len(bucket)+1:]
}

// Delete removes a single entry.
func (db *BadgerDatabase) Delete(bucket, key []byte) error {
	return db.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(bucketKey(bucket, key))
	})
}

// Put inserts or updates a single entry.
func (db *BadgerDatabase) Put(bucket, key []byte, value []byte) error {
	return db.db.Update(func(txn *badger.Txn) error {
		return txn.Set(bucketKey(bucket, key), value)
	})
}

// Get returns the value for a given key if it's present.
func (db *BadgerDatabase) Get(bucket, key []byte) ([]byte, error) {
	var val []byte
	err := db.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(bucketKey(bucket, key))
		if err != nil {
			return err
		}
		val, err = item.ValueCopy(nil)
		return err
	})
	if err == badger.ErrKeyNotFound {
		return nil, ErrKeyNotFound
	}
	return val, err
}

// PutS adds a new entry to the historical buckets:
// hBucket (unless changeSetBucketOnly) and ChangeSet.
func (db *BadgerDatabase) PutS(hBucket, key, value []byte, timestamp uint64, changeSetBucketOnly bool) error {
	composite, encodedTS := dbutils.CompositeKeySuffix(key, timestamp)
	hKey := bucketKey(hBucket, composite)
	changeSetKey := bucketKey(dbutils.ChangeSetBucket, dbutils.CompositeChangeSetKey(encodedTS, hBucket))

	return db.db.Update(func(tx *badger.Txn) error {
		if !changeSetBucketOnly {
			if err := tx.Set(hKey, value); err != nil {
				return err
			}
		}

		changeSetItem, err := tx.Get(changeSetKey)
		if err != nil && err != badger.ErrKeyNotFound {
			return err
		}

		var sh dbutils.ChangeSet
		if err == nil {
			err = changeSetItem.Value(func(val []byte) error {
				var err2 error
				sh, err2 = dbutils.Decode(val)
				if err2 != nil {
					log.Error("PutS Decode suffix err", "err", err2)
					return err2
				}
				return nil
			})
			if err != nil {
				return err
			}
		}

		sh = sh.Add(key, value)
		dat, err := dbutils.Encode(sh)
		if err != nil {
			log.Error("PutS Decode suffix err", "err", err)
			return err
		}

		return tx.Set(changeSetKey, dat)
	})
}

// DeleteTimestamp removes data for a given timestamp from all historical buckets (incl. ChangeSet).
func (db *BadgerDatabase) DeleteTimestamp(timestamp uint64) error {
	encodedTS := dbutils.EncodeTimestamp(timestamp)
	prefix := bucketKey(dbutils.ChangeSetBucket, encodedTS)
	return db.db.Update(func(tx *badger.Txn) error {
		var keys [][]byte
		it := tx.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			k := item.Key()

			var changedAccounts dbutils.ChangeSet
			err := item.Value(func(v []byte) error {
				var err2 error
				changedAccounts, err2 = dbutils.Decode(v)
				return err2
			})
			if err != nil {
				return err
			}

			bucket := k[len(prefix):]
			err = changedAccounts.Walk(func(kk, _ []byte) error {
				kk = append(kk, encodedTS...)
				return tx.Delete(bucketKey(bucket, kk))
			})
			if err != nil {
				return err
			}

			keys = append(keys, k)
		}
		for _, k := range keys {
			if err := tx.Delete(k); err != nil {
				return err
			}
		}
		return nil
	})
}

// GetS returns the value that was recorded in a given historical bucket for an exact timestamp.
func (db *BadgerDatabase) GetS(hBucket, key []byte, timestamp uint64) ([]byte, error) {
	composite, _ := dbutils.CompositeKeySuffix(key, timestamp)
	return db.Get(hBucket, composite)
}

// GetAsOf returns the value valid as of a given timestamp.
func (db *BadgerDatabase) GetAsOf(bucket, hBucket, key []byte, timestamp uint64) ([]byte, error) {
	composite, _ := dbutils.CompositeKeySuffix(key, timestamp)
	var dat []byte
	err := db.db.View(func(tx *badger.Txn) error {
		{ // first look in the historical bucket
			it := tx.NewIterator(badger.DefaultIteratorOptions)
			defer it.Close()
			it.Seek(bucketKey(hBucket, composite))

			if it.ValidForPrefix(bucketKey(hBucket, key)) {
				var err2 error
				dat, err2 = it.Item().ValueCopy(nil)
				return err2
			}
		}

		{ // fall back to the current bucket
			item, err2 := tx.Get(bucketKey(bucket, key))
			if err2 != nil {
				return err2
			}
			dat, err2 = item.ValueCopy(nil)
			return err2
		}
	})

	if err == badger.ErrKeyNotFound {
		return dat, ErrKeyNotFound
	}
	return dat, err
}

// Has indicates whether a key exists in the database.
func (db *BadgerDatabase) Has(bucket, key []byte) (bool, error) {
	_, err := db.Get(bucket, key)
	if err == ErrKeyNotFound {
		return false, nil
	}
	return err == nil, err
}

// Walk iterates over entries with keys greater or equals to startkey.
// Only the keys whose first fixedbits match those of startkey are iterated over.
// walker is called for each eligible entry.
// If walker returns false or an error, the walk stops.
func (db *BadgerDatabase) Walk(bucket, startkey []byte, fixedbits uint, walker func(k, v []byte) (bool, error)) error {
	fixedbytes, mask := bytesmask(fixedbits)
	prefix := bucketKey(bucket, startkey)
	err := db.db.View(func(tx *badger.Txn) error {
		it := tx.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(prefix); ; it.Next() {
			item := it.Item()
			k := keyWithoutBucket(item.Key(), bucket)
			if k == nil {
				break
			}

			goOn := fixedbits == 0 || bytes.Equal(k[:fixedbytes-1], startkey[:fixedbytes-1]) && (k[fixedbytes-1]&mask) == (startkey[fixedbytes-1]&mask)
			if !goOn {
				break
			}

			err := item.Value(func(v []byte) error {
				var err2 error
				goOn, err2 = walker(k, v)
				return err2
			})
			if err != nil {
				return err
			}
			if !goOn {
				break
			}
		}
		return nil
	})
	return err
}

// TODO [Andrew] implement the full Database interface

func (db *BadgerDatabase) MultiWalk(bucket []byte, startkeys [][]byte, fixedbits []uint, walker func(int, []byte, []byte) (bool, error)) error {
	panic("Not implemented")
}

func (db *BadgerDatabase) WalkAsOf(bucket, hBucket, startkey []byte, fixedbits uint, timestamp uint64, walker func([]byte, []byte) (bool, error)) error {
	panic("Not implemented")
}

func (db *BadgerDatabase) MultiWalkAsOf(bucket, hBucket []byte, startkeys [][]byte, fixedbits []uint, timestamp uint64, walker func(int, []byte, []byte) (bool, error)) error {
	panic("Not implemented")
}

func (db *BadgerDatabase) MultiPut(tuples ...[]byte) (uint64, error) {
	panic("Not implemented")
}

func (db *BadgerDatabase) RewindData(timestampSrc, timestampDst uint64, df func(bucket, key, value []byte) error) error {
	panic("Not implemented")
}

func (db *BadgerDatabase) NewBatch() DbWithPendingMutations {
	panic("Not implemented")
}

func (db *BadgerDatabase) Size() int {
	panic("Not implemented")
}

func (db *BadgerDatabase) Keys() ([][]byte, error) {
	panic("Not implemented")
}

func (db *BadgerDatabase) MemCopy() Database {
	panic("Not implemented")
}

func (db *BadgerDatabase) Ancients() (uint64, error) {
	panic("Not implemented")
}

func (db *BadgerDatabase) TruncateAncients(items uint64) error {
	panic("Not implemented")
}
