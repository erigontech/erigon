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

// Get returns a single value.
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
	return val, err
}

// PutS adds a new entry to the historical buckets:
// hBucket (unless changeSetBucketOnly) and ChangeSet.
func (db *BadgerDatabase) PutS(hBucket, key, value []byte, timestamp uint64, changeSetBucketOnly bool) error {
	composite, encodedStamp := dbutils.CompositeKeySuffix(key, timestamp)
	hKey := bucketKey(hBucket, composite)

	suffixkey := make([]byte, len(encodedStamp)+len(hBucket))
	copy(suffixkey, encodedStamp)
	copy(suffixkey[len(encodedStamp):], hBucket)

	changeSetKey := bucketKey(dbutils.ChangeSetBucket, suffixkey)

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
	encodedStamp := dbutils.EncodeTimestamp(timestamp)
	prefix := bucketKey(dbutils.ChangeSetBucket, encodedStamp)
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

			bucket := k[len(encodedStamp):]
			err = changedAccounts.Walk(func(kk, _ []byte) error {
				kk = append(kk, encodedStamp...)
				return tx.Delete(bucketKey(bucket, kk))
			})
			if err != nil {
				return err
			}

			keys = append(keys, k)
		}
		for _, k := range keys {
			if err := tx.Delete(bucketKey(dbutils.ChangeSetBucket, k)); err != nil {
				return err
			}
		}
		return nil
	})
}

// GetS returns a single value that was put into a given historical bucket for an exact timestamp.
func (db *BadgerDatabase) GetS(hBucket, key []byte, timestamp uint64) ([]byte, error) {
	composite, _ := dbutils.CompositeKeySuffix(key, timestamp)
	return db.Get(hBucket, composite)
}

// TODO [Andrew] implement the full Database interface
