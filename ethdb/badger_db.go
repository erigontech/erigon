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
	err := db.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(bucketKey(bucket, key))
	})
	return err
}

// Put inserts or updates a single entry.
func (db *BadgerDatabase) Put(bucket, key []byte, value []byte) error {
	err := db.db.Update(func(txn *badger.Txn) error {
		return txn.Set(bucketKey(bucket, key), value)
	})
	return err
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
	composite, suffix := dbutils.CompositeKeySuffix(key, timestamp)
	suffixkey := make([]byte, len(suffix)+len(hBucket))
	copy(suffixkey, suffix)
	copy(suffixkey[len(suffix):], hBucket)

	hKey := bucketKey(hBucket, composite)
	changeSetKey := bucketKey(dbutils.ChangeSetBucket, suffixkey)

	err := db.db.Update(func(tx *badger.Txn) error {
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
				sh, err = dbutils.Decode(val)
				if err != nil {
					log.Error("PutS Decode suffix err", "err", err)
					return err
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
	return err
}

// TODO [Andrew] implement the full Database interface
