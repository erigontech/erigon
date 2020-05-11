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
	"io/ioutil"
	"os"
	"runtime"
	"time"

	"github.com/dgraph-io/badger/v2"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/log"
)

// https://github.com/dgraph-io/badger#frequently-asked-questions
// https://groups.google.com/forum/#!topic/golang-nuts/jPb_h3TvlKE/discussion
const minGoMaxProcs = 128

// https://github.com/dgraph-io/badger#garbage-collection
const gcPeriod = 35 * time.Minute

// BadgerDatabase is a wrapper over BadgerDb,
// compatible with the Database interface.
type BadgerDatabase struct {
	db       *badger.DB   // BadgerDB instance
	log      log.Logger   // Contextual logger tracking the database path
	tmpDir   string       // Temporary data directory
	gcTicker *time.Ticker // Garbage Collector
	id       uint64
}

// NewBadgerDatabase returns a BadgerDB wrapper.
func NewBadgerDatabase(dir string) (*BadgerDatabase, error) {
	logger := log.New("database", dir)

	oldMaxProcs := runtime.GOMAXPROCS(0)
	if oldMaxProcs < minGoMaxProcs {
		runtime.GOMAXPROCS(minGoMaxProcs)
		logger.Info("Bumping GOMAXPROCS", "old", oldMaxProcs, "new", minGoMaxProcs)
	}
	options := badger.DefaultOptions(dir).WithMaxTableSize(512 << 20)

	db, err := badger.Open(options)
	if err != nil {
		return nil, err
	}

	ticker := time.NewTicker(gcPeriod)
	// Start GC in backround
	go func() {
		for range ticker.C {
			err := db.RunValueLogGC(0.5)
			if err != badger.ErrNoRewrite {
				logger.Info("Badger GC run", "err", err)
			}
		}
	}()

	return &BadgerDatabase{
		db:       db,
		log:      logger,
		gcTicker: ticker,
		id:       id(),
	}, nil
}

// NewEphemeralBadger returns a new BadgerDB in a temporary directory.
func NewEphemeralBadger() (*BadgerDatabase, error) {
	dir, err := ioutil.TempDir(os.TempDir(), "badger_db_")
	if err != nil {
		return nil, err
	}

	logger := log.New("database", dir)

	db, err := badger.Open(badger.DefaultOptions(dir))
	if err != nil {
		return nil, err
	}

	return &BadgerDatabase{
		db:     db,
		log:    logger,
		tmpDir: dir,
	}, nil
}

// Close closes the database.
func (db *BadgerDatabase) Close() {
	if db.gcTicker != nil {
		db.gcTicker.Stop()
	}

	if err := db.db.Close(); err == nil {
		db.log.Info("Database closed")
		if len(db.tmpDir) > 0 {
			os.RemoveAll(db.tmpDir)
		}
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

func (db *BadgerDatabase) GetIndexChunk(bucket, key []byte, timestamp uint64) ([]byte, error) {
	var val []byte
	var innerErr error

	err := db.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		it.Seek(bucketKey(bucket, dbutils.IndexChunkKey(key, timestamp)))
		if it.ValidForPrefix(bucketKey(bucket, key)) {
			val, innerErr = it.Item().ValueCopy(nil)
			if innerErr != nil {
				return innerErr
			}
			return nil
		}
		return badger.ErrKeyNotFound
	})
	if err == badger.ErrKeyNotFound {
		return nil, ErrKeyNotFound
	}
	return val, err
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
func (db *BadgerDatabase) Walk(bucket, startkey []byte, fixedbits int, walker func(k, v []byte) (bool, error)) error {
	fixedbytes, mask := Bytesmask(fixedbits)
	prefix := bucketKey(bucket, startkey)
	err := db.db.View(func(tx *badger.Txn) error {
		it := tx.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(prefix); it.Valid(); it.Next() {
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

// MultiWalk is similar to multiple Walk calls folded into one.
func (db *BadgerDatabase) MultiWalk(bucket []byte, startkeys [][]byte, fixedbits []int, walker func(int, []byte, []byte) error) error {
	if len(startkeys) == 0 {
		return nil
	}

	rangeIdx := 0 // What is the current range we are extracting
	fixedbytes, mask := Bytesmask(fixedbits[rangeIdx])
	startkey := startkeys[rangeIdx]

	err := db.db.View(func(tx *badger.Txn) error {
		it := tx.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(bucketKey(bucket, startkey)); it.Valid(); it.Next() {
			item := it.Item()
			k := keyWithoutBucket(item.Key(), bucket)
			if k == nil {
				return nil
			}

			// Adjust rangeIdx if needed
			if fixedbytes > 0 {
				cmp := int(-1)
				for cmp != 0 {
					cmp = bytes.Compare(k[:fixedbytes-1], startkey[:fixedbytes-1])
					if cmp == 0 {
						k1 := k[fixedbytes-1] & mask
						k2 := startkey[fixedbytes-1] & mask
						if k1 < k2 {
							cmp = -1
						} else if k1 > k2 {
							cmp = 1
						}
					}
					if cmp < 0 {
						it.Seek(bucketKey(bucket, startkey))
						if !it.Valid() {
							return nil
						}
						item = it.Item()
						k = keyWithoutBucket(item.Key(), bucket)
						if k == nil {
							return nil
						}
					} else if cmp > 0 {
						rangeIdx++
						if rangeIdx == len(startkeys) {
							return nil
						}
						fixedbytes, mask = Bytesmask(fixedbits[rangeIdx])
						startkey = startkeys[rangeIdx]
					}
				}
			}

			err := item.Value(func(v []byte) error {
				if len(v) == 0 {
					return nil
				}
				return walker(rangeIdx, k, v)
			})
			if err != nil {
				return err
			}
		}
		return nil
	})

	return err
}

// MultiPut inserts or updates multiple entries.
// Entries are passed as an array:
// bucket0, key0, val0, bucket1, key1, val1, ...
func (db *BadgerDatabase) MultiPut(triplets ...[]byte) (uint64, error) {
	l := len(triplets)
	err := db.db.Update(func(tx *badger.Txn) error {
		for i := 0; i < l; i += 3 {
			bucket := triplets[i]
			key := triplets[i+1]
			val := triplets[i+2]
			if err := tx.Set(bucketKey(bucket, key), val); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return uint64(l / 3), err
}

func (db *BadgerDatabase) RewindData(timestampSrc, timestampDst uint64) (map[string][]byte, map[string][]byte, error) {
	return RewindData(db, timestampSrc, timestampDst)
}

func (db *BadgerDatabase) NewBatch() DbWithPendingMutations {
	m := &mutation{
		db:   db,
		puts: newPuts(),
	}
	return m
}

// IdealBatchSize defines the size of the data batches should ideally add in one write.
func (db *BadgerDatabase) IdealBatchSize() int {
	return 100 * 1024
}

// DiskSize returns the total disk size of the database in bytes.
func (db *BadgerDatabase) DiskSize() int64 {
	lsm, vlog := db.db.Size()
	return lsm + vlog
}

// MemCopy creates a copy of the database in a temporary directory.
// We don't do it in memory because BadgerDB doesn't support that.
func (db *BadgerDatabase) MemCopy() Database {
	newDb, err := NewEphemeralBadger()
	if err != nil {
		panic("failed to create tmp database: " + err.Error())
	}

	err = db.db.View(func(readTx *badger.Txn) error {
		return newDb.db.Update(func(writeTx *badger.Txn) error {
			it := readTx.NewIterator(badger.DefaultIteratorOptions)
			defer it.Close()

			for it.Rewind(); it.Valid(); it.Next() {
				item := it.Item()
				k := item.Key()
				err2 := item.Value(func(v []byte) error {
					return writeTx.Set(k, v)
				})
				if err2 != nil {
					return err2
				}
			}
			return nil
		})
	})

	if err != nil {
		panic(err)
	}

	return newDb
}

// TODO [Issue 144] Implement the methods

func (db *BadgerDatabase) WalkAsOf(bucket, hBucket, startkey []byte, fixedbits int, timestamp uint64, walker func([]byte, []byte) (bool, error)) error {
	panic("Not implemented")
}

func (db *BadgerDatabase) Keys() ([][]byte, error) {
	panic("Not implemented")
}

func (db *BadgerDatabase) Ancients() (uint64, error) {
	return 0, errNotSupported
}

func (db *BadgerDatabase) TruncateAncients(items uint64) error {
	return errNotSupported
}

func (db *BadgerDatabase) ID() uint64 {
	return db.id
}
