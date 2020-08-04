// Copyright 2014 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

// Package ethdb defines the interfaces for an Ethereum data store.
package ethdb

import (
	"bytes"
	"context"
	"strings"
	"time"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/common/debug"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/metrics"
)

var (
	dbGetTimer = metrics.NewRegisteredTimer("db/get", nil)
	dbPutTimer = metrics.NewRegisteredTimer("db/put", nil)
)

// ObjectDatabase - is an object-style interface of DB accessing
type ObjectDatabase struct {
	kv  KV
	log log.Logger
	id  uint64
}

// NewObjectDatabase returns a AbstractDB wrapper.
func NewObjectDatabase(kv KV) *ObjectDatabase {
	logger := log.New("database", "object")
	return &ObjectDatabase{
		kv:  kv,
		log: logger,
		id:  id(),
	}
}

func MustOpen(path string) *ObjectDatabase {
	db, err := Open(path)
	if err != nil {
		panic(err)
	}
	return db
}

// Open - main method to open database. Choosing driver based on path suffix.
// If env TEST_DB provided - choose driver based on it. Some test using this method to open non-in-memory db
func Open(path string) (*ObjectDatabase, error) {
	var kv KV
	var err error
	testDB := debug.TestDB()
	switch true {
	case testDB == "lmdb" || strings.HasSuffix(path, "_lmdb"):
		kv, err = NewLMDB().Path(path).Open()
	case testDB == "badger" || strings.HasSuffix(path, "_badger"):
		kv, err = NewBadger().Path(path).Open()
	case testDB == "bolt" || strings.HasSuffix(path, "_bolt"):
		kv, err = NewBolt().Path(path).Open()
	default:
		kv, err = NewLMDB().Path(path).Open()
	}
	if err != nil {
		return nil, err
	}
	return NewObjectDatabase(kv), nil
}

// Put inserts or updates a single entry.
func (db *ObjectDatabase) Put(bucket, key []byte, value []byte) error {
	if metrics.Enabled {
		defer dbPutTimer.UpdateSince(time.Now())
	}

	err := db.kv.Update(context.Background(), func(tx Tx) error {
		return tx.Bucket(bucket).Put(key, value)
	})
	return err
}

// MultiPut - requirements: input must be sorted and without duplicates
func (db *ObjectDatabase) MultiPut(tuples ...[]byte) (uint64, error) {
	err := db.kv.Update(context.Background(), func(tx Tx) error {
		for bucketStart := 0; bucketStart < len(tuples); {
			bucketEnd := bucketStart
			for ; bucketEnd < len(tuples) && bytes.Equal(tuples[bucketEnd], tuples[bucketStart]); bucketEnd += 3 {
			}
			b := tx.Bucket(tuples[bucketStart])
			c := b.Cursor()

			// move cursor to a first element in batch
			// if it's nil, it means all keys in batch gonna be inserted after end of bucket (batch is sorted and has no duplicates here)
			// can apply optimisations for this case
			firstKey, _, err := c.Seek(tuples[bucketStart+1])
			if err != nil {
				return err
			}
			isEndOfBucket := firstKey == nil

			l := (bucketEnd - bucketStart) / 3
			for i := 0; i < l; i++ {
				k := tuples[bucketStart+3*i+1]
				v := tuples[bucketStart+3*i+2]
				if isEndOfBucket {
					if v == nil {
						// nothing to delete after end of bucket
					} else {
						if err := c.Append(k, v); err != nil {
							return err
						}
					}
				} else {
					if v == nil {
						if err := c.Delete(k); err != nil {
							return err
						}
					} else {
						if err := c.Put(k, v); err != nil {
							return err
						}
					}
				}
			}

			bucketStart = bucketEnd
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return 0, nil
}

func (db *ObjectDatabase) Has(bucket, key []byte) (bool, error) {
	var has bool
	err := db.kv.View(context.Background(), func(tx Tx) error {
		v, _ := tx.Bucket(bucket).Get(key)
		has = v != nil
		return nil
	})
	return has, err
}

func (db *ObjectDatabase) DiskSize(ctx context.Context) (uint64, error) {
	casted, ok := db.kv.(HasStats)
	if !ok {
		return 0, nil
	}
	return casted.DiskSize(ctx)
}

// Get returns the value for a given key if it's present.
func (db *ObjectDatabase) Get(bucket, key []byte) (dat []byte, err error) {
	if metrics.Enabled {
		defer dbGetTimer.UpdateSince(time.Now())
	}

	err = db.kv.View(context.Background(), func(tx Tx) error {
		v, _ := tx.Bucket(bucket).Get(key)
		if v != nil {
			dat = make([]byte, len(v))
			copy(dat, v)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if dat == nil {
		return nil, ErrKeyNotFound
	}
	return dat, nil
}

// GetIndexChunk returns proper index chunk or return error if index is not created.
// key must contain inverted block number in the end
func (db *ObjectDatabase) GetIndexChunk(bucket, key []byte, timestamp uint64) ([]byte, error) {
	var dat []byte
	err := db.kv.View(context.Background(), func(tx Tx) error {
		c := tx.Bucket(bucket).Cursor()
		k, v, err := c.Seek(dbutils.IndexChunkKey(key, timestamp))
		if err != nil {
			return err
		}
		if !bytes.HasPrefix(k, dbutils.CompositeKeyWithoutIncarnation(key)) {
			return ErrKeyNotFound
		}
		dat = make([]byte, len(v))
		copy(dat, v)
		return nil
	})
	if dat == nil {
		return nil, ErrKeyNotFound
	}
	return dat, err
}

// getChangeSetByBlockNoLock returns changeset by block and dbi
func (db *ObjectDatabase) GetChangeSetByBlock(storage bool, timestamp uint64) ([]byte, error) {
	key := dbutils.EncodeTimestamp(timestamp)

	var dat []byte
	err := db.kv.View(context.Background(), func(tx Tx) error {
		v, _ := tx.Bucket(dbutils.ChangeSetByIndexBucket(true /* plain */, storage)).Get(key)
		if v != nil {
			dat = make([]byte, len(v))
			copy(dat, v)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return dat, nil
}

func (db *ObjectDatabase) Walk(bucket, startkey []byte, fixedbits int, walker func(k, v []byte) (bool, error)) error {
	fixedbytes, mask := Bytesmask(fixedbits)
	err := db.kv.View(context.Background(), func(tx Tx) error {
		b := tx.Bucket(bucket)
		if b == nil {
			return nil
		}
		c := b.Cursor()
		k, v, err := c.Seek(startkey)
		if err != nil {
			return err
		}
		for k != nil && len(k) >= fixedbytes && (fixedbits == 0 || bytes.Equal(k[:fixedbytes-1], startkey[:fixedbytes-1]) && (k[fixedbytes-1]&mask) == (startkey[fixedbytes-1]&mask)) {
			goOn, err := walker(k, v)
			if err != nil {
				return err
			}
			if !goOn {
				break
			}
			k, v, err = c.Next()
			if err != nil {
				return err
			}
		}
		return nil
	})
	return err
}

func (db *ObjectDatabase) MultiWalk(bucket []byte, startkeys [][]byte, fixedbits []int, walker func(int, []byte, []byte) error) error {

	rangeIdx := 0 // What is the current range we are extracting
	fixedbytes, mask := Bytesmask(fixedbits[rangeIdx])
	startkey := startkeys[rangeIdx]
	err := db.kv.View(context.Background(), func(tx Tx) error {
		c := tx.Bucket(bucket).Cursor()
		k, v, err := c.Seek(startkey)
		if err != nil {
			return err
		}
		for k != nil {
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
						k, v, err = c.SeekTo(startkey)
						if err != nil {
							return err
						}
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
			if len(v) > 0 {
				if err = walker(rangeIdx, k, v); err != nil {
					return err
				}
			}
			k, v, err = c.Next()
			if err != nil {
				return err
			}
		}
		return nil
	})
	return err
}

// Delete deletes the key from the queue and database
func (db *ObjectDatabase) Delete(bucket, key []byte) error {
	// Execute the actual operation
	err := db.kv.Update(context.Background(), func(tx Tx) error {
		return tx.Bucket(bucket).Delete(key)
	})
	return err
}

func (db *ObjectDatabase) BucketExists(name []byte) (bool, error) {
	return db.kv.BucketExists(name)
}

func (db *ObjectDatabase) ClearBuckets(buckets ...[]byte) error {
	if err := db.kv.DropBuckets(buckets...); err != nil {
		return nil
	}
	return db.kv.CreateBuckets(buckets...)
}

func (db *ObjectDatabase) DropBuckets(buckets ...[]byte) error {
	return db.kv.DropBuckets(buckets...)
}

func (db *ObjectDatabase) Close() {
	db.kv.Close()
}

func (db *ObjectDatabase) Keys() ([][]byte, error) {
	var keys [][]byte
	err := db.kv.View(context.Background(), func(tx Tx) error {
		for _, name := range dbutils.Buckets {
			var nameCopy = make([]byte, len(name))
			copy(nameCopy, name)
			return tx.Bucket(name).Cursor().Walk(func(k, _ []byte) (bool, error) {
				var kCopy = make([]byte, len(k))
				copy(kCopy, k)
				keys = append(append(keys, nameCopy), kCopy)
				return true, nil
			})
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return keys, err
}

func (db *ObjectDatabase) KV() KV {
	return db.kv
}

func (db *ObjectDatabase) MemCopy() *ObjectDatabase {
	var mem *ObjectDatabase
	// Open the db and recover any potential corruptions
	switch db.kv.(type) {
	case *LmdbKV:
		mem = NewObjectDatabase(NewLMDB().InMem().MustOpen())
	case *BoltKV:
		mem = NewObjectDatabase(NewBolt().InMem().MustOpen())
	case *badgerKV:
		mem = NewObjectDatabase(NewBadger().InMem().MustOpen())
	}

	if err := db.kv.View(context.Background(), func(readTx Tx) error {
		for _, name := range dbutils.Buckets {
			name := name
			b := readTx.Bucket(name)
			if err := mem.kv.Update(context.Background(), func(writeTx Tx) error {
				newBucketToWrite := writeTx.Bucket(name)
				return b.Cursor().Walk(func(k, v []byte) (bool, error) {
					if err := newBucketToWrite.Put(common.CopyBytes(k), common.CopyBytes(v)); err != nil {
						return false, err
					}
					return true, nil
				})
			}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		panic(err)
	}

	return mem
}

func (db *ObjectDatabase) NewBatch() DbWithPendingMutations {
	m := &mutation{
		db:   db,
		puts: newPuts(),
	}
	return m
}

// IdealBatchSize defines the size of the data batches should ideally add in one write.
func (db *ObjectDatabase) IdealBatchSize() int {
	return db.kv.IdealBatchSize()
}

// [TURBO-GETH] Freezer support (not implemented yet)
// Ancients returns an error as we don't have a backing chain freezer.
func (db *ObjectDatabase) Ancients() (uint64, error) {
	return 0, errNotSupported
}

// TruncateAncients returns an error as we don't have a backing chain freezer.
func (db *ObjectDatabase) TruncateAncients(items uint64) error {
	return errNotSupported
}

func (db *ObjectDatabase) ID() uint64 {
	return db.id
}
