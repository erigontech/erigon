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

	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/log"
)

// ObjectDatabase - is an object-style interface of DB accessing
type ObjectDatabase struct {
	kv  KV
	log log.Logger
}

// NewObjectDatabase returns a AbstractDB wrapper.
func NewObjectDatabase(kv KV) *ObjectDatabase {
	logger := log.New("database", "object")
	return &ObjectDatabase{
		kv:  kv,
		log: logger,
	}
}

// Put inserts or updates a single entry.
func (db *ObjectDatabase) Put(bucket, key []byte, value []byte) error {
	err := db.kv.Update(context.Background(), func(tx Tx) error {
		return tx.Bucket(bucket).Put(key, value)
	})
	return err
}

func (db *ObjectDatabase) MultiPut(tuples ...[]byte) (uint64, error) {
	//var savedTx Tx
	err := db.kv.Update(context.Background(), func(tx Tx) error {
		for bucketStart := 0; bucketStart < len(tuples); {
			bucketEnd := bucketStart
			for ; bucketEnd < len(tuples) && bytes.Equal(tuples[bucketEnd], tuples[bucketStart]); bucketEnd += 3 {
			}
			b := tx.Bucket(tuples[bucketStart])
			l := (bucketEnd - bucketStart) / 3
			pairs := make([][]byte, 2*l)
			for i := 0; i < l; i++ {
				pairs[2*i] = tuples[bucketStart+3*i+1]
				pairs[2*i+1] = tuples[bucketStart+3*i+2]
				if pairs[2*i+1] == nil {
					if err := b.Delete(pairs[2*i]); err != nil {
						return err
					}
				} else {
					if err := b.Put(pairs[2*i], pairs[2*i+1]); err != nil {
						return err
					}
				}
			}
			// TODO: Add MultiPut to abstraction
			//if err := b.MultiPut(pairs...); err != nil {
			//	return err
			//}

			bucketStart = bucketEnd
		}
		//savedTx = tx
		return nil
	})
	if err != nil {
		return 0, err
	}
	return 0, nil
	//return uint64(savedTx.Stats().Write), nil
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

func (db *ObjectDatabase) DiskSize() uint64 {
	return db.kv.Size()
}

// Get returns the value for a given key if it's present.
func (db *ObjectDatabase) Get(bucket, key []byte) ([]byte, error) {
	// Retrieve the key and increment the miss counter if not found
	var dat []byte
	err := db.kv.View(context.Background(), func(tx Tx) error {
		v, _ := tx.Bucket(bucket).Get(key)
		if v != nil {
			dat = make([]byte, len(v))
			copy(dat, v)
		}
		return nil
	})
	if dat == nil {
		return nil, ErrKeyNotFound
	}
	return dat, err
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
func (db *ObjectDatabase) GetChangeSetByBlock(hBucket []byte, timestamp uint64) ([]byte, error) {
	key := dbutils.EncodeTimestamp(timestamp)

	var dat []byte
	err := db.kv.View(context.Background(), func(tx Tx) error {
		v, _ := tx.Bucket(dbutils.ChangeSetByIndexBucket(hBucket)).Get(key)
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

func (db *ObjectDatabase) DeleteBucket(bucket []byte) error {
	panic("not implemented") // probably need replace by TruncateBucket()?
}

func (db *ObjectDatabase) Close() {
	db.kv.Close()
}

func (db *ObjectDatabase) Keys() ([][]byte, error) {
	panic("not implemented")
}

func (db *ObjectDatabase) AbstractKV() KV {
	return db.kv
}

func (db *ObjectDatabase) MemCopy() Database {
	panic("not implemented")
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
	return 50 * 1024 * 1024 // 50 Mb
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
	panic("not implemented")
	//return db.id
}
