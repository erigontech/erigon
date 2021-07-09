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
package kv

import (
	"bytes"
	"context"
	"fmt"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/log"
	"github.com/torquem-ch/mdbx-go/mdbx"
)

// ObjectDatabase - is an object-style interface of DB accessing
type ObjectDatabase struct {
	kv ethdb.RwKV
}

// NewObjectDatabase returns a AbstractDB wrapper.
func NewObjectDatabase(kv ethdb.RwKV) *ObjectDatabase {
	return &ObjectDatabase{
		kv: kv,
	}
}

func MustOpen(path string) ethdb.RwKV {
	db, err := Open(path, false)
	if err != nil {
		panic(err)
	}
	return db
}

// Open - main method to open database.
func Open(path string, readOnly bool) (ethdb.RwKV, error) {
	var db ethdb.RwKV
	var err error
	opts := NewMDBX().Path(path)
	if readOnly {
		opts = opts.Flags(func(flags uint) uint { return flags | mdbx.Readonly })
	}
	db, err = opts.Open()

	if err != nil {
		return nil, err
	}
	return db, nil
}

// Put inserts or updates a single entry.
func (db *ObjectDatabase) Put(bucket string, key []byte, value []byte) error {
	err := db.kv.Update(context.Background(), func(tx ethdb.RwTx) error {
		return tx.Put(bucket, key, value)
	})
	return err
}

// Append appends a single entry to the end of the bucket.
func (db *ObjectDatabase) Append(bucket string, key []byte, value []byte) error {
	err := db.kv.Update(context.Background(), func(tx ethdb.RwTx) error {
		c, err := tx.RwCursor(bucket)
		if err != nil {
			return err
		}
		return c.Append(key, value)
	})
	return err
}

// AppendDup appends a single entry to the end of the bucket.
func (db *ObjectDatabase) AppendDup(bucket string, key []byte, value []byte) error {
	err := db.kv.Update(context.Background(), func(tx ethdb.RwTx) error {
		c, err := tx.RwCursorDupSort(bucket)
		if err != nil {
			return err
		}
		return c.AppendDup(key, value)
	})
	return err
}

// MultiPut - requirements: input must be sorted and without duplicates
func (db *ObjectDatabase) MultiPut(tuples ...[]byte) (uint64, error) {
	err := db.kv.Update(context.Background(), func(tx ethdb.RwTx) error {
		return ethdb.MultiPut(tx, tuples...)
	})
	if err != nil {
		return 0, err
	}
	return 0, nil
}

func (db *ObjectDatabase) Has(bucket string, key []byte) (bool, error) {
	var has bool
	err := db.kv.View(context.Background(), func(tx ethdb.Tx) error {
		v, err := tx.GetOne(bucket, key)
		if err != nil {
			return err
		}
		has = v != nil
		return nil
	})
	return has, err
}

func (db *ObjectDatabase) IncrementSequence(bucket string, amount uint64) (res uint64, err error) {
	err = db.kv.Update(context.Background(), func(tx ethdb.RwTx) error {
		res, err = tx.IncrementSequence(bucket, amount)
		return err
	})
	return res, err
}
func (db *ObjectDatabase) ReadSequence(bucket string) (res uint64, err error) {
	err = db.kv.View(context.Background(), func(tx ethdb.Tx) error {
		res, err = tx.ReadSequence(bucket)
		return err
	})
	return res, err
}

// Get returns the value for a given key if it's present.
func (db *ObjectDatabase) GetOne(bucket string, key []byte) ([]byte, error) {
	var dat []byte
	err := db.kv.View(context.Background(), func(tx ethdb.Tx) error {
		v, err := tx.GetOne(bucket, key)
		if err != nil {
			return err
		}
		if v != nil {
			dat = make([]byte, len(v))
			copy(dat, v)
		}
		return nil
	})
	return dat, err
}

func (db *ObjectDatabase) Get(bucket string, key []byte) ([]byte, error) {
	dat, err := db.GetOne(bucket, key)
	return ethdb.GetOneWrapper(dat, err)
}

func (db *ObjectDatabase) Last(bucket string) ([]byte, []byte, error) {
	var key, value []byte
	if err := db.kv.View(context.Background(), func(tx ethdb.Tx) error {
		c, err := tx.Cursor(bucket)
		if err != nil {
			return err
		}
		k, v, err := c.Last()
		if err != nil {
			return err
		}
		if k != nil {
			key, value = common.CopyBytes(k), common.CopyBytes(v)
		}
		return nil
	}); err != nil {
		return nil, nil, err
	}
	return key, value, nil
}

func (db *ObjectDatabase) Walk(bucket string, startkey []byte, fixedbits int, walker func(k, v []byte) (bool, error)) error {
	err := db.kv.View(context.Background(), func(tx ethdb.Tx) error {
		c, err := tx.Cursor(bucket)
		if err != nil {
			return err
		}
		return ethdb.Walk(c, startkey, fixedbits, walker)
	})
	return err
}

func (db *ObjectDatabase) ForEach(bucket string, fromPrefix []byte, walker func(k, v []byte) error) error {
	return db.kv.View(context.Background(), func(tx ethdb.Tx) error {
		return tx.ForEach(bucket, fromPrefix, walker)
	})
}
func (db *ObjectDatabase) ForAmount(bucket string, fromPrefix []byte, amount uint32, walker func(k, v []byte) error) error {
	return db.kv.View(context.Background(), func(tx ethdb.Tx) error {
		return tx.ForAmount(bucket, fromPrefix, amount, walker)
	})
}

func (db *ObjectDatabase) ForPrefix(bucket string, prefix []byte, walker func(k, v []byte) error) error {
	return db.kv.View(context.Background(), func(tx ethdb.Tx) error {
		return tx.ForPrefix(bucket, prefix, walker)
	})
}

// Delete deletes the key from the queue and database
func (db *ObjectDatabase) Delete(bucket string, k, v []byte) error {
	// Execute the actual operation
	err := db.kv.Update(context.Background(), func(tx ethdb.RwTx) error {
		return tx.Delete(bucket, k, v)
	})
	return err
}

func (db *ObjectDatabase) BucketExists(name string) (bool, error) {
	exists := false
	if err := db.kv.View(context.Background(), func(tx ethdb.Tx) error {
		migrator, ok := tx.(ethdb.BucketMigrator)
		if !ok {
			return fmt.Errorf("%T doesn't implement ethdb.TxMigrator interface", db.kv)
		}
		exists = migrator.ExistsBucket(name)
		return nil
	}); err != nil {
		return false, err
	}
	return exists, nil
}

func (db *ObjectDatabase) ClearBuckets(buckets ...string) error {
	for i := range buckets {
		name := buckets[i]
		if err := db.kv.Update(context.Background(), func(tx ethdb.RwTx) error {
			migrator, ok := tx.(ethdb.BucketMigrator)
			if !ok {
				return fmt.Errorf("%T doesn't implement ethdb.TxMigrator interface", db.kv)
			}
			if err := migrator.ClearBucket(name); err != nil {
				return err
			}
			return nil
		}); err != nil {
			return err
		}
	}

	return nil
}

func (db *ObjectDatabase) DropBuckets(buckets ...string) error {
	for i := range buckets {
		name := buckets[i]
		log.Info("Dropping bucket", "name", name)
		if err := db.kv.Update(context.Background(), func(tx ethdb.RwTx) error {
			migrator, ok := tx.(ethdb.BucketMigrator)
			if !ok {
				return fmt.Errorf("%T doesn't implement ethdb.TxMigrator interface", db.kv)
			}
			if err := migrator.DropBucket(name); err != nil {
				return err
			}
			return nil
		}); err != nil {
			return err
		}
	}
	return nil
}

func (db *ObjectDatabase) Close() {
	db.kv.Close()
}

func (db *ObjectDatabase) RwKV() ethdb.RwKV {
	return db.kv
}

func (db *ObjectDatabase) SetRwKV(kv ethdb.RwKV) {
	db.kv = kv
}

func (db *ObjectDatabase) BeginGetter(ctx context.Context) (ethdb.GetterTx, error) {
	batch := &TxDb{db: db}
	if err := batch.begin(ctx, ethdb.RO); err != nil {
		return batch, err
	}
	return batch, nil
}

func (db *ObjectDatabase) Begin(ctx context.Context, flags ethdb.TxFlags) (ethdb.DbWithPendingMutations, error) {
	batch := &TxDb{db: db}
	if err := batch.begin(ctx, flags); err != nil {
		return batch, err
	}
	return batch, nil
}

// Type which expecting sequence of triplets: dbi, key, value, ....
// It sorts entries by dbi name, then inside dbi clusters sort by keys
type MultiPutTuples [][]byte

func (t MultiPutTuples) Len() int { return len(t) / 3 }

func (t MultiPutTuples) Less(i, j int) bool {
	i3, j3 := i*3, j*3
	cmp := bytes.Compare(t[i3], t[j3])
	if cmp == -1 {
		return true
	}
	if cmp == 0 {
		return bytes.Compare(t[i3+1], t[j3+1]) == -1
	}
	return false
}

func (t MultiPutTuples) Swap(i, j int) {
	i3, j3 := i*3, j*3
	t[i3], t[j3] = t[j3], t[i3]
	t[i3+1], t[j3+1] = t[j3+1], t[i3+1]
	t[i3+2], t[j3+2] = t[j3+2], t[i3+2]
}

func InspectDatabase(db ethdb.Database) error {
	// FIXME: implement in Erigon
	// see https://github.com/ethereum/go-ethereum/blob/f5d89cdb72c1e82e9deb54754bef8dd20bf12591/core/rawdb/database.go#L224
	return ethdb.ErrNotSupported
}

func NewDatabaseWithFreezer(db *ObjectDatabase, dir, suffix string) (*ObjectDatabase, error) {
	// FIXME: implement freezer in Erigon
	return db, nil
}
