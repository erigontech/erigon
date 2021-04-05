// Copyright 2018 The go-ethereum Authors
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

package ethdb

import (
	"context"
	"errors"
)

// DESCRIBED: For info on database buckets see docs/programmers_guide/db_walkthrough.MD

// ErrKeyNotFound is returned when key isn't found in the database.
var ErrKeyNotFound = errors.New("db: key not found")

type TxFlags uint

const (
	RW TxFlags = 0x00 // default
	RO TxFlags = 0x02
)

type DatabaseReader interface {
	KVGetter

	// Get returns the value for a given key if it's present.
	Get(bucket string, key []byte) ([]byte, error)
}

// Getter wraps the database read operations.
type Getter interface {
	DatabaseReader

	// Walk iterates over entries with keys greater or equal to startkey.
	// Only the keys whose first fixedbits match those of startkey are iterated over.
	// walker is called for each eligible entry.
	// If walker returns false or an error, the walk stops.
	Walk(bucket string, startkey []byte, fixedbits int, walker func(k, v []byte) (bool, error)) error
}

type GetterTx interface {
	Getter

	Rollback()
}

type GetterBeginner interface {
	Getter

	BeginGetter(ctx context.Context) (GetterTx, error)
}

type GetterPutter interface {
	Getter
	Putter
}

// Database wraps all database operations. All methods are safe for concurrent use.
type Database interface {
	GetterBeginner
	Putter
	Deleter
	Closer

	// MultiPut inserts or updates multiple entries.
	// Entries are passed as an array:
	// bucket0, key0, val0, bucket1, key1, val1, ...
	MultiPut(tuples ...[]byte) (uint64, error)

	// NewBatch - starts in-mem batch
	//
	// Common pattern:
	//
	// batch := db.NewBatch()
	// defer batch.Rollback()
	// ... some calculations on `batch`
	// batch.Commit()
	//
	NewBatch() DbWithPendingMutations                                         //
	Begin(ctx context.Context, flags TxFlags) (DbWithPendingMutations, error) // starts db transaction
	Last(bucket string) ([]byte, []byte, error)

	Keys() ([][]byte, error)

	Append(bucket string, key, value []byte) error
	AppendDup(bucket string, key, value []byte) error
	IncrementSequence(bucket string, amount uint64) (uint64, error)
	ReadSequence(bucket string) (uint64, error)
}

// MinDatabase is a minimalistic version of the Database interface.
type MinDatabase interface {
	Get(bucket string, key []byte) ([]byte, error)
	Put(bucket string, key, value []byte) error
	Delete(bucket string, k, v []byte) error
}

// DbWithPendingMutations is an extended version of the Database,
// where all changes are first made in memory.
// Later they can either be committed to the database or rolled back.
type DbWithPendingMutations interface {
	Database

	// Commit - commits transaction (or flush data into underlying db object in case of `mutation`)
	//
	// Common pattern:
	//
	// tx := db.Begin()
	// defer tx.Rollback()
	// ... some calculations on `tx`
	// tx.Commit()
	//
	Commit() error

	// CommitAndBegin - commits and starts new transaction inside same db object.
	// useful for periodical commits implementation.
	//
	// Common pattern:
	//
	// tx := db.Begin()
	// defer tx.Rollback()
	// for {
	// 		... some calculations on `tx`
	//       tx.CommitAndBegin()
	//       // defer here - is not useful, because 'tx' object is reused and first `defer` will work perfectly
	// }
	// tx.Commit()
	//
	CommitAndBegin(ctx context.Context) error
	RollbackAndBegin(ctx context.Context) error
	Rollback()
	BatchSize() int
}

type HasRwKV interface {
	RwKV() RwKV
	SetRwKV(kv RwKV)
}

type HasTx interface {
	Tx() Tx
}

type HasNetInterface interface {
	DB() Database
}

type BucketsMigrator interface {
	BucketExists(bucket string) (bool, error) // makes them empty
	ClearBuckets(buckets ...string) error     // makes them empty
	DropBuckets(buckets ...string) error      // drops them, use of them after drop will panic
}

func getOneWrapper(dat []byte, err error) ([]byte, error) {
	if err != nil {
		return nil, err
	}
	if dat == nil {
		return nil, ErrKeyNotFound
	}
	return dat, nil
}

var errNotSupported = errors.New("not supported")
