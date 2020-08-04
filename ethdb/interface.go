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
	"errors"
)

// DESCRIBED: For info on database buckets see docs/programmers_guide/db_walkthrough.MD

// ErrKeyNotFound is returned when key isn't found in the database.
var ErrKeyNotFound = errors.New("db: key not found")

// Putter wraps the database write operations.
type Putter interface {
	// Put inserts or updates a single entry.
	Put(bucket, key, value []byte) error
}

// Getter wraps the database read operations.
type Getter interface {
	// Get returns the value for a given key if it's present.
	Get(bucket, key []byte) ([]byte, error)

	// Get returns prober chunk of index or error if index is not created.
	// Key must contain 8byte inverted block number in the end.
	GetIndexChunk(bucket, key []byte, timestamp uint64) ([]byte, error)

	// Has indicates whether a key exists in the database.
	Has(bucket, key []byte) (bool, error)

	// Walk iterates over entries with keys greater or equal to startkey.
	// Only the keys whose first fixedbits match those of startkey are iterated over.
	// walker is called for each eligible entry.
	// If walker returns false or an error, the walk stops.
	Walk(bucket, startkey []byte, fixedbits int, walker func(k, v []byte) (bool, error)) error

	// MultiWalk is similar to multiple Walk calls folded into one.
	MultiWalk(bucket []byte, startkeys [][]byte, fixedbits []int, walker func(int, []byte, []byte) error) error
}

type GetterPutter interface {
	Getter
	Putter
}

// Deleter wraps the database delete operations.
type Deleter interface {
	// Delete removes a single entry.
	Delete(bucket, key []byte) error
}

// Database wraps all database operations. All methods are safe for concurrent use.
type Database interface {
	Getter
	Putter
	Deleter

	// MultiPut inserts or updates multiple entries.
	// Entries are passed as an array:
	// bucket0, key0, val0, bucket1, key1, val1, ...
	MultiPut(tuples ...[]byte) (uint64, error)
	Close()
	NewBatch() DbWithPendingMutations

	// IdealBatchSize defines the size of the data batches should ideally add in one write.
	IdealBatchSize() int

	Keys() ([][]byte, error)

	// [TURBO-GETH] Freezer support (minimum amount that is actually used)
	// FIXME: implement support if needed
	Ancients() (uint64, error)
	TruncateAncients(items uint64) error

	ID() uint64
}

// MinDatabase is a minimalistic version of the Database interface.
type MinDatabase interface {
	Get(bucket, key []byte) ([]byte, error)
	Put(bucket, key, value []byte) error
	Delete(bucket, key []byte) error
}

// DbWithPendingMutations is an extended version of the Database,
// where all changes are first made in memory.
// Later they can either be committed to the database or rolled back.
type DbWithPendingMutations interface {
	Database
	Commit() (uint64, error)
	Rollback()
	BatchSize() int
}

type HasKV interface {
	KV() KV
}

type HasNetInterface interface {
	DB() Database
}

type NonTransactional interface {
	ClearBuckets(buckets ...[]byte) error // makes them empty
	DropBuckets(buckets ...[]byte) error  // drops them, use of them after drop will panic
}

var errNotSupported = errors.New("not supported")
