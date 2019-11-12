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

// ErrKeyNotFound is returned when key isn't found in the database.
var ErrKeyNotFound = errors.New("db: key not found")

// IdealBatchSize defines the size of the data batches should ideally add in one write.
const IdealBatchSize = 100 * 1024

// Putter wraps the database write operations.
type Putter interface {
	// Put inserts or updates a single entry.
	Put(bucket, key, value []byte) error

	// PutS adds a new entry to the historical buckets:
	// hBucket (unless changeSetBucketOnly) and ChangeSet.
	// timestamp == block number
	PutS(hBucket, key, value []byte, timestamp uint64, changeSetBucketOnly bool) error
}

// Getter wraps the database read operations.
type Getter interface {
	// Get returns the value for a given key if it's present.
	Get(bucket, key []byte) ([]byte, error)

	// GetS returns the value that was recorded in a given historical bucket for an exact timestamp.
	// timestamp == block number
	GetS(hBucket, key []byte, timestamp uint64) ([]byte, error)

	// GetAsOf returns the value valid as of a given timestamp.
	// timestamp == block number
	GetAsOf(bucket, hBucket, key []byte, timestamp uint64) ([]byte, error)

	// Has indicates whether a key exists in the database.
	Has(bucket, key []byte) (bool, error)

	Walk(bucket, startkey []byte, fixedbits uint, walker func([]byte, []byte) (bool, error)) error
	MultiWalk(bucket []byte, startkeys [][]byte, fixedbits []uint, walker func(int, []byte, []byte) (bool, error)) error
	WalkAsOf(bucket, hBucket, startkey []byte, fixedbits uint, timestamp uint64, walker func([]byte, []byte) (bool, error)) error
	MultiWalkAsOf(bucket, hBucket []byte, startkeys [][]byte, fixedbits []uint, timestamp uint64, walker func(int, []byte, []byte) (bool, error)) error
}

// Deleter wraps the database delete operations.
type Deleter interface {
	// Delete removes a single entry.
	Delete(bucket, key []byte) error

	// DeleteTimestamp removes data for a given timestamp from all historical buckets (incl. ChangeSet).
	// timestamp == block number
	DeleteTimestamp(timestamp uint64) error
}

// Database wraps all database operations. All methods are safe for concurrent use.
type Database interface {
	Getter
	Putter
	Deleter
	MultiPut(tuples ...[]byte) (uint64, error)
	RewindData(timestampSrc, timestampDst uint64, df func(bucket, key, value []byte) error) error
	Close()
	NewBatch() DbWithPendingMutations
	Size() int
	Keys() ([][]byte, error)
	MemCopy() Database
	// [TURBO-GETH] Freezer support (minimum amount that is actually used)
	// FIXME: implement support if needed
	Ancients() (uint64, error)
	TruncateAncients(items uint64) error
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
