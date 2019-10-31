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

package ethdb

// Code using batches should try to add this much data to the batch.
// The value was determined empirically.
const IdealBatchSize = 100 * 1024

// Putter wraps the database write operation supported by both batches and regular databases.
type Putter interface {
	Put(bucket, key, value []byte) error
	PutS(hBucket, key, value []byte, timestamp uint64, noHistory bool) error
	DeleteTimestamp(timestamp uint64) error
}

type Getter interface {
	Get(bucket, key []byte) ([]byte, error)
	GetS(hBucket, key []byte, timestamp uint64) ([]byte, error)
	GetAsOf(bucket, hBucket, key []byte, timestamp uint64) ([]byte, error)
	Has(bucket, key []byte) (bool, error)
	Walk(bucket, startkey []byte, fixedbits uint, walker func([]byte, []byte) (bool, error)) error
	MultiWalk(bucket []byte, startkeys [][]byte, fixedbits []uint, walker func(int, []byte, []byte) (bool, error)) error
	WalkAsOf(bucket, hBucket, startkey []byte, fixedbits uint, timestamp uint64, walker func([]byte, []byte) (bool, error)) error
	MultiWalkAsOf(bucket, hBucket []byte, startkeys [][]byte, fixedbits []uint, timestamp uint64, walker func(int, []byte, []byte) (bool, error)) error
}

// Deleter wraps the database delete operation supported by both batches and regular databases.
type Deleter interface {
	Delete(bucket, key []byte) error
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
