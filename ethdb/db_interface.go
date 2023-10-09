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

	"github.com/ledgerwatch/erigon-lib/kv"
)

// DESCRIBED: For info on database buckets see docs/programmers_guide/db_walkthrough.MD

// ErrKeyNotFound is returned when key isn't found in the database.
var ErrKeyNotFound = errors.New("db: key not found")

// MinDatabase is a minimalistic version of the Database interface.
type MinDatabase interface {
	Get(bucket string, key []byte) ([]byte, error)
	Put(table string, k, v []byte) error
	Delete(table string, k []byte) error
}

// DbWithPendingMutations is an extended version of the Database,
// where all changes are first made in memory.
// Later they can either be committed to the database or rolled back.
type DbWithPendingMutations interface {
	kv.StatelessRwTx

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

	Rollback()
	BatchSize() int
}

type HasTx interface {
	Tx() kv.Tx
}

type BucketsMigrator interface {
	BucketExists(bucket string) (bool, error) // makes them empty
	ClearBuckets(buckets ...string) error     // makes them empty
	DropBuckets(buckets ...string) error      // drops them, use of them after drop will panic
}
