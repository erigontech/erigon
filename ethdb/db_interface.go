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

	"github.com/gateway-fm/cdk-erigon-lib/kv"
)

// DESCRIBED: For info on database buckets see docs/programmers_guide/db_walkthrough.MD

// ErrKeyNotFound is returned when key isn't found in the database.
var ErrKeyNotFound = errors.New("db: key not found")

type TxFlags uint

const (
	RW TxFlags = 0x00 // default
	RO TxFlags = 0x02
)

// DBGetter wraps the database read operations.
type DBGetter interface {
	kv.Getter

	// Get returns the value for a given key if it's present.
	Get(bucket string, key []byte) ([]byte, error)
}

// Database wraps all database operations. All methods are safe for concurrent use.
type Database interface {
	DBGetter
	kv.Putter
	kv.Deleter
	kv.Closer

	Begin(ctx context.Context, flags TxFlags) (DbWithPendingMutations, error) // starts db transaction
	Last(bucket string) ([]byte, []byte, error)

	IncrementSequence(bucket string, amount uint64) (uint64, error)
	ReadSequence(bucket string) (uint64, error)
	RwKV() kv.RwDB
}

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

	Rollback()
	BatchSize() int
}

type HasRwKV interface {
	RwKV() kv.RwDB
	SetRwKV(kv kv.RwDB)
}

type HasTx interface {
	Tx() kv.Tx
}

type BucketsMigrator interface {
	BucketExists(bucket string) (bool, error) // makes them empty
	ClearBuckets(buckets ...string) error     // makes them empty
	DropBuckets(buckets ...string) error      // drops them, use of them after drop will panic
}

func GetOneWrapper(dat []byte, err error) ([]byte, error) {
	if err != nil {
		return nil, err
	}
	if dat == nil {
		return nil, ErrKeyNotFound
	}
	return dat, nil
}
