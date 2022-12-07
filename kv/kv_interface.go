/*
   Copyright 2022 Erigon contributors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package kv

import (
	"context"
	"errors"

	"github.com/VictoriaMetrics/metrics"
)

const ReadersLimit = 32000 // MDBX_READERS_LIMIT=32767

var (
	ErrAttemptToDeleteNonDeprecatedBucket = errors.New("only buckets from dbutils.ChaindataDeprecatedTables can be deleted")
	ErrUnknownBucket                      = errors.New("unknown bucket. add it to dbutils.ChaindataTables")

	DbSize    = metrics.NewCounter(`db_size`)    //nolint
	TxLimit   = metrics.NewCounter(`tx_limit`)   //nolint
	TxSpill   = metrics.NewCounter(`tx_spill`)   //nolint
	TxUnspill = metrics.NewCounter(`tx_unspill`) //nolint
	TxDirty   = metrics.NewCounter(`tx_dirty`)   //nolint

	DbCommitPreparation = metrics.GetOrCreateSummary(`db_commit_seconds{phase="preparation"}`) //nolint
	DbCommitGc          = metrics.GetOrCreateSummary(`db_commit_seconds{phase="gc"}`)          //nolint
	DbCommitAudit       = metrics.GetOrCreateSummary(`db_commit_seconds{phase="audit"}`)       //nolint
	DbCommitWrite       = metrics.GetOrCreateSummary(`db_commit_seconds{phase="write"}`)       //nolint
	DbCommitSync        = metrics.GetOrCreateSummary(`db_commit_seconds{phase="sync"}`)        //nolint
	DbCommitEnding      = metrics.GetOrCreateSummary(`db_commit_seconds{phase="ending"}`)      //nolint
	DbCommitTotal       = metrics.GetOrCreateSummary(`db_commit_seconds{phase="total"}`)       //nolint

	DbPgopsNewly   = metrics.NewCounter(`db_pgops_newly`)           //nolint
	DbPgopsCow     = metrics.NewCounter(`db_pgops_cow`)             //nolint
	DbPgopsClone   = metrics.NewCounter(`db_pgops_clone`)           //nolint
	DbPgopsSplit   = metrics.NewCounter(`db_pgops_split`)           //nolint
	DbPgopsMerge   = metrics.NewCounter(`db_pgops_merge`)           //nolint
	DbPgopsSpill   = metrics.NewCounter(`db_pgops_spill`)           //nolint
	DbPgopsUnspill = metrics.NewCounter(`db_pgops_unspill`)         //nolint
	DbPgopsWops    = metrics.NewCounter(`db_pgops_wops`)            //nolint
	DbPgopsGcrtime = metrics.GetOrCreateSummary(`db_pgops_gcrtime`) //nolint

	GcLeafMetric     = metrics.NewCounter(`db_gc_leaf`)     //nolint
	GcOverflowMetric = metrics.NewCounter(`db_gc_overflow`) //nolint
	GcPagesMetric    = metrics.NewCounter(`db_gc_pages`)    //nolint

)

type DBVerbosityLvl int8
type Label uint8

const (
	ChainDB      Label = 0
	TxPoolDB     Label = 1
	SentryDB     Label = 2
	ConsensusDB  Label = 3
	DownloaderDB Label = 4
)

func (l Label) String() string {
	switch l {
	case ChainDB:
		return "chaindata"
	case TxPoolDB:
		return "txpool"
	case SentryDB:
		return "sentry"
	case ConsensusDB:
		return "consensus"
	case DownloaderDB:
		return "downloader"
	default:
		return "unknown"
	}
}

type Has interface {
	// Has indicates whether a key exists in the database.
	Has(bucket string, key []byte) (bool, error)
}
type GetPut interface {
	Getter
	Putter
}
type Getter interface {
	Has

	// GetOne references a readonly section of memory that must not be accessed after txn has terminated
	GetOne(bucket string, key []byte) (val []byte, err error)

	// ForEach iterates over entries with keys greater or equal to fromPrefix.
	// walker is called for each eligible entry.
	// If walker returns an error:
	//   - implementations of local db - stop
	//   - implementations of remote db - do not handle this error and may finish (send all entries to client) before error happen.
	ForEach(bucket string, fromPrefix []byte, walker func(k, v []byte) error) error
	ForPrefix(bucket string, prefix []byte, walker func(k, v []byte) error) error
	ForAmount(bucket string, prefix []byte, amount uint32, walker func(k, v []byte) error) error
}

// Putter wraps the database write operations.
type Putter interface {
	// Put inserts or updates a single entry.
	Put(table string, k, v []byte) error
}

// Deleter wraps the database delete operations.
type Deleter interface {
	// Delete removes a single entry.
	Delete(table string, k []byte) error
}

type Closer interface {
	Close()
}

// RoDB - Read-only version of KV.
type RoDB interface {
	Closer

	View(ctx context.Context, f func(tx Tx) error) error

	// BeginRo - creates transaction
	// 	tx may be discarded by .Rollback() method
	//
	// A transaction and its cursors must only be used by a single
	// 	thread (not goroutine), and a thread may only have a single transaction at a time.
	//  It happen automatically by - because this method calls runtime.LockOSThread() inside (Rollback/Commit releases it)
	//  By this reason application code can't call runtime.UnlockOSThread() - it leads to undefined behavior.
	//
	// If this `parent` is non-NULL, the new transaction
	//	will be a nested transaction, with the transaction indicated by parent
	//	as its parent. Transactions may be nested to any level. A parent
	//	transaction and its cursors may not issue any other operations than
	//	Commit and Rollback while it has active child transactions.
	BeginRo(ctx context.Context) (Tx, error)
	AllBuckets() TableCfg
	PageSize() uint64
}

// RwDB low-level database interface - main target is - to provide common abstraction over top of MDBX and RemoteKV.
//
// Common pattern for short-living transactions:
//
//	 if err := db.View(ctx, func(tx ethdb.Tx) error {
//	    ... code which uses database in transaction
//	 }); err != nil {
//			return err
//	}
//
// Common pattern for long-living transactions:
//
//	tx, err := db.Begin()
//	if err != nil {
//		return err
//	}
//	defer tx.Rollback()
//
//	... code which uses database in transaction
//
//	err := tx.Commit()
//	if err != nil {
//		return err
//	}
type RwDB interface {
	RoDB

	Update(ctx context.Context, f func(tx RwTx) error) error

	BeginRw(ctx context.Context) (RwTx, error)
	BeginRwAsync(ctx context.Context) (RwTx, error)
}

type StatelessReadTx interface {
	Getter

	Commit() error // Commit all the operations of a transaction into the database.
	Rollback()     // Rollback - abandon all the operations of the transaction instead of saving them.

	// ReadSequence - allows to create a linear sequence of unique positive integers for each table.
	// Can be called for a read transaction to retrieve the current sequence value, and the increment must be zero.
	// Sequence changes become visible outside the current write transaction after it is committed, and discarded on abort.
	// Starts from 0.
	ReadSequence(bucket string) (uint64, error)

	BucketSize(bucket string) (uint64, error)
}

type StatelessWriteTx interface {
	Putter
	Deleter

	/*
		// if need N id's:
		baseId, err := tx.IncrementSequence(bucket, N)
		if err != nil {
		   return err
		}
		for i := 0; i < N; i++ {    // if N == 0, it will work as expected
		    id := baseId + i
		    // use id
		}


		// or if need only 1 id:
		id, err := tx.IncrementSequence(bucket, 1)
		if err != nil {
		    return err
		}
		// use id
	*/
	IncrementSequence(bucket string, amount uint64) (uint64, error)
	Append(bucket string, k, v []byte) error
	AppendDup(bucket string, k, v []byte) error
}

type StatelessRwTx interface {
	StatelessReadTx
	StatelessWriteTx
}

// Tx
// WARNING:
//   - Tx is not threadsafe and may only be used in the goroutine that created it
//   - ReadOnly transactions do not lock goroutine to thread, RwTx does
type Tx interface {
	StatelessReadTx

	// ID returns the identifier associated with this transaction. For a
	// read-only transaction, this corresponds to the snapshot being read;
	// concurrent readers will frequently have the same transaction ID.
	ViewID() uint64

	// Cursor - creates cursor object on top of given bucket. Type of cursor - depends on bucket configuration.
	// If bucket was created with mdbx.DupSort flag, then cursor with interface CursorDupSort created
	// Otherwise - object of interface Cursor created
	//
	// Cursor, also provides a grain of magic - it can use a declarative configuration - and automatically break
	// long keys into DupSort key/values. See docs for `bucket.go:TableCfgItem`
	Cursor(bucket string) (Cursor, error)
	CursorDupSort(bucket string) (CursorDupSort, error) // CursorDupSort - can be used if bucket has mdbx.DupSort flag

	ForEach(bucket string, fromPrefix []byte, walker func(k, v []byte) error) error
	ForPrefix(bucket string, prefix []byte, walker func(k, v []byte) error) error
	ForAmount(bucket string, prefix []byte, amount uint32, walker func(k, v []byte) error) error

	DBSize() (uint64, error)
}

// RwTx
//
// WARNING:
//   - RwTx is not threadsafe and may only be used in the goroutine that created it.
//   - ReadOnly transactions do not lock goroutine to thread, RwTx does
//   - User Can't call runtime.LockOSThread/runtime.UnlockOSThread in same goroutine until RwTx Commit/Rollback
type RwTx interface {
	Tx
	StatelessWriteTx
	BucketMigrator

	RwCursor(bucket string) (RwCursor, error)
	RwCursorDupSort(bucket string) (RwCursorDupSort, error)

	// CollectMetrics - does collect all DB-related and Tx-related metrics
	// this method exists only in RwTx to avoid concurrency
	CollectMetrics()
	Reset() error
}

// BucketMigrator used for buckets migration, don't use it in usual app code
type BucketMigrator interface {
	DropBucket(string) error
	CreateBucket(string) error
	ExistsBucket(string) (bool, error)
	ClearBucket(string) error
	ListBuckets() ([]string, error)
}

// Cursor - class for navigating through a database
// CursorDupSort are inherit this class
//
// If methods (like First/Next/Seek) return error, then returned key SHOULD not be nil (can be []byte{} for example).
// Then looping code will look as:
// c := kv.Cursor(bucketName)
//
//	for k, v, err := c.First(); k != nil; k, v, err = c.Next() {
//	   if err != nil {
//	       return err
//	   }
//	   ... logic
//	}
type Cursor interface {
	First() ([]byte, []byte, error)               // First - position at first key/data item
	Seek(seek []byte) ([]byte, []byte, error)     // Seek - position at first key greater than or equal to specified key
	SeekExact(key []byte) ([]byte, []byte, error) // SeekExact - position at exact matching key if exists
	Next() ([]byte, []byte, error)                // Next - position at next key/value (can iterate over DupSort key/values automatically)
	Prev() ([]byte, []byte, error)                // Prev - position at previous key
	Last() ([]byte, []byte, error)                // Last - position at last key and last possible value
	Current() ([]byte, []byte, error)             // Current - return key/data at current cursor position

	Count() (uint64, error) // Count - fast way to calculate amount of keys in bucket. It counts all keys even if Prefix was set.

	Close()
}

type RwCursor interface {
	Cursor

	Put(k, v []byte) error           // Put - based on order
	Append(k []byte, v []byte) error // Append - append the given key/data pair to the end of the database. This option allows fast bulk loading when keys are already known to be in the correct order.
	Delete(k []byte) error           // Delete - short version of SeekExact+DeleteCurrent or SeekBothExact+DeleteCurrent

	// DeleteCurrent This function deletes the key/data pair to which the cursor refers.
	// This does not invalidate the cursor, so operations such as MDB_NEXT
	// can still be used on it.
	// Both MDB_NEXT and MDB_GET_CURRENT will return the same record after
	// this operation.
	DeleteCurrent() error
}

type CursorDupSort interface {
	Cursor

	// SeekBothExact -
	// second parameter can be nil only if searched key has no duplicates, or return error
	SeekBothExact(key, value []byte) ([]byte, []byte, error)
	SeekBothRange(key, value []byte) ([]byte, error) // SeekBothRange - exact match of the key, but range match of the value
	FirstDup() ([]byte, error)                       // FirstDup - position at first data item of current key
	NextDup() ([]byte, []byte, error)                // NextDup - position at next data item of current key
	NextNoDup() ([]byte, []byte, error)              // NextNoDup - position at first data item of next key
	LastDup() ([]byte, error)                        // LastDup - position at last data item of current key

	CountDuplicates() (uint64, error) // CountDuplicates - number of duplicates for the current key
}

type RwCursorDupSort interface {
	CursorDupSort
	RwCursor

	PutNoDupData(key, value []byte) error // PutNoDupData - inserts key without dupsort
	DeleteCurrentDuplicates() error       // DeleteCurrentDuplicates - deletes all of the data items for the current key
	DeleteExact(k1, k2 []byte) error      // DeleteExact - delete 1 value from given key
	AppendDup(key, value []byte) error    // AppendDup - same as Append, but for sorted dup data
}

var ErrNotSupported = errors.New("not supported")
