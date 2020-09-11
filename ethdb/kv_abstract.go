package ethdb

import (
	"context"
	"errors"
	"github.com/ledgerwatch/turbo-geth/common"

	"github.com/ledgerwatch/turbo-geth/common/dbutils"
)

var (
	ErrAttemptToDeleteNonDeprecatedBucket = errors.New("only buckets from dbutils.DeprecatedBuckets can be deleted")
	ErrUnknownBucket                      = errors.New("unknown bucket. add it to dbutils.Buckets")
)

// KV low-level database interface - main target is - to provide common abstraction over top of LMDB and RemoteKV.
//
// Common pattern for short-living transactions:
//
//  if err := db.View(ctx, func(tx ethdb.Tx) error {
//     ... code which uses database in transaction
//  }); err != nil {
//		return err
// }
//
// Common pattern for long-living transactions:
//	tx, err := db.Begin(true)
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
//
type KV interface {
	View(ctx context.Context, f func(tx Tx) error) error
	Update(ctx context.Context, f func(tx Tx) error) error
	Close()

	// Begin - creates transaction
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
	Begin(ctx context.Context, parent Tx, writable bool) (Tx, error)
	AllBuckets() dbutils.BucketsCfg
}

type Tx interface {
	// Cursor - creates cursor object on top of given bucket. Type of cursor - depends on bucket configuration.
	// If bucket was created with lmdb.DupSort flag, then cursor with interface CursorDupSort created
	// If bucket was created with lmdb.DupFixed flag, then cursor with interface CursorDupFixed created
	// Otherwise - object of interface Cursor created
	//
	// Cursor, also provides a grain of magic - it can use a declarative configuration - and automatically break
	// long keys into DupSort key/values. See docs for `bucket.go:BucketConfigItem`
	Cursor(bucket string) Cursor
	CursorDupSort(bucket string) CursorDupSort   // CursorDupSort - can be used if bucket has lmdb.DupSort flag
	CursorDupFixed(bucket string) CursorDupFixed // CursorDupSort - can be used if bucket has lmdb.DupFixed flag
	Get(bucket string, key []byte) (val []byte, err error)

	Commit(ctx context.Context) error // Commit all the operations of a transaction into the database.
	Rollback()                        // Rollback - abandon all the operations of the transaction instead of saving them.

	BucketSize(name string) (uint64, error)

	Comparator(bucket string) dbutils.CmpFunc
	Cmp(bucket string, a, b []byte) int
	DCmp(bucket string, a, b []byte) int
}

// Interface used for buckets migration, don't use it in usual app code
type BucketMigrator interface {
	DropBucket(string) error
	CreateBucket(string) error
	ExistsBucket(string) bool
	ClearBucket(string) error
	ExistingBuckets() ([]string, error)
}

// Cursor - class for navigating through a database
// CursorDupSort and CursorDupFixed are inherit this class
//
// If methods (like First/Next/Seek) return error, then returned key SHOULD not be nil (can be []byte{} for example).
// Then looping code will look as:
// c := kv.Cursor(bucketName)
// for k, v, err := c.First(); k != nil; k, v, err = c.Next() {
//    if err != nil {
//        return err
//    }
//    ... logic
// }
type Cursor interface {
	Prefix(v []byte) Cursor // Prefix returns only keys with given prefix, useful RemoteKV - because filtering done by server
	Prefetch(v uint) Cursor // Prefetch enables data streaming - used only by RemoteKV

	First() ([]byte, []byte, error)           // First - position at first key/data item
	Seek(seek []byte) ([]byte, []byte, error) // Seek - position at first key greater than or equal to specified key
	SeekExact(key []byte) ([]byte, error)     // SeekExact - position at first key greater than or equal to specified key
	Next() ([]byte, []byte, error)            // Next - position at next key/value (can iterate over DupSort key/values automatically)
	Prev() ([]byte, []byte, error)            // Prev - position at previous key
	Last() ([]byte, []byte, error)            // Last - position at last key and last possible value
	Current() ([]byte, []byte, error)         // Current - return key/data at current cursor position

	Put(k, v []byte) error           // Put - based on order
	Append(k []byte, v []byte) error // Append - append the given key/data pair to the end of the database. This option allows fast bulk loading when keys are already known to be in the correct order.
	Delete(key []byte) error

	// DeleteCurrent This function deletes the key/data pair to which the cursor refers.
	// This does not invalidate the cursor, so operations such as MDB_NEXT
	// can still be used on it.
	// Both MDB_NEXT and MDB_GET_CURRENT will return the same record after
	// this operation.
	DeleteCurrent() error

	// PutNoOverwrite(key, value []byte) error
	// Reserve()

	// PutCurrent - replace the item at the current cursor position.
	// Warning! this method doesn't check order of keys, it means you can insert key in wrong place of bucket
	//	The key parameter must still be provided, and must match it.
	//	If using sorted duplicates (#MDB_DUPSORT) the data item must still
	//	sort into the same place. This is intended to be used when the
	//	new data is the same size as the old. Otherwise it will simply
	//	perform a delete of the old record followed by an insert.
	//
	//PutCurrent(key, value []byte) error

	Count() (uint64, error) // Count - fast way to calculate amount of keys in bucket. It counts all keys even if Prefix was set.
}

type CursorDupSort interface {
	Cursor

	// SeekBothExact -
	// second parameter can be nil only if searched key has no duplicates, or return error
	SeekBothExact(key, value []byte) ([]byte, []byte, error)
	SeekBothRange(key, value []byte) ([]byte, []byte, error)
	FirstDup() ([]byte, error)          // FirstDup - position at first data item of current key
	NextDup() ([]byte, []byte, error)   // NextDup - position at next data item of current key
	NextNoDup() ([]byte, []byte, error) // NextNoDup - position at first data item of next key
	LastDup() ([]byte, error)           // LastDup - position at last data item of current key

	CountDuplicates() (uint64, error)  // CountDuplicates - number of duplicates for the current key
	DeleteCurrentDuplicates() error    // DeleteCurrentDuplicates - deletes all of the data items for the current key
	AppendDup(key, value []byte) error // AppendDup - same as Append, but for sorted dup data

	//PutIfNoDup()      // Store the key-value pair only if key is not present
}

// CursorDupFixed - has methods valid for buckets with lmdb.DupFixed flag
// See also lmdb.WrapMulti
type CursorDupFixed interface {
	CursorDupSort

	// GetMulti - return up to a page of duplicate data items from current cursor position
	// After return - move cursor to prepare for #MDB_NEXT_MULTIPLE
	GetMulti() ([]byte, error)
	// NextMulti - return up to a page of duplicate data items from next cursor position
	// After return - move cursor to prepare for #MDB_NEXT_MULTIPLE
	NextMulti() ([]byte, []byte, error)
	// PutMulti store multiple contiguous data elements in a single request.
	// Panics if len(page) is not a multiple of stride.
	// The cursor's bucket must be DupFixed and DupSort.
	PutMulti(key []byte, page []byte, stride int) error
	// ReserveMulti()
}

type HasStats interface {
	DiskSize(context.Context) (uint64, error) // db size
}

type Backend interface {
	AddLocal([]byte) ([]byte, error)
	Etherbase() (common.Address, error)
	NetVersion() (uint64, error)
	BloomStatus() (uint64, uint64, common.Hash)
}

type DbProvider uint8

const (
	Remote DbProvider = iota
	Lmdb
)
