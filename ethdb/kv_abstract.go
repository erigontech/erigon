package ethdb

import (
	"context"
	"errors"
	"unsafe"

	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/metrics"
)

const ReadersLimit = 2000 // MDBX_READERS_LIMIT on 64bit system

var (
	ErrAttemptToDeleteNonDeprecatedBucket = errors.New("only buckets from dbutils.DeprecatedBuckets can be deleted")
	ErrUnknownBucket                      = errors.New("unknown bucket. add it to dbutils.Buckets")

	dbSize    = metrics.GetOrRegisterGauge("db/size", metrics.DefaultRegistry)    //nolint
	txLimit   = metrics.GetOrRegisterGauge("tx/limit", metrics.DefaultRegistry)   //nolint
	txSpill   = metrics.GetOrRegisterGauge("tx/spill", metrics.DefaultRegistry)   //nolint
	txUnspill = metrics.GetOrRegisterGauge("tx/unspill", metrics.DefaultRegistry) //nolint
	txDirty   = metrics.GetOrRegisterGauge("tx/dirty", metrics.DefaultRegistry)   //nolint

	dbCommitPreparation = metrics.GetOrRegisterTimer("db/commit/preparation", metrics.DefaultRegistry) //nolint
	dbCommitGc          = metrics.GetOrRegisterTimer("db/commit/gc", metrics.DefaultRegistry)          //nolint
	dbCommitAudit       = metrics.GetOrRegisterTimer("db/commit/audit", metrics.DefaultRegistry)       //nolint
	dbCommitWrite       = metrics.GetOrRegisterTimer("db/commit/write", metrics.DefaultRegistry)       //nolint
	dbCommitSync        = metrics.GetOrRegisterTimer("db/commit/sync", metrics.DefaultRegistry)        //nolint
	dbCommitEnding      = metrics.GetOrRegisterTimer("db/commit/ending", metrics.DefaultRegistry)      //nolint

	dbPgopsNewly   = metrics.GetOrRegisterGauge("db/pgops/newly", metrics.DefaultRegistry)   //nolint
	dbPgopsCow     = metrics.GetOrRegisterGauge("db/pgops/cow", metrics.DefaultRegistry)     //nolint
	dbPgopsClone   = metrics.GetOrRegisterGauge("db/pgops/clone", metrics.DefaultRegistry)   //nolint
	dbPgopsSplit   = metrics.GetOrRegisterGauge("db/pgops/split", metrics.DefaultRegistry)   //nolint
	dbPgopsMerge   = metrics.GetOrRegisterGauge("db/pgops/merge", metrics.DefaultRegistry)   //nolint
	dbPgopsSpill   = metrics.GetOrRegisterGauge("db/pgops/spill", metrics.DefaultRegistry)   //nolint
	dbPgopsUnspill = metrics.GetOrRegisterGauge("db/pgops/unspill", metrics.DefaultRegistry) //nolint
	dbPgopsWops    = metrics.GetOrRegisterGauge("db/pgops/wops", metrics.DefaultRegistry)    //nolint

	gcLeafMetric     = metrics.GetOrRegisterGauge("db/gc/leaf", metrics.DefaultRegistry)     //nolint
	gcOverflowMetric = metrics.GetOrRegisterGauge("db/gc/overflow", metrics.DefaultRegistry) //nolint
	gcPagesMetric    = metrics.GetOrRegisterGauge("db/gc/pages", metrics.DefaultRegistry)    //nolint

	stateLeafMetric     = metrics.GetOrRegisterGauge("db/state/leaf", metrics.DefaultRegistry)   //nolint
	stateBranchesMetric = metrics.GetOrRegisterGauge("db/state/branch", metrics.DefaultRegistry) //nolint
)

type DBVerbosityLvl int8
type Label uint8

const (
	Chain  Label = 0
	TxPool Label = 1
	Sentry Label = 2
)

type Has interface {
	// Has indicates whether a key exists in the database.
	Has(bucket string, key []byte) (bool, error)
}

type KVGetter interface {
	Has

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
	Put(bucket string, key, value []byte) error
}

// Deleter wraps the database delete operations.
type Deleter interface {
	// Delete removes a single entry.
	Delete(bucket string, k, v []byte) error
}

type Closer interface {
	Close()
}

// Read-only version of KV.
type RoKV interface {
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
	AllBuckets() dbutils.BucketsCfg

	CollectMetrics()
}

// KV low-level database interface - main target is - to provide common abstraction over top of MDBX and RemoteKV.
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
//
type RwKV interface {
	RoKV

	Update(ctx context.Context, f func(tx RwTx) error) error

	BeginRw(ctx context.Context) (RwTx, error)
}

type StatelessReadTx interface {
	KVGetter

	Commit() error // Commit all the operations of a transaction into the database.
	Rollback()     // Rollback - abandon all the operations of the transaction instead of saving them.

	// ReadSequence - allows to create a linear sequence of unique positive integers for each table.
	// Can be called for a read transaction to retrieve the current sequence value, and the increment must be zero.
	// Sequence changes become visible outside the current write transaction after it is committed, and discarded on abort.
	// Starts from 0.
	ReadSequence(bucket string) (uint64, error)
}

type StatelessWriteTx interface {
	Putter
	Deleter

	IncrementSequence(bucket string, amount uint64) (uint64, error)
	Append(bucket string, k, v []byte) error
	AppendDup(bucket string, k, v []byte) error
}

type StatelessRwTx interface {
	StatelessReadTx
	StatelessWriteTx
}

type Tx interface {
	StatelessReadTx

	// Cursor - creates cursor object on top of given bucket. Type of cursor - depends on bucket configuration.
	// If bucket was created with mdbx.DupSort flag, then cursor with interface CursorDupSort created
	// Otherwise - object of interface Cursor created
	//
	// Cursor, also provides a grain of magic - it can use a declarative configuration - and automatically break
	// long keys into DupSort key/values. See docs for `bucket.go:BucketConfigItem`
	Cursor(bucket string) (Cursor, error)
	CursorDupSort(bucket string) (CursorDupSort, error) // CursorDupSort - can be used if bucket has mdbx.DupSort flag

	ForEach(bucket string, fromPrefix []byte, walker func(k, v []byte) error) error
	ForPrefix(bucket string, prefix []byte, walker func(k, v []byte) error) error
	ForAmount(bucket string, prefix []byte, amount uint32, walker func(k, v []byte) error) error

	Comparator(bucket string) dbutils.CmpFunc

	CHandle() unsafe.Pointer // Pointer to the underlying C transaction handle (e.g. *C.MDB_txn)
	CollectMetrics()
}

type RwTx interface {
	Tx
	StatelessWriteTx
	BucketMigrator

	RwCursor(bucket string) (RwCursor, error)
	RwCursorDupSort(bucket string) (RwCursorDupSort, error)
}

// BucketMigrator used for buckets migration, don't use it in usual app code
type BucketMigrator interface {
	DropBucket(string) error
	CreateBucket(string) error
	ExistsBucket(string) bool
	ClearBucket(string) error
	ExistingBuckets() ([]string, error)
}

// Cursor - class for navigating through a database
// CursorDupSort are inherit this class
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
	First() ([]byte, []byte, error)               // First - position at first key/data item
	Seek(seek []byte) ([]byte, []byte, error)     // Seek - position at first key greater than or equal to specified key
	SeekExact(key []byte) ([]byte, []byte, error) // SeekExact - position at first key greater than or equal to specified key
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
	Delete(k, v []byte) error        // Delete - short version of SeekExact+DeleteCurrent or SeekBothExact+DeleteCurrent

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
	SeekBothRange(key, value []byte) ([]byte, error)
	FirstDup() ([]byte, error)          // FirstDup - position at first data item of current key
	NextDup() ([]byte, []byte, error)   // NextDup - position at next data item of current key
	NextNoDup() ([]byte, []byte, error) // NextNoDup - position at first data item of next key
	LastDup() ([]byte, error)           // LastDup - position at last data item of current key

	CountDuplicates() (uint64, error) // CountDuplicates - number of duplicates for the current key
}

type RwCursorDupSort interface {
	CursorDupSort
	RwCursor

	DeleteCurrentDuplicates() error    // DeleteCurrentDuplicates - deletes all of the data items for the current key
	AppendDup(key, value []byte) error // AppendDup - same as Append, but for sorted dup data
}

type HasStats interface {
	BucketSize(name string) (uint64, error)
	DiskSize(context.Context) (uint64, error) // db size
}
