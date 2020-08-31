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

type KV interface {
	View(ctx context.Context, f func(tx Tx) error) error
	Update(ctx context.Context, f func(tx Tx) error) error
	Close()

	Begin(ctx context.Context, parent Tx, writable bool) (Tx, error)
	AllBuckets() dbutils.BucketsCfg
}

type Tx interface {
	Cursor(bucket string) Cursor
	CursorDupSort(bucket string) CursorDupSort
	CursorDupFixed(bucket string) CursorDupFixed
	CursorNoValues(bucket string) CursorNoValues
	Get(bucket string, key []byte) (val []byte, err error)

	Commit(ctx context.Context) error
	Rollback()
	BucketSize(name string) (uint64, error)
}

// Interface used for buckets migration, don't use it in usual app code
type BucketMigrator interface {
	DropBucket(string) error
	CreateBucket(string) error
	ExistsBucket(string) bool
	ClearBucket(string) error
	ExistingBuckets() ([]string, error)
}

type Cursor interface {
	Prefix(v []byte) Cursor
	Prefetch(v uint) Cursor

	First() ([]byte, []byte, error)
	Seek(seek []byte) ([]byte, []byte, error)
	SeekExact(key []byte) ([]byte, error)
	Next() ([]byte, []byte, error) // Next - returns next key/value (can iterate over DupSort key/values automatically)
	Last() ([]byte, []byte, error)

	Put(key []byte, value []byte) error
	// PutNoOverride() error
	// Reserve()
	Current() ([]byte, []byte, error)

	Delete(key []byte) error
	Append(key []byte, value []byte) error // Returns error if provided data not sorted or has duplicates
}

type CursorDupSort interface {
	Cursor

	SeekBothExact(key, value []byte) ([]byte, []byte, error)
	SeekBothRange(key, value []byte) ([]byte, []byte, error)
	FirstDup() ([]byte, error)
	NextDup() ([]byte, []byte, error)   // NextDup - iterate only over duplicates of current key
	NextNoDup() ([]byte, []byte, error) // NextNoDup - iterate with skipping all duplicates
	LastDup() ([]byte, error)

	CountDuplicates() (uint64, error)         // Count returns the number of duplicates for the current key. See mdb_cursor_count
	AppendDup(key []byte, value []byte) error // Returns error if provided data not sorted or has duplicates

	//PutIfNoDup()      // Store the key-value pair only if key is not present
}

type CursorDupFixed interface {
	CursorDupSort

	// GetMulti - return up to a page of duplicate data items from current cursor position
	// After return - move cursor to prepare for #MDB_NEXT_MULTIPLE
	// See also lmdb.WrapMulti
	GetMulti() ([]byte, error)
	// NextMulti - return up to a page of duplicate data items from next cursor position
	// After return - move cursor to prepare for #MDB_NEXT_MULTIPLE
	// See also lmdb.WrapMulti
	NextMulti() ([]byte, []byte, error)
	// PutMulti store multiple contiguous data elements in a single request.
	// Panics if len(page) is not a multiple of stride.
	// The cursor's bucket must be DupFixed and DupSort.
	PutMulti(key []byte, page []byte, stride int) error
	// ReserveMulti()
}

type CursorNoValues interface {
	First() ([]byte, uint32, error)
	Seek(seek []byte) ([]byte, uint32, error)
	Next() ([]byte, uint32, error)
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
	Bolt DbProvider = iota
	Remote
	Lmdb
)
