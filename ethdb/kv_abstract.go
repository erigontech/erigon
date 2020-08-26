package ethdb

import (
	"context"
	"errors"

	"github.com/ledgerwatch/turbo-geth/common"
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
}

type Tx interface {
	Cursor(bucket string) Cursor
	NoValuesCursor(bucket string) NoValuesCursor
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
	Next() ([]byte, []byte, error)
	Last() ([]byte, []byte, error)

	Put(key []byte, value []byte) error
	Delete(key []byte) error
	Append(key []byte, value []byte) error // Returns error if provided data not sorted or has duplicates
}

type NoValuesCursor interface {
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
}

type DbProvider uint8

const (
	Bolt DbProvider = iota
	Remote
	Lmdb
)
