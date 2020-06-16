package ethdb

import (
	"context"

	"github.com/ledgerwatch/turbo-geth/common"
)

type KV interface {
	View(ctx context.Context, f func(tx Tx) error) error
	Update(ctx context.Context, f func(tx Tx) error) error
	Close()

	Begin(ctx context.Context, writable bool) (Tx, error)
}

type NativeGet interface {
	Get(ctx context.Context, bucket, key []byte) ([]byte, error)
	Has(ctx context.Context, bucket, key []byte) (bool, error)
}

type Tx interface {
	Bucket(name []byte) Bucket

	Commit(ctx context.Context) error
	Rollback() error
}

type Bucket interface {
	Get(key []byte) (val []byte, err error)
	Put(key []byte, value []byte) error
	Delete(key []byte) error
	Cursor() Cursor

	Size() (uint64, error)
	Clear() error
}

type Cursor interface {
	Prefix(v []byte) Cursor
	MatchBits(uint) Cursor
	Prefetch(v uint) Cursor
	NoValues() NoValuesCursor

	First() ([]byte, []byte, error)
	Seek(seek []byte) ([]byte, []byte, error)
	SeekTo(seek []byte) ([]byte, []byte, error)
	Next() ([]byte, []byte, error)
	Walk(walker func(k, v []byte) (bool, error)) error

	Put(key []byte, value []byte) error
	Delete(key []byte) error
}

type NoValuesCursor interface {
	First() ([]byte, uint32, error)
	Seek(seek []byte) ([]byte, uint32, error)
	Next() ([]byte, uint32, error)
	Walk(walker func(k []byte, vSize uint32) (bool, error)) error
}

type HasStats interface {
	DiskSize(context.Context) (common.StorageSize, error) // db size
	BucketsStat(context.Context) (map[string]common.StorageBucketWriteStats, error)
}

type DbProvider uint8

const (
	Bolt DbProvider = iota
	Badger
	Remote
	Lmdb
)
