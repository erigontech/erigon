package ethdb

import (
	"context"
)

type KV interface {
	View(ctx context.Context, f func(tx Tx) error) error
	Update(ctx context.Context, f func(tx Tx) error) error
	Close()

	Begin(ctx context.Context, writable bool) (Tx, error)
	Size() uint64 // in bytes
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
}

type NoValuesCursor interface {
	First() ([]byte, uint32, error)
	Seek(seek []byte) ([]byte, uint32, error)
	Next() ([]byte, uint32, error)
	Walk(walker func(k []byte, vSize uint32) (bool, error)) error
}

type DbProvider uint8

const (
	Bolt DbProvider = iota
	Badger
	Remote
)
