package ethdb

import (
	"context"
	"strconv"

	"github.com/dgraph-io/badger/v2"
	"github.com/ledgerwatch/bolt"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/ethdb/remote"
)

type KV interface {
	Options() Options
	View(ctx context.Context, f func(tx Tx) error) (err error)
	Update(ctx context.Context, f func(tx Tx) error) (err error)
	Close() error
}

type Tx interface {
	Bucket(name []byte) Bucket
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
	Next() ([]byte, []byte, error)
	Walk(walker func(k, v []byte) (bool, error)) error
}

type NoValuesCursor interface {
	First() ([]byte, uint64, error)
	Seek(seek []byte) ([]byte, uint64, error)
	Next() ([]byte, uint64, error)
	Walk(walker func(k []byte, vSize uint64) (bool, error)) error
}

type DbProvider uint8

const (
	Bolt DbProvider = iota
	Badger
	Remote
)

const DefaultProvider = Bolt

type Options struct {
	provider DbProvider

	Remote remote.DbOpts
	Bolt   *bolt.Options
	Badger badger.Options

	path string
}

func NewBolt() Options {
	opts := Options{provider: Bolt, Bolt: bolt.DefaultOptions}
	return opts
}

func NewBadger() Options {
	opts := Options{provider: Badger, Badger: badger.DefaultOptions("")}
	return opts
}

func NewRemote() Options {
	opts := Options{provider: Remote, Remote: remote.DefaultOpts}
	return opts
}

func NewMemDb() Options {
	return Opts().InMem(true)
}

func ProviderOpts(provider DbProvider) Options {
	switch provider {
	case Bolt:
		return NewBolt()
	case Badger:
		return NewBadger()
	case Remote:
		return NewRemote()
	default:
		panic("unknown db provider: " + strconv.Itoa(int(provider)))
	}
}

func Opts() Options {
	return ProviderOpts(DefaultProvider)
}

func (opts Options) Path(path string) Options {
	opts.path = path
	switch opts.provider {
	case Bolt:
		// nothing to do
	case Badger:
		opts.Badger = opts.Badger.WithDir(path).WithValueDir(path)
	case Remote:
		opts.Remote = opts.Remote.Addr(path)
	}
	return opts
}

func (opts Options) InMem(val bool) Options {
	switch opts.provider {
	case Bolt:
		opts.Bolt.MemOnly = val
	case Badger:
		opts.Badger = opts.Badger.WithInMemory(val)
	case Remote:
		panic("not supported")
	}
	return opts
}

func (opts Options) Open(ctx context.Context) (db KV, err error) {
	return Open(ctx, opts)
}

func Open(ctx context.Context, opts Options) (KV, error) {
	switch opts.provider {
	case Bolt:
		db := &BoltKV{opts: opts}
		boltDB, err := bolt.Open(opts.path, 0600, opts.Bolt)
		if err != nil {
			return nil, err
		}
		db.bolt = boltDB
		err = db.bolt.Update(func(tx *bolt.Tx) error {
			for _, name := range dbutils.Buckets {
				_, createErr := tx.CreateBucketIfNotExists(name, false)
				if createErr != nil {
					return createErr
				}
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
		return db, nil
	case Badger:
		db := &badgerDB{opts: opts}
		badgerDB, err := badger.Open(opts.Badger)
		if err != nil {
			return nil, err
		}
		db.badger = badgerDB
		return db, nil
	case Remote:
		db := &remoteDB{opts: opts}
		remoteDb, err := remote.Open(ctx, opts.Remote)
		if err != nil {
			return nil, err
		}
		db.remote = remoteDb
		return db, nil
	}
	panic("unknown db provider")
}
